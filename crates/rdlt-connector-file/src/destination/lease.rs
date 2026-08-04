//! The session lease: one durable document per pipeline scope naming
//! which session's [`Load`](super::load::Load) may write. 037 US2's
//! whole point is that a SECOND session of the same pipeline — a
//! second `rdlt run` against the same output, not the engine's own
//! retry of one attempt — must be refused rather than silently
//! corrupting the first session's staging and receipts.
//!
//! The document itself ([`LeaseDoc`]) carries an `owner` token (the
//! connector instance's identity, stable across the engine's
//! run-level retries within ONE session, distinct across processes)
//! and a `renewed_at_ms` heartbeat stamp. Acquisition order:
//!
//! 1. Exclusive create. Wins outright on a fresh scope.
//! 2. `AlreadyExists` — read the holder. Undecodable bytes (a doc that
//!    exists but will not parse) are treated as neither a live holder
//!    nor an error outright: wait one full `HEARTBEAT_SECS` and read
//!    again, since a live acquirer's own create-then-write can look
//!    identical to abandoned debris for a brief window (see the
//!    `acquire` body). Still undecodable that much later is debris,
//!    reclaimed exactly like step 4 below.
//! 3. Same `owner` as the caller — this is the engine's own retry of
//!    a failed attempt reopening the connector, not a second session
//!    — CAS-replace to reacquire with fresh stamps.
//! 4. A different owner whose `renewed_at_ms` is older than
//!    `TTL_SECS` — that session is presumed dead — take the lease
//!    over. TAKEOVER IS PER-ARM (037 US2 review round 1, C2): the S3
//!    arm's CAS-replace genuinely serializes two racing takeovers (one
//!    wins the `Precondition`, the other observes
//!    [`is_stale_version`]); the LOCAL arm has no such CAS
//!    (`replace_doc_if`'s own doc explains why), so two local
//!    processes racing to take over the SAME stale lease instead race
//!    [`Location::delete_doc_exclusive`]'s atomic unlink — the winner
//!    loops back to `create_doc_exclusive` (which now finds nothing in
//!    its way), the loser loops back and finds a FRESH doc under
//!    whoever's create actually landed, and refuses it exactly like
//!    step 5 below.
//! 5. A different owner whose lease is still fresh — refuse, typed,
//!    naming the holder and the ages (the frozen spelling below).
//!
//! Once held, [`Lease::start_heartbeat`] spawns a task that keeps the
//! document's `renewed_at_ms` moving so a live session's lease never
//! looks stale to a competitor. But the fence a write actually checks
//! — [`Lease::check_still_held`] — is SYMMETRIC, and deliberately does
//! not simply trust that the beat ran at all:
//!
//! - If a beat ever finds the document gone or foreign-owned, or loses
//!   its CAS, it flips `lost` immediately — a DEFINITIVE observation
//!   that this session has been outvoted.
//! - Independently, `check_still_held` also self-expires when
//!   `last_ok_ms` (stamped at acquire and on every successful
//!   renewal) is older than `TTL_SECS` — covering the case `lost`
//!   cannot: a heartbeat task that is starved, panicked, or wedged
//!   behind a stuck IO call never gets to observe anything, so `lost`
//!   would stay `false` forever even though this session's own
//!   evidence of still holding the lease is just as stale as a dead
//!   competitor's would be. The write path does not trust the beat to
//!   have run; it trusts the CLOCK.
//!
//! Both arms refuse with a typed, frozen message (see
//! `check_still_held`'s two spellings) rather than let this session
//! keep writing into what might now be another session's staging.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdlt_connector_sdk::spi::DestinationError;
use rdlt_connector_sdk::spi::core::crash_point;
use serde::{Deserialize, Serialize};

use super::layout::lease_file;
use crate::location::{CreateDoc, DocVersion, Location, is_stale_version};

/// The persisted lease document's format. Bumped only if the shape
/// changes; there is no reader compatibility burden today (a lease is
/// transient — a version mismatch could simply refuse and let the
/// operator retry once every holder has upgraded), but the field
/// exists from the start so a future change has somewhere to land.
/// Enforced one-way by [`decode`]: a doc NEWER than this build
/// understands refuses rather than being renewed-over blind (037 US2
/// review round 1, I5) — `serde`'s own field defaults stay in force
/// for additive evolution, so an OLDER doc (or one missing a field
/// this build added) still parses; only "newer than I can understand"
/// refuses.
pub(super) const LEASE_FORMAT_VERSION: u32 = 1;

/// How often a held lease renews its stamp.
pub(super) const HEARTBEAT_SECS: u64 = 15;

/// How old a holder's `renewed_at_ms` must be before a competitor may
/// take the lease over as a dead session's.
///
/// Deliberately generous: 20 missed beats (`TTL_SECS` /
/// `HEARTBEAT_SECS`), not 2 or 3. The clock this compares against is
/// the READER's — `now_ms()` on whichever session is deciding whether
/// to take over — never a server-reported `last_modified` (the
/// destination `Location` has no accessor for one, and adding it buys
/// little): the 023 lesson was that trusting a service's own clock
/// against a local one is a real hazard, but a 20x margin over the
/// heartbeat interval dominates any plausible clock skew or GC pause
/// between two machines on the same order of latency as this
/// protocol. A false takeover (evicting a session that is merely slow,
/// not dead) is the failure this margin buys down; a missed takeover
/// (waiting a few extra minutes for a genuinely dead session's lease
/// to expire) costs nothing but patience.
pub(super) const TTL_SECS: u64 = 300;

/// Milliseconds since the epoch, on this process's clock. The lease
/// protocol never compares this against a remote clock (see
/// `TTL_SECS`'s doc) — only against stamps this same protocol wrote,
/// so the only clock that has to agree with itself is this one.
///
/// Saturates to `0` rather than panicking when the clock reports a
/// time before the epoch (037 US2 review round 1, M10) — a real
/// system clock never does this, but a spawned heartbeat task
/// panicking on a clock fault would be a far worse failure than a
/// wrong number. A broken clock then reads as age-0 to whichever side
/// of a comparison it lands on: a `renewed_at_ms` stamp of `0` looks
/// ancient to a reader with a working clock (refusing to trust it,
/// i.e. eligible for takeover / self-expiry — never silently treated
/// as fresh), and a `now_ms()` of `0` looks like "just now" against
/// any positive stamp (refusing to take over what might still be
/// live). Either direction degrades to the SAFE refusal, never to
/// corruption.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The persisted lease: who holds it, and when they last proved they
/// are still alive.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct LeaseDoc {
    pub format_version: u32,
    pub pipeline: String,
    /// The connector instance's token — stable across the engine's
    /// run-level retry attempts, distinct across processes.
    pub owner: String,
    pub acquired_at_ms: u64,
    pub renewed_at_ms: u64,
}

/// Build a fresh document this session is about to write — either
/// claiming an empty scope or reasserting/taking over an existing one.
fn fresh_doc(pipeline: &str, owner: &str, now: u64) -> LeaseDoc {
    LeaseDoc {
        format_version: LEASE_FORMAT_VERSION,
        pipeline: pipeline.to_owned(),
        owner: owner.to_owned(),
        acquired_at_ms: now,
        renewed_at_ms: now,
    }
}

/// Serialize a lease document, or a fatal error if it somehow cannot
/// encode (it never fails in practice — every field is a plain string
/// or integer — but `serde_json` still returns a `Result`).
fn encode(doc: &LeaseDoc) -> Result<Vec<u8>, DestinationError> {
    serde_json::to_vec(doc)
        .map_err(|e| DestinationError::fatal(format!("encoding the destination lease: {e}")))
}

/// Decode a lease document read back from storage, naming `path` (the
/// document's name) in both failure messages this can raise:
///
/// - Bytes that will not parse as JSON at all are a genuine read
///   failure, not "no holder" — inventing absence for bytes that ARE
///   there could let a second session past a live lock it merely
///   could not read. (`acquire`'s caller treats THIS specific failure
///   specially — see its body — rather than propagating it outright,
///   because it is also what a live acquirer's own in-progress write
///   looks like for a brief window.)
/// - Bytes that parse but declare a `format_version` newer than this
///   build understands (037 US2 review round 1, I5) refuse rather
///   than being renewed over blind: an older build CAS-replacing a
///   newer doc's fields it does not know how to preserve would
///   silently drop them.
fn decode(bytes: &[u8], path: &str) -> Result<LeaseDoc, DestinationError> {
    let doc: LeaseDoc = serde_json::from_slice(bytes).map_err(|e| {
        DestinationError::fatal(format!("unreadable destination lease `{path}`: {e}"))
    })?;
    if doc.format_version > LEASE_FORMAT_VERSION {
        return Err(DestinationError::fatal(format!(
            "destination lease `{path}` format v{} is newer than this build supports \
             (v{LEASE_FORMAT_VERSION}); upgrade rdlt",
            doc.format_version
        )));
    }
    Ok(doc)
}

/// A held session lease. Dropping it (without calling
/// [`Lease::release`]) simply stops the heartbeat — the document
/// itself is reclaimed by [`TTL_SECS`], not by anything Drop can do,
/// since the delete is an async IO call Drop cannot make.
#[derive(Debug)]
pub(super) struct Lease {
    location: Location,
    doc_name: String,
    pipeline: String,
    owner: String,
    /// Flipped the moment a beat can no longer prove this session
    /// still holds the lease — a DEFINITIVE observation (foreign
    /// owner, absence, or a lost CAS). See the module doc: this is
    /// only HALF of `check_still_held`'s fence.
    lost: Arc<AtomicBool>,
    /// Milliseconds-since-epoch of the last proof this session held
    /// the lease — stamped at acquire and on every successful
    /// renewal. The other half of the fence: even if `lost` was never
    /// set because the beat never got to run at all, a write can
    /// still see that its evidence has gone stale (037 US2 review
    /// round 1, I3).
    last_ok_ms: Arc<AtomicU64>,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

/// The result of one takeover attempt against a specific document
/// version — see the module doc's step 4 for why this is per-arm.
enum Takeover {
    /// S3: the CAS-replace itself landed the fresh doc — already
    /// holding.
    Won,
    /// LOCAL: the exclusive delete was ours; the caller loops back to
    /// `create_doc_exclusive`, which now finds nothing in its way.
    Cleared,
    /// Someone else's write (S3) or delete (LOCAL) got there first;
    /// loop back and re-evaluate the — now fresh — document from
    /// scratch rather than assume anything about who holds it now.
    LostRace,
}

/// Take over a stale or abandoned lease document (037 US2 review
/// round 1, C2). LOCAL uses the atomic
/// [`Location::delete_doc_exclusive`] — a plain CAS-replace there lets
/// two racing takeovers both "succeed", since the local arm never
/// checks the version it is handed (see `replace_doc_if`'s amended
/// safety doc in `location/kind.rs`). S3 keeps the real CAS-replace,
/// which genuinely serializes there.
async fn takeover(
    location: &Location,
    doc_name: &str,
    version: DocVersion,
    fresh: &LeaseDoc,
) -> Result<Takeover, DestinationError> {
    if location.is_local() {
        return Ok(if location.delete_doc_exclusive(doc_name).await? {
            Takeover::Cleared
        } else {
            Takeover::LostRace
        });
    }
    match location
        .replace_doc_if(doc_name, encode(fresh)?, version)
        .await
    {
        Ok(_new_version) => Ok(Takeover::Won),
        Err(e) if is_stale_version(&e) => Ok(Takeover::LostRace),
        Err(e) => Err(e),
    }
}

impl Lease {
    fn new(
        location: Location,
        doc_name: String,
        pipeline: String,
        owner: String,
        acquired_now_ms: u64,
    ) -> Self {
        Self {
            location,
            doc_name,
            pipeline,
            owner,
            lost: Arc::new(AtomicBool::new(false)),
            last_ok_ms: Arc::new(AtomicU64::new(acquired_now_ms)),
            heartbeat: None,
        }
    }

    /// Acquire the scope's lease, or refuse. See the module doc for
    /// the five-way order this implements.
    pub(super) async fn acquire(
        location: Location,
        scope: &str,
        pipeline: &str,
        owner: &str,
    ) -> Result<Lease, DestinationError> {
        let doc_name = lease_file(scope);
        crash_point!(
            "file.lease.acquire",
            Err(DestinationError::fatal(
                "injected crash at file.lease.acquire"
            ))
        );
        loop {
            let now = now_ms();
            let fresh = fresh_doc(pipeline, owner, now);

            if let CreateDoc::Created(_version) = location
                .create_doc_exclusive(&doc_name, encode(&fresh)?)
                .await?
            {
                return Ok(Lease::new(
                    location,
                    doc_name,
                    pipeline.to_owned(),
                    owner.to_owned(),
                    now,
                ));
            }

            // Someone's name is already on the document — read it to
            // decide whose. `None` means it existed a moment ago (the
            // create above lost the race) but is gone again by the
            // time we read — a concurrent release or takeover in the
            // narrow window between the two calls. That window closes
            // on its own; loop back and try the exclusive create
            // again rather than treating a momentary gap as either a
            // win or a refusal.
            let Some((bytes, holder_version)) = location.read_doc_versioned(&doc_name).await?
            else {
                continue;
            };

            let holder = match decode(&bytes, &doc_name) {
                Ok(holder) => holder,
                Err(_first_err) => {
                    // Undecodable is ambiguous by itself (037 US2
                    // review round 1, I6): it could be a LIVE
                    // acquirer mid-`create_doc_exclusive` — that call
                    // is deliberately O_EXCL-then-`write_all`, not
                    // tmp+rename, precisely so O_EXCL can serve as the
                    // exclusivity primitive, which means a reader CAN
                    // observe a just-created, empty file between the
                    // open and the write. Or it could be debris from
                    // a process that crashed in that exact window. A
                    // live writer's `write_all` completes in
                    // milliseconds; waiting one full heartbeat
                    // interval and reading again resolves the
                    // ambiguity honestly — still undecodable that
                    // much later cannot be an in-progress acquire.
                    tokio::time::sleep(Duration::from_secs(HEARTBEAT_SECS)).await;
                    let Some((retry_bytes, retry_version)) =
                        location.read_doc_versioned(&doc_name).await?
                    else {
                        continue;
                    };
                    match decode(&retry_bytes, &doc_name) {
                        Ok(holder) => holder,
                        Err(_second_err) => {
                            // Confirmed debris: reclaim it exactly
                            // like a stale lease, through the same
                            // per-arm takeover primitive. Timestamps
                            // are recomputed here rather than reusing
                            // the loop-top `now`/`fresh` — a
                            // `HEARTBEAT_SECS` sleep just happened,
                            // and the doc this call may write should
                            // not claim a stamp that old.
                            let now = now_ms();
                            let fresh = fresh_doc(pipeline, owner, now);
                            match takeover(&location, &doc_name, retry_version, &fresh).await? {
                                Takeover::Won => {
                                    return Ok(Lease::new(
                                        location,
                                        doc_name,
                                        pipeline.to_owned(),
                                        owner.to_owned(),
                                        now,
                                    ));
                                }
                                Takeover::Cleared | Takeover::LostRace => continue,
                            }
                        }
                    }
                }
            };

            if holder.owner == owner {
                // The engine's own retry of a failed attempt: a new
                // session, same stable connector token, opening
                // against a lease this same identity already holds
                // (or held moments ago). Reacquire through a
                // CAS-replace rather than treating the existing
                // document as a foreign holder. This is NOT routed
                // through `takeover`: it is not a takeover at all,
                // and the local arm's lack of real CAS is fine here
                // because only this same identity is ever the caller
                // (see `replace_doc_if`'s amended safety doc).
                match location
                    .replace_doc_if(&doc_name, encode(&fresh)?, holder_version)
                    .await
                {
                    Ok(_version) => {
                        return Ok(Lease::new(
                            location,
                            doc_name,
                            pipeline.to_owned(),
                            owner.to_owned(),
                            now,
                        ));
                    }
                    // Lost the CAS to yet another concurrent
                    // reacquire attempt under the same owner — loop
                    // and try again from a fresh read.
                    Err(e) if is_stale_version(&e) => continue,
                    Err(e) => return Err(e),
                }
            }

            let age_secs = now.saturating_sub(holder.renewed_at_ms) / 1000;
            if age_secs > TTL_SECS {
                // The holder has missed too many beats to still be
                // alive — take the lease over.
                match takeover(&location, &doc_name, holder_version, &fresh).await? {
                    Takeover::Won => {
                        return Ok(Lease::new(
                            location,
                            doc_name,
                            pipeline.to_owned(),
                            owner.to_owned(),
                            now,
                        ));
                    }
                    Takeover::Cleared | Takeover::LostRace => continue,
                }
            }

            let remaining = TTL_SECS.saturating_sub(age_secs);
            return Err(DestinationError::fatal(format!(
                "another session of pipeline `{pipeline}` holds the destination lease \
                 (owner {holder_owner}, renewed {age_secs}s ago); if that session is dead \
                 the lease expires in {remaining}s",
                holder_owner = holder.owner,
            )));
        }
    }

    /// Spawn the renewal task. It owns its own [`Location`] clone and
    /// copies of the identifying strings — never a reference back to
    /// this `Lease` or the `Load` that holds it, because `Load` carries
    /// an open `ArrowWriter` and is `Send` but not `Sync` (see
    /// `load.rs`'s `commit_log` doc for the same constraint), so a
    /// spawned task could never borrow it across an await point.
    pub(super) fn start_heartbeat(&mut self) {
        let location = self.location.clone();
        let doc_name = self.doc_name.clone();
        let owner = self.owner.clone();
        let lost = Arc::clone(&self.lost);
        let last_ok_ms = Arc::clone(&self.last_ok_ms);
        self.heartbeat = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECS));
            // The first tick fires immediately; `acquire` just wrote
            // fresh stamps, so that beat would do nothing but a
            // redundant round trip. Consume it before the loop.
            interval.tick().await;
            loop {
                interval.tick().await;
                if lost.load(Ordering::SeqCst) {
                    return;
                }
                renew(&location, &doc_name, &owner, &lost, &last_ok_ms).await;
                if lost.load(Ordering::SeqCst) {
                    return;
                }
            }
        }));
    }

    /// A test seam: drive exactly one beat of [`renew`] synchronously,
    /// without racing a spawned task's own timer.
    #[cfg(test)]
    pub(super) async fn renew_once(&self) {
        renew(
            &self.location,
            &self.doc_name,
            &self.owner,
            &self.lost,
            &self.last_ok_ms,
        )
        .await;
    }

    /// The write-path guard — SYMMETRIC (see the module doc): refuses
    /// both when a beat definitively observed a takeover (`lost`) and
    /// when this session's own evidence of holding the lease has
    /// gone stale regardless of why (`last_ok_ms` older than
    /// `TTL_SECS`), which covers a heartbeat that never got to run at
    /// all. Synchronous and cheap on purpose — every write can afford
    /// to check a bool and a clock, not a round trip to storage.
    pub(super) fn check_still_held(&self) -> Result<(), DestinationError> {
        if self.lost.load(Ordering::SeqCst) {
            return Err(DestinationError::fatal(format!(
                "the destination lease for pipeline `{}` was taken over by another session \
                 (this session's heartbeat lost the race); aborting rather than corrupting \
                 the new holder's staging",
                self.pipeline
            )));
        }
        let last_ok = self.last_ok_ms.load(Ordering::SeqCst);
        if now_ms().saturating_sub(last_ok) > TTL_SECS * 1000 {
            return Err(DestinationError::fatal(format!(
                "the destination lease for pipeline `{}` has not been renewed within its TTL \
                 (heartbeat starved or stopped); aborting rather than risking a takeover \
                 this session cannot see",
                self.pipeline
            )));
        }
        Ok(())
    }

    /// Best-effort delete, called at the end of a successful publish.
    ///
    /// The heartbeat is stopped FIRST, before any IO (037 US2 review
    /// round 1, I4) — a beat still in flight while this deletes could
    /// otherwise race the delete, or a subsequent takeover's write,
    /// with its own renewal. Once stopped, a lease already marked
    /// `lost` has nothing of OURS left on the document — it now
    /// belongs to whoever took it over, and deleting it would destroy
    /// THEIR lease, not release ours — so the delete is skipped
    /// entirely in that case. Otherwise the delete stays best-effort:
    /// absence is success (`Location::delete_doc`'s own contract) —
    /// this session's own release racing a takeover's write, or a
    /// prior crashed release, both look identical to "already gone" —
    /// and a failed delete is likewise swallowed, since an unreleased
    /// lease still expires by `TTL_SECS` regardless (see the struct
    /// doc).
    pub(super) async fn release(mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }
        if self.lost.load(Ordering::SeqCst) {
            return;
        }
        crash_point!("file.lease.release", ());
        let _ = self.location.delete_doc(&self.doc_name).await;
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        // Synchronous and safe: `JoinHandle::abort` only requests
        // cancellation, it does not await the task's teardown, so
        // this needs no runtime context of its own. A no-op when
        // `release` already took the handle.
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }
    }
}

/// One heartbeat's worth of work, free of `&Lease` so it can run
/// identically from the spawned task (owned clones only) and from the
/// `renew_once` test seam (borrowing the live `Lease`).
///
/// Every failure mode maps to exactly one of three outcomes:
/// - The document is gone, or is owned by someone else: the lease is
///   LOST. This is also what restores takeover detection on the
///   LOCAL storage arm, which otherwise has none — `replace_doc_if`'s
///   own doc explains that a local write always "succeeds" no matter
///   how stale the version handed to it. Re-reading the document
///   fresh on every beat and comparing its `owner` closes that gap: a
///   local takeover changes the file's `owner` field, and the very
///   next beat sees it and stops, even though the write that landed
///   it raised no CAS alarm of its own.
/// - The CAS-replace lost (`is_stale_version`): another write — a
///   real takeover, on the S3 arm where CAS is real — landed between
///   this beat's read and its write. LOST.
/// - Any other error (a transport blip, a timeout, an undecodable
///   snapshot presumed torn mid-write): NOT lost. A single failed
///   beat must not end a healthy lease — `TTL_SECS` carries a
///   deliberate 20-beat margin over `HEARTBEAT_SECS` precisely so
///   that a run of blips is absorbed rather than treated as death.
///   Only a confirmed absence, a confirmed foreign owner, or a
///   confirmed lost CAS may set `lost`; everything else is left to
///   `check_still_held`'s independent `last_ok_ms` fence (see the
///   module doc) to catch IF the blips never stop.
///
/// On success, `last_ok_ms` is stamped with the instant just written
/// — the other half of that same fence.
async fn renew(
    location: &Location,
    doc_name: &str,
    owner: &str,
    lost: &AtomicBool,
    last_ok_ms: &AtomicU64,
) {
    let read = match location.read_doc_versioned(doc_name).await {
        Ok(read) => read,
        Err(_transient) => return,
    };
    let Some((bytes, version)) = read else {
        lost.store(true, Ordering::SeqCst);
        return;
    };
    let Ok(doc) = decode(&bytes, doc_name) else {
        // An unreadable snapshot is presumed to be a torn mid-write
        // read rather than proof of loss — try again next beat rather
        // than ending a lease over a decode hiccup.
        return;
    };
    if doc.owner != owner {
        lost.store(true, Ordering::SeqCst);
        return;
    }
    let renewed_at_ms = now_ms();
    let renewed = LeaseDoc {
        renewed_at_ms,
        ..doc
    };
    let Ok(bytes) = encode(&renewed) else {
        return;
    };
    match location.replace_doc_if(doc_name, bytes, version).await {
        Ok(_new_version) => last_ok_ms.store(renewed_at_ms, Ordering::SeqCst),
        Err(e) if is_stale_version(&e) => lost.store(true, Ordering::SeqCst),
        Err(_transient) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh local `Location` plus a scope name, isolated per test —
    /// each call gets its own temp directory, so tests never share a
    /// lease document. The directory is deliberately leaked (`keep`)
    /// rather than dropped: a dropped `TempDir` would delete the root
    /// out from under the `Location` the test still uses.
    fn test_location() -> (Location, String) {
        let dir = tempfile::tempdir().expect("tempdir").keep();
        let location = Location::local_dir(dir).expect("local location");
        (location, "scope".to_owned())
    }

    /// Write a lease document directly, bypassing `Lease::acquire` —
    /// the shape a dead-session or a takeover-in-progress lease would
    /// have on disk.
    async fn plant_lease(location: &Location, scope: &str, owner: &str, renewed_at_ms: u64) {
        let doc = LeaseDoc {
            format_version: LEASE_FORMAT_VERSION,
            pipeline: "p".to_owned(),
            owner: owner.to_owned(),
            acquired_at_ms: renewed_at_ms,
            renewed_at_ms,
        };
        let bytes = encode(&doc).expect("encodes");
        match location
            .create_doc_exclusive(&lease_file(scope), bytes.clone())
            .await
            .expect("io")
        {
            CreateDoc::Created(_) => {}
            CreateDoc::AlreadyExists => {
                let (_, version) = location
                    .read_doc_versioned(&lease_file(scope))
                    .await
                    .expect("io")
                    .expect("present");
                location
                    .replace_doc_if(&lease_file(scope), bytes, version)
                    .await
                    .expect("replace");
            }
        }
    }

    /// Read the lease doc back and decode it — the two-racer and
    /// takeover tests use this to confirm not just that ONE side won,
    /// but that the document on disk actually names that winner.
    async fn read_lease(location: &Location, scope: &str) -> LeaseDoc {
        let (bytes, _) = location
            .read_doc_versioned(&lease_file(scope))
            .await
            .expect("io")
            .expect("present");
        serde_json::from_slice(&bytes).expect("decodes")
    }

    #[tokio::test]
    async fn a_fresh_foreign_lease_refuses_with_the_frozen_spelling() {
        let (location, scope) = test_location();
        Lease::acquire(location.clone(), &scope, "p", "owner-a")
            .await
            .expect("first");
        let err = Lease::acquire(location, &scope, "p", "owner-b")
            .await
            .expect_err("a live lease refuses the second session");
        let text = err.to_string();
        assert!(text.contains("holds the destination lease"), "{text}");
        assert!(text.contains("owner-a"), "names the holder: {text}");
    }

    /// M10: the frozen refusal, pinned byte-for-byte — not just
    /// `contains`. `age`/`remaining` are made deterministic by
    /// planting the holder's stamp a controlled 5s in the past rather
    /// than going through a natural `acquire` (whose "just now" stamp
    /// would put age on a 0/1s boundary a slow CI run could cross).
    #[tokio::test]
    async fn the_fresh_foreign_refusal_message_is_pinned_exactly() {
        let (location, scope) = test_location();
        plant_lease(&location, &scope, "owner-a", now_ms() - 5_000).await;
        let err = Lease::acquire(location, &scope, "p", "owner-b")
            .await
            .expect_err("a live foreign lease refuses");
        assert_eq!(
            err.to_string(),
            "fatal destination error: another session of pipeline `p` holds the destination \
             lease (owner owner-a, renewed 5s ago); if that session is dead the lease expires \
             in 295s"
        );
    }

    #[tokio::test]
    async fn the_same_owner_reacquires_through_its_own_lease() {
        // The engine's run-level retry opens a fresh session per attempt;
        // the connector token is stable, so attempt 2 must not be locked
        // out by attempt 1's unreleased lease.
        let (location, scope) = test_location();
        Lease::acquire(location.clone(), &scope, "p", "owner-a")
            .await
            .expect("attempt 1");
        Lease::acquire(location, &scope, "p", "owner-a")
            .await
            .expect("attempt 2 reacquires");
    }

    #[tokio::test]
    async fn a_stale_lease_is_taken_over() {
        let (location, scope) = test_location();
        plant_lease(
            &location,
            &scope,
            "owner-dead",
            now_ms() - (TTL_SECS + 1) * 1000,
        )
        .await;
        Lease::acquire(location.clone(), &scope, "p", "owner-b")
            .await
            .expect("a lease older than the TTL is a dead session's");

        // (037 US2 review round 1, C2) Not just "acquire returned
        // Ok" — the document itself must actually name the new
        // owner, the same strengthening the two-racer test below
        // pins for the concurrent case.
        assert_eq!(read_lease(&location, &scope).await.owner, "owner-b");
    }

    /// C2: two DIFFERENT sessions racing to take over the SAME stale
    /// local lease must produce exactly one winner, never two — the
    /// defect a plain CAS-replace takeover had, since the local arm's
    /// `replace_doc_if` never checks its version and both racers would
    /// "succeed". Real concurrency (`flavor = "multi_thread"`) and a
    /// `Barrier` so both tasks reach `Lease::acquire` at essentially
    /// the same instant, rather than one incidentally running to
    /// completion before the other starts.
    ///
    /// Occasionally (observed ~1 run in 15) this test genuinely takes
    /// `HEARTBEAT_SECS` real seconds rather than milliseconds — this
    /// is CORRECT, not flaky, and is I6 in action: the two racers'
    /// RECREATE step (after the delete race) is itself a second race
    /// on `create_doc_exclusive`, whose local implementation is
    /// O_EXCL-open-then-`write_all`, not atomic — if the loser's own
    /// `read_doc_versioned` lands in that narrow window it observes a
    /// just-created, still-empty document, which is indistinguishable
    /// from debris until the one-heartbeat-interval wait in `acquire`
    /// resolves the ambiguity (see the module doc's step 2 and I6's
    /// comment in `acquire`). The assertions below hold either way.
    #[tokio::test(flavor = "multi_thread")]
    async fn exactly_one_of_two_racing_takeovers_wins_a_stale_local_lease() {
        let (location, scope) = test_location();
        plant_lease(
            &location,
            &scope,
            "owner-dead",
            now_ms() - (TTL_SECS + 1) * 1000,
        )
        .await;

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut tasks = Vec::new();
        for owner in ["owner-x", "owner-y"] {
            let location = location.clone();
            let scope = scope.clone();
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                Lease::acquire(location, &scope, "p", owner).await
            }));
        }

        let mut oks = 0;
        let mut errs = 0;
        let mut winner_owner = None;
        for task in tasks {
            match task.await.expect("racer task must not panic") {
                Ok(lease) => {
                    oks += 1;
                    winner_owner = Some(lease.owner.clone());
                }
                Err(_) => errs += 1,
            }
        }
        assert_eq!(
            (oks, errs),
            (1, 1),
            "exactly one racer must win a takeover of one stale lease"
        );
        assert_eq!(
            Some(read_lease(&location, &scope).await.owner),
            winner_owner,
            "the document on disk must actually name the racer that won"
        );
    }

    /// Renamed from `losing_the_cas_flips_check_still_held` (037 US2
    /// review round 1, I7): the local arm has no real CAS, so what
    /// this actually exercises — both here and for a real spawned
    /// heartbeat in the paused-time tests below — is the beat
    /// observing a foreign `owner` on re-read, not a lost CAS. The
    /// genuine CAS-loss shape only exists on the S3 arm and stays
    /// covered by the live probe.
    #[tokio::test]
    async fn an_owner_mismatch_on_a_beat_flips_check_still_held() {
        let (location, scope) = test_location();
        let lease = Lease::acquire(location.clone(), &scope, "p", "owner-a")
            .await
            .expect("acquire");
        // Simulate a takeover: overwrite the doc as another owner.
        plant_lease(&location, &scope, "owner-b", now_ms()).await;
        // A renewal attempt now observes the foreign owner; drive one
        // beat synchronously.
        lease.renew_once().await;
        let err = lease.check_still_held().expect_err("taken over");
        assert_eq!(
            err.to_string(),
            "fatal destination error: the destination lease for pipeline `p` was taken over \
             by another session (this session's heartbeat lost the race); aborting rather \
             than corrupting the new holder's staging"
        );
    }

    /// I3/M10: the self-expiry arm of `check_still_held` fires from
    /// the clock alone, independent of `lost` — the scenario a
    /// starved or wedged heartbeat produces, which never gets to flip
    /// `lost` at all. `last_ok_ms` is poked directly (a private field,
    /// reachable from this same module) rather than actually waiting
    /// out a `TTL_SECS` heartbeat gap.
    #[tokio::test]
    async fn the_self_expiry_refusal_message_is_pinned_exactly() {
        let (location, scope) = test_location();
        let lease = Lease::acquire(location, &scope, "p", "owner-a")
            .await
            .expect("acquire");
        lease
            .last_ok_ms
            .store(now_ms() - (TTL_SECS + 1) * 1000, Ordering::SeqCst);
        let err = lease
            .check_still_held()
            .expect_err("stale evidence self-expires even though `lost` was never set");
        assert_eq!(
            err.to_string(),
            "fatal destination error: the destination lease for pipeline `p` has not been \
             renewed within its TTL (heartbeat starved or stopped); aborting rather than \
             risking a takeover this session cannot see"
        );
    }

    /// I5/M10: a doc declaring a format newer than this build
    /// understands refuses, byte-for-byte, rather than being silently
    /// renewed over.
    #[test]
    fn the_version_gate_refusal_message_is_pinned_exactly() {
        let future = serde_json::json!({
            "format_version": LEASE_FORMAT_VERSION + 1,
            "pipeline": "p",
            "owner": "o",
            "acquired_at_ms": 0,
            "renewed_at_ms": 0,
        });
        let bytes = serde_json::to_vec(&future).expect("encodes");
        let err = decode(&bytes, "_rdlt_lease.abc.json").expect_err("refuses a future version");
        assert_eq!(
            err.to_string(),
            format!(
                "fatal destination error: destination lease `_rdlt_lease.abc.json` format \
                 v{} is newer than this build supports (v{LEASE_FORMAT_VERSION}); upgrade rdlt",
                LEASE_FORMAT_VERSION + 1
            )
        );
    }

    /// I6: a doc that exists but never decodes — the shape a process
    /// crashing between `create_doc_exclusive`'s O_EXCL open and its
    /// `write_all` leaves behind — must not wedge every future
    /// `acquire` forever. Paused time drives the built-in
    /// `HEARTBEAT_SECS` wait instantly instead of actually sleeping.
    #[tokio::test(start_paused = true)]
    async fn an_undecodable_lease_is_reclaimed_after_one_heartbeat_interval() {
        let (location, scope) = test_location();
        location
            .create_doc_exclusive(&lease_file(&scope), Vec::new())
            .await
            .expect("io");

        let handle = tokio::spawn({
            let location = location.clone();
            let scope = scope.clone();
            async move { Lease::acquire(location, &scope, "p", "owner-b").await }
        });

        tokio::time::advance(Duration::from_secs(HEARTBEAT_SECS)).await;

        let lease = handle
            .await
            .expect("acquire task must not panic")
            .expect("an undecodable doc a full heartbeat old must be reclaimed");
        assert_eq!(lease.owner, "owner-b");
    }

    /// I7: a REAL spawned heartbeat (not `renew_once`) observing a
    /// takeover and flipping `lost` — paused time drives the beat
    /// interval instantly instead of actually waiting `HEARTBEAT_SECS`.
    #[tokio::test(start_paused = true)]
    async fn a_real_heartbeat_beat_detects_a_foreign_takeover() {
        let (location, scope) = test_location();
        let mut lease = Lease::acquire(location.clone(), &scope, "p", "owner-a")
            .await
            .expect("acquire");
        lease.start_heartbeat();
        // Spawning only QUEUES the task — it is not polled until the
        // executor is given a chance to run it, and nothing else in
        // this test yields on its own (every IO call below resolves
        // synchronously on a local filesystem, never returning
        // `Pending`). Yield once now so the task's `interval` gets
        // CREATED — and its immediate first tick consumed — before
        // the `advance` below, rather than after: an `advance` issued
        // first would shift the interval's own not-yet-existing
        // anchor instead of actually elapsing one of its periods.
        tokio::task::yield_now().await;

        // A takeover lands between beats.
        plant_lease(&location, &scope, "owner-b", now_ms()).await;

        // Now the interval is genuinely waiting on its second tick,
        // due `HEARTBEAT_SECS` from creation — advance exactly that
        // far and hand control back so the now-ready spawned task
        // runs its (fully synchronous, local filesystem) beat to
        // completion.
        tokio::time::advance(Duration::from_secs(HEARTBEAT_SECS)).await;
        tokio::task::yield_now().await;

        assert!(
            lease.check_still_held().is_err(),
            "a real heartbeat beat must observe the foreign owner and flip lost"
        );
    }

    /// I4: once a beat has flipped `lost`, `release` must skip the
    /// delete entirely — the document now belongs to whoever took the
    /// lease over, and deleting it would destroy THEIR lease rather
    /// than release ours.
    #[tokio::test(start_paused = true)]
    async fn release_skips_the_delete_once_the_lease_is_lost() {
        let (location, scope) = test_location();
        let mut lease = Lease::acquire(location.clone(), &scope, "p", "owner-a")
            .await
            .expect("acquire");
        lease.start_heartbeat();
        // See the sibling test above for why this yield must come
        // BEFORE `advance`.
        tokio::task::yield_now().await;
        plant_lease(&location, &scope, "owner-b", now_ms()).await;
        tokio::time::advance(Duration::from_secs(HEARTBEAT_SECS)).await;
        tokio::task::yield_now().await;
        assert!(
            lease.check_still_held().is_err(),
            "heartbeat must have flipped lost before release runs"
        );

        lease.release().await;

        assert_eq!(
            read_lease(&location, &scope).await.owner,
            "owner-b",
            "a lost lease's release must not delete the new holder's document"
        );
    }

    /// I4/I7: a normal (not lost) release deletes the document, and —
    /// because the heartbeat is aborted BEFORE that delete — no beat
    /// is left running to observe (or otherwise act on) the document
    /// afterward. Advancing well past another beat interval and
    /// finding the document still gone is the observable proof: a
    /// heartbeat that outlived `release` would still find nothing to
    /// renew (its `renew` only ever sets `lost` on absence, never
    /// recreates), so the real assertion is that the deletion itself
    /// is not undone or raced by anything left running.
    #[tokio::test(start_paused = true)]
    async fn release_deletes_the_doc_and_it_stays_deleted() {
        let (location, scope) = test_location();
        let mut lease = Lease::acquire(location.clone(), &scope, "p", "owner-a")
            .await
            .expect("acquire");
        lease.start_heartbeat();

        lease.release().await;

        tokio::time::advance(Duration::from_secs(HEARTBEAT_SECS * 2)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        assert!(
            location
                .read_doc_versioned(&lease_file(&scope))
                .await
                .expect("io")
                .is_none(),
            "the doc must stay deleted after release"
        );
    }
}
