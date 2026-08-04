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
//! 2. `AlreadyExists` — read the holder.
//! 3. Same `owner` as the caller — this is the engine's own retry of
//!    a failed attempt reopening the connector, not a second session
//!    — CAS-replace to reacquire with fresh stamps.
//! 4. A different owner whose `renewed_at_ms` is older than
//!    `TTL_SECS` — that session is presumed dead — CAS-takeover.
//! 5. A different owner whose lease is still fresh — refuse, typed,
//!    naming the holder and the ages (the frozen spelling below).
//!
//! Once held, [`Lease::start_heartbeat`] spawns a task that keeps the
//! document's `renewed_at_ms` moving so a live session's lease never
//! looks stale to a competitor. If a beat ever finds the document
//! gone, owned by someone else, or loses its CAS, the lease is
//! LOST — `lost` flips and every future [`Lease::check_still_held`]
//! call (the write path's guard) refuses with the frozen abort
//! spelling, rather than let this session keep writing into what is
//! now another session's staging.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdlt_connector_sdk::spi::DestinationError;
use rdlt_connector_sdk::spi::core::crash_point;
use serde::{Deserialize, Serialize};

use super::layout::lease_file;
use crate::location::{CreateDoc, Location, is_stale_version};

/// The persisted lease document's format. Bumped only if the shape
/// changes; there is no reader compatibility burden today (a lease is
/// transient — a version mismatch could simply refuse and let the
/// operator retry once every holder has upgraded), but the field
/// exists from the start so a future change has somewhere to land.
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
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before the unix epoch")
        .as_millis() as u64
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

/// Serialize a lease document, or a fatal error if it somehow cannot
/// encode (it never fails in practice — every field is a plain string
/// or integer — but `serde_json` still returns a `Result`).
fn encode(doc: &LeaseDoc) -> Result<Vec<u8>, DestinationError> {
    serde_json::to_vec(doc)
        .map_err(|e| DestinationError::fatal(format!("encoding the destination lease: {e}")))
}

/// Decode a lease document read back from storage. A lease that fails
/// to parse is fatal rather than treated as absent — inventing "no
/// holder" for bytes that ARE there could let a second session past a
/// live lock it merely could not read.
fn decode(bytes: &[u8]) -> Result<LeaseDoc, DestinationError> {
    serde_json::from_slice(bytes)
        .map_err(|e| DestinationError::fatal(format!("unreadable destination lease: {e}")))
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
    /// Flipped by the heartbeat task the moment it can no longer prove
    /// this session still holds the lease. Read synchronously by
    /// [`Lease::check_still_held`] on the write path, so that check
    /// never has to await.
    lost: Arc<AtomicBool>,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl Lease {
    fn new(location: Location, doc_name: String, pipeline: String, owner: String) -> Self {
        Self {
            location,
            doc_name,
            pipeline,
            owner,
            lost: Arc::new(AtomicBool::new(false)),
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
            let fresh = LeaseDoc {
                format_version: LEASE_FORMAT_VERSION,
                pipeline: pipeline.to_owned(),
                owner: owner.to_owned(),
                acquired_at_ms: now,
                renewed_at_ms: now,
            };

            if let CreateDoc::Created(_version) = location
                .create_doc_exclusive(&doc_name, encode(&fresh)?)
                .await?
            {
                return Ok(Lease::new(
                    location,
                    doc_name,
                    pipeline.to_owned(),
                    owner.to_owned(),
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
            let holder = decode(&bytes)?;

            if holder.owner == owner {
                // The engine's own retry of a failed attempt: a new
                // session, same stable connector token, opening
                // against a lease this same identity already holds
                // (or held moments ago). Reacquire through a
                // CAS-replace rather than treating the existing
                // document as a foreign holder.
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
                        ));
                    }
                    // Someone else's takeover (or the original
                    // holder's own renewal) landed first — loop and
                    // re-read; that write may itself now be fresh, or
                    // may again be stale.
                    Err(e) if is_stale_version(&e) => continue,
                    Err(e) => return Err(e),
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
                renew(&location, &doc_name, &owner, &lost).await;
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
        renew(&self.location, &self.doc_name, &self.owner, &self.lost).await;
    }

    /// The write-path guard: refuse once the heartbeat has determined
    /// this session no longer holds the lease. Synchronous and cheap
    /// on purpose — every write can afford to check a bool, not a
    /// round trip to storage.
    pub(super) fn check_still_held(&self) -> Result<(), DestinationError> {
        if self.lost.load(Ordering::SeqCst) {
            return Err(DestinationError::fatal(format!(
                "the destination lease for pipeline `{}` was taken over by another session \
                 (this session's heartbeat lost the race); aborting rather than corrupting \
                 the new holder's staging",
                self.pipeline
            )));
        }
        Ok(())
    }

    /// Best-effort delete, called at the end of a successful publish.
    /// Absence is success (`Location::delete_doc`'s own contract) —
    /// this session's own release racing a takeover's CAS-replace, or
    /// a prior crashed release, both look identical to "already gone"
    /// and neither is an error here. A failed delete is likewise
    /// swallowed: the lease still expires by `TTL_SECS` if it lingers,
    /// which is the same fate an unreleased lease has anyway (see the
    /// struct doc).
    pub(super) async fn release(self) {
        crash_point!("file.lease.release", ());
        let _ = self.location.delete_doc(&self.doc_name).await;
        // `self` drops here; `Drop for Lease` aborts the heartbeat.
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        // Synchronous and safe: `JoinHandle::abort` only requests
        // cancellation, it does not await the task's teardown, so
        // this needs no runtime context of its own.
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
///   own doc explains that a local CAS write always "succeeds" no
///   matter how stale the version handed to it, because a single
///   filesystem can only ever have one lease-holding process at a
///   time (O_EXCL already serializes that). Re-reading the document
///   fresh on every beat and comparing its `owner` closes that gap: a
///   local takeover changes the file's `owner` field, and the very
///   next beat sees it and stops, even though the CAS write itself
///   raised no alarm.
/// - The CAS-replace lost (`is_stale_version`): another write — a
///   real takeover, on the S3 arm where CAS is real — landed between
///   this beat's read and its write. LOST.
/// - Any other error (a transport blip, a timeout): NOT lost. A
///   single failed beat must not end a healthy lease — `TTL_SECS`
///   carries a deliberate 20-beat margin over `HEARTBEAT_SECS`
///   precisely so that a run of blips is absorbed rather than treated
///   as death. Only a confirmed absence, a confirmed foreign owner, or
///   a confirmed lost CAS may end the lease; an unreadable network is
///   none of those three things, and the next beat gets another
///   chance.
async fn renew(location: &Location, doc_name: &str, owner: &str, lost: &AtomicBool) {
    let read = match location.read_doc_versioned(doc_name).await {
        Ok(read) => read,
        Err(_transient) => return,
    };
    let Some((bytes, version)) = read else {
        lost.store(true, Ordering::SeqCst);
        return;
    };
    let Ok(doc) = decode(&bytes) else {
        // An unreadable snapshot is presumed to be a torn mid-write
        // read rather than proof of loss — try again next beat rather
        // than ending a lease over a decode hiccup.
        return;
    };
    if doc.owner != owner {
        lost.store(true, Ordering::SeqCst);
        return;
    }
    let renewed = LeaseDoc {
        renewed_at_ms: now_ms(),
        ..doc
    };
    let Ok(bytes) = encode(&renewed) else {
        return;
    };
    match location.replace_doc_if(doc_name, bytes, version).await {
        Ok(_new_version) => {}
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
        Lease::acquire(location, &scope, "p", "owner-b")
            .await
            .expect("a lease older than the TTL is a dead session's");
    }

    #[tokio::test]
    async fn losing_the_cas_flips_check_still_held() {
        let (location, scope) = test_location();
        let lease = Lease::acquire(location.clone(), &scope, "p", "owner-a")
            .await
            .expect("acquire");
        // Simulate a takeover: overwrite the doc as another owner.
        plant_lease(&location, &scope, "owner-b", now_ms()).await;
        // A renewal attempt now loses; drive one beat synchronously.
        lease.renew_once().await;
        let err = lease.check_still_held().expect_err("taken over");
        assert!(err.to_string().contains("taken over by another session"));
    }
}
