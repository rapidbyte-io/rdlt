//! Destination certification: spawn the target's binary and certify it
//! over the wire — the role-generic protocol clauses (P1/P2/P4, probes
//! in [`crate::target`]), the handshake-borne wire clauses (P3
//! identity/skew and P7 the v0 state-format map, judged on a raw
//! handshake below the adapters — [`crate::wire`]), the testkit's
//! destination conformance clauses (D1–D6, D8) reused against the
//! managed adapter, plus the clauses that exist ONLY out of
//! process: P8, the one-session ceiling (a second concurrent
//! `OpenSession` on the LIVE socket must refuse `FailedPrecondition` —
//! the 038 frozen class), P9, close-on-abandonment (a session
//! dropped without `Close` must be reclaimed: within
//! [`RECLAIM_WINDOW`] a fresh session opens), P10, the
//! Backend-direct order book (the raw session choreography driven
//! frame by frame WITHOUT the sdk `Session` — see [`probe_order_book`]
//! for the four assertions), P11, one Arrow batch per write frame (a
//! two-batch `write` frame must be refused — [`probe_one_batch_write`]
//! authors the violation), and P12, write-side error frames carrying
//! bare cause text (P10's induction sites re-driven with the refusal
//! frames READ — [`probe_error_frame_text`]). The session probes drive
//! RAW wire sessions ([`crate::wire::open_wire_session`]) on their own
//! dials of the live socket — wire moves the managed adapter
//! deliberately never makes, and the seam that lets the rogue suite
//! prove the clauses can fail.
//!
//! The D-reuse rides a settling adapter, not the managed destination
//! raw: the in-process suite assumes dropping a session releases it
//! SYNCHRONOUSLY (its D4 arm drops one session and immediately opens
//! the next), but over the wire release is asynchronous — the server
//! must observe the stream end and run its close before the one
//! session slot frees. The adapter's `open` retries exactly the typed
//! one-session refusal within [`RECLAIM_WINDOW`], bridging the timing
//! model WITHOUT masking anything a clause certifies: a connector that
//! never reclaims still fails (the retry exhausts into the suite's own
//! failure, and P9 certifies the reclaim explicitly).
//!
//! Every clause rides under [`CLAUSE_TIMEOUT`] — a stalling connector
//! FAILS the clause, the certifier never hangs — and no failure message
//! ever carries config bytes.

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use rdlt_connector::core::{LoadId, PipelineId};
use rdlt_connector::{
    ConnectorSpec, Destination, DestinationCapabilities, DestinationError, LoadSession, OpenContext,
};
use rdlt_connector_client::{destination_client, dial};
use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_connector_protocol::proto::SessionRequest;
use rdlt_runtime::{
    ClientError, ConnectorProvider, LocalBinaryConnectorProvider, ManagedDestination, Role,
};
use rdlt_testkit::conformance::destination::{TableProbe, verify_destination};
use serde_json::Value;

use crate::report::{CLAUSE_TIMEOUT, Report, timed_out};
use crate::target::{
    GENERIC_CLAUSES, Target, fetch_spec, probe_handshake_line, report_p2, report_p4,
    resolved_requirement,
};
use crate::wire::{
    self, WireOpenError, WireReply, WireSession, ensure_request, expect, meta_json_for, mismatch,
    open_wire_session, publish_request, read_state_request, receipt_reply, refusal_shape,
    replay_request, two_batch_write_request, write_request,
};

/// The D-clauses the reused testkit suite covers — its module doc's
/// exact set (D7 has no check there yet; renumbering is forbidden).
pub(crate) const DEST_CLAUSES: [&str; 7] = ["D1", "D2", "D3", "D4", "D5", "D6", "D8"];

/// The clauses that exist only out of process, probed on raw sessions
/// over the live socket (module doc) — the tail of the destination's
/// cascade set and reported one by one below.
pub(crate) const SESSION_CLAUSES: [&str; 5] = ["P8", "P9", "P10", "P11", "P12"];

/// How long an abandoned session gets to be reclaimed before P9 fails —
/// and how long the settling adapter's `open` retries the one-session
/// refusal for. One window deliberately: both are the same question,
/// "how quickly does dropping a session free its slot".
const RECLAIM_WINDOW: Duration = Duration::from_secs(10);

/// The pause between reclaim polls.
const RECLAIM_POLL: Duration = Duration::from_millis(100);

/// The skip reason every read-back clause carries when no probe was
/// supplied — certification never silently narrows to a smaller
/// passing set. Shared with the kill matrix's destination arms
/// ([`crate::kill`]): their convergence assert is a read-back too.
pub(crate) const NO_PROBE_SKIP: &str = "no table probe supplied — read-back clauses need one; pass --probe-cmd '<sh line>' \
     (the library API takes a TableProbe directly). Single-writer stores (duckdb) refuse \
     every open beside the live connector, a read-only one included — probe a COPY: copy \
     the store file plus its WAL sidecar, then count in the copy";

/// The skip reason D8 carries for a destination that declares no merge
/// capability — the suite asserts D8 only for merge-capable
/// destinations, so folding it into the asserted set would mint a
/// `Pass` for a clause that never ran. Public as the ONE spelling
/// merge-less connectors' certify cells assert against (round-8 fix —
/// two cells carried their own copies).
pub const NO_MERGE_SKIP: &str = "the destination does not declare the merge capability — D8 certifies merge upsert and was \
     not exercised";

/// Certify `target` as a DESTINATION connector. Never hangs and never
/// panics on connector misbehavior: every clause's outcome — including
/// "the binary is not a connector at all" — is a report entry.
///
/// `probe` is the read-back seam the D-suite needs (row counting is the
/// one thing the SPI cannot do). With `None`, the D-clauses are
/// Skip-with-reason — never silently dropped, never vacuously passed —
/// while the probe-independent P-clauses still certify.
pub async fn certify_destination(target: &Target, probe: Option<&dyn TableProbe>) -> Report {
    let mut report = Report::default();

    // P1 — the handshake-line discipline, probed on a direct spawn whose
    // only purpose is P1; certification re-spawns cleanly afterward.
    match tokio::time::timeout(
        CLAUSE_TIMEOUT,
        probe_handshake_line(target, Role::Destination),
    )
    .await
    {
        Ok(Ok(())) => report.pass("P1"),
        Ok(Err(why)) => report.fail("P1", why),
        Err(_elapsed) => report.fail("P1", timed_out()),
    }

    let provider = LocalBinaryConnectorProvider::new();

    // The Spec reply feeds P4 below — and, for a path-only target,
    // identity: the operator named a binary, not an id, so the id the
    // wire handshake verifies strictly (D-039-2) is learned from the
    // connector's own report.
    let spec = fetch_spec(&provider, &target.requirement).await;

    // Everything past P1 (whose probe already wrote its entry) runs
    // over a verified handshake; without one, every remaining clause
    // fails with the one cause. `post_wire` is the same cascade AFTER
    // the wire block has judged (or failed) P3/P7 on its own spawn —
    // those two already carry their entries by then, and a cascade
    // re-failing them would write a second verdict per clause.
    let post_wire = || {
        GENERIC_CLAUSES
            .into_iter()
            .filter(|clause| *clause != "P1")
            .chain(DEST_CLAUSES)
            .chain(SESSION_CLAUSES)
    };
    let downstream = || post_wire().chain(wire::DEST_WIRE_CLAUSES);

    let requirement = match resolved_requirement(&target.requirement, &spec) {
        Ok(requirement) => requirement,
        Err(why) => {
            for clause in downstream() {
                report.fail(clause, why.clone());
            }
            return report;
        }
    };

    // The wire clauses — P3/P7 from one raw handshake — ride their OWN
    // spawn, and that spawn runs BEFORE the managed adapter exists and
    // is killed-and-REAPED before it spawns (042 Task 6): a
    // single-writer destination (duckdb) holds an exclusive
    // cross-process lock on its store from handshake to process death,
    // so two live processes handshaking the same config cannot
    // coexist — the second's open is refused by the store, failing
    // clauses about the WIRE with a cause that is really the
    // certifier's own process overlap. Sequencing the two spawns (the
    // reap included — SIGKILL alone only SENDS the signal) is free for
    // every multi-writer destination and is what makes single-writer
    // ones certifiable at all. The plain timeout-drop shape (round-5
    // unification — the source block's, the dedicated task deleted):
    // the spawned child and its advertised socket park in the shared
    // slot for the attach's AND the probe's whole life, so dropping
    // the timed-out future costs nothing the slot cannot recover —
    // the timeout arm claims the slot and awaits the child's DEATH
    // (and the socket's unlink) before the managed spawn below opens
    // the same store. The wave-4 pins hold that guarantee.
    let slot = wire::ChildSlot::default();
    match tokio::time::timeout(
        CLAUSE_TIMEOUT,
        wire::attach_for(&requirement, Role::Destination, &target.config, &slot),
    )
    .await
    {
        Ok(Ok(mut probe)) => {
            wire::certify_destination_wire(&mut report, &mut probe, &requirement.id).await;
            probe.kill().await;
        }
        Ok(Err(why)) => {
            for clause in wire::DEST_WIRE_CLAUSES {
                report.fail(clause, why.clone());
            }
        }
        Err(_elapsed) => {
            wire::reap_parked(&slot).await;
            for clause in wire::DEST_WIRE_CLAUSES {
                report.fail(clause, timed_out());
            }
        }
    }

    // The certification subject: one managed destination, spawned
    // honestly through the provider (resolution is part of the bar).
    let managed = tokio::time::timeout(
        CLAUSE_TIMEOUT,
        provider.destination(&requirement, &target.config),
    )
    .await;
    // The SPI's `Destination` demands `'static`, so the settling
    // adapter OWNS the managed destination for the rest of the run —
    // `.inner` is the raw adapter wherever settling must not apply.
    let managed = match managed {
        Ok(Ok(managed)) => SettledDestination { inner: managed },
        Ok(Err(error)) => {
            let why =
                format!("the provider could not spawn the connector as a destination: {error}");
            for clause in post_wire() {
                report.fail(clause, why.clone());
            }
            return report;
        }
        Err(_elapsed) => {
            for clause in post_wire() {
                report.fail(clause, timed_out());
            }
            return report;
        }
    };

    // P2 — typed config refusal, probed on its own spawn with a
    // one-unknown-field document.
    let bogus = serde_json::json!({ "__rdlt_certify_bogus__": true });
    report_p2(
        &mut report,
        tokio::time::timeout(CLAUSE_TIMEOUT, provider.destination(&requirement, &bogus)).await,
    );

    // P4 — the pre-handshake Spec: name/version non-empty and a JSON
    // -object config schema, answered with no config at all.
    report_p4(&mut report, &spec);

    // D-reuse — the testkit's destination conformance suite, verbatim,
    // against the (settling) managed adapter: the wire is certified by
    // the SAME clauses an in-process connector answers to. The suite's
    // own skips (clauses an abort left unreached) fold through as SKIP
    // entries — never a vacuous Pass. D8 is asserted only when the
    // connector declares merge — otherwise the suite never ran it, and
    // the honest verdict is a Skip.
    match probe {
        Some(probe) => {
            let merge = managed.capabilities().merge;
            // The clause budget bounds SPI traffic ALONE (round-5 fix):
            // the probe clock stops while a count runs — each count
            // carries its own probe budget and fails naming itself, so
            // a slow-but-legal probe can no longer exhaust a suite
            // budget that was never sized for it.
            let (metered, probe_clock) = crate::clock::StopClockProbe::new(probe);
            match crate::clock::timeout_excluding_probe(
                CLAUSE_TIMEOUT,
                &probe_clock,
                verify_destination(&managed, &metered),
            )
            .await
            {
                Ok(outcome) => {
                    // The report's absorb renders skips honestly, so
                    // this caller ACKNOWLEDGES them by name (round-7:
                    // the fields went private).
                    let (failures, skips) = outcome.tolerating_skips();
                    if merge {
                        report.absorb(failures, skips, &DEST_CLAUSES);
                    } else {
                        // DEST_CLAUSES minus D8, derived — the hand
                        // copy is the drift the skip arm already shed.
                        let without_d8: Vec<&'static str> = DEST_CLAUSES
                            .into_iter()
                            .filter(|clause| *clause != "D8")
                            .collect();
                        report.absorb(failures, skips, &without_d8);
                        report.skip("D8", NO_MERGE_SKIP.to_string());
                    }
                }
                Err(_elapsed) => {
                    for clause in DEST_CLAUSES {
                        report.fail(clause, timed_out());
                    }
                }
            }
        }
        None => {
            for clause in DEST_CLAUSES {
                report.skip(clause, NO_PROBE_SKIP.to_string());
            }
        }
    }

    // P8, P9 and P10 need the LIVE socket — the provider's guard
    // carries it.
    match managed
        .inner
        .guard()
        .map(|guard| guard.socket_path().to_path_buf())
    {
        None => {
            let why = "the provider returned no process guard, so the live socket cannot be \
                       re-dialed"
                .to_string();
            // The one clause list ([`SESSION_CLAUSES`]) — this arm's
            // hand copy had to grow when P11/P12 joined, which is
            // exactly the drift a loop forecloses.
            for clause in SESSION_CLAUSES {
                report.skip(clause, why.clone());
            }
        }
        Some(socket) => {
            // One entropy suffix for this invocation's session loads
            // (round-13, `mint_run_entropy`): the probes' publishes
            // mint DURABLE receipts in real warehouses, and a
            // deterministic load id would hand a re-certification the
            // previous run's receipts.
            let entropy = crate::target::mint_run_entropy();
            // P8 — the one-session ceiling: with a session held, a
            // second `OpenSession` on a SECOND dial of the same socket
            // must refuse `FailedPrecondition` (the 038 frozen class).
            report_session_probe(
                &mut report,
                "P8",
                probe_one_session_ceiling(&socket, &entropy),
            )
            .await;

            // P9 — close-on-abandonment: a session dropped without
            // `Close` must be reclaimed within the window.
            report_session_probe(
                &mut report,
                "P9",
                probe_abandonment_reclaim(&socket, &entropy),
            )
            .await;

            // P10 — the Backend-direct order book.
            report_p10(&mut report, &socket, &entropy).await;

            // P11 — one Arrow batch per write frame, induced with a
            // two-batch frame on its own session.
            report_session_probe(&mut report, "P11", probe_one_batch_write(&socket, &entropy))
                .await;

            // P12 — write-side error frames carry cause text, judged
            // at P10's two induction sites re-driven on this clause's
            // own sessions.
            report_session_probe(
                &mut report,
                "P12",
                probe_error_frame_text(&socket, &entropy),
            )
            .await;
        }
    }

    report
}

/// Is `error` the wire's one-session refusal — a FATAL wrapping the
/// transport `FailedPrecondition` status? Typed end to end: the
/// judgment reads the tonic code through the client's own error type,
/// never a rendered message.
fn is_session_ceiling_refusal(error: &DestinationError) -> bool {
    let DestinationError::Fatal(cause) = error else {
        return false;
    };
    matches!(
        cause.downcast_ref::<ClientError>(),
        Some(ClientError::Transport(status))
            if status.code() == tonic::Code::FailedPrecondition
    )
}

/// Open a session, retrying exactly the one-session refusal within
/// [`RECLAIM_WINDOW`] — the wire-honest equivalent of the in-process
/// assumption that a dropped predecessor has already released its slot.
/// Any OTHER failure surfaces immediately, and exhausting the window
/// surfaces the refusal itself.
async fn settle_open(
    dest: &ManagedDestination,
    pipeline: &str,
    load_id: &str,
) -> Result<Box<dyn LoadSession>, DestinationError> {
    let deadline = tokio::time::Instant::now() + RECLAIM_WINDOW;
    loop {
        let context = OpenContext::new(PipelineId::new(pipeline), LoadId::new(load_id));
        match dest.open(context).await {
            Ok(session) => return Ok(session),
            Err(error)
                if is_session_ceiling_refusal(&error) && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(RECLAIM_POLL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// The D-reuse subject: the managed destination with the settling
/// `open` — see the module doc for why the suite cannot run against
/// the raw adapter. Owns the managed destination because the SPI's
/// `Destination` demands `'static`.
struct SettledDestination {
    inner: ManagedDestination,
}

#[async_trait]
impl Destination for SettledDestination {
    fn spec(&self) -> ConnectorSpec {
        self.inner.spec()
    }

    async fn check(&self) -> Result<(), DestinationError> {
        self.inner.check().await
    }

    fn capabilities(&self) -> DestinationCapabilities {
        self.inner.capabilities()
    }

    async fn open(&self, context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        settle_open(
            &self.inner,
            context.pipeline.as_str(),
            context.load_id.as_str(),
        )
        .await
    }
}

/// The wire twin of [`settle_open`]: open a RAW session on the live
/// socket, retrying exactly the transport ceiling refusal within
/// [`RECLAIM_WINDOW`] — release over the wire is asynchronous, so the
/// slot the previous session held may free a beat after its stream
/// ended. Exhausting the window surfaces the refusal itself; any other
/// failure surfaces immediately.
async fn settle_open_wire(
    socket: &Path,
    pipeline: &str,
    load_id: &str,
) -> Result<WireSession, String> {
    let deadline = tokio::time::Instant::now() + RECLAIM_WINDOW;
    loop {
        match open_wire_session(socket, pipeline, load_id).await {
            Ok(session) => return Ok(session),
            Err(WireOpenError::Ceiling(_)) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(RECLAIM_POLL).await;
            }
            Err(WireOpenError::Ceiling(status)) => {
                return Err(format!("the one-session refusal never lifted: {status}"));
            }
            Err(WireOpenError::Other(why)) => return Err(why),
        }
    }
}

/// The P8 probe, all wire moves on the LIVE socket (the managed
/// adapter deliberately never makes them — and probing raw is what
/// lets the rogue suite prove the clause can fail): hold one raw
/// session, dial the socket a second time, and ask for a second
/// session with an empty request stream — the refusal must be the
/// transport-level `FailedPrecondition` status (the RPC never opens),
/// anything else fails the clause. The held session is closed orderly
/// afterward whatever P8 concluded.
async fn probe_one_session_ceiling(socket: &Path, entropy: &str) -> Result<(), String> {
    let session = settle_open_wire(socket, "rdlt-certify-p8", &format!("certify-p8-{entropy}"))
        .await
        .map_err(|why| format!("could not open the session that holds the slot: {why}"))?;

    let second = async {
        let channel = dial(socket, MAX_FRAME_BYTES as u64)
            .await
            .map_err(|error| format!("re-dialing the live socket: {error}"))?;
        Ok::<_, String>(
            destination_client(channel)
                .open_session(tokio_stream::empty::<SessionRequest>())
                .await,
        )
    }
    .await;

    let verdict = match second {
        Err(why) => Err(why),
        Ok(Ok(_accepted)) => Err(
            "a second concurrent session was ACCEPTED — v0 allows exactly one session per \
             connector process; the second OpenSession must be refused with FailedPrecondition"
                .to_string(),
        ),
        Ok(Err(status)) if status.code() == tonic::Code::FailedPrecondition => Ok(()),
        Ok(Err(status)) => Err(format!(
            "a second concurrent session must be refused with the FailedPrecondition status — \
             the connector answered {:?}: {}",
            status.code(),
            status.message()
        )),
    };

    // Orderly close of the slot holder — P8 probes the ceiling, not
    // abandonment (that is P9's clause).
    session.close().await;
    verdict
}

/// The P9 probe: open a raw session, then DROP it — no `Close` frame,
/// the stream just ends (the wire's abandonment signal). Within
/// [`RECLAIM_WINDOW`] a fresh session on the SAME pipeline must open:
/// the slot was released and the abandoned session's staging claim
/// (a lease, for destinations that hold one) reclaimed. The fresh
/// session is closed orderly.
async fn probe_abandonment_reclaim(socket: &Path, entropy: &str) -> Result<(), String> {
    let abandoned = settle_open_wire(
        socket,
        "rdlt-certify-p9",
        &format!("certify-p9-abandoned-{entropy}"),
    )
    .await
    .map_err(|why| format!("could not open the session to abandon: {why}"))?;
    drop(abandoned);

    match settle_open_wire(
        socket,
        "rdlt-certify-p9",
        &format!("certify-p9-fresh-{entropy}"),
    )
    .await
    {
        Ok(fresh) => {
            fresh.close().await;
            Ok(())
        }
        Err(why) => Err(format!(
            "abandoned session was not reclaimed: a fresh session still refused {}s after the \
             stream ended without Close: {why}",
            RECLAIM_WINDOW.as_secs()
        )),
    }
}

/// Fold one session probe's outcome into its clause entry — the ONE
/// timeout/pass/fail match all five session clauses share (round-6
/// fix: P11/P12 had grown the copy count to five).
async fn report_session_probe<F>(report: &mut Report, clause: &'static str, probe: F)
where
    F: std::future::Future<Output = Result<(), String>>,
{
    match tokio::time::timeout(CLAUSE_TIMEOUT, probe).await {
        Ok(Ok(())) => report.pass(clause),
        Ok(Err(why)) => report.fail(clause, why),
        Err(_elapsed) => report.fail(clause, timed_out()),
    }
}

/// The P10 identities — one pipeline of their own so the order-book
/// probe's commits never collide with the D-suite's or P8/P9's.
const P10_PIPELINE: &str = "rdlt-certify-p10";
/// The one load id both passes commit and interrogate — the
/// invocation's entropy joins it in `probe_order_book` (round-13).
const P10_LOAD_PREFIX: &str = "certify-p10";
/// The one `(load, seq)` idempotency key the probe drives.
const P10_SEQ: u64 = 1;
/// The probe's table.
const P10_TABLE: &str = "p10_order_book";

/// P10 under the clause budget — a probe that stalls (the hang-on-close
/// rogue's arm) FAILS the clause with the one timeout spelling, the
/// certifier outliving the hang.
async fn report_p10(report: &mut Report, socket: &Path, entropy: &str) {
    report_session_probe(report, "P10", probe_order_book(socket, entropy)).await;
}

/// The P10 probe — the Backend-direct order book: the raw destination
/// choreography driven frame by frame over the live socket, WITHOUT
/// the sdk `Session`'s good manners between the certifier and the
/// server, certifying the exactly-once grammar the wire actually
/// speaks. Four assertions, two session passes:
///
/// - reply-per-frame: every request frame is answered with its OWN
///   tag ([`expect`]; a stream that ends or stalls instead fails);
/// - write-before-ensure REFUSED: the deliberate out-of-order `write`
///   is driven FIRST and must earn a typed error frame;
/// - replay-vs-publish exclusivity: a fresh session must find the
///   receipt an earlier session committed (`Backend::existing_receipt`
///   durability), accept `replay` for it, and answer a `publish` of
///   that same load with a refusal OR the SAME receipt — never a
///   fresh mint (rdlt-core's own `CommitReceipt` contract:
///   "Re-committing the same `(load_id, commit_seq)` MUST return the
///   prior receipt without re-publishing"; the sdk serve module's 038
///   F-4 record asked for exactly this Backend-direct clause);
/// - part-event legality: `part_closed` events are legal anywhere
///   before `close`'s answer and NOWHERE after it
///   ([`WireSession::close_judged`] holds the boundary).
async fn probe_order_book(socket: &Path, entropy: &str) -> Result<(), String> {
    let p10_load = format!("{P10_LOAD_PREFIX}-{entropy}");
    let meta_json = meta_json_for(P10_PIPELINE, &p10_load, P10_SEQ);

    // ——— Pass 1: the out-of-order probe, then the canonical sequence.
    // A violation returns mid-session; the drop is the wire's
    // abandonment signal and P9 already certified its reclaim.
    let mut session = settle_open_wire(socket, P10_PIPELINE, &p10_load)
        .await
        .map_err(|why| format!("could not open the order-book session: {why}"))?;

    // The deliberate out-of-order move FIRST: a `write` on a table no
    // `ensure` ever named must be refused with a typed error frame.
    match session.request(write_request(P10_TABLE)).await? {
        WireReply::Error(_frame) => {}
        other => {
            let answer = match other {
                WireReply::Written => "ACCEPTED".to_string(),
                other => format!("answered `{}`", other.tag()),
            };
            return Err(format!(
                "an out-of-order `write` was {answer} — a write to a never-ensured table \
                 must be refused with a typed error frame"
            ));
        }
    }

    expect(
        session.request(ensure_request(P10_TABLE)).await?,
        "ensure",
        "ensured",
    )?;
    expect(
        session.request(write_request(P10_TABLE)).await?,
        "write",
        "written",
    )?;

    match receipt_reply(&mut session, &p10_load, P10_SEQ).await? {
        // A fresh load: `publish` is the legal continuation.
        None => expect(
            session.request(publish_request(&meta_json)).await?,
            "publish",
            "published",
        )?,
        // An earlier certification of this target already committed
        // the load: `replay` is the legal continuation.
        Some(receipt) => expect(
            session.request(replay_request(&meta_json, receipt)).await?,
            "replay",
            "replayed",
        )?,
    }
    expect(
        session.request(read_state_request(P10_PIPELINE)).await?,
        "read_state",
        "state",
    )?;
    session.close_judged().await?;

    // ——— Pass 2: the exclusivity pass, a FRESH session on the same
    // load — the receipt must have outlived the session that minted it.
    let mut session = settle_open_wire(socket, P10_PIPELINE, &p10_load)
        .await
        .map_err(|why| format!("could not open the exclusivity session: {why}"))?;
    let Some(existing) = receipt_reply(&mut session, &p10_load, P10_SEQ).await? else {
        return Err(
            "a fresh session's `existing_receipt` reported no receipt for a load an earlier \
             session committed — the receipt must be durable across sessions"
                .to_string(),
        );
    };
    expect(
        session
            .request(replay_request(&meta_json, existing.clone()))
            .await?,
        "replay",
        "replayed",
    )?;
    match session.request(publish_request(&meta_json)).await? {
        // A refusal is one legal answer to the choreography violation.
        WireReply::Error(_frame) => {}
        // The other: the PRIOR receipt, returned without re-publishing.
        WireReply::Published(published) => {
            let same_receipt = match (receipt_value(&existing), receipt_value(&published)) {
                (Some(existing), Some(published)) => existing == published,
                // Undecodable bytes on either side never equal anything.
                _ => false,
            };
            if !same_receipt {
                return Err(format!(
                    "a `publish` for a load whose receipt already exists minted a NEW receipt \
                     — after `existing_receipt` reports a receipt, `publish` must be refused \
                     or answer that same receipt (existing {}, published {})",
                    render_receipt(&existing),
                    render_receipt(&published)
                ));
            }
        }
        other => return Err(mismatch("publish", &other, "published")),
    }
    session.close_judged().await
}

/// The P11 identities — a pipeline of this clause's own, away from the
/// P10 order book's commits.
const P11_PIPELINE: &str = "rdlt-certify-p11";
/// P11's load id.
const P11_LOAD_PREFIX: &str = "certify-p11";
/// P11's table.
const P11_TABLE: &str = "p11_one_batch";

/// The P11 probe — one Arrow batch per write frame, the write
/// direction's twin of P5's read-side rule, induced rather than
/// observed (the certifier authors the violation): ensure a table,
/// then send a `write` whose arrow_ipc payload is ONE IPC stream
/// carrying TWO record batches. The refusal must arrive as a typed
/// error frame — its SHAPE is P12's clause, not this one's — while
/// `written` means every row after the first batch was at the
/// connector's mercy, and fails. The sdk's serve half refuses a
/// multi-batch frame itself, so first-party destinations pass with no
/// connector code. The session is closed best-effort whatever the
/// verdict: the refusal is answered in-stream, the session outlives it.
async fn probe_one_batch_write(socket: &Path, entropy: &str) -> Result<(), String> {
    let p11_load = format!("{P11_LOAD_PREFIX}-{entropy}");
    let mut session = settle_open_wire(socket, P11_PIPELINE, &p11_load)
        .await
        .map_err(|why| format!("could not open the one-batch session: {why}"))?;
    let verdict = async {
        expect(
            session.request(ensure_request(P11_TABLE)).await?,
            "ensure",
            "ensured",
        )?;
        match session.request(two_batch_write_request(P11_TABLE)).await? {
            WireReply::Error(_frame) => Ok(()),
            other => {
                let answer = match other {
                    WireReply::Written => "ACCEPTED".to_string(),
                    other => format!("answered `{}`", other.tag()),
                };
                Err(format!(
                    "a two-batch `write` was {answer} — a write frame's arrow_ipc payload \
                     must carry exactly one record batch, and a multi-batch frame must be \
                     refused with a typed error frame"
                ))
            }
        }
    }
    .await;
    session.close().await;
    verdict
}

/// The P12 identities — a pipeline of this clause's own.
const P12_PIPELINE: &str = "rdlt-certify-p12";
/// The one load id both P12 sessions commit and interrogate.
const P12_LOAD_PREFIX: &str = "certify-p12";
/// The one `(load, seq)` idempotency key the probe drives.
const P12_SEQ: u64 = 1;
/// P12's table.
const P12_TABLE: &str = "p12_error_text";

/// The P12 probe — write-side error frames carry cause text: P10's two
/// induction sites (the out-of-order `write`, the already-receipted
/// `publish`) re-driven on this clause's OWN sessions, with the
/// refusal frames READ where P10 deliberately discards them — P10
/// certifies that the refusals arrive, this clause certifies what they
/// say. Each frame answers to [`refusal_shape`], the same judgment P6
/// holds the read direction to: a real classification enum value, and
/// a message that is bare cause text, never one of the four client
/// renderings. A violation returns mid-session; the drop is the wire's
/// abandonment signal and P9 already certified its reclaim.
async fn probe_error_frame_text(socket: &Path, entropy: &str) -> Result<(), String> {
    let p12_load = format!("{P12_LOAD_PREFIX}-{entropy}");
    let meta_json = meta_json_for(P12_PIPELINE, &p12_load, P12_SEQ);

    // ——— Induction 1: the out-of-order `write` on a fresh session,
    // its refusal frame judged.
    let mut session = settle_open_wire(socket, P12_PIPELINE, &p12_load)
        .await
        .map_err(|why| format!("could not open the error-text session: {why}"))?;
    match session.request(write_request(P12_TABLE)).await? {
        WireReply::Error(frame) => refusal_shape(&frame)
            .map_err(|why| format!("the out-of-order `write` refusal: {why}"))?,
        other => {
            return Err(format!(
                "an out-of-order `write` was answered `{}` — the induced refusal never \
                 arrived as an error frame, so its message could not be judged",
                other.tag()
            ));
        }
    }

    // The canonical sequence commits the load, so induction 2 has a
    // receipt to collide with (`replay` when an earlier certification
    // of this target already committed it — the P10 posture).
    expect(
        session.request(ensure_request(P12_TABLE)).await?,
        "ensure",
        "ensured",
    )?;
    expect(
        session.request(write_request(P12_TABLE)).await?,
        "write",
        "written",
    )?;
    match receipt_reply(&mut session, &p12_load, P12_SEQ).await? {
        None => expect(
            session.request(publish_request(&meta_json)).await?,
            "publish",
            "published",
        )?,
        Some(receipt) => expect(
            session.request(replay_request(&meta_json, receipt)).await?,
            "replay",
            "replayed",
        )?,
    }
    session.close().await;

    // ——— Induction 2: the already-receipted `publish` on a fresh
    // session. A refusal is one legal answer — its frame is the
    // judged subject; the prior receipt is the other, and carries no
    // frame to judge (the same-receipt equality is P10's clause).
    let mut session = settle_open_wire(socket, P12_PIPELINE, &p12_load)
        .await
        .map_err(|why| format!("could not open the publish-induction session: {why}"))?;
    let Some(existing) = receipt_reply(&mut session, &p12_load, P12_SEQ).await? else {
        return Err(
            "a fresh session's `existing_receipt` reported no receipt for a load an earlier \
             session committed — the already-receipted `publish` induction cannot be driven"
                .to_string(),
        );
    };
    expect(
        session
            .request(replay_request(&meta_json, existing))
            .await?,
        "replay",
        "replayed",
    )?;
    match session.request(publish_request(&meta_json)).await? {
        WireReply::Error(frame) => refusal_shape(&frame)
            .map_err(|why| format!("the already-receipted `publish` refusal: {why}"))?,
        WireReply::Published(_receipt) => {}
        other => return Err(mismatch("publish", &other, "published")),
    }
    session.close().await;
    Ok(())
}

/// A receipt's JSON value, for the same-receipt judgment — `None` when
/// the bytes do not decode (undecodable never equals anything).
fn receipt_value(receipt_json: &[u8]) -> Option<Value> {
    serde_json::from_slice(receipt_json).ok()
}

/// A receipt for an evidence line — its JSON document, or the honest
/// marker when the server's bytes were not JSON at all.
fn render_receipt(receipt_json: &[u8]) -> String {
    receipt_value(receipt_json)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "<undecodable receipt>".to_string())
}

#[cfg(test)]
mod tests {
    //! The P8/P9/P10 rogue suite (the T3 carry — the P8/P9 Fail arms
    //! were code-present but untested; P10's rogues are its
    //! certification bar): each designated rogue proves its clause
    //! fails with the pinned evidence. In-process over UDS — no spawn,
    //! no built bin — so all ride the bare (ungated) suite, driving
    //! the probe functions directly (the exact strings
    //! `certify_destination` folds into the report's Fail entries).
    //! The P10 rogue tests live HERE, beside the pub(crate) probe seam
    //! they drive — the crate's precedent supersedes the plan's
    //! tests/cases placement.

    use super::*;
    use crate::report::Verdict;
    use crate::rogue::{self, OrderBookScript, SessionDiscipline};

    /// `DEST_CLAUSES` keeps the report's render order, but its SET must
    /// equal the testkit suite's own asserted set — a clause added to
    /// the suite without a certify entry (or the other way round) would
    /// silently narrow one side's report; the drift fails here by name
    /// instead.
    #[test]
    fn dest_clauses_cover_exactly_the_testkit_suites_asserted_set() {
        use std::collections::BTreeSet;
        let report_side: BTreeSet<&str> = DEST_CLAUSES.into_iter().collect();
        let suite_side: BTreeSet<&str> = rdlt_testkit::conformance::destination::ASSERTED_CLAUSES
            .into_iter()
            .collect();
        assert_eq!(report_side, suite_side);
    }

    /// P8's designated rogue: a destination that ACCEPTS a second
    /// concurrent session fails the clause with the pinned evidence.
    #[tokio::test]
    async fn a_destination_accepting_a_second_session_fails_p8() {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("rogue.sock");
        let _serving = rogue::serve_destination(&socket, SessionDiscipline::AcceptEverySession);

        let why = probe_one_session_ceiling(&socket, "pinned")
            .await
            .expect_err("a second session was accepted — P8 must fail");
        assert_eq!(
            why,
            "a second concurrent session was ACCEPTED — v0 allows exactly one session per \
             connector process; the second OpenSession must be refused with FailedPrecondition"
        );
    }

    /// P9's designated rogue: a destination that never releases the
    /// slot after abandonment fails the clause within its window —
    /// paused tokio time auto-advances the poll sleeps, so the 10s
    /// window elapses without wall-clock cost.
    #[tokio::test(start_paused = true)]
    async fn a_destination_that_never_reclaims_fails_p9() {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("rogue.sock");
        let _serving = rogue::serve_destination(&socket, SessionDiscipline::NeverReclaim);

        let why = probe_abandonment_reclaim(&socket, "pinned")
            .await
            .expect_err("the slot was never reclaimed — P9 must fail");
        let pinned = "abandoned session was not reclaimed: a fresh session still refused 10s \
                      after the stream ended without Close: ";
        let suffix = why.strip_prefix(pinned).unwrap_or_else(|| {
            panic!("the evidence must carry the pinned prefix `{pinned}`, got: {why}")
        });
        // The cause must be `settle_open_wire`'s window-exhaustion
        // spelling specifically — proof the failure came from the
        // ceiling refusal outlasting the reclaim window, not from some
        // other open error folded into the same prefix.
        let exhaustion = "the one-session refusal never lifted: ";
        assert!(
            suffix.starts_with(exhaustion),
            "the evidence must carry the exhaustion suffix `{exhaustion}`, got: {why}"
        );
    }

    /// Serve the order-book rogue and hand back the socket (the
    /// tempdir rides along so it outlives the probe).
    fn order_book_rogue(script: OrderBookScript) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("rogue.sock");
        let _serving = rogue::serve_order_book(&socket, script);
        (dir, socket)
    }

    /// The P10 control: a server that keeps the whole grammar passes
    /// the probe — proof the driver's happy path completes in the bare
    /// suite, without a spawned bin (the gated file cell is the
    /// real-connector twin of this pin).
    #[tokio::test]
    async fn a_conformant_order_book_passes_p10() {
        let (_dir, socket) = order_book_rogue(OrderBookScript::Conformant);
        probe_order_book(&socket, "pinned")
            .await
            .expect("a conformant order book must pass P10");
    }

    /// P10's first designated rogue: a destination that answers
    /// `written` to a write on a never-ensured table fails with the
    /// pinned evidence — the out-of-order probe is driven FIRST, so
    /// nothing else in the sequence can mask the missing refusal.
    #[tokio::test]
    async fn a_destination_accepting_an_unordered_write_fails_p10() {
        let (_dir, socket) = order_book_rogue(OrderBookScript::AcceptWriteBeforeEnsure);
        let why = probe_order_book(&socket, "pinned")
            .await
            .expect_err("the unordered write was accepted — P10 must fail");
        assert_eq!(
            why,
            "an out-of-order `write` was ACCEPTED — a write to a never-ensured table must \
             be refused with a typed error frame"
        );
    }

    /// P10's second designated rogue: a destination that reports an
    /// existing receipt, accepts `replay`, then ALSO accepts `publish`
    /// with a freshly minted receipt fails with both receipts named —
    /// the replay-vs-publish exclusivity violated in the only
    /// wire-observable way (a refusal and the prior receipt are the
    /// two legal answers).
    #[tokio::test]
    async fn a_destination_minting_a_fresh_receipt_on_a_replayed_load_fails_p10() {
        let (_dir, socket) = order_book_rogue(OrderBookScript::PublishOnReplay);
        let why = probe_order_book(&socket, "pinned")
            .await
            .expect_err("the publish minted a fresh receipt — P10 must fail");
        assert_eq!(
            why,
            "a `publish` for a load whose receipt already exists minted a NEW receipt — \
             after `existing_receipt` reports a receipt, `publish` must be refused or answer \
             that same receipt (existing {\"load_id\":\"certify-p10-pinned\",\"commit_seq\":1}, \
             published {\"load_id\":\"certify-p10-pinned\",\"commit_seq\":2})"
        );
    }

    /// P10's part-event boundary rogue: a destination that answers
    /// `closed` and then emits a `part_closed` event fails with the
    /// pinned evidence — part events are legal anywhere before
    /// `close`'s answer and nowhere after it.
    #[tokio::test]
    async fn a_part_event_after_the_close_reply_fails_p10() {
        let (_dir, socket) = order_book_rogue(OrderBookScript::PartEventAfterClose);
        let why = probe_order_book(&socket, "pinned")
            .await
            .expect_err("a part event crossed the close boundary — P10 must fail");
        assert_eq!(
            why,
            "a `part_closed` reply arrived after `close` was answered — part events and \
             replies are legal only before the session's end"
        );
    }

    /// The P11/P12 control: the conformant order book passes BOTH
    /// write-side clauses — the two-batch frame is refused with a
    /// well-shaped error frame, and every induced refusal carries bare
    /// cause text (the gated file cell is the real-connector twin).
    #[tokio::test]
    async fn a_conformant_order_book_passes_p11_and_p12() {
        let (_dir, socket) = order_book_rogue(OrderBookScript::Conformant);
        probe_one_batch_write(&socket, "pinned")
            .await
            .expect("a conformant order book must pass P11");
        probe_error_frame_text(&socket, "pinned")
            .await
            .expect("a conformant order book must pass P12");
    }

    /// P11's designated rogue: a destination that answers `written` to
    /// a write frame whose arrow_ipc payload carries TWO record batches
    /// fails with the pinned evidence — and violates P11 ALONE (the
    /// order book and the refusal texts hold, so the other session
    /// clauses pass against the same rogue).
    #[tokio::test]
    async fn a_destination_accepting_a_two_batch_write_fails_p11() {
        let (_dir, socket) = order_book_rogue(OrderBookScript::AcceptMultiBatchWrite);
        let why = probe_one_batch_write(&socket, "pinned")
            .await
            .expect_err("the two-batch write was accepted — P11 must fail");
        assert_eq!(
            why,
            "a two-batch `write` was ACCEPTED — a write frame's arrow_ipc payload must carry \
             exactly one record batch, and a multi-batch frame must be refused with a typed \
             error frame"
        );
        probe_order_book(&socket, "pinned")
            .await
            .expect("the rogue violates P11 alone — P10 must pass");
        probe_error_frame_text(&socket, "pinned")
            .await
            .expect("the rogue violates P11 alone — P12 must pass");
    }

    /// P12's designated rogue: a destination whose induced refusal
    /// carries a client rendering in its message (`fatal destination
    /// error: boom`) fails with the pinned evidence — and violates P12
    /// ALONE (the refusal still ARRIVES as a typed error frame, so the
    /// order book holds and the other session clauses pass).
    #[tokio::test]
    async fn a_client_rendering_in_a_session_refusal_fails_p12() {
        let (_dir, socket) = order_book_rogue(OrderBookScript::RenderedRefusal);
        let why = probe_error_frame_text(&socket, "pinned")
            .await
            .expect_err("the refusal message carries a client rendering — P12 must fail");
        assert_eq!(
            why,
            "the out-of-order `write` refusal: classification rendered inside the message — \
             the frame carries cause text; classification travels as the enum (the message \
             begins with `fatal destination error: `)"
        );
        probe_order_book(&socket, "pinned")
            .await
            .expect("the rogue violates P12 alone — P10 must pass");
        probe_one_batch_write(&socket, "pinned")
            .await
            .expect("the rogue violates P12 alone — P11 must pass");
    }

    /// P10's third designated rogue: a destination that never answers
    /// `close` proves the CLAUSE_TIMEOUT arm — the certifier OUTLIVES
    /// the hang and renders the one timeout spelling. The test itself
    /// is bounded at 45s (CLAUSE_TIMEOUT plus margin) so a broken
    /// timeout fails THIS test, not the suite; the paused clock
    /// auto-advances the waits, so neither bound costs wall time.
    #[tokio::test(start_paused = true)]
    async fn a_destination_hanging_on_close_fails_p10_by_timeout() {
        let (_dir, socket) = order_book_rogue(OrderBookScript::HangOnClose);

        let mut report = Report::default();
        let outcome = tokio::time::timeout(
            Duration::from_secs(45),
            report_p10(&mut report, &socket, "pinned"),
        )
        .await;
        assert!(
            outcome.is_ok(),
            "the certifier must outlive the hang — CLAUSE_TIMEOUT never fired"
        );
        let entry = report
            .entries
            .iter()
            .find(|entry| entry.clause == "P10")
            .expect("report_p10 always writes a P10 entry");
        match &entry.verdict {
            Verdict::Fail(why) => assert_eq!(
                why,
                "clause timed out after 30s — a connector that stalls fails the clause"
            ),
            other => panic!("a hang must Fail P10 by timeout, got {other:?}"),
        }
    }
}
