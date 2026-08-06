//! Destination certification: spawn the target's binary and certify it
//! over the wire — the role-generic protocol clauses (P1/P2/P4, probes
//! in [`crate::target`]), the handshake-borne wire clauses (P3
//! identity/skew and P7 the v0 state-format map, judged on a raw
//! handshake below the adapters — [`crate::wire`]), the testkit's
//! destination conformance clauses (D1–D6, D8) reused against the
//! managed adapter, plus the two clauses that exist ONLY out of
//! process: P8, the one-session ceiling (a second concurrent
//! `OpenSession` on the LIVE socket must refuse `FailedPrecondition` —
//! the 038 frozen class), and P9, close-on-abandonment (a session
//! dropped without `Close` must be reclaimed: within
//! [`RECLAIM_WINDOW`] a fresh session opens). The P8/P9 probes drive
//! RAW wire sessions ([`crate::wire::open_wire_session`]) on their own
//! dials of the live socket — wire moves the managed adapter
//! deliberately never makes, and the seam that lets the rogue suite
//! prove both clauses can fail.
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

use crate::report::{CLAUSE_TIMEOUT, Report, timed_out};
use crate::target::{
    Target, fetch_spec, probe_handshake_line, report_p2, report_p4, resolved_requirement,
};
use crate::wire::{self, WireOpenError, WireSession, open_wire_session};

/// The D-clauses the reused testkit suite covers — its module doc's
/// exact set (D7 has no check there yet; renumbering is forbidden).
const DEST_CLAUSES: [&str; 7] = ["D1", "D2", "D3", "D4", "D5", "D6", "D8"];

/// How long an abandoned session gets to be reclaimed before P9 fails —
/// and how long the settling adapter's `open` retries the one-session
/// refusal for. One window deliberately: both are the same question,
/// "how quickly does dropping a session free its slot".
const RECLAIM_WINDOW: Duration = Duration::from_secs(10);

/// The pause between reclaim polls.
const RECLAIM_POLL: Duration = Duration::from_millis(100);

/// The skip reason every read-back D-clause carries when no probe was
/// supplied — certification never silently narrows to a smaller
/// passing set.
const NO_PROBE_SKIP: &str =
    "no table probe supplied — read-back clauses need one; pass --probe or use the library API";

/// The skip reason D8 carries for a destination that declares no merge
/// capability — the suite asserts D8 only for merge-capable
/// destinations, so folding it into the asserted set would mint a
/// `Pass` for a clause that never ran.
const NO_MERGE_SKIP: &str = "the destination does not declare the merge capability — D8 certifies merge upsert and was \
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

    // Everything past P1 runs over a verified handshake; without one,
    // every remaining clause fails with the one cause.
    let downstream = || {
        ["P2", "P4"]
            .into_iter()
            .chain(wire::DEST_WIRE_CLAUSES)
            .chain(DEST_CLAUSES)
            .chain(["P8", "P9"])
    };

    let requirement = match resolved_requirement(&target.requirement, &spec) {
        Ok(requirement) => requirement,
        Err(why) => {
            for clause in downstream() {
                report.fail(clause, why.clone());
            }
            return report;
        }
    };

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
            for clause in downstream() {
                report.fail(clause, why.clone());
            }
            return report;
        }
        Err(_elapsed) => {
            for clause in downstream() {
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

    // The wire clauses — P3/P7 from one raw handshake — ride their OWN
    // spawn: the kit watches the actual handshake frame BELOW the
    // adapters, and the managed adapter's process has already spent its
    // one handshake.
    match tokio::time::timeout(
        CLAUSE_TIMEOUT,
        wire::attach_for(&requirement, Role::Destination, &target.config),
    )
    .await
    {
        Ok(Ok(mut probe)) => {
            wire::certify_destination_wire(&mut report, &mut probe, &requirement.id).await;
        }
        Ok(Err(why)) => {
            for clause in wire::DEST_WIRE_CLAUSES {
                report.fail(clause, why.clone());
            }
        }
        Err(_elapsed) => {
            for clause in wire::DEST_WIRE_CLAUSES {
                report.fail(clause, timed_out());
            }
        }
    }

    // D-reuse — the testkit's destination conformance suite, verbatim,
    // against the (settling) managed adapter: the wire is certified by
    // the SAME clauses an in-process connector answers to. D8 is
    // asserted only when the connector declares merge — otherwise the
    // suite never ran it, and the honest verdict is a Skip.
    match probe {
        Some(probe) => {
            let merge = managed.capabilities().merge;
            match tokio::time::timeout(CLAUSE_TIMEOUT, verify_destination(&managed, probe)).await {
                Ok(failures) => {
                    if merge {
                        report.absorb(failures, &DEST_CLAUSES);
                    } else {
                        report.absorb(failures, &["D1", "D2", "D3", "D4", "D5", "D6"]);
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

    // P8 and P9 need the LIVE socket — the provider's guard carries it.
    match managed
        .inner
        .guard()
        .map(|guard| guard.socket_path().to_path_buf())
    {
        None => {
            let why = "the provider returned no process guard, so the live socket cannot be \
                       re-dialed"
                .to_string();
            report.skip("P8", why.clone());
            report.skip("P9", why);
        }
        Some(socket) => {
            // P8 — the one-session ceiling: with a session held, a
            // second `OpenSession` on a SECOND dial of the same socket
            // must refuse `FailedPrecondition` (the 038 frozen class).
            match tokio::time::timeout(CLAUSE_TIMEOUT, probe_one_session_ceiling(&socket)).await {
                Ok(Ok(())) => report.pass("P8"),
                Ok(Err(why)) => report.fail("P8", why),
                Err(_elapsed) => report.fail("P8", timed_out()),
            }

            // P9 — close-on-abandonment: a session dropped without
            // `Close` must be reclaimed within the window.
            match tokio::time::timeout(CLAUSE_TIMEOUT, probe_abandonment_reclaim(&socket)).await {
                Ok(Ok(())) => report.pass("P9"),
                Ok(Err(why)) => report.fail("P9", why),
                Err(_elapsed) => report.fail("P9", timed_out()),
            }
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
async fn probe_one_session_ceiling(socket: &Path) -> Result<(), String> {
    let session = settle_open_wire(socket, "rdlt-certify-p8", "certify-p8")
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
async fn probe_abandonment_reclaim(socket: &Path) -> Result<(), String> {
    let abandoned = settle_open_wire(socket, "rdlt-certify-p9", "certify-p9-abandoned")
        .await
        .map_err(|why| format!("could not open the session to abandon: {why}"))?;
    drop(abandoned);

    match settle_open_wire(socket, "rdlt-certify-p9", "certify-p9-fresh").await {
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

#[cfg(test)]
mod tests {
    //! The P8/P9 rogue suite (the T3 carry — both Fail arms were
    //! code-present but untested): each designated rogue proves its
    //! clause fails with the pinned evidence. In-process over UDS —
    //! no spawn, no built bin — so both ride the bare (ungated) suite,
    //! driving the probe functions directly (the exact strings
    //! `certify_destination` folds into the report's Fail entries).

    use super::*;
    use crate::rogue::{self, SessionDiscipline};

    /// P8's designated rogue: a destination that ACCEPTS a second
    /// concurrent session fails the clause with the pinned evidence.
    #[tokio::test]
    async fn a_destination_accepting_a_second_session_fails_p8() {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("rogue.sock");
        let _serving = rogue::serve_destination(&socket, SessionDiscipline::AcceptEverySession);

        let why = probe_one_session_ceiling(&socket)
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

        let why = probe_abandonment_reclaim(&socket)
            .await
            .expect_err("the slot was never reclaimed — P9 must fail");
        let pinned = "abandoned session was not reclaimed: a fresh session still refused 10s \
                      after the stream ended without Close: ";
        assert!(
            why.starts_with(pinned),
            "the evidence must carry the pinned prefix `{pinned}`, got: {why}"
        );
    }
}
