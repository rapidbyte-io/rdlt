//! The kill matrix — the K-vocabulary: SIGKILL the served connector at
//! every message boundary and hold the wire to two promises. First,
//! TYPED-ERROR-NOT-HANG: within [`KILL_ERROR_WINDOW`] of the kill the
//! client side must surface an error — a certifier (or an engine) that
//! outlives a dead connector by hanging has failed the connector's
//! author twice. Second, for the destination arms, EXACTLY-ONCE
//! CONVERGENCE proven by re-run: a FRESH spawn drives the same load to
//! completion and the read-back probe must count the fixture rows
//! EXACTLY — a kill that lost a committed row and a kill that let one
//! land twice both break the count.
//!
//! The boundaries are fixed at birth: K-S1 post-handshake pre-Read,
//! K-S2 mid-Read after frame 1, K-S3 after the first checkpoint frame;
//! K-D1 post-Open, K-D2 post-ensure, K-D3 after a write is accepted,
//! K-D4 post-write pre-publish (the receipt query answered, the publish
//! never sent), K-D5 post-publish pre-read_state, K-D6 post-close —
//! where convergence must be a NO-OP: the receipt outlives the process,
//! the rerun replays, and the count does not move.
//!
//! THE CONVERGENCE PIPELINE SCOPE, decided not defaulted: the arms
//! killed while their session was live (K-D1..K-D5) re-run under a
//! sibling pipeline (`{pipeline}-r`), not the killed one. A destination
//! may hold a durable per-pipeline session claim — the file
//! destination's lease is the recorded case (037): a SIGKILLed holder
//! cannot release it, dead-process takeover was considered and REJECTED
//! by the owner, and the claim stands until a 300 s TTL no clause
//! budget can pay. Convergence is therefore judged on the DATA (the
//! probe's exact count over the shared table), which the sibling scope
//! reaches without waiting out a claim the connector is CORRECT to
//! enforce. K-D6 killed a process whose session had already closed —
//! its claim released — so its rerun rides the SAME pipeline and
//! additionally proves the receipt was durable across processes.
//!
//! The source arms dial with a deliberately SMALL h2 window budget
//! ([`READ_BUDGET_BYTES`]): flow control then caps how much of the
//! stream can be in flight, so against a fixture larger than the window
//! the server is provably still mid-stream when the kill lands — with
//! the frame-ceiling window a small stream could be fully buffered
//! (end-of-stream included) before the SIGKILL, and the client would
//! observe a clean end instead of the failure under test. A stream that
//! still completes cleanly (or ends before its boundary) earns an
//! honest Skip naming the fixture, never a vacuous Pass.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rdlt_connector::core::TableName;
use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_runtime::Role;
use rdlt_testkit::conformance::destination::TableProbe;

use crate::destination::NO_PROBE_SKIP;
use crate::report::{CLAUSE_TIMEOUT, Entry, Report, timed_out};
use crate::target::{Target, resolve_binary};
use crate::wire::{
    FIXTURE_IDS, RawFrame, WireProbe, WireSession, decode_read_frame, ensure_request, expect,
    meta_json_for, open_wire_session, publish_request, read_state_request, receipt_reply,
    replay_request, write_request,
};

/// How long the wire gets to surface a typed error after the SIGKILL
/// before the arm fails as a hang.
const KILL_ERROR_WINDOW: Duration = Duration::from_secs(10);

/// The source arms' dial budget: h2's window floor, so the read stream
/// cannot be fully in flight before a mid-stream kill (module doc).
const READ_BUDGET_BYTES: u64 = 64 * 1024;

/// The one `commit_seq` every destination arm drives.
const KILL_SEQ: u64 = 1;

/// The destination K-clauses in matrix order — also the Skip set when
/// no probe was supplied.
const DEST_KILL_CLAUSES: [&str; 6] = ["K-D1", "K-D2", "K-D3", "K-D4", "K-D5", "K-D6"];

/// The one spelling a post-kill hang fails with.
fn no_error() -> String {
    format!(
        "no typed error surfaced within {}s of SIGKILL — a dead connector must fail the \
         wire, never hang it",
        KILL_ERROR_WINDOW.as_secs()
    )
}

/// An arm that ran to a verdict: the clause held, or it could not be
/// exercised (with the reason). Violations travel as the `Err` string.
enum Outcome {
    Pass,
    Skip(String),
}

/// Fold one timeout-bounded arm outcome into the report.
fn record(
    report: &mut Report,
    clause: &'static str,
    outcome: Result<Result<Outcome, String>, tokio::time::error::Elapsed>,
) {
    match outcome {
        Ok(Ok(Outcome::Pass)) => report.pass(clause),
        Ok(Ok(Outcome::Skip(why))) => report.skip(clause, why),
        Ok(Err(why)) => report.fail(clause, why),
        Err(_elapsed) => report.fail(clause, timed_out()),
    }
}

/// Where a source arm kills.
#[derive(Clone, Copy)]
enum SourceBoundary {
    /// K-S1 — the handshake answered, no Read opened.
    PostHandshake,
    /// K-S2 — mid-Read, after frame 1.
    AfterFirstFrame,
    /// K-S3 — after the first checkpoint frame.
    AfterFirstCheckpoint,
}

/// The source half of the kill matrix: one spawned connector per
/// boundary, killed there, the wire held to typed-error-not-hang.
/// Entries come back in K order, every clause present.
pub async fn kill_matrix_source(target: &Target) -> Vec<Entry> {
    let mut report = Report::default();
    for (clause, boundary) in [
        ("K-S1", SourceBoundary::PostHandshake),
        ("K-S2", SourceBoundary::AfterFirstFrame),
        ("K-S3", SourceBoundary::AfterFirstCheckpoint),
    ] {
        let outcome = tokio::time::timeout(CLAUSE_TIMEOUT, source_arm(target, boundary)).await;
        record(&mut report, clause, outcome);
    }
    report.entries
}

/// One source arm: spawn, drive to the boundary, SIGKILL, observe.
async fn source_arm(target: &Target, boundary: SourceBoundary) -> Result<Outcome, String> {
    let bin = resolve_binary(&target.requirement)?;
    let mut probe = WireProbe::attach(&bin, Role::Source, &target.config, READ_BUDGET_BYTES)
        .await
        .map_err(|why| format!("could not spawn the connector: {why}"))?;
    probe
        .handshake_raw()
        .await
        .map_err(|why| format!("could not drive to the boundary: {why}"))?;

    if matches!(boundary, SourceBoundary::PostHandshake) {
        probe.kill().await;
        // The post-kill observation: the next RPC must fail typed.
        return match tokio::time::timeout(KILL_ERROR_WINDOW, probe.streams_raw()).await {
            Ok(Err(_typed)) => Ok(Outcome::Pass),
            Ok(Ok(_streams)) => Err(
                "the Streams RPC still answered after the connector process was SIGKILLed"
                    .to_string(),
            ),
            Err(_elapsed) => Err(no_error()),
        };
    }
    let want_checkpoint = matches!(boundary, SourceBoundary::AfterFirstCheckpoint);

    let streams = probe
        .streams_raw()
        .await
        .map_err(|why| format!("could not drive to the boundary: {why}"))?;
    let Some(spec_json) = streams.into_iter().next() else {
        return Ok(Outcome::Skip(
            "the source declares no streams — no read boundary exists to kill at".to_string(),
        ));
    };
    let mut stream = probe
        .open_read(spec_json)
        .await
        .map_err(|why| format!("could not drive to the boundary: {why}"))?;

    // Pull frames until the boundary frame arrives.
    loop {
        match stream.message().await {
            Ok(Some(frame)) => match decode_read_frame(frame) {
                RawFrame::Error(error) => {
                    return Err(format!(
                        "the read failed before the boundary: {}",
                        error.message
                    ));
                }
                RawFrame::Checkpoint => break,
                _other if !want_checkpoint => break,
                _other => continue,
            },
            Ok(None) => {
                return Ok(Outcome::Skip(
                    "the stream ended before the boundary was reached — the fixture is too \
                     small to stage this kill"
                        .to_string(),
                ));
            }
            Err(status) => {
                return Err(format!(
                    "the read stream failed before the boundary: {status}"
                ));
            }
        }
    }

    probe.kill().await;

    // Drain whatever the wire had already buffered; the terminal
    // outcome must be a typed error within the window.
    let deadline = tokio::time::Instant::now() + KILL_ERROR_WINDOW;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, stream.message()).await {
            Ok(Err(_typed_status)) => return Ok(Outcome::Pass),
            Ok(Ok(Some(_buffered_frame))) => continue,
            Ok(Ok(None)) => {
                return Ok(Outcome::Skip(
                    "the stream completed cleanly despite the kill — every frame was already \
                     in flight, so no mid-stream failure was observable; enlarge the fixture \
                     past the read window"
                        .to_string(),
                ));
            }
            Err(_elapsed) => return Err(no_error()),
        }
    }
}

/// Where a destination arm kills. Ordered: each boundary drives every
/// step the ones before it drove.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DestBoundary {
    /// K-D1 — `opened` answered, nothing else sent.
    PostOpen,
    /// K-D2 — `ensured` answered.
    PostEnsure,
    /// K-D3 — `written` answered.
    PostWrite,
    /// K-D4 — the `existing_receipt` query answered, the publish never
    /// sent: poised at the commit point.
    PrePublish,
    /// K-D5 — `published` answered, `read_state` never sent.
    PostPublish,
    /// K-D6 — `closed` answered and the reply stream ended.
    PostClose,
}

/// One destination arm's identities — its own pipeline, load and table
/// so no arm's commits, claims or rows can mask another's.
struct ArmIdentity {
    pipeline: String,
    load: String,
    table: String,
}

impl ArmIdentity {
    fn for_clause(clause: &str) -> Self {
        let slug = clause.to_lowercase();
        Self {
            pipeline: format!("rdlt-certify-{slug}"),
            load: format!("certify-{slug}"),
            table: slug.replace('-', "_"),
        }
    }
}

/// The destination half of the kill matrix: one spawned connector per
/// boundary, killed there; typed-error-not-hang on the dead wire, then
/// a FRESH spawn re-runs the load and the probe's count must be exact.
/// Without a probe every arm Skips with the read-back reason — never
/// silently narrowed, never vacuously passed.
pub async fn kill_matrix_destination(
    target: &Target,
    probe: Option<&dyn TableProbe>,
) -> Vec<Entry> {
    let mut report = Report::default();
    let Some(probe) = probe else {
        for clause in DEST_KILL_CLAUSES {
            report.skip(clause, NO_PROBE_SKIP.to_string());
        }
        return report.entries;
    };
    for (clause, boundary) in [
        ("K-D1", DestBoundary::PostOpen),
        ("K-D2", DestBoundary::PostEnsure),
        ("K-D3", DestBoundary::PostWrite),
        ("K-D4", DestBoundary::PrePublish),
        ("K-D5", DestBoundary::PostPublish),
        ("K-D6", DestBoundary::PostClose),
    ] {
        let outcome = tokio::time::timeout(
            CLAUSE_TIMEOUT,
            destination_arm(target, probe, clause, boundary),
        )
        .await;
        record(&mut report, clause, outcome);
    }
    report.entries
}

/// One destination arm: spawn, drive the session to the boundary,
/// SIGKILL, observe the dead wire, then converge and count.
async fn destination_arm(
    target: &Target,
    probe: &dyn TableProbe,
    clause: &str,
    boundary: DestBoundary,
) -> Result<Outcome, String> {
    let identity = ArmIdentity::for_clause(clause);
    let bin = resolve_binary(&target.requirement)?;
    let (mut wire, socket) = spawn_destination(&bin, target).await?;

    let mut session = open_wire_session(&socket, &identity.pipeline, &identity.load)
        .await
        .map_err(|error| {
            format!(
                "could not open the session to kill: {}",
                render_open_error(error)
            )
        })?;
    drive_session(&mut session, &identity, boundary)
        .await
        .map_err(|why| format!("could not drive to the {clause} boundary: {why}"))?;
    let session = if boundary == DestBoundary::PostClose {
        session
            .close_judged()
            .await
            .map_err(|why| format!("could not drive to the {clause} boundary: {why}"))?;
        None
    } else {
        Some(session)
    };

    wire.kill().await;

    // Typed-error-not-hang on the dead wire: a live session's next
    // request must fail; after a closed session (K-D6) the dead socket
    // must refuse a fresh dial.
    match session {
        Some(mut session) => {
            let request = session.request(read_state_request(&identity.pipeline));
            match tokio::time::timeout(KILL_ERROR_WINDOW, request).await {
                Ok(Err(_typed)) => {}
                Ok(Ok(reply)) => {
                    return Err(format!(
                        "the session still answered `{}` after the connector process was \
                         SIGKILLed",
                        reply.tag()
                    ));
                }
                Err(_elapsed) => return Err(no_error()),
            }
        }
        None => {
            let open = open_wire_session(&socket, &identity.pipeline, &identity.load);
            match tokio::time::timeout(KILL_ERROR_WINDOW, open).await {
                Ok(Err(_typed)) => {}
                Ok(Ok(_session)) => {
                    return Err(
                        "a session opened on the socket after the connector process was \
                         SIGKILLed"
                            .to_string(),
                    );
                }
                Err(_elapsed) => return Err(no_error()),
            }
        }
    }

    converge(target, probe, &bin, &identity, boundary).await
}

/// Spawn the target's binary as a destination and handshake it; hand
/// back the probe (whose drop reaps the child) and its live socket.
async fn spawn_destination(bin: &Path, target: &Target) -> Result<(WireProbe, PathBuf), String> {
    let mut wire = WireProbe::attach(
        bin,
        Role::Destination,
        &target.config,
        MAX_FRAME_BYTES as u64,
    )
    .await
    .map_err(|why| format!("could not spawn the connector: {why}"))?;
    wire.handshake_raw()
        .await
        .map_err(|why| format!("the handshake failed: {why}"))?;
    let socket = wire
        .socket()
        .expect("attach spawned this probe, so it carries the socket")
        .to_path_buf();
    Ok((wire, socket))
}

/// Drive an open session up to (and including) `boundary`'s last
/// answered frame — except `close`, which consumes the session and is
/// the caller's move.
async fn drive_session(
    session: &mut WireSession,
    identity: &ArmIdentity,
    boundary: DestBoundary,
) -> Result<(), String> {
    if boundary >= DestBoundary::PostEnsure {
        expect(
            session.request(ensure_request(&identity.table)).await?,
            "ensure",
            "ensured",
        )?;
    }
    if boundary >= DestBoundary::PostWrite {
        expect(
            session.request(write_request(&identity.table)).await?,
            "write",
            "written",
        )?;
    }
    if boundary >= DestBoundary::PrePublish {
        let receipt = receipt_reply(session, &identity.load, KILL_SEQ).await?;
        if boundary >= DestBoundary::PostPublish {
            let meta = meta_json_for(&identity.pipeline, &identity.load, KILL_SEQ);
            match receipt {
                None => expect(
                    session.request(publish_request(&meta)).await?,
                    "publish",
                    "published",
                )?,
                Some(receipt) => expect(
                    session.request(replay_request(&meta, receipt)).await?,
                    "replay",
                    "replayed",
                )?,
            }
        }
    }
    if boundary >= DestBoundary::PostClose {
        expect(
            session
                .request(read_state_request(&identity.pipeline))
                .await?,
            "read_state",
            "state",
        )?;
    }
    Ok(())
}

/// The convergence re-run: a FRESH spawn drives the arm's load to
/// completion and the probe must count the fixture rows exactly. The
/// K-D1..K-D5 arms ride the sibling pipeline scope; K-D6 rides the SAME
/// pipeline and must find the killed process's receipt still durable —
/// its rerun replays, reads 0 new, and the count not moving is the
/// kill-can-fail proof (a kill that duplicated rows would break it).
/// See the module doc for why the scopes differ.
async fn converge(
    target: &Target,
    probe: &dyn TableProbe,
    bin: &Path,
    identity: &ArmIdentity,
    boundary: DestBoundary,
) -> Result<Outcome, String> {
    let (_wire, socket) = spawn_destination(bin, target)
        .await
        .map_err(|why| format!("the convergence run failed: {why}"))?;
    let no_op = boundary == DestBoundary::PostClose;
    let pipeline = if no_op {
        identity.pipeline.clone()
    } else {
        format!("{}-r", identity.pipeline)
    };
    converge_session(&socket, &pipeline, identity, no_op)
        .await
        .map_err(|why| format!("the convergence run failed: {why}"))?;

    let expected = FIXTURE_IDS.len() as u64;
    let found = probe.count(&TableName::new(&identity.table)).await;
    if found != expected {
        return Err(format!(
            "convergence failed: table `{}` holds {found} rows where the kill matrix wrote \
             {expected} — the re-run lost or duplicated rows",
            identity.table
        ));
    }
    Ok(Outcome::Pass)
}

/// The convergence run's one session: replay the durable receipt
/// (K-D6's no-op arm, where its absence is the violation) or drive the
/// load to a publish-or-replay completion, then a judged close.
async fn converge_session(
    socket: &Path,
    pipeline: &str,
    identity: &ArmIdentity,
    no_op: bool,
) -> Result<(), String> {
    let mut session = open_wire_session(socket, pipeline, &identity.load)
        .await
        .map_err(render_open_error)?;
    let meta = meta_json_for(pipeline, &identity.load, KILL_SEQ);
    if no_op {
        let Some(receipt) = receipt_reply(&mut session, &identity.load, KILL_SEQ).await? else {
            return Err(
                "no receipt was found for a load the killed process had cleanly closed — the \
                 receipt must be durable across processes"
                    .to_string(),
            );
        };
        expect(
            session.request(replay_request(&meta, receipt)).await?,
            "replay",
            "replayed",
        )?;
    } else {
        expect(
            session.request(ensure_request(&identity.table)).await?,
            "ensure",
            "ensured",
        )?;
        expect(
            session.request(write_request(&identity.table)).await?,
            "write",
            "written",
        )?;
        match receipt_reply(&mut session, &identity.load, KILL_SEQ).await? {
            None => expect(
                session.request(publish_request(&meta)).await?,
                "publish",
                "published",
            )?,
            Some(receipt) => expect(
                session.request(replay_request(&meta, receipt)).await?,
                "replay",
                "replayed",
            )?,
        }
    }
    session.close_judged().await
}

/// Render a raw-session open failure for an evidence line.
fn render_open_error(error: crate::wire::WireOpenError) -> String {
    match error {
        crate::wire::WireOpenError::Ceiling(status) => {
            format!("the session was refused at the one-session ceiling: {status}")
        }
        crate::wire::WireOpenError::Other(why) => why,
    }
}
