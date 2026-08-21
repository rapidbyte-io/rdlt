//! The protocol clauses P1–P13, top to bottom in clause-id order —
//! every P verdict the certifier writes is minted in this module.
//!
//! Three kinds of probe: the role-generic clauses (P1 the handshake
//! line, P2 typed config refusal, P4 the pre-handshake Spec, P13 the
//! unserved role's refusal) ride direct spawns and the provider; the
//! wire clauses (P3 identity, P7 the state-format map, P5 one-batch,
//! P6 error-frame shape) are judged on the RAW frames a served
//! connector speaks, below the client adapters whose good manners would
//! otherwise stand between the certifier and a misbehaving server; the
//! session clauses (P8 the one-session ceiling, P9 abandonment reclaim,
//! P10 the order book, P11 one batch per write frame, P12 write-side
//! error text) drive raw sessions on their own dials of the live
//! destination socket. Every probe rides under the 30 s clause timeout:
//! a stalling connector FAILS its clause and the certifier never hangs.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use rdlt_connector::source::StreamSpec;
use rdlt_connector::spec::ConnectorSpec;
use rdlt_connector_client::error::Error as ClientError;
use rdlt_connector_client::handshake::{Requirement, Role};
use rdlt_connector_client::wire::{DEFAULT_DEADLINE, destination_client, dial};
use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::{self, SessionRequest};
use rdlt_runtime::provider;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use crate::report::{self, Report};
use crate::target::{self, Target};
use crate::wire::{self, RawFrame, WireProbe, WireReply, WireSession};

/// The role-generic clauses: P1 and P13 self-probed on direct spawns,
/// P2 and P4 through the provider. Both certifiers build their
/// dead-handshake cascade sets from this list with [`SELF_PROBED`]
/// filtered out.
pub const GENERIC: [&str; 4] = ["P1", "P13", "P2", "P4"];

/// The generic clauses whose probes write their own entries BEFORE any
/// cascade point — a cascade set that kept them would write a second
/// verdict per clause.
pub const SELF_PROBED: [&str; 2] = ["P1", "P13"];

/// The wire clauses a SOURCE certification judges, in report order —
/// also the cascade set when the probe cannot attach or handshake.
pub const SOURCE_WIRE: [&str; 4] = ["P3", "P7", "P5", "P6"];

/// The DESTINATION's wire clauses: the handshake-borne pair alone —
/// P5/P6 are read-direction clauses, and the write direction's own
/// wire clauses are the session clauses below.
pub const DEST_WIRE: [&str; 2] = ["P3", "P7"];

/// The clauses that exist only out of process, probed on raw sessions
/// over the live socket, in report order.
pub const SESSION: [&str; 5] = ["P8", "P9", "P10", "P11", "P12"];

// ——— P1: one handshake line on stdout.

/// P1 — the handshake-line discipline, probed on a direct spawn whose
/// only purpose is P1; certification re-spawns cleanly afterward. The
/// clause timeout rides INSIDE the probe so its reap runs even on a
/// timeout.
pub(crate) async fn report_p1(report: &mut Report, target: &Target, role: Role) {
    match probe_handshake_line(target, role, report::CLAUSE_TIMEOUT).await {
        Ok(()) => report.pass("P1"),
        Err(why) => report.fail("P1", why),
    }
}

/// How long the P1 probe listens for a SECOND stdout line after the
/// handshake line. Stdout is the machine channel and carries EXACTLY one
/// line; anything more within this window is a P1 violation.
const SECOND_LINE_WINDOW: Duration = Duration::from_millis(500);

/// The P1 probe: spawn the binary directly under `role`, read the FIRST
/// stdout line, parse it as a handshake line, then listen
/// [`SECOND_LINE_WINDOW`] for any further stdout byte — one is a
/// violation (stdout is the machine channel; logs belong on stderr).
/// `budget` (the caller's clause timeout) wraps the probe I/O alone
/// and [`wire::reap_parked`] sits outside it, so on every exit, timeout
/// included, the child is dead AND reaped and any socket its line
/// advertised is unlinked — the P1 child is a live server holding its
/// store while the P2 spawn follows immediately.
async fn probe_handshake_line(target: &Target, role: Role, budget: Duration) -> Result<(), String> {
    let path = target::resolve_binary(&target.requirement)?;
    let slot = wire::ChildSlot::default();
    let verdict =
        match tokio::time::timeout(budget, first_line_discipline(&path, role, &slot)).await {
            Ok(verdict) => verdict,
            Err(_elapsed) => Err(report::timed_out()),
        };
    wire::reap_parked(&slot).await;
    verdict
}

/// The P1 judgments proper, over the shared funnel; the caller owns
/// the one reap on the way out (a helper error path has already
/// reaped — [`wire::reap_parked`] is idempotent).
async fn first_line_discipline(
    path: &Path,
    role: Role,
    slot: &wire::ChildSlot,
) -> Result<(), String> {
    let (mut reader, line) = wire::spawn_and_read_line(path, role, slot).await?;
    if !line.ends_with('\n') && line.len() as u64 >= target::MAX_LINE_BYTES {
        return Err(format!(
            "wrote {} bytes of stdout without completing a handshake line",
            target::MAX_LINE_BYTES
        ));
    }
    let parsed = Line::parse(line.trim_end_matches(['\n', '\r']))
        .map_err(|error| format!("the first stdout line is not a handshake line: {error}"))?;
    // The advertised socket joins the parked state the moment it is
    // KNOWN, so the caller's reap unlinks it too.
    slot.lock()
        .expect("child slot lock")
        .park_socket(parsed.socket_path);

    // The second-line poll: silence (or EOF — nothing more CAN be
    // spoken) passes; any byte fails.
    let mut byte = [0u8; 1];
    match tokio::time::timeout(SECOND_LINE_WINDOW, reader.read(&mut byte)).await {
        Err(/* window elapsed in silence */ _) | Ok(Ok(0)) => Ok(()),
        Ok(Ok(_more)) => Err(
            "stdout spoke after the handshake line — stdout is the machine channel and \
             carries EXACTLY one line; logs belong on stderr"
                .to_string(),
        ),
        Ok(Err(error)) => Err(format!("reading stdout after the handshake line: {error}")),
    }
}

// ——— P2: typed unknown-config-field refusal.

/// Judge P2 from a bogus-config spawn's outcome: an unknown field must
/// come back as a typed handshake refusal (classification carried),
/// never as a dial failure, a dead stream, or an acceptance. Generic
/// over the managed type because the SPI half is the only thing the
/// two certifications' probes differ in.
pub(crate) fn report_p2<T>(
    report: &mut Report,
    outcome: Result<Result<T, provider::Error>, tokio::time::error::Elapsed>,
) {
    match outcome {
        Ok(Ok(_accepted)) => report.fail(
            "P2",
            "the connector accepted a config document consisting of one unknown field — \
             the config gate must refuse unknown fields with a typed handshake refusal"
                .to_string(),
        ),
        Ok(Err(provider::Error::Client(ClientError::Handshake { .. }))) => report.pass("P2"),
        Ok(Err(error)) => report.fail(
            "P2",
            format!(
                "an unknown config field must be refused with a typed handshake refusal — \
                 the connector instead produced: {error}"
            ),
        ),
        Err(_elapsed) => report.fail("P2", report::timed_out()),
    }
}

// ——— The wire clauses P3/P7 (both roles) and P5/P6 (source), on one
// raw spawn of the target.

/// The wire clauses on their OWN spawn of the target: the probe
/// watches actual frames BELOW the adapters, and the managed adapter's
/// process has already spent its one handshake. The child parks in a
/// shared slot so the timeout arm can claim and REAP it — a timed-out
/// future only drops, and `kill_on_drop` only SENDS the signal. An
/// attach that fails or stalls fails every wire clause of `role` with
/// the one cause.
pub(crate) async fn wire_clauses(
    report: &mut Report,
    requirement: &Requirement,
    role: Role,
    config: &Value,
) {
    let clauses: &[&'static str] = match role {
        Role::Source => &SOURCE_WIRE,
        Role::Destination => &DEST_WIRE,
    };
    let slot = wire::ChildSlot::default();
    match tokio::time::timeout(
        report::CLAUSE_TIMEOUT,
        wire::attach_for(requirement, role, config, &slot),
    )
    .await
    {
        Ok(Ok(mut probe)) => {
            match role {
                Role::Source => source_wire(report, &mut probe, &requirement.id).await,
                Role::Destination => destination_wire(report, &mut probe, &requirement.id).await,
            }
            probe.kill().await;
        }
        Ok(Err(why)) => {
            for clause in clauses {
                report.fail(clause, why.clone());
            }
        }
        Err(_elapsed) => {
            wire::reap_parked(&slot).await;
            for clause in clauses {
                report.fail(clause, report::timed_out());
            }
        }
    }
}

/// The source wire clauses over one attached probe, in [`SOURCE_WIRE`]
/// order: P3/P7 from one raw handshake, P5 over every declared stream's
/// frames, P6 on an induced refusal. A probe whose handshake fails
/// fails ALL of them with the one cause.
pub(crate) async fn source_wire(report: &mut Report, probe: &mut WireProbe, required_id: &str) {
    let Some(ok) = raw_handshake_or_cascade(report, probe, &SOURCE_WIRE).await else {
        return;
    };
    report_p3(report, &ok, required_id);
    report_p7(report, &ok);
    report_p5(report, probe).await;
    report_p6(report, probe).await;
}

/// The destination wire clauses: the handshake-borne P3/P7 alone
/// ([`DEST_WIRE`]).
pub(crate) async fn destination_wire(
    report: &mut Report,
    probe: &mut WireProbe,
    required_id: &str,
) {
    let Some(ok) = raw_handshake_or_cascade(report, probe, &DEST_WIRE).await else {
        return;
    };
    report_p3(report, &ok, required_id);
    report_p7(report, &ok);
}

/// One raw handshake under the clause budget; a refusal, a transport
/// failure, or a stall fails EVERY clause in `clauses` with the one
/// cause — the cascade a dead handshake earns.
async fn raw_handshake_or_cascade(
    report: &mut Report,
    probe: &mut WireProbe,
    clauses: &[&'static str],
) -> Option<proto::HandshakeOk> {
    match tokio::time::timeout(report::CLAUSE_TIMEOUT, probe.handshake_raw()).await {
        Ok(Ok(ok)) => Some(ok),
        Ok(Err(why)) => {
            for clause in clauses {
                report.fail(clause, why.clone());
            }
            None
        }
        Err(_elapsed) => {
            for clause in clauses {
                report.fail(clause, report::timed_out());
            }
            None
        }
    }
}

// ——— P3: spec/handshake identity agreement.

/// P3 — identity/skew: the handshake's VALUES must agree with
/// themselves (`spec_json` vs the wire's reported identity — the skew
/// case) and, when the target names an id, with that requirement.
/// Values, never spellings: an in-process rogue's own name is as
/// legitimate an identity as `io.rapidbyte.*`.
fn report_p3(report: &mut Report, ok: &proto::HandshakeOk, required_id: &str) {
    let mut problems = Vec::new();
    match serde_json::from_slice::<ConnectorSpec>(&ok.spec_json) {
        Ok(spec) => {
            if spec.name != ok.connector_id {
                problems.push(format!(
                    "spec_json names `{}` but the wire reported connector_id `{}`",
                    spec.name, ok.connector_id
                ));
            }
            if spec.version != ok.connector_version {
                problems.push(format!(
                    "spec_json carries version `{}` but the wire reported connector_version `{}`",
                    spec.version, ok.connector_version
                ));
            }
        }
        Err(error) => problems.push(format!(
            "spec_json does not decode as a ConnectorSpec: {error}"
        )),
    }
    if !required_id.is_empty() && ok.connector_id != required_id {
        problems.push(format!(
            "the wire reported connector_id `{}` but the target requires `{required_id}`",
            ok.connector_id
        ));
    }
    if problems.is_empty() {
        report.pass("P3");
    } else {
        report.fail(
            "P3",
            format!("the handshake identity is skewed: {}", problems.join("; ")),
        );
    }
}

// ——— P4: complete pre-handshake Spec reply.

/// Judge P4 from the Spec reply: name/version non-empty and a JSON
/// -object config schema, answered with no config at all.
pub(crate) fn report_p4(report: &mut Report, spec: &Result<ConnectorSpec, String>) {
    match spec {
        Ok(spec) => {
            let mut problems = Vec::new();
            if spec.name.is_empty() {
                problems.push("`name` is empty".to_string());
            }
            if spec.version.is_empty() {
                problems.push("`version` is empty".to_string());
            }
            match &spec.config_schema {
                Some(schema) if schema.is_object() => {}
                Some(_) => problems.push("`config_schema` is not a JSON object".to_string()),
                None => problems.push(
                    "`config_schema` is absent — the Spec reply must describe the config"
                        .to_string(),
                ),
            }
            if problems.is_empty() {
                report.pass("P4");
            } else {
                report.fail(
                    "P4",
                    format!("the Spec reply is incomplete: {}", problems.join("; ")),
                );
            }
        }
        Err(why) => report.fail("P4", format!("the Spec RPC did not answer: {why}")),
    }
}

// ——— P5: one Arrow batch per read frame.

/// Keep P5 evidence useful without letting a fast rogue turn one
/// bounded read probe into millions of retained report strings.
const MAX_P5_VIOLATIONS: usize = 100;

/// P5 — the one-batch rule, judged on the wire bytes: every
/// `arrow_ipc` read frame across every DECLARED stream must decode as
/// an Arrow IPC stream carrying exactly one record batch. Frames that
/// are not arrow are exempt (the rule is per-frame, not per-source);
/// a source that serves no arrow frames at all passes vacuously — the
/// clause still ran, and a violation's evidence carries the full frame
/// census so the observation is auditable either way.
async fn report_p5(report: &mut Report, probe: &mut WireProbe) {
    match tokio::time::timeout(report::CLAUSE_TIMEOUT, p5_violations(probe)).await {
        Ok(Ok(violations)) if violations.is_empty() => report.pass("P5"),
        Ok(Ok(violations)) => {
            for violation in violations {
                report.fail("P5", violation);
            }
        }
        Ok(Err(why)) => report.fail("P5", why),
        Err(_elapsed) => report.fail("P5", report::timed_out()),
    }
}

/// Walk every declared stream's frames and collect one-batch
/// violations, each suffixed with the complete frame census.
async fn p5_violations(probe: &mut WireProbe) -> Result<Vec<String>, String> {
    let streams = probe.streams_raw().await?;
    let mut census = wire::Census::default();
    let mut violations = Vec::new();
    let mut omitted = 0usize;
    for spec_json in streams {
        // The stream's own name, for the evidence line — undecodable
        // spec bytes still get read (the connector declared them).
        let name = serde_json::from_slice::<StreamSpec>(&spec_json)
            .map(|spec| spec.name.to_string())
            .unwrap_or_else(|_| "<undecodable stream spec>".to_string());
        for frame in probe.read_frames(spec_json).await? {
            census.record(&frame);
            if let RawFrame::Arrow(bytes) = &frame {
                match wire::count_batches(bytes) {
                    Ok(1) => {}
                    Ok(count) => retain_p5_violation(&mut violations, &mut omitted, || {
                        format!(
                            "an arrow read frame carried {count} record batches — the one-batch \
                             rule requires exactly one (stream `{name}`)"
                        )
                    }),
                    Err(error) => retain_p5_violation(&mut violations, &mut omitted, || {
                        format!(
                            "an arrow read frame does not decode as one Arrow IPC stream \
                             (stream `{name}`): {error}"
                        )
                    }),
                }
            }
        }
    }
    let mut violations: Vec<String> = violations
        .into_iter()
        .map(|violation| format!("{violation}; frame census: {census}"))
        .collect();
    if omitted != 0 {
        violations.push(format!(
            "and {omitted} more one-batch violations were omitted after the first \
             {MAX_P5_VIOLATIONS}; frame census: {census}"
        ));
    }
    Ok(violations)
}

fn retain_p5_violation(
    violations: &mut Vec<String>,
    omitted: &mut usize,
    message: impl FnOnce() -> String,
) {
    if violations.len() < MAX_P5_VIOLATIONS {
        violations.push(message());
    } else {
        *omitted = omitted.saturating_add(1);
    }
}

// ——— P6: typed terminal error frame.

/// The stream name P6 reads to induce a refusal: reserved by spelling —
/// no real connector stream may collide with it.
const P6_BOGUS_STREAM: &str = "__rdlt_certify_no_such_stream__";

/// P6 — error-frame shape, on an induced refusal: reading
/// [`P6_BOGUS_STREAM`] must produce a TERMINAL `ErrorFrame` whose
/// classification is a real enum value and whose message carries the
/// bare cause text — never one of the client renderings
/// ([`wire::refusal_shape`]).
async fn report_p6(report: &mut Report, probe: &mut WireProbe) {
    match tokio::time::timeout(report::CLAUSE_TIMEOUT, p6_verdict(probe)).await {
        Ok(Ok(())) => report.pass("P6"),
        Ok(Err(why)) => report.fail("P6", why),
        Err(_elapsed) => report.fail("P6", report::timed_out()),
    }
}

/// The P6 judgment — `Ok(())` when the induced refusal arrived shaped
/// as the protocol demands.
async fn p6_verdict(probe: &mut WireProbe) -> Result<(), String> {
    let spec_json = serde_json::to_vec(&StreamSpec::new(P6_BOGUS_STREAM))
        .expect("a StreamSpec serializes to JSON infallibly");
    let frames = probe.read_frames(spec_json).await?;

    let mut found = None;
    for (position, frame) in frames.iter().enumerate() {
        if let RawFrame::Error(error) = frame {
            found = Some((position, error));
            break;
        }
    }
    let Some((position, frame)) = found else {
        return Err(
            "reading a nonexistent stream produced no terminal ErrorFrame — a refusal must \
             arrive as a typed error frame, never a clean end of stream"
                .to_string(),
        );
    };
    let trailing = frames.len() - position - 1;
    if trailing > 0 {
        return Err(format!(
            "the ErrorFrame was not terminal — {trailing} frame(s) followed it"
        ));
    }
    wire::refusal_shape(frame)
}

// ——— P7: tolerated state-format version map.

/// P7 — the state-format versions document: it must decode
/// as a `map<string, u32>`, which protobuf decoding already enforced
/// by the time a `HandshakeOk` exists (an undecodable field fails the
/// whole handshake and cascades). Empty passes — the
/// nothing-to-negotiate posture — and
/// a populated map ALSO passes: tolerated, threaded, never negotiated.
/// `Pass` carries no payload, so the tolerance evidence is pinned by
/// the populated-map rogue rather than rendered here.
fn report_p7(report: &mut Report, ok: &proto::HandshakeOk) {
    let _ = &ok.state_format_versions_json;
    report.pass("P7");
}

// ——— The session clauses P8–P12, on raw sessions over the live
// destination socket.

/// How long an abandoned session gets to be reclaimed before P9 fails —
/// and how long a settling `open` retries the one-session refusal for.
/// One window deliberately: both are the same question, "how quickly
/// does dropping a session free its slot".
pub(crate) const RECLAIM_WINDOW: Duration = Duration::from_secs(10);

/// The pause between reclaim polls.
pub(crate) const RECLAIM_POLL: Duration = Duration::from_millis(100);

/// The session clauses in [`SESSION`] order over the LIVE socket the
/// provider's guard carries — `None` when the provider returned no
/// guard, which skips every session clause with the reason. One
/// entropy suffix serves the whole invocation: the probes' publishes
/// mint DURABLE receipts in real warehouses, and a deterministic load
/// id would hand a re-certification the previous run's receipts.
pub(crate) async fn session(report: &mut Report, socket: Option<&Path>) {
    let Some(socket) = socket else {
        let why = "the provider returned no process guard, so the live socket cannot be \
                   re-dialed"
            .to_string();
        for clause in SESSION {
            report.skip(clause, why.clone());
        }
        return;
    };
    let entropy = target::mint_run_entropy();
    report_session_probe(report, "P8", probe_one_session_ceiling(socket, &entropy)).await;
    report_session_probe(report, "P9", probe_abandonment_reclaim(socket, &entropy)).await;
    report_p10(report, socket, &entropy).await;
    report_session_probe(report, "P11", probe_one_batch_write(socket, &entropy)).await;
    report_session_probe(report, "P12", probe_error_frame_text(socket, &entropy)).await;
}

/// Fold one session probe's outcome into its clause entry — the ONE
/// timeout/pass/fail match all five session clauses share.
async fn report_session_probe<F>(report: &mut Report, clause: &'static str, probe: F)
where
    F: std::future::Future<Output = Result<(), String>>,
{
    match tokio::time::timeout(report::CLAUSE_TIMEOUT, probe).await {
        Ok(Ok(())) => report.pass(clause),
        Ok(Err(why)) => report.fail(clause, why),
        Err(_elapsed) => report.fail(clause, report::timed_out()),
    }
}

/// Open a RAW session on the live socket, retrying exactly the
/// transport ceiling refusal within [`RECLAIM_WINDOW`] — release over
/// the wire is asynchronous, so the slot the previous session held may
/// free a beat after its stream ended. Exhausting the window surfaces
/// the refusal itself; any other failure surfaces immediately.
async fn settle_open_wire(
    socket: &Path,
    pipeline: &str,
    load_id: &str,
) -> Result<WireSession, String> {
    let deadline = tokio::time::Instant::now() + RECLAIM_WINDOW;
    loop {
        match wire::open_wire_session(socket, pipeline, load_id).await {
            Ok(session) => return Ok(session),
            Err(wire::WireOpenError::Ceiling(_)) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(RECLAIM_POLL).await;
            }
            Err(wire::WireOpenError::Ceiling(status)) => {
                return Err(format!("the one-session refusal never lifted: {status}"));
            }
            Err(wire::WireOpenError::Other(why)) => return Err(why),
        }
    }
}

// ——— P8: one-session-per-process ceiling.

/// The P8 probe, all raw moves on the LIVE socket: hold one session,
/// dial the socket a second time, and ask for a second session with an
/// empty request stream — the refusal must be the transport-level
/// `FailedPrecondition` status (the RPC never opens); anything else
/// fails the clause. The held session is closed orderly afterward
/// whatever P8 concluded.
async fn probe_one_session_ceiling(socket: &Path, entropy: &str) -> Result<(), String> {
    let session = settle_open_wire(socket, "rdlt-certify-p8", &format!("certify-p8-{entropy}"))
        .await
        .map_err(|why| format!("could not open the session that holds the slot: {why}"))?;

    let second = async {
        let channel = dial(socket, MAX_FRAME_BYTES as u64, DEFAULT_DEADLINE)
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
            "a second concurrent session was ACCEPTED — the wire allows exactly one session per \
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

// ——— P9: abandoned-session reclaim.

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

// ——— P10: Backend-direct order book.

/// The P10 identities — one pipeline of their own so the order-book
/// probe's commits never collide with the D-suite's or P8/P9's.
const P10_PIPELINE: &str = "rdlt-certify-p10";
/// The one load id both passes commit and interrogate — the
/// invocation's entropy joins it in `probe_order_book`.
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

/// The P10 probe: the raw destination choreography driven frame by
/// frame over the live socket, with no client manners between the
/// certifier and the server. Four rules across three session passes:
/// every request frame is answered with its OWN tag (a stream that ends
/// or stalls instead fails); the deliberately out-of-order first
/// `write`, on a never-ensured table, must earn a typed error frame; a
/// fresh session must find the receipt an earlier session committed,
/// accept `replay` for it, and answer a `publish` of that same load
/// with a refusal OR the SAME receipt — never a fresh mint; and
/// `part_closed` events are legal anywhere before `close`'s answer and
/// nowhere after it ([`WireSession::close_judged`] holds the boundary).
async fn probe_order_book(socket: &Path, entropy: &str) -> Result<(), String> {
    let p10_load = format!("{P10_LOAD_PREFIX}-{entropy}");
    let meta_json = wire::meta_json_for(P10_PIPELINE, &p10_load, P10_SEQ);

    // ——— Pass 1: the out-of-order probe, then the canonical sequence.
    // A violation returns mid-session; the drop is the wire's
    // abandonment signal and P9 already certified its reclaim.
    let mut session = settle_open_wire(socket, P10_PIPELINE, &p10_load)
        .await
        .map_err(|why| format!("could not open the order-book session: {why}"))?;

    // The deliberate out-of-order move FIRST: a `write` on a table no
    // `ensure` ever named must be refused with a typed error frame.
    match session.request(wire::write_request(P10_TABLE)).await? {
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

    wire::expect(
        session.request(wire::ensure_request(P10_TABLE)).await?,
        "ensure",
        "ensured",
    )?;
    wire::expect(
        session.request(wire::write_request(P10_TABLE)).await?,
        "write",
        "written",
    )?;

    match wire::receipt_reply(&mut session, &p10_load, P10_SEQ).await? {
        // A fresh load: `publish` is the legal continuation.
        None => wire::expect(
            session.request(wire::publish_request(&meta_json)).await?,
            "publish",
            "published",
        )?,
        // An earlier certification of this target already committed
        // the load: `replay` is the legal continuation — but only a
        // receipt naming THIS commit proves that.
        Some(receipt) if !receipt_matches(&receipt, &p10_load, P10_SEQ) => {
            return Err(format!(
                "`existing_receipt` answered with a receipt for a DIFFERENT commit than the \
                 one asked about ({})",
                render_receipt(&receipt)
            ));
        }
        Some(receipt) => wire::expect(
            session
                .request(wire::replay_request(&meta_json, receipt))
                .await?,
            "replay",
            "replayed",
        )?,
    }
    wire::expect(
        session
            .request(wire::read_state_request(P10_PIPELINE))
            .await?,
        "read_state",
        "state",
    )?;
    session.close_judged().await?;

    // ——— Pass 2: the exclusivity pass, a FRESH session on the same
    // load — the receipt must have outlived the session that minted it.
    let mut session = settle_open_wire(socket, P10_PIPELINE, &p10_load)
        .await
        .map_err(|why| format!("could not open the exclusivity session: {why}"))?;
    let Some(existing) = wire::receipt_reply(&mut session, &p10_load, P10_SEQ).await? else {
        return Err(
            "a fresh session's `existing_receipt` reported no receipt for a load an earlier \
             session committed — the receipt must be durable across sessions"
                .to_string(),
        );
    };
    if !receipt_matches(&existing, &p10_load, P10_SEQ) {
        return Err(format!(
            "the durable receipt names a DIFFERENT commit than the one asked about ({}) — \
             a receipt naming another (load_id, commit_seq) proves nothing about this commit",
            render_receipt(&existing)
        ));
    }
    wire::expect(
        session
            .request(wire::replay_request(&meta_json, existing.clone()))
            .await?,
        "replay",
        "replayed",
    )?;
    match session.request(wire::publish_request(&meta_json)).await? {
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
        other => return Err(wire::mismatch("publish", &other, "published")),
    }
    session.close_judged().await?;

    // ——— Pass 3: the no-ask double-publish: a FRESH session publishes
    // the SAME (load, seq) again with NO `existing_receipt` in between
    // — the wire shape a non-conforming client can always drive, and
    // the one the backend's own durable guard must answer. Refused is
    // one legal answer; the PRIOR receipt (byte-equal to the durable
    // one pass 2 read) is the other; a fresh mint is the exactly-once
    // violation. Two seats enforce this: the sdk `Session` wrapper
    // guards every engine-ridden path (it asks for the existing receipt
    // and replays before it ever publishes), and the backend's own
    // durable receipt guard inside `publish` is the wire-side seat this
    // raw sequence reaches — the only one standing for a client that
    // never asks. Silent row re-application under an identical receipt
    // is invisible to a wire-only judgment; it is the backend guard's
    // job to drop the restaged rows, and a table read-back to catch.
    let mut session = settle_open_wire(socket, P10_PIPELINE, &p10_load)
        .await
        .map_err(|why| format!("could not open the no-ask republish session: {why}"))?;
    match session.request(wire::publish_request(&meta_json)).await? {
        WireReply::Error(_frame) => {}
        WireReply::Published(published) => {
            let same_receipt = match (receipt_value(&existing), receipt_value(&published)) {
                (Some(durable), Some(published)) => durable == published,
                _ => false,
            };
            if !same_receipt {
                return Err(format!(
                    "a no-ask re-`publish` of a committed load minted a fresh receipt \
                     (durable {}, published {}) — with no `existing_receipt` in between, \
                     `publish` must be refused or answer the prior receipt",
                    render_receipt(&existing),
                    render_receipt(&published)
                ));
            }
        }
        other => return Err(wire::mismatch("publish", &other, "published")),
    }
    session.close_judged().await
}

/// A receipt's JSON value, for the same-receipt judgment — `None` when
/// the bytes do not decode (undecodable never equals anything).
fn receipt_value(receipt_json: &[u8]) -> Option<Value> {
    serde_json::from_slice(receipt_json).ok()
}

/// The receipt-identity check the runtime's own commit guard performs
/// — the certifier owes the same skepticism of the receipts IT
/// consumes: a receipt naming a different commit than the one asked
/// about proves nothing, and a backend whose lookup returns one (stale
/// cache, unkeyed read) must fail the clause, not pass it.
fn receipt_matches(receipt_json: &[u8], load_id: &str, commit_seq: u64) -> bool {
    let Some(Value::Object(map)) = receipt_value(receipt_json) else {
        return false;
    };
    matches!(
        (map.get("load_id"), map.get("commit_seq")),
        (Some(Value::String(id)), Some(Value::String(seq)))
            if id == load_id && seq.parse::<u64>() == Ok(commit_seq)
    ) || matches!(
        (map.get("load_id"), map.get("commit_seq")),
        (Some(Value::String(id)), Some(Value::Number(seq)))
            if id == load_id && seq.as_u64() == Some(commit_seq)
    )
}

/// A receipt for an evidence line — its JSON document, or the honest
/// marker when the server's bytes were not JSON at all.
fn render_receipt(receipt_json: &[u8]) -> String {
    receipt_value(receipt_json)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "<undecodable receipt>".to_string())
}

// ——— P11: one Arrow batch per write frame.

/// The P11 identities — a pipeline of this clause's own, away from the
/// P10 order book's commits.
const P11_PIPELINE: &str = "rdlt-certify-p11";
/// P11's load id.
const P11_LOAD_PREFIX: &str = "certify-p11";
/// P11's table.
const P11_TABLE: &str = "p11_one_batch";

/// The P11 probe, P5's write-direction twin, induced rather than
/// observed: ensure a table, then send a `write` whose arrow_ipc
/// payload is ONE IPC stream carrying TWO record batches. The refusal
/// must arrive as a typed error frame (its shape is P12's clause, not
/// this one's); `written` fails. The session is closed best-effort
/// whatever the verdict — the refusal is answered in-stream and the
/// session outlives it.
async fn probe_one_batch_write(socket: &Path, entropy: &str) -> Result<(), String> {
    let p11_load = format!("{P11_LOAD_PREFIX}-{entropy}");
    let mut session = settle_open_wire(socket, P11_PIPELINE, &p11_load)
        .await
        .map_err(|why| format!("could not open the one-batch session: {why}"))?;
    let verdict = async {
        wire::expect(
            session.request(wire::ensure_request(P11_TABLE)).await?,
            "ensure",
            "ensured",
        )?;
        match session
            .request(wire::two_batch_write_request(P11_TABLE))
            .await?
        {
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

// ——— P12: write-side error frames carry cause text.

/// The P12 identities — a pipeline of this clause's own.
const P12_PIPELINE: &str = "rdlt-certify-p12";
/// The one load id both P12 sessions commit and interrogate.
const P12_LOAD_PREFIX: &str = "certify-p12";
/// The one `(load, seq)` idempotency key the probe drives.
const P12_SEQ: u64 = 1;
/// P12's table.
const P12_TABLE: &str = "p12_error_text";

/// The P12 probe: P10's two induction sites (the out-of-order `write`,
/// the already-receipted `publish`) re-driven on this clause's own
/// sessions, with the refusal frames READ where P10 discards them —
/// P10 certifies that the refusals arrive, P12 what they say. Each
/// frame answers to [`wire::refusal_shape`], the judgment P6 holds the
/// read direction to. A violation returns mid-session; the drop is the
/// wire's abandonment signal, whose reclaim P9 already certified.
async fn probe_error_frame_text(socket: &Path, entropy: &str) -> Result<(), String> {
    let p12_load = format!("{P12_LOAD_PREFIX}-{entropy}");
    let meta_json = wire::meta_json_for(P12_PIPELINE, &p12_load, P12_SEQ);

    // ——— Induction 1: the out-of-order `write` on a fresh session,
    // its refusal frame judged.
    let mut session = settle_open_wire(socket, P12_PIPELINE, &p12_load)
        .await
        .map_err(|why| format!("could not open the error-text session: {why}"))?;
    match session.request(wire::write_request(P12_TABLE)).await? {
        WireReply::Error(frame) => wire::refusal_shape(&frame)
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
    wire::expect(
        session.request(wire::ensure_request(P12_TABLE)).await?,
        "ensure",
        "ensured",
    )?;
    wire::expect(
        session.request(wire::write_request(P12_TABLE)).await?,
        "write",
        "written",
    )?;
    match wire::receipt_reply(&mut session, &p12_load, P12_SEQ).await? {
        None => wire::expect(
            session.request(wire::publish_request(&meta_json)).await?,
            "publish",
            "published",
        )?,
        Some(receipt) => wire::expect(
            session
                .request(wire::replay_request(&meta_json, receipt))
                .await?,
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
    let Some(existing) = wire::receipt_reply(&mut session, &p12_load, P12_SEQ).await? else {
        return Err(
            "a fresh session's `existing_receipt` reported no receipt for a load an earlier \
             session committed — the already-receipted `publish` induction cannot be driven"
                .to_string(),
        );
    };
    wire::expect(
        session
            .request(wire::replay_request(&meta_json, existing))
            .await?,
        "replay",
        "replayed",
    )?;
    match session.request(wire::publish_request(&meta_json)).await? {
        WireReply::Error(frame) => wire::refusal_shape(&frame)
            .map_err(|why| format!("the already-receipted `publish` refusal: {why}"))?,
        WireReply::Published(_receipt) => {}
        other => return Err(wire::mismatch("publish", &other, "published")),
    }
    session.close().await;
    Ok(())
}

// ——— P13: unserved-role refusal.

/// The P13 skip reason a SOURCE certification announces for a
/// dual-role connector — the ONE spelling dual-role connectors' certify
/// cells assert against (a spelling copied into suites drifts).
pub const SOURCE_DUAL_ROLE_SKIP: &str = "`--role=destination` answered its spawn with a handshake line beside the certified \
     `--role=source` — a connector serving both roles has no unserved role to refuse";

/// [`SOURCE_DUAL_ROLE_SKIP`]'s destination-certification twin.
pub const DESTINATION_DUAL_ROLE_SKIP: &str = "`--role=source` answered its spawn with a handshake line beside the certified \
     `--role=destination` — a connector serving both roles has no unserved role to refuse";

/// The role a certification does NOT certify — the flag the P13 probe
/// spawns.
fn unserved_role(certified: Role) -> Role {
    match certified {
        Role::Source => Role::Destination,
        Role::Destination => Role::Source,
    }
}

/// What the P13 probe observed of the unserved-role spawn.
enum RoleRefusal {
    /// Exit code 2 with zero stdout bytes, AND the served role's
    /// control spawn handshaking from the same bare argv — the
    /// documented refusal, attributed to the role.
    Refused,
    /// The spawn answered with a handshake line: the connector SERVES
    /// that role too, so there is no unserved role to judge.
    Serves,
    /// Anything else, rendered for the Fail entry.
    Violation(String),
}

/// P13 — the unserved role's refusal: a connector binary must refuse a
/// `--role` it does not serve by exiting with code 2 before writing
/// any stdout byte — the flagless schema probe keys on exactly this
/// signal to retry the other role. The probe spawns the OTHER role on
/// its own child; a handshake line instead means the connector serves
/// both roles, and the clause is skipped with the reason naming them.
pub(crate) async fn report_p13(report: &mut Report, target: &Target, certified: Role) {
    let path = match target::resolve_binary(&target.requirement) {
        Ok(path) => path,
        Err(why) => {
            report.fail("P13", why);
            return;
        }
    };
    match probe_role_refusal(&path, certified, report::CLAUSE_TIMEOUT).await {
        Ok(RoleRefusal::Refused) => report.pass("P13"),
        Ok(RoleRefusal::Serves) => report.skip(
            "P13",
            match certified {
                Role::Source => SOURCE_DUAL_ROLE_SKIP,
                Role::Destination => DESTINATION_DUAL_ROLE_SKIP,
            }
            .to_string(),
        ),
        Ok(RoleRefusal::Violation(why)) | Err(why) => report.fail("P13", why),
    }
}

/// The P13 probe under `budget`: the unserved-role spawn and, on its
/// silent exit 2, the served-role control. The timeout wraps the probe
/// I/O alone and [`wire::reap_parked`] sits outside it, so on every
/// exit path — timeout included — the children are dead AND reaped and
/// any advertised socket unlinked; a reap inside the timed future would
/// be cancelled with it, leaving a serving child merely signalled and
/// still holding its store lock against the certifier's next spawn.
async fn probe_role_refusal(
    path: &Path,
    certified: Role,
    budget: Duration,
) -> Result<RoleRefusal, String> {
    let slot = wire::ChildSlot::default();
    let verdict = match tokio::time::timeout(
        budget,
        unserved_role_discipline(path, certified, &slot),
    )
    .await
    {
        Ok(verdict) => verdict,
        Err(_elapsed) => Err(report::timed_out()),
    };
    wire::reap_parked(&slot).await;
    verdict
}

/// The P13 judgments proper, over one direct spawn of the unserved
/// role (plus [`served_role_control`] where the refusal shape needs
/// attribution).
async fn unserved_role_discipline(
    path: &Path,
    certified: Role,
    slot: &wire::ChildSlot,
) -> Result<RoleRefusal, String> {
    let unserved = unserved_role(certified);
    let mut child = tokio::process::Command::new(path)
        .arg(target::role_arg(unserved))
        // The probes' shared stdio discipline: stdout is the subject,
        // stderr is the human log, stdin has nothing to say.
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("spawning `{}`: {error}", path.display()))?;
    let stdout = child
        .stdout
        .take()
        .expect("stdout was piped at spawn, so the child carries it");
    slot.lock().expect("child slot lock").park_child(child);

    // The refusal must come BEFORE any stdout byte, so the first read
    // decides the arm: EOF with nothing read is the refusal shape (the
    // exit code still to check), a handshake line is a served role,
    // any other byte is already half the violation. Byte-capped like
    // every probe read; unbounded in time deliberately — the caller's
    // clause timeout is the bound.
    let mut reader = BufReader::new(stdout.take(target::MAX_LINE_BYTES));
    let mut line = String::new();
    let first = reader
        .read_line(&mut line)
        .await
        .map_err(|error| format!("reading the unserved role's stdout: {error}"))?;

    if first == 0 {
        let status = wait_parked(slot).await?;
        return match status.code() {
            Some(2) => served_role_control(path, certified, slot).await,
            _ => Ok(RoleRefusal::Violation(role_refusal_violation(
                unserved, 0, &status,
            ))),
        };
    }

    if let Ok(parsed) = Line::parse(line.trim_end_matches(['\n', '\r'])) {
        // The handshake line is written only for a role the binary
        // serves, so this spawn IS a served role — the advertised
        // socket joins the parked state so the caller's reap kills the
        // serving child AND unlinks the socket its line advertised.
        slot.lock()
            .expect("child slot lock")
            .park_socket(parsed.socket_path);
        return Ok(RoleRefusal::Serves);
    }

    // Stdout spoke something else: drain to EOF (a violator's own exit
    // closes the pipe; one that hangs instead rides the clause
    // timeout) so the evidence names the full byte count.
    let mut rest = Vec::new();
    reader
        .read_to_end(&mut rest)
        .await
        .map_err(|error| format!("reading the unserved role's stdout: {error}"))?;
    let status = wait_parked(slot).await?;
    Ok(RoleRefusal::Violation(role_refusal_violation(
        unserved,
        first + rest.len(),
        &status,
    )))
}

/// The P13 control: exit 2 with no stdout is also clap's usage-error
/// shape, so a binary needing more argv than `--role=<x>` would mint
/// `PASS P13` for a role it serves. The refusal counts only once the
/// SERVED role answers a handshake line from the same bare argv; a
/// binary whose served role cannot is FAILED, not excused — the
/// flagless schema probe spawns exactly this shape, so the ambiguity
/// is itself the defect. The control's live child parks in `slot` for
/// the caller's reap.
async fn served_role_control(
    path: &Path,
    certified: Role,
    slot: &wire::ChildSlot,
) -> Result<RoleRefusal, String> {
    // spawn_and_read_line's own error paths reap the slot before
    // returning; its success parks the served child for the caller.
    let line = match wire::spawn_and_read_line(path, certified, slot).await {
        Ok((_reader, line)) => line,
        Err(why) => {
            return Ok(RoleRefusal::Violation(ambiguous_refusal_violation(
                certified, &why,
            )));
        }
    };
    let trimmed = line.trim_end_matches(['\n', '\r']);
    match Line::parse(trimmed) {
        Ok(parsed) => {
            slot.lock()
                .expect("child slot lock")
                .park_socket(parsed.socket_path);
            Ok(RoleRefusal::Refused)
        }
        Err(_) if trimmed.is_empty() => Ok(RoleRefusal::Violation(ambiguous_refusal_violation(
            certified,
            "it wrote no stdout either",
        ))),
        Err(_) => Ok(RoleRefusal::Violation(ambiguous_refusal_violation(
            certified,
            "its first stdout line is not a handshake line",
        ))),
    }
}

/// The one spelling for an exit 2 the control could not attribute to
/// the role.
fn ambiguous_refusal_violation(certified: Role, control_outcome: &str) -> String {
    format!(
        "the unserved {unserved} exited with code 2 and no stdout, but the served {served} \
         spawned from the same bare argv did not answer with a handshake line \
         ({control_outcome}) — exit 2 cannot be attributed to a role refusal when the binary \
         refuses the bare `--role=` argv for both roles",
        unserved = target::role_arg(unserved_role(certified)),
        served = target::role_arg(certified),
    )
}

/// Claim the parked child and await its exit status.
async fn wait_parked(slot: &wire::ChildSlot) -> Result<std::process::ExitStatus, String> {
    let child = slot.lock().expect("child slot lock").claim_child();
    let mut child = child.expect("the probe parked its child before waiting on it");
    child
        .wait()
        .await
        .map_err(|error| format!("awaiting the unserved role's exit: {error}"))
}

/// The one P13 violation spelling: the flag, the byte count, and how
/// the process ended.
fn role_refusal_violation(
    unserved: Role,
    stdout_bytes: usize,
    status: &std::process::ExitStatus,
) -> String {
    let ended = match status.code() {
        Some(code) => format!("exited with code {code}"),
        None => "was terminated by a signal".to_string(),
    };
    format!(
        "the unserved {role} must be refused with exit code 2 before any stdout byte — the \
         connector wrote {stdout_bytes} stdout byte(s) and {ended}",
        role = target::role_arg(unserved)
    )
}

#[cfg(test)]
#[path = "generic_tests.rs"]
mod generic_tests;
#[cfg(test)]
#[path = "session_tests.rs"]
mod session_tests;
#[cfg(test)]
#[path = "support.rs"]
mod support;
#[cfg(test)]
#[path = "wire_tests.rs"]
mod wire_tests;
