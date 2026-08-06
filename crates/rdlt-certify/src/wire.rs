//! The raw wire observation layer BELOW the adapters: the wire
//! P-clauses — P3 (identity/skew), P5 (one-batch), P6 (error-frame
//! shape), P7 (the v0 state-format map) — judged on the ACTUAL frames a
//! served connector speaks, through a probe that deliberately bypasses
//! the client adapters. The adapters' own good manners (identity
//! verification at handshake, the one-batch refusal, classification
//! mapping) would otherwise stand between the certifier and a
//! misbehaving server — a clause that can only see what the adapter
//! lets through certifies the adapter, not the connector.
//!
//! The same layer carries the write direction's raw session substrate
//! ([`WireSession`]) — the P8/P9 probes in [`crate::destination`] ride
//! it, opening sessions with bare `Open` frames on their own dials, and
//! the P10 order-book probe drives the WHOLE session grammar through
//! [`WireSession::request`]/[`WireSession::close_judged`]: one tagged
//! reply per request frame, interleaved `part_closed` events skipped
//! where they are legal, and a judged end where the reply stream must
//! actually END after `closed`.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use rdlt_connector::core::{LoadId, PipelineId, WriteMode};
use rdlt_connector::{ConnectorSpec, StreamSpec};
use rdlt_connector_client::{connector_client, destination_client, dial, source_client};
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::{
    self, handshake_reply, read_frame, session_reply, session_request, streams_reply,
};
use rdlt_connector_protocol::{MAX_FRAME_BYTES, PROTOCOL_VERSION};
use rdlt_runtime::{ConnectorRequirement, Role};
use rdlt_testkit::{batch_of, commit_meta_for, schema_for};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::report::{CLAUSE_TIMEOUT, Report, timed_out};
use crate::target::{LINE_TIMEOUT, MAX_LINE_BYTES, resolve_binary, role_arg};

/// The wire clauses a SOURCE certification judges, in report order —
/// also the cascade set when the probe cannot attach or handshake
/// (nothing downstream of a dead handshake can be observed).
pub(crate) const SOURCE_WIRE_CLAUSES: [&str; 4] = ["P3", "P7", "P5", "P6"];

/// The DESTINATION's wire clauses: the handshake-borne pair alone —
/// P5/P6 are read-direction clauses, and the write direction's own
/// wire clauses (P8/P9) are probed in [`crate::destination`].
pub(crate) const DEST_WIRE_CLAUSES: [&str; 2] = ["P3", "P7"];

/// The stream name P6 reads to induce a refusal: reserved by spelling —
/// no real connector stream may collide with it.
const P6_BOGUS_STREAM: &str = "__rdlt_certify_no_such_stream__";

/// The four CLIENT renderings P6 refuses on the frame MESSAGE: a frame
/// whose message begins with one of these has the classification
/// rendered into text — the server put a client's framing on the wire,
/// where the bare cause text belongs (classification travels as the
/// enum; the receiving client renders the frame exactly once on
/// reconstruction — the 026 double-frame class, kept dead).
const CLIENT_RENDERINGS: [&str; 4] = [
    "transient source error: ",
    "fatal source error: ",
    "transient destination error: ",
    "fatal destination error: ",
];

/// The proto's `expected_role` spelling for `role` — the bare wire
/// words, as distinct from the bin contract's `--role=` argument
/// ([`role_arg`]).
fn wire_role(role: Role) -> &'static str {
    match role {
        Role::Source => "source",
        Role::Destination => "destination",
    }
}

/// One observed `ReadFrame`, exactly as the wire carried it. Payloads
/// are carried only where a clause consumes them today (P5 decodes
/// arrow, P6 judges the error frame); a future clause that reads the
/// JSON or checkpoint payloads widens its variant when it arrives —
/// an unread payload here would be dead weight pretending to be
/// observation.
pub(crate) enum RawFrame {
    /// `raw_json` — a JSON row document.
    Json,
    /// `arrow_ipc` — an Arrow IPC stream, P5's subject.
    Arrow(Vec<u8>),
    /// `checkpoint_cursor_json` — a cursor checkpoint.
    Checkpoint,
    /// `error` — the terminal refusal frame, P6's subject.
    Error(proto::ErrorFrame),
    /// A frame whose oneof carried no payload — protocol-undefined,
    /// counted in the census rather than crashed on.
    Empty,
}

/// The frame census P5's evidence carries: what the read direction
/// actually served, counted by kind — so a vacuous pass (no arrow
/// frames at all) and a violation both say what was observed.
#[derive(Default)]
struct Census {
    arrow: usize,
    raw_json: usize,
    checkpoint: usize,
    error: usize,
    empty: usize,
}

impl Census {
    /// Count one frame.
    fn record(&mut self, frame: &RawFrame) {
        match frame {
            RawFrame::Arrow(_) => self.arrow += 1,
            RawFrame::Json => self.raw_json += 1,
            RawFrame::Checkpoint => self.checkpoint += 1,
            RawFrame::Error(_) => self.error += 1,
            RawFrame::Empty => self.empty += 1,
        }
    }
}

impl std::fmt::Display for Census {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} arrow, {} raw_json, {} checkpoint, {} error, {} empty",
            self.arrow, self.raw_json, self.checkpoint, self.error, self.empty
        )
    }
}

/// The child a [`WireProbe::attach`] spawned. Dropping it SIGKILLs the
/// process (`kill_on_drop` was set at spawn) and unlinks the socket its
/// handshake line advertised — no `LifecycleGuard` ever owned this
/// spawn, so the cleanup lives here.
struct SpawnedConnector {
    child: tokio::process::Child,
    socket: PathBuf,
}

impl Drop for SpawnedConnector {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket);
    }
}

/// The raw probe: one served connector (spawned, or test-attached to an
/// in-process rogue), its dialed channel, and the handshake inputs.
/// Every method is a bare RPC with NO verification layered on — the
/// clauses see the wire itself.
pub(crate) struct WireProbe {
    channel: Channel,
    role: Role,
    config: Value,
    /// The spawned process — `None` when a test attached the probe to
    /// an in-process rogue server's socket.
    spawned: Option<SpawnedConnector>,
}

impl WireProbe {
    /// Spawn `bin` under `role`, read the one handshake line (the same
    /// cap and timeout the P1 probe applies), and dial the advertised
    /// socket RAW — no identity verification, no adapter.
    /// [`Self::handshake_raw`] is where certification then looks at
    /// what the line pointed to.
    ///
    /// `budget_bytes` is the dial's h2 window budget ([`dial`] clamps
    /// it to the workable range): the wire clauses pass the frame
    /// ceiling, and the kill matrix passes a deliberately SMALL budget
    /// so a read stream cannot be fully in flight before a mid-stream
    /// SIGKILL lands.
    pub(crate) async fn attach(
        bin: &Path,
        role: Role,
        config: &Value,
        budget_bytes: u64,
    ) -> Result<Self, String> {
        let mut child = Command::new(bin)
            .arg(role_arg(role))
            // stderr is nulled: the probe observes the machine channel
            // and the wire, not the connector's human log.
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| format!("spawning `{}`: {error}", bin.display()))?;

        let stdout = child
            .stdout
            .take()
            .expect("stdout was piped at spawn, so the child carries it");
        let mut reader = BufReader::new(stdout.take(MAX_LINE_BYTES));
        let mut line = String::new();
        match tokio::time::timeout(LINE_TIMEOUT, reader.read_line(&mut line)).await {
            Err(_elapsed) => {
                return Err(format!(
                    "wrote no handshake line within {}s — the first stdout line must be the \
                     handshake line",
                    LINE_TIMEOUT.as_secs()
                ));
            }
            Ok(Err(error)) => return Err(format!("reading the handshake line: {error}")),
            Ok(Ok(_bytes)) => {}
        }
        let parsed = Line::parse(line.trim_end_matches(['\n', '\r']))
            .map_err(|error| format!("the first stdout line is not a handshake line: {error}"))?;

        let channel = dial(&parsed.socket_path, budget_bytes)
            .await
            .map_err(|error| format!("dialing the advertised socket: {error}"))?;
        Ok(Self {
            channel,
            role,
            config: config.clone(),
            spawned: Some(SpawnedConnector {
                child,
                socket: parsed.socket_path,
            }),
        })
    }

    /// The advertised socket of a spawned probe — `None` on a
    /// test-attached one. The kill matrix re-dials it for raw sessions
    /// and for the post-kill "a dead socket refuses, never hangs"
    /// observation.
    pub(crate) fn socket(&self) -> Option<&Path> {
        self.spawned
            .as_ref()
            .map(|spawned| spawned.socket.as_path())
    }

    /// SIGKILL the spawned connector and wait until it is reaped — the
    /// house mechanism (`tokio::process::Child::start_kill`, the same
    /// signal the runtime's `LifecycleGuard` sends on drop), never a
    /// shelled-out `kill(1)`. `start_kill` only SENDS the signal, so
    /// the wait is what makes "the process is dead" true when this
    /// returns rather than eventually.
    pub(crate) async fn kill(&mut self) {
        let spawned = self
            .spawned
            .as_mut()
            .expect("only spawned probes are killed — the kill matrix never test-attaches");
        let _ = spawned.child.start_kill();
        let _ = spawned.child.wait().await;
    }

    /// Attach to an already-serving socket — the test seam the rogue
    /// suites use (an in-process rogue serves shapes no spawnable sdk
    /// binary can produce). The SAME code path as [`Self::attach`]
    /// after the dial, so what the rogues prove holds for real spawns.
    #[cfg(test)]
    pub(crate) async fn attach_socket(
        socket: &Path,
        role: Role,
        config: &Value,
    ) -> Result<Self, String> {
        let channel = dial(socket, MAX_FRAME_BYTES as u64)
            .await
            .map_err(|error| format!("dialing `{}`: {error}", socket.display()))?;
        Ok(Self {
            channel,
            role,
            config: config.clone(),
            spawned: None,
        })
    }

    /// The one `Handshake` RPC, UNVERIFIED: returns the raw
    /// `HandshakeOk` exactly as the wire carried it — P3 does the
    /// judging, this method only speaks the protocol.
    pub(crate) async fn handshake_raw(&mut self) -> Result<proto::HandshakeOk, String> {
        let mut client = connector_client(self.channel.clone());
        let reply = client
            .handshake(proto::HandshakeRequest {
                protocol_version: PROTOCOL_VERSION,
                expected_role: wire_role(self.role).to_string(),
                config_json: serde_json::to_vec(&self.config)
                    .expect("a serde_json::Value serializes to JSON infallibly"),
            })
            .await
            .map_err(|status| format!("the Handshake RPC failed: {status}"))?
            .into_inner();
        match reply.outcome {
            Some(handshake_reply::Outcome::Ok(ok)) => Ok(ok),
            Some(handshake_reply::Outcome::Error(frame)) => {
                let classification = proto::Classification::try_from(frame.classification)
                    .map(|c| c.as_str_name().to_string())
                    .unwrap_or_else(|_| frame.classification.to_string());
                Err(format!(
                    "the handshake was refused ({classification}): {}",
                    frame.message
                ))
            }
            None => Err("the handshake reply carried no outcome".to_string()),
        }
    }

    /// The `Streams` RPC, raw: each declared stream's `stream_spec_json`
    /// bytes, undecoded — [`Self::read_frames`] feeds them back verbatim
    /// so P5 reads exactly the streams the connector itself declared,
    /// and the kill matrix picks its boundary stream from the same list.
    pub(crate) async fn streams_raw(&mut self) -> Result<Vec<Vec<u8>>, String> {
        let mut client = source_client(self.channel.clone());
        let reply = client
            .streams(proto::StreamsRequest {})
            .await
            .map_err(|status| format!("the Streams RPC failed: {status}"))?
            .into_inner();
        match reply.outcome {
            Some(streams_reply::Outcome::Ok(list)) => Ok(list.stream_spec_json),
            Some(streams_reply::Outcome::Error(frame)) => {
                Err(format!("the Streams RPC was refused: {}", frame.message))
            }
            None => Err("the streams reply carried no outcome".to_string()),
        }
    }

    /// Open one `Read` RPC and hand back the raw frame stream — the
    /// incremental seam the kill matrix pulls frame by frame so a
    /// SIGKILL can land at a chosen mid-stream boundary.
    pub(crate) async fn open_read(
        &mut self,
        stream_spec_json: Vec<u8>,
    ) -> Result<tonic::Streaming<proto::ReadFrame>, String> {
        let mut client = source_client(self.channel.clone());
        client
            .read(proto::ReadRequest {
                stream_spec_json,
                since_cursor_json: None,
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| format!("the Read RPC failed to open: {status}"))
    }

    /// One `Read` RPC, collecting every frame as the wire carried it
    /// until the stream ends. A mid-stream transport `Status` is an
    /// error — the protocol's refusal shape inside a read stream is the
    /// terminal `ErrorFrame`, never a bare status.
    async fn read_frames(&mut self, stream_spec_json: Vec<u8>) -> Result<Vec<RawFrame>, String> {
        let mut stream = self.open_read(stream_spec_json).await?;
        let mut frames = Vec::new();
        loop {
            match stream.message().await {
                Ok(Some(frame)) => frames.push(decode_read_frame(frame)),
                Ok(None) => return Ok(frames),
                Err(status) => {
                    return Err(format!(
                        "the read stream failed mid-flight with a transport status: {status}"
                    ));
                }
            }
        }
    }
}

/// Map one wire `ReadFrame` to its [`RawFrame`].
pub(crate) fn decode_read_frame(frame: proto::ReadFrame) -> RawFrame {
    match frame.frame {
        Some(read_frame::Frame::RawJson(_bytes)) => RawFrame::Json,
        Some(read_frame::Frame::ArrowIpc(bytes)) => RawFrame::Arrow(bytes),
        Some(read_frame::Frame::CheckpointCursorJson(_bytes)) => RawFrame::Checkpoint,
        Some(read_frame::Frame::Error(error)) => RawFrame::Error(error),
        None => RawFrame::Empty,
    }
}

/// Spawn the target's own binary for the wire clauses: the SAME
/// resolution the P1 probe uses ([`resolve_binary`] — one helper, no
/// fourth copy), then [`WireProbe::attach`].
pub(crate) async fn attach_for(
    requirement: &ConnectorRequirement,
    role: Role,
    config: &Value,
) -> Result<WireProbe, String> {
    let bin = resolve_binary(requirement)?;
    WireProbe::attach(&bin, role, config, MAX_FRAME_BYTES as u64).await
}

/// The source wire clauses over one attached probe, in
/// [`SOURCE_WIRE_CLAUSES`] order: P3/P7 from one raw handshake, P5
/// over every declared stream's frames, P6 on an induced refusal. A
/// probe whose handshake fails fails ALL of them with the one cause.
pub(crate) async fn certify_source_wire(
    report: &mut Report,
    probe: &mut WireProbe,
    required_id: &str,
) {
    let Some(ok) = raw_handshake_or_cascade(report, probe, &SOURCE_WIRE_CLAUSES).await else {
        return;
    };
    report_p3(report, &ok, required_id);
    report_p7(report, &ok);
    report_p5(report, probe).await;
    report_p6(report, probe).await;
}

/// The destination wire clauses: the handshake-borne P3/P7 alone (see
/// [`DEST_WIRE_CLAUSES`]).
pub(crate) async fn certify_destination_wire(
    report: &mut Report,
    probe: &mut WireProbe,
    required_id: &str,
) {
    let Some(ok) = raw_handshake_or_cascade(report, probe, &DEST_WIRE_CLAUSES).await else {
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
    match tokio::time::timeout(CLAUSE_TIMEOUT, probe.handshake_raw()).await {
        Ok(Ok(ok)) => Some(ok),
        Ok(Err(why)) => {
            for clause in clauses {
                report.fail(clause, why.clone());
            }
            None
        }
        Err(_elapsed) => {
            for clause in clauses {
                report.fail(clause, timed_out());
            }
            None
        }
    }
}

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

/// P7 — the v0 state-format map: `state_format_versions` must decode
/// as a `map<string, u32>`, which protobuf decoding already enforced
/// by the time a `HandshakeOk` exists (an undecodable field fails the
/// whole handshake and cascades). Empty passes — the v0 posture — and
/// a populated map ALSO passes: tolerated, threaded, never negotiated
/// (D-040-1). `Pass` carries no payload, so the tolerance evidence is
/// pinned by the populated-map rogue rather than rendered here.
fn report_p7(report: &mut Report, ok: &proto::HandshakeOk) {
    let _ = &ok.state_format_versions;
    report.pass("P7");
}

/// P5 — the one-batch rule, judged on the wire bytes: every
/// `arrow_ipc` read frame across every DECLARED stream must decode as
/// an Arrow IPC stream carrying exactly one record batch. Frames that
/// are not arrow are exempt (the rule is per-frame, not per-source);
/// a source that serves no arrow frames at all passes vacuously — the
/// clause still ran, and a violation's evidence carries the full frame
/// census so the observation is auditable either way.
async fn report_p5(report: &mut Report, probe: &mut WireProbe) {
    match tokio::time::timeout(CLAUSE_TIMEOUT, p5_violations(probe)).await {
        Ok(Ok(violations)) if violations.is_empty() => report.pass("P5"),
        Ok(Ok(violations)) => {
            for violation in violations {
                report.fail("P5", violation);
            }
        }
        Ok(Err(why)) => report.fail("P5", why),
        Err(_elapsed) => report.fail("P5", timed_out()),
    }
}

/// Walk every declared stream's frames and collect one-batch
/// violations, each suffixed with the complete frame census.
async fn p5_violations(probe: &mut WireProbe) -> Result<Vec<String>, String> {
    let streams = probe.streams_raw().await?;
    let mut census = Census::default();
    let mut violations = Vec::new();
    for spec_json in streams {
        // The stream's own name, for the evidence line — undecodable
        // spec bytes still get read (the connector declared them).
        let name = serde_json::from_slice::<StreamSpec>(&spec_json)
            .map(|spec| spec.name.to_string())
            .unwrap_or_else(|_| "<undecodable stream spec>".to_string());
        for frame in probe.read_frames(spec_json).await? {
            census.record(&frame);
            if let RawFrame::Arrow(bytes) = &frame {
                match count_batches(bytes) {
                    Ok(1) => {}
                    Ok(count) => violations.push(format!(
                        "an arrow read frame carried {count} record batches — the one-batch \
                         rule requires exactly one (stream `{name}`)"
                    )),
                    Err(error) => violations.push(format!(
                        "an arrow read frame does not decode as one Arrow IPC stream \
                         (stream `{name}`): {error}"
                    )),
                }
            }
        }
    }
    Ok(violations
        .into_iter()
        .map(|violation| format!("{violation}; frame census: {census}"))
        .collect())
}

/// The record batches one `arrow_ipc` payload carries.
fn count_batches(bytes: &[u8]) -> Result<usize, String> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| error.to_string())?;
    let mut count = 0;
    for batch in reader {
        batch.map_err(|error| error.to_string())?;
        count += 1;
    }
    Ok(count)
}

/// P6 — error-frame shape, on an induced refusal: reading
/// [`P6_BOGUS_STREAM`] must produce a TERMINAL `ErrorFrame` whose
/// classification is a real enum value and whose message carries the
/// bare cause text — never one of the four client renderings
/// ([`CLIENT_RENDERINGS`]).
async fn report_p6(report: &mut Report, probe: &mut WireProbe) {
    match tokio::time::timeout(CLAUSE_TIMEOUT, p6_verdict(probe)).await {
        Ok(Ok(())) => report.pass("P6"),
        Ok(Err(why)) => report.fail("P6", why),
        Err(_elapsed) => report.fail("P6", timed_out()),
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
    match proto::Classification::try_from(frame.classification) {
        Ok(
            proto::Classification::Transient
            | proto::Classification::RateLimited
            | proto::Classification::Fatal,
        ) => {}
        Ok(proto::Classification::Unspecified) => {
            return Err(
                "the error frame's classification is CLASSIFICATION_UNSPECIFIED — a refusal \
                 must carry a real classification"
                    .to_string(),
            );
        }
        Err(_) => {
            return Err(format!(
                "the error frame's classification is not a known enum value ({})",
                frame.classification
            ));
        }
    }
    for rendering in CLIENT_RENDERINGS {
        if frame.message.starts_with(rendering) {
            return Err(format!(
                "classification rendered inside the message — the frame carries cause text; \
                 classification travels as the enum (the message begins with `{rendering}`)"
            ));
        }
    }
    Ok(())
}

/// One raw destination session over its own dial: `Open` sent,
/// `Opened` received, nothing else. Dropping it without
/// [`Self::close`] is the wire's abandonment signal (the request
/// stream just ends) — exactly what the P9 probe induces.
pub(crate) struct WireSession {
    requests: mpsc::Sender<proto::SessionRequest>,
    replies: tonic::Streaming<proto::SessionReply>,
}

impl WireSession {
    /// Orderly end: send `Close` and drain replies until the stream
    /// ends — best-effort, the session is over either way.
    pub(crate) async fn close(mut self) {
        let _ = self
            .requests
            .send(session_frame(session_request::Request::Close(
                proto::Close {},
            )))
            .await;
        while let Ok(Some(_reply)) = self.replies.message().await {}
    }

    /// Send one request frame and await its TAGGED answer. Interleaved
    /// `part_closed` events are skipped — they are legal anywhere
    /// before `Close`'s answer (the proto's own interleaving contract),
    /// and P10's part-event legality judgment lives at
    /// [`Self::close_judged`], the one boundary an event may not cross.
    /// A reply stream that ends (or fails) before answering is the
    /// reply-per-frame violation, named after the unanswered frame.
    pub(crate) async fn request(
        &mut self,
        request: session_request::Request,
    ) -> Result<WireReply, String> {
        let tag = request_tag(&request);
        if self.requests.send(session_frame(request)).await.is_err() {
            return Err(format!(
                "the request stream closed before `{tag}` could be sent"
            ));
        }
        loop {
            match self.replies.message().await {
                Ok(Some(reply)) => match decode_reply(reply, tag)? {
                    WireReply::PartClosed => continue,
                    answer => return Ok(answer),
                },
                Ok(None) => {
                    return Err(format!(
                        "the session reply stream ended before answering `{tag}`"
                    ));
                }
                Err(status) => {
                    return Err(format!(
                        "the session reply stream failed answering `{tag}`: {status}"
                    ));
                }
            }
        }
    }

    /// The JUDGED end — P10's close arm, distinct from [`Self::close`]
    /// (P8/P9's best-effort teardown): `Close` must answer `closed`,
    /// and after that answer the reply stream must actually END — a
    /// `part_closed` event (or any other frame) arriving after
    /// `closed` violates the order book.
    pub(crate) async fn close_judged(mut self) -> Result<(), String> {
        match self
            .request(session_request::Request::Close(proto::Close {}))
            .await?
        {
            WireReply::Closed => {}
            WireReply::Error(frame) => {
                return Err(format!("`close` was refused: {}", frame.message));
            }
            other => {
                return Err(format!(
                    "`close` was answered `{}` — every request's reply must carry its own \
                     tag (`closed`)",
                    other.tag()
                ));
            }
        }
        match self.replies.message().await {
            Ok(None) => Ok(()),
            Ok(Some(reply)) => Err(format!(
                "a `{}` reply arrived after `close` was answered — part events and replies \
                 are legal only before the session's end",
                decode_reply(reply, "close").map_or("<no payload>", |r| r.tag())
            )),
            Err(status) => Err(format!(
                "the reply stream failed after `close` was answered: {status}"
            )),
        }
    }
}

/// One tagged session reply, as [`WireSession::request`] returns it.
/// Payloads are carried only where the P10 probe consumes them today
/// (the two receipt-bearing replies); the rest are bare tags — an
/// unread payload here would be dead weight pretending to be
/// observation (the [`RawFrame`] rule, write direction).
pub(crate) enum WireReply {
    /// `opened`.
    Opened,
    /// `ensured`.
    Ensured,
    /// `written`.
    Written,
    /// `receipt` — `ReceiptReply.receipt_json`, `None` when the server
    /// knows no receipt for the asked `(load_id, commit_seq)`.
    Receipt(Option<Vec<u8>>),
    /// `replayed`.
    Replayed,
    /// `published` — `Published.receipt_json`.
    Published(Vec<u8>),
    /// `state`.
    State,
    /// `closed`.
    Closed,
    /// `error` — the in-stream refusal.
    Error(proto::ErrorFrame),
    /// `part_closed` — the interleaved telemetry event;
    /// [`WireSession::request`] skips it, [`WireSession::close_judged`]
    /// refuses it after `closed`.
    PartClosed,
}

impl WireReply {
    /// The reply's wire tag — the proto oneof's own field name, the
    /// vocabulary every P10 evidence line speaks.
    pub(crate) fn tag(&self) -> &'static str {
        match self {
            WireReply::Opened => "opened",
            WireReply::Ensured => "ensured",
            WireReply::Written => "written",
            WireReply::Receipt(_) => "receipt",
            WireReply::Replayed => "replayed",
            WireReply::Published(_) => "published",
            WireReply::State => "state",
            WireReply::Closed => "closed",
            WireReply::Error(_) => "error",
            WireReply::PartClosed => "part_closed",
        }
    }
}

/// The request's wire tag (the proto oneof's own field name) — names
/// the frame in evidence when its answer never arrives or misbehaves.
fn request_tag(request: &session_request::Request) -> &'static str {
    match request {
        session_request::Request::Open(_) => "open",
        session_request::Request::Ensure(_) => "ensure",
        session_request::Request::Write(_) => "write",
        session_request::Request::ExistingReceipt(_) => "existing_receipt",
        session_request::Request::Replay(_) => "replay",
        session_request::Request::Publish(_) => "publish",
        session_request::Request::ReadState(_) => "read_state",
        session_request::Request::Close(_) => "close",
    }
}

/// Map one wire `SessionReply` to its [`WireReply`]. A reply whose
/// oneof carries no payload is protocol-undefined — an error naming
/// the request it was supposed to answer, not a census entry (the
/// session grammar, unlike the read stream, has no legal empty frame).
fn decode_reply(reply: proto::SessionReply, answering: &str) -> Result<WireReply, String> {
    match reply.reply {
        Some(session_reply::Reply::Opened(_)) => Ok(WireReply::Opened),
        Some(session_reply::Reply::Ensured(_)) => Ok(WireReply::Ensured),
        Some(session_reply::Reply::Written(_)) => Ok(WireReply::Written),
        Some(session_reply::Reply::Receipt(receipt)) => {
            Ok(WireReply::Receipt(receipt.receipt_json))
        }
        Some(session_reply::Reply::Replayed(_)) => Ok(WireReply::Replayed),
        Some(session_reply::Reply::Published(published)) => {
            Ok(WireReply::Published(published.receipt_json))
        }
        Some(session_reply::Reply::State(_)) => Ok(WireReply::State),
        Some(session_reply::Reply::Closed(_)) => Ok(WireReply::Closed),
        Some(session_reply::Reply::Error(frame)) => Ok(WireReply::Error(frame)),
        Some(session_reply::Reply::PartClosed(_)) => Ok(WireReply::PartClosed),
        None => Err(format!(
            "a session reply carried no payload (answering `{answering}`)"
        )),
    }
}

/// The canonical row ids every certifier-authored `write` frame
/// carries (the testkit's `id: Int64` fixture) — the count is what the
/// kill matrix's convergence assert holds the probe to.
pub(crate) const FIXTURE_IDS: [i64; 3] = [1, 2, 3];

/// An `ensure` frame for `table`: the testkit's canonical schema,
/// Append mode.
pub(crate) fn ensure_request(table: &str) -> session_request::Request {
    session_request::Request::Ensure(proto::Ensure {
        table_schema_json: serde_json::to_vec(&schema_for(table))
            .expect("a TableSchema serializes to JSON infallibly"),
        write_mode_json: serde_json::to_vec(&WriteMode::Append)
            .expect("a WriteMode serializes to JSON infallibly"),
    })
}

/// A `write` frame for `table`: a single-batch Arrow IPC stream of the
/// [`FIXTURE_IDS`] rows.
pub(crate) fn write_request(table: &str) -> session_request::Request {
    let batch = batch_of(&FIXTURE_IDS);
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())
            .expect("an IPC stream writer opens over a Vec");
        writer.write(&batch).expect("the fixture batch writes");
        writer.finish().expect("the IPC stream finishes");
    }
    session_request::Request::Write(proto::Write {
        table: table.to_string(),
        arrow_ipc: bytes,
    })
}

/// An `existing_receipt` frame for one `(load, seq)` idempotency key.
pub(crate) fn existing_receipt_request(load_id: &str, commit_seq: u64) -> session_request::Request {
    session_request::Request::ExistingReceipt(proto::ExistingReceipt {
        load_id: load_id.to_string(),
        commit_seq,
    })
}

/// A `publish` frame.
pub(crate) fn publish_request(meta_json: &[u8]) -> session_request::Request {
    session_request::Request::Publish(proto::Publish {
        commit_meta_json: meta_json.to_vec(),
    })
}

/// A `replay` frame, carrying `receipt` back verbatim.
pub(crate) fn replay_request(meta_json: &[u8], receipt: Vec<u8>) -> session_request::Request {
    session_request::Request::Replay(proto::Replay {
        commit_meta_json: meta_json.to_vec(),
        receipt_json: receipt,
    })
}

/// A `read_state` frame for `pipeline`.
pub(crate) fn read_state_request(pipeline: &str) -> session_request::Request {
    session_request::Request::ReadState(proto::ReadState {
        pipeline: pipeline.to_string(),
    })
}

/// The `CommitMeta` document for `(pipeline, load, seq)`, serialized.
pub(crate) fn meta_json_for(pipeline: &str, load_id: &str, commit_seq: u64) -> Vec<u8> {
    serde_json::to_vec(&commit_meta_for(
        &PipelineId::new(pipeline),
        &LoadId::new(load_id),
        commit_seq,
    ))
    .expect("a CommitMeta serializes to JSON infallibly")
}

/// Ask `existing_receipt` and demand the `receipt` tag back — the
/// payload (`Some` bytes or an honest `None`) is the caller's judgment.
pub(crate) async fn receipt_reply(
    session: &mut WireSession,
    load_id: &str,
    commit_seq: u64,
) -> Result<Option<Vec<u8>>, String> {
    match session
        .request(existing_receipt_request(load_id, commit_seq))
        .await?
    {
        WireReply::Receipt(receipt) => Ok(receipt),
        WireReply::Error(frame) => {
            Err(format!("`existing_receipt` was refused: {}", frame.message))
        }
        other => Err(mismatch("existing_receipt", &other, "receipt")),
    }
}

/// The reply-per-frame judgment: `reply` must carry `want`'s tag. An
/// error frame renders as a refusal (its cause text is the evidence);
/// any other tag is the mismatch.
pub(crate) fn expect(reply: WireReply, request: &str, want: &str) -> Result<(), String> {
    if reply.tag() == want {
        return Ok(());
    }
    Err(match reply {
        WireReply::Error(frame) => format!("`{request}` was refused: {}", frame.message),
        other => mismatch(request, &other, want),
    })
}

/// The tags-match violation spelling.
pub(crate) fn mismatch(request: &str, got: &WireReply, want: &str) -> String {
    format!(
        "`{request}` was answered `{}` — every request's reply must carry its own tag \
         (`{want}`)",
        got.tag()
    )
}

/// Why a raw session did not open.
pub(crate) enum WireOpenError {
    /// The transport-level `FailedPrecondition` refusal — the
    /// one-session ceiling class (038's frozen refusal), whichever
    /// seat of the RPC it surfaced at.
    Ceiling(tonic::Status),
    /// Anything else, rendered.
    Other(String),
}

/// Wrap one request payload in its `SessionRequest` envelope.
fn session_frame(request: session_request::Request) -> proto::SessionRequest {
    proto::SessionRequest {
        request: Some(request),
    }
}

/// Dial `socket` fresh and open one raw session: send
/// `Open{pipeline, load_id}` and await `Opened`. The ceiling refusal
/// is recognized at BOTH seats it can surface at (the RPC call and the
/// first reply read — trailers-only responses land at either,
/// depending on timing).
pub(crate) async fn open_wire_session(
    socket: &Path,
    pipeline: &str,
    load_id: &str,
) -> Result<WireSession, WireOpenError> {
    let channel = dial(socket, MAX_FRAME_BYTES as u64)
        .await
        .map_err(|error| WireOpenError::Other(format!("dialing the live socket: {error}")))?;
    let mut client = destination_client(channel);
    // Capacity 2: the Open frame preloads into one slot, and every
    // later frame (P8/P9's Close, P10's order-book frames alike) is
    // sent one at a time by a request/reply-paced caller — never more
    // than one in flight.
    let (requests, feed) = mpsc::channel(2);
    requests
        .try_send(session_frame(session_request::Request::Open(proto::Open {
            pipeline: pipeline.to_string(),
            load_id: load_id.to_string(),
        })))
        .expect("a fresh channel has capacity for the Open frame");
    let mut replies = match client.open_session(ReceiverStream::new(feed)).await {
        Ok(response) => response.into_inner(),
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            return Err(WireOpenError::Ceiling(status));
        }
        Err(status) => {
            return Err(WireOpenError::Other(format!(
                "the OpenSession RPC failed: {status}"
            )));
        }
    };
    match replies.message().await {
        Ok(Some(reply)) => match reply.reply {
            Some(session_reply::Reply::Opened(_)) => Ok(WireSession { requests, replies }),
            Some(session_reply::Reply::Error(frame)) => Err(WireOpenError::Other(format!(
                "the Open frame was refused: {}",
                frame.message
            ))),
            other => Err(WireOpenError::Other(format!(
                "the Open frame's reply was not `opened`: {other:?}"
            ))),
        },
        Ok(None) => Err(WireOpenError::Other(
            "the session reply stream ended before answering Open".to_string(),
        )),
        Err(status) if status.code() == tonic::Code::FailedPrecondition => {
            Err(WireOpenError::Ceiling(status))
        }
        Err(status) => Err(WireOpenError::Other(format!(
            "reading the Open reply: {status}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    //! The rogue suite for the source wire clauses: each designated
    //! rogue proves its clause CAN fail, with the evidence pinned
    //! full-string. The rogues serve in-process over UDS — no spawn,
    //! no built bin — so these ride the bare (ungated) suite; the seam
    //! is [`WireProbe::attach_socket`], pub(crate), so the pins live
    //! beside the clause code (the report.rs precedent).

    use super::*;
    use crate::report::Verdict;
    use crate::rogue::{self, HandshakeScript, RogueSource};
    use proto::Classification;

    /// Serve `rogue` in-process and run the full source wire-clause
    /// sequence against it, requiring identity `required_id`.
    async fn certify_rogue(rogue: RogueSource, required_id: &str) -> Report {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("rogue.sock");
        let _serving = rogue::serve_source(&socket, rogue);
        let mut probe = WireProbe::attach_socket(&socket, Role::Source, &serde_json::json!({}))
            .await
            .expect("the rogue's socket dials");
        let mut report = Report::default();
        certify_source_wire(&mut report, &mut probe, required_id).await;
        report
    }

    fn verdict<'a>(report: &'a Report, clause: &str) -> &'a Verdict {
        &report
            .entries
            .iter()
            .find(|entry| entry.clause == clause)
            .unwrap_or_else(|| panic!("no {clause} entry:\n{}", report.render_text()))
            .verdict
    }

    #[track_caller]
    fn assert_fail(report: &Report, clause: &str, evidence: &str) {
        match verdict(report, clause) {
            Verdict::Fail(why) => assert_eq!(why, evidence, "clause {clause}"),
            other => panic!(
                "{clause} must Fail, got {other:?}:\n{}",
                report.render_text()
            ),
        }
    }

    #[track_caller]
    fn assert_pass(report: &Report, clause: &str) {
        assert!(
            matches!(verdict(report, clause), Verdict::Pass),
            "{clause} must Pass:\n{}",
            report.render_text()
        );
    }

    /// A well-shaped induced refusal: FATAL, bare cause text.
    fn shaped_refusal() -> Vec<proto::ReadFrame> {
        vec![rogue::error_read_frame(rogue::error_frame(
            Classification::Fatal,
            "no such stream",
        ))]
    }

    /// THE SKEW CASE (the T1 carry — no other test anywhere exercises
    /// `spec.version != connector_version`): a rogue whose spec
    /// document and wire identity disagree on VERSION fails P3 with
    /// both values named, and ONLY P3. Its populated state-format map
    /// is tolerated — P7 passes with it (D-040-1's pin).
    #[tokio::test]
    async fn a_version_skewed_handshake_fails_p3_alone() {
        let report = certify_rogue(
            RogueSource {
                handshake: HandshakeScript::Ok {
                    connector_id: "rogue",
                    connector_version: "0.0.0",
                    spec_name: "rogue",
                    spec_version: "9.9.9",
                    state_format_versions: &[("cursor", 2)],
                },
                streams: vec![],
                read_declared: vec![],
                read_undeclared: shaped_refusal(),
                read_hold_open: false,
            },
            "rogue",
        )
        .await;
        assert_fail(
            &report,
            "P3",
            "the handshake identity is skewed: spec_json carries version `9.9.9` but the wire \
             reported connector_version `0.0.0`",
        );
        assert_pass(&report, "P7");
        assert_pass(&report, "P5");
        assert_pass(&report, "P6");
    }

    /// The name half of the skew: spec_json naming somebody else than
    /// the wire's connector_id fails P3 by VALUES — no `io.rapidbyte.*`
    /// spelling is assumed anywhere.
    #[tokio::test]
    async fn a_name_skewed_handshake_fails_p3() {
        let report = certify_rogue(
            RogueSource {
                handshake: HandshakeScript::Ok {
                    connector_id: "rogue",
                    connector_version: "0.0.0",
                    spec_name: "somebody-else",
                    spec_version: "0.0.0",
                    state_format_versions: &[],
                },
                streams: vec![],
                read_declared: vec![],
                read_undeclared: shaped_refusal(),
                read_hold_open: false,
            },
            "rogue",
        )
        .await;
        assert_fail(
            &report,
            "P3",
            "the handshake identity is skewed: spec_json names `somebody-else` but the wire \
             reported connector_id `rogue`",
        );
    }

    /// The requirement arm: a self-consistent identity that is not the
    /// one the target requires still fails P3.
    #[tokio::test]
    async fn a_wrong_connector_id_fails_p3_against_the_requirement() {
        let report = certify_rogue(
            RogueSource {
                handshake: HandshakeScript::truthful(),
                streams: vec![],
                read_declared: vec![],
                read_undeclared: shaped_refusal(),
                read_hold_open: false,
            },
            "somebody-else",
        )
        .await;
        assert_fail(
            &report,
            "P3",
            "the handshake identity is skewed: the wire reported connector_id `rogue` but the \
             target requires `somebody-else`",
        );
    }

    /// P5's designated rogue: ONE arrow frame carrying TWO record
    /// batches fails P5 with the count and the census, and only P5.
    #[tokio::test]
    async fn a_two_batch_arrow_frame_fails_p5_alone() {
        let report = certify_rogue(
            RogueSource {
                handshake: HandshakeScript::truthful(),
                streams: vec![StreamSpec::new("rogue_stream")],
                read_declared: vec![rogue::arrow_read_frame(2)],
                read_undeclared: shaped_refusal(),
                read_hold_open: false,
            },
            "rogue",
        )
        .await;
        assert_fail(
            &report,
            "P5",
            "an arrow read frame carried 2 record batches — the one-batch rule requires \
             exactly one (stream `rogue_stream`); frame census: 1 arrow, 0 raw_json, \
             0 checkpoint, 0 error, 0 empty",
        );
        assert_pass(&report, "P3");
        assert_pass(&report, "P6");
        assert_pass(&report, "P7");
    }

    /// P6's designated rogue: an error frame whose MESSAGE begins with
    /// a client rendering fails P6 with the pinned diagnosis, and only
    /// P6 — the frame carries cause text; classification travels as
    /// the enum.
    #[tokio::test]
    async fn a_client_rendering_in_the_frame_message_fails_p6_alone() {
        let report = certify_rogue(
            RogueSource {
                handshake: HandshakeScript::truthful(),
                streams: vec![],
                read_declared: vec![],
                read_undeclared: vec![rogue::error_read_frame(rogue::error_frame(
                    Classification::Fatal,
                    "fatal source error: boom",
                ))],
                read_hold_open: false,
            },
            "rogue",
        )
        .await;
        assert_fail(
            &report,
            "P6",
            "classification rendered inside the message — the frame carries cause text; \
             classification travels as the enum (the message begins with `fatal source \
             error: `)",
        );
        assert_pass(&report, "P3");
        assert_pass(&report, "P5");
        assert_pass(&report, "P7");
    }

    /// A refusal that never arrives is also a P6 failure: a clean end
    /// of stream on a nonexistent stream hides the refusal entirely.
    #[tokio::test]
    async fn a_clean_end_on_the_bogus_stream_fails_p6() {
        let report = certify_rogue(
            RogueSource {
                handshake: HandshakeScript::truthful(),
                streams: vec![],
                read_declared: vec![],
                read_undeclared: vec![],
                read_hold_open: false,
            },
            "rogue",
        )
        .await;
        assert_fail(
            &report,
            "P6",
            "reading a nonexistent stream produced no terminal ErrorFrame — a refusal must \
             arrive as a typed error frame, never a clean end of stream",
        );
    }

    /// An unclassified refusal fails P6: CLASSIFICATION_UNSPECIFIED is
    /// the proto's zero value, not a classification.
    #[tokio::test]
    async fn an_unspecified_classification_fails_p6() {
        let report = certify_rogue(
            RogueSource {
                handshake: HandshakeScript::truthful(),
                streams: vec![],
                read_declared: vec![],
                read_undeclared: vec![rogue::error_read_frame(proto::ErrorFrame {
                    classification: Classification::Unspecified as i32,
                    message: "boom".to_string(),
                    retry_after_ms: None,
                })],
                read_hold_open: false,
            },
            "rogue",
        )
        .await;
        assert_fail(
            &report,
            "P6",
            "the error frame's classification is CLASSIFICATION_UNSPECIFIED — a refusal must \
             carry a real classification",
        );
    }

    /// A refused handshake cascades: EVERY wire clause fails with the
    /// one cause — including P7, whose only failure mode this is (its
    /// map shape is enforced by protobuf decoding itself).
    #[tokio::test]
    async fn a_refused_handshake_cascades_every_wire_clause() {
        let report = certify_rogue(
            RogueSource {
                handshake: HandshakeScript::Refuse {
                    message: "the config document is not mine",
                },
                streams: vec![],
                read_declared: vec![],
                read_undeclared: vec![],
                read_hold_open: false,
            },
            "rogue",
        )
        .await;
        for clause in SOURCE_WIRE_CLAUSES {
            assert_fail(
                &report,
                clause,
                "the handshake was refused (FATAL): the config document is not mine",
            );
        }
    }
}
