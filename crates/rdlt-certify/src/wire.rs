//! The raw wire substrate BELOW the client adapters, which the P and
//! K families ride: a spawn-and-attach probe ([`WireProbe`]) whose
//! every method is a bare RPC with NO verification layered on, so the
//! clauses see the ACTUAL frames a served connector speaks — the
//! adapters' own good manners (identity verification at handshake, the
//! one-batch refusal, classification mapping) would otherwise stand
//! between the certifier and a misbehaving server, and a clause that
//! can only see what the adapter lets through certifies the adapter,
//! not the connector.
//!
//! The same layer carries the write direction's raw session substrate:
//! [`WireSession`] opens with a bare `Open` frame on its own dial and
//! drives the WHOLE session grammar through [`WireSession::request`]
//! and [`WireSession::close_judged`] — one tagged reply per request
//! frame, interleaved `part_closed` events skipped where they are
//! legal, and a judged end where the reply stream must actually END
//! after `closed` — plus the certifier-authored request frames and
//! [`refusal_shape`], the error-frame judgment both wire directions
//! share. No clause verdict is minted here; the families in
//! [`crate::clause`] write every report entry.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use rdlt_connector::core::commit::WriteMode;
use rdlt_connector::core::id::{LoadId, PipelineId};
use rdlt_connector_client::handshake::{Requirement, Role};
use rdlt_connector_client::wire::{
    DEFAULT_DEADLINE, connector_client, destination_client, dial, source_client,
};
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::{
    self, handshake_reply, read_frame, session_reply, session_request, streams_reply,
};
use rdlt_connector_protocol::{MAX_FRAME_BYTES, PROTOCOL_VERSION};
use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::target;

/// The four CLIENT renderings the refusal-shape judgment refuses on
/// the frame MESSAGE: a frame whose message begins with one of these
/// has the classification rendered into text — the server put a
/// client's framing on the wire, where the bare cause text belongs
/// (classification travels as the enum; the receiving client renders
/// the frame exactly once on reconstruction).
const CLIENT_RENDERINGS: [&str; 4] = [
    "transient source error: ",
    "fatal source error: ",
    "transient destination error: ",
    "fatal destination error: ",
];

/// The proto's `expected_role` spelling for `role` — the bare wire
/// words, as distinct from the bin contract's `--role=` argument
/// ([`target::role_arg`]).
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

impl RawFrame {
    /// The bytes this frame RETAINS in the certifier's collection —
    /// what the [`READ_RETENTION_CEILING`] meters. Carried payloads
    /// only; the fixed collection slot is the meter's own per-frame
    /// constant.
    fn retained_bytes(&self) -> usize {
        match self {
            RawFrame::Arrow(bytes) => bytes.len(),
            RawFrame::Error(frame) => frame.message.len(),
            RawFrame::Json | RawFrame::Checkpoint | RawFrame::Empty => 0,
        }
    }
}

/// The aggregate retention ceiling on ONE read stream's collected
/// frames: [`WireProbe::read_frames`] holds every frame to the stream's
/// end, and while each frame is individually capped by the dial's
/// [`MAX_FRAME_BYTES`] decode limit, the frame COUNT is not — a fast
/// rogue could otherwise OOM the certifier inside its own clause
/// timeout. Four frame-ceilings of room is generous for any
/// certification stream (fixtures are rows, not datasets); a stream
/// retaining more is refused typed. Per-frame accounting counts the
/// carried payload plus the collection slot, so a flood of EMPTY
/// frames is bounded by the same ceiling.
pub(crate) const READ_RETENTION_CEILING: usize = MAX_FRAME_BYTES * 4;

/// The frame census P5's evidence carries: what the read direction
/// actually served, counted by kind — so a vacuous pass (no arrow
/// frames at all) and a violation both say what was observed.
#[derive(Default)]
pub(crate) struct Census {
    arrow: usize,
    raw_json: usize,
    checkpoint: usize,
    error: usize,
    empty: usize,
}

impl Census {
    /// Count one frame.
    pub(crate) fn record(&mut self, frame: &RawFrame) {
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

/// What a wire attach parks for whoever must clean up after it: the
/// spawned child, and — once the handshake line names it — the
/// advertised socket's path. The
/// child lives HERE for the probe's WHOLE life, not in any future, so
/// a caller that must abandon the attach (a timeout aborting the task)
/// or the arm holding a live probe (a whole-arm clause timeout) can
/// still claim the child and await its DEATH: dropping a future only
/// runs Drop, and `kill_on_drop` only SENDS the SIGKILL, so without
/// the slot a dying single-writer connector could still hold its store
/// lock when the next spawn opens the same store. The socket rides
/// along because the old attach's guarantee — the advertised socket
/// file is unlinked on EVERY abandonment path — must survive the slot:
/// an abort landing mid-dial knows the path only through here.
#[derive(Default)]
pub(crate) struct Parked {
    child: Option<tokio::process::Child>,
    socket: Option<PathBuf>,
}

impl Parked {
    /// Park the advertised socket the moment it is known, so every
    /// subsequent reap unlinks it — the P1 probe's seam into the
    /// shared cleanup (attach parks its own inline).
    pub(crate) fn park_socket(&mut self, socket: PathBuf) {
        self.socket = Some(socket);
    }

    /// Park the spawned child — the P13 probe's seam (attach parks its
    /// own inline).
    pub(crate) fn park_child(&mut self, child: tokio::process::Child) {
        self.child = Some(child);
    }

    /// Claim the parked child back — for a caller that must AWAIT its
    /// exit rather than kill it (the P13 refusal arm); the eventual
    /// [`reap_parked`] then finds nothing and no-ops.
    pub(crate) fn claim_child(&mut self) -> Option<tokio::process::Child> {
        self.child.take()
    }
}

/// The shared handle to one attach's [`Parked`] state.
pub(crate) type ChildSlot = std::sync::Arc<std::sync::Mutex<Parked>>;

/// Unlink `path` ONLY when a socket actually sits there — every certify
/// unlink seat's one rule, the runtime `Guard`'s own: the path
/// came verbatim from the connector's stdout handshake line, and rogue
/// connectors are this tool's explicit subject, so a rogue naming an
/// unrelated file must not commission the certifier to delete it. The
/// check rides `symlink_metadata` — a symlink AT the path is already
/// not a socket, and following it would judge the wrong inode.
pub(crate) fn unlink_advertised_socket(path: &Path) {
    let is_socket = std::fs::symlink_metadata(path)
        .map(|meta| {
            use std::os::unix::fs::FileTypeExt as _;
            meta.file_type().is_socket()
        })
        .unwrap_or(false);
    if is_socket {
        let _ = std::fs::remove_file(path);
    }
}

/// Claim and REAP whatever `slot` still parks — the caller's move after
/// abandoning an attach or a live probe: the child is killed and
/// AWAITED (dead, not dying, when this returns), the advertised socket
/// unlinked. A no-op when a `kill` already reaped or nothing spawned.
pub(crate) async fn reap_parked(slot: &ChildSlot) {
    let (child, socket) = {
        let mut parked = slot.lock().expect("child slot lock");
        (parked.child.take(), parked.socket.take())
    };
    if let Some(mut child) = child {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    if let Some(socket) = socket {
        unlink_advertised_socket(&socket);
    }
}

/// Spawn `bin` under `role` with the probes' shared stdio discipline
/// (stdout piped and capped, stderr nulled, stdin nulled,
/// `kill_on_drop` as the net under the slot), park the child in
/// `slot`, and read the FIRST stdout line under
/// [`target::MAX_LINE_BYTES`] and [`target::LINE_TIMEOUT`] — the one
/// first-line funnel the wire attach and the P1 line probe both ride,
/// kill-and-reap included. Error paths REAP the parked child
/// before returning; the returned reader carries whatever stdout
/// follows the line, for callers that keep listening.
pub(crate) async fn spawn_and_read_line(
    bin: &Path,
    role: Role,
    slot: &ChildSlot,
) -> Result<
    (
        BufReader<tokio::io::Take<tokio::process::ChildStdout>>,
        String,
    ),
    String,
> {
    let mut child = Command::new(bin)
        .arg(target::role_arg(role))
        // stderr is nulled: the probes observe the machine channel and
        // the wire, not the connector's human log.
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .stdin(Stdio::null())
        // The safety net under the slot: if the slot itself is
        // dropped with the child parked, the drop still signals.
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("spawning `{}`: {error}", bin.display()))?;

    let stdout = child
        .stdout
        .take()
        .expect("stdout was piped at spawn, so the child carries it");
    slot.lock().expect("child slot lock").child = Some(child);

    let mut reader = BufReader::new(stdout.take(target::MAX_LINE_BYTES));
    let mut line = String::new();
    match tokio::time::timeout(target::LINE_TIMEOUT, reader.read_line(&mut line)).await {
        Err(_elapsed) => {
            reap_parked(slot).await;
            Err(format!(
                "wrote no handshake line within {}s — the first stdout line must be the \
                 handshake line",
                target::LINE_TIMEOUT.as_secs()
            ))
        }
        Ok(Err(error)) => {
            reap_parked(slot).await;
            Err(format!("reading the handshake line: {error}"))
        }
        Ok(Ok(_bytes)) => Ok((reader, line)),
    }
}

/// The record of a [`WireProbe::attach`] spawn: the shared slot the
/// child stays PARKED in (claimable by an abandoning caller — see
/// [`Parked`]) and the advertised socket. Dropping this unlinks the
/// socket; the child, if still parked and unclaimed, dies with the
/// slot's last clone (`kill_on_drop`) or under the caller's
/// [`reap_parked`].
struct SpawnedConnector {
    slot: ChildSlot,
    socket: PathBuf,
}

impl Drop for SpawnedConnector {
    fn drop(&mut self) {
        unlink_advertised_socket(&self.socket);
        self.slot.lock().expect("child slot lock").socket = None;
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
    ///
    /// The child parks in `slot` across every await (see [`ChildSlot`]);
    /// each error path below reaps it before returning, so an `Err`
    /// never leaves a live process behind.
    pub(crate) async fn attach(
        bin: &Path,
        role: Role,
        config: &Value,
        budget_bytes: u64,
        slot: &ChildSlot,
    ) -> Result<Self, String> {
        let (_reader, line) = spawn_and_read_line(bin, role, slot).await?;
        let parsed = match Line::parse(line.trim_end_matches(['\n', '\r'])) {
            Ok(parsed) => parsed,
            Err(error) => {
                reap_parked(slot).await;
                return Err(format!(
                    "the first stdout line is not a handshake line: {error}"
                ));
            }
        };
        // The socket joins the parked state the moment it is KNOWN —
        // from here, every abandonment path (this fn's own errors, a
        // caller's abort landing mid-dial) unlinks it via reap_parked.
        slot.lock().expect("child slot lock").socket = Some(parsed.socket_path.clone());

        let channel = match dial(&parsed.socket_path, budget_bytes, DEFAULT_DEADLINE).await {
            Ok(channel) => channel,
            Err(error) => {
                reap_parked(slot).await;
                return Err(format!("dialing the advertised socket: {error}"));
            }
        };
        // Success: the child STAYS parked (a whole-arm timeout dropping
        // this probe must still find it claimable); an already-empty
        // slot means the caller abandoned this attach and reaped —
        // honor the cancellation rather than resurrect a dead pid.
        if slot.lock().expect("child slot lock").child.is_none() {
            unlink_advertised_socket(&parsed.socket_path);
            return Err("the attach was abandoned by its caller".to_owned());
        }
        Ok(Self {
            channel,
            role,
            config: config.clone(),
            spawned: Some(SpawnedConnector {
                slot: slot.clone(),
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
    /// signal the runtime's `Guard` sends on drop), never a
    /// shelled-out `kill(1)`. `start_kill` only SENDS the signal, so
    /// the wait is what makes "the process is dead" true when this
    /// returns rather than eventually.
    pub(crate) async fn kill(&mut self) {
        let spawned = self
            .spawned
            .as_mut()
            .expect("only spawned probes are killed — the kill matrix never test-attaches");
        // The child lives in the shared slot for the probe's whole life
        // (see [`Parked`]); an empty slot means an abandoning caller
        // already claimed and reaped it — dying twice is a no-op.
        let child = spawned.slot.lock().expect("child slot lock").child.take();
        let Some(mut child) = child else { return };
        let _ = child.start_kill();
        let _ = child.wait().await;
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
        let channel = dial(socket, MAX_FRAME_BYTES as u64, DEFAULT_DEADLINE)
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
    /// until the stream ends, under [`READ_RETENTION_CEILING`]. A
    /// mid-stream transport `Status` is an error — the protocol's
    /// refusal shape inside a read stream is the terminal `ErrorFrame`,
    /// never a bare status.
    pub(crate) async fn read_frames(
        &mut self,
        stream_spec_json: Vec<u8>,
    ) -> Result<Vec<RawFrame>, String> {
        self.read_frames_within(stream_spec_json, READ_RETENTION_CEILING)
            .await
    }

    /// [`Self::read_frames`] under an explicit retention ceiling — the
    /// seam the ceiling's own pin drives with a small budget, so
    /// proving the refusal fires never needs a quarter-gigabyte rogue
    /// stream in the suite.
    async fn read_frames_within(
        &mut self,
        stream_spec_json: Vec<u8>,
        ceiling: usize,
    ) -> Result<Vec<RawFrame>, String> {
        let mut stream = self.open_read(stream_spec_json).await?;
        let mut frames = Vec::new();
        let mut retained: usize = 0;
        loop {
            match stream.message().await {
                Ok(Some(frame)) => {
                    let frame = decode_read_frame(frame);
                    retained = retained
                        .saturating_add(std::mem::size_of::<RawFrame>())
                        .saturating_add(frame.retained_bytes());
                    if retained > ceiling {
                        return Err(format!(
                            "the read stream exceeded the certifier's {ceiling}-byte retention \
                             ceiling without ending — certification observes whole streams, so \
                             a certifiable stream must fit the ceiling"
                        ));
                    }
                    frames.push(frame);
                }
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
/// resolution the P1 probe uses ([`target::resolve_binary`]), then
/// [`WireProbe::attach`].
pub(crate) async fn attach_for(
    requirement: &Requirement,
    role: Role,
    config: &Value,
    slot: &ChildSlot,
) -> Result<WireProbe, String> {
    let bin = target::resolve_binary(requirement)?;
    WireProbe::attach(&bin, role, config, MAX_FRAME_BYTES as u64, slot).await
}

/// The record batches one `arrow_ipc` payload carries.
///
/// This seat decodes a CONNECTOR UNDER CERTIFICATION's frames — the
/// primary adversary — so it gets both halves of the decode defense:
/// the SPI's shared framing pre-pass holds every declared length
/// against the frame's real bytes before arrow's reader can allocate
/// from them (the memset and allocation-abort arms are neither panics
/// nor errors), and `catch_unwind` contains arrow's panic arms, so a
/// crafted frame fails the clause TYPED instead of killing the
/// certifier.
pub(crate) fn count_batches(bytes: &[u8]) -> Result<usize, String> {
    rdlt_connector::gate::refuse_overdeclared_framing(bytes)?;
    caught_decode(|| count_batches_decoding(bytes))
}

/// Contain one Arrow decode's unwind as a typed failure. `catch_unwind`
/// contains PANICS only — the allocation-abort class is closed upstream
/// by the framing pre-pass (same division of labor as the WAL seat's).
fn caught_decode<T>(work: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)).unwrap_or_else(|payload| {
        Err(format!(
            "the Arrow decoder panicked: {}",
            // The shared bounded rendering: P5 caps its violation
            // COUNT, but a count cap never bounds each string's LENGTH
            // — a payload embedding frame-derived text would otherwise
            // ride the report file whole.
            rdlt_connector::gate::panic_text(payload.as_ref())
        ))
    })
}

/// The decode half of [`count_batches`], behind its belt.
fn count_batches_decoding(bytes: &[u8]) -> Result<usize, String> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| error.to_string())?;
    let mut count = 0;
    for batch in reader {
        batch.map_err(|error| error.to_string())?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod decode_belt_tests {
    //! The belt pinned DIRECTLY: a synthetic panic inside the decode
    /// closure must surface as the typed failure, never an escaped
    /// unwind — the live inputs that once reached this arm now refuse
    /// earlier at the pre-pass, so the belt's proof is synthetic (the
    /// same pattern the WAL seat's belt pin uses).
    use super::caught_decode;

    #[test]
    fn a_decoder_panic_is_contained_as_a_typed_failure() {
        let error = caught_decode::<()>(|| panic!("crafted metadata"))
            .expect_err("a decoder unwind must be contained");
        assert!(error.contains("decoder panicked"), "{error}");
        assert!(error.contains("crafted metadata"), "{error}");
    }
}

/// The refusal-shape judgment P6 and P12 share, either direction of
/// the wire: the frame's classification must be a real enum value and
/// its message bare cause text — never one of the four client
/// renderings ([`CLIENT_RENDERINGS`]).
pub(crate) fn refusal_shape(frame: &proto::ErrorFrame) -> Result<(), String> {
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

/// A `write` frame for `table` whose arrow_ipc payload is ONE Arrow
/// IPC stream carrying TWO record batches — the P11 induction (the
/// write-direction twin of the P5 rogue's two-batch read frame; the
/// sdk's own encoder writes exactly one batch by construction, so the
/// violation needs the certifier to author it).
pub(crate) fn two_batch_write_request(table: &str) -> session_request::Request {
    let first = batch_of(&[1, 2]);
    let second = batch_of(&[3]);
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &first.schema())
            .expect("an IPC stream writer opens over a Vec");
        writer.write(&first).expect("the first batch writes");
        writer.write(&second).expect("the second batch writes");
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
    /// The transport-level `FailedPrecondition` refusal — the frozen
    /// one-session ceiling class, whichever seat of the RPC it
    /// surfaced at.
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
    let channel = dial(socket, MAX_FRAME_BYTES as u64, DEFAULT_DEADLINE)
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
    //! The retention ceiling's rogue, driven through the ceiling's own
    //! seam — no spawn, no built bin.

    use rdlt_connector::source::StreamSpec;

    use super::*;
    use crate::rogue::{self, HandshakeScript, RogueSource};

    /// The retention ceiling's rogue: a read stream carrying
    /// more than the collector may retain is refused TYPED, with the
    /// pinned spelling — driven through the ceiling's own seam with a
    /// small budget so the pin costs kilobytes rather than the
    /// production quarter-gigabyte flood. The production ceiling's
    /// value rides the same pin: the seam and the constant together
    /// are the whole defense.
    #[tokio::test]
    async fn a_flooding_read_stream_is_refused_at_the_retention_ceiling() {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("rogue.sock");
        let _serving = rogue::serve_source(
            &socket,
            RogueSource {
                handshake: HandshakeScript::truthful(),
                streams: vec![StreamSpec::new("rogue_stream")],
                read_declared: vec![rogue::json_read_frame(); 64],
                read_undeclared: vec![],
                read_hold_open: false,
            },
        );
        let mut probe = WireProbe::attach_socket(&socket, Role::Source, &serde_json::json!({}))
            .await
            .expect("the rogue's socket dials");
        let spec_json =
            serde_json::to_vec(&StreamSpec::new("rogue_stream")).expect("a StreamSpec serializes");
        let Err(error) = probe.read_frames_within(spec_json, 256).await else {
            panic!("a stream flooding past the ceiling must be refused");
        };
        assert_eq!(
            error,
            "the read stream exceeded the certifier's 256-byte retention ceiling without \
             ending — certification observes whole streams, so a certifiable stream must \
             fit the ceiling"
        );
        assert_eq!(READ_RETENTION_CEILING, 4 * MAX_FRAME_BYTES);
    }
}

#[cfg(test)]
mod parked_tests {
    //! The abandonment cleanups the slot carries: a caller that
    //! aborts an attach MID-DIAL must still be able to
    //! unlink the advertised socket and await the child's death
    //! through [`reap_parked`] — dropping the future alone only ran
    //! Drop and only SENT the SIGKILL, and knew no socket path at all.

    use std::os::unix::fs::PermissionsExt;

    use super::*;

    /// A script fake: one valid handshake line naming `socket`, then
    /// stay alive holding the pipes (`exec` so the pid the reap awaits
    /// is the one holding them).
    fn fake_connector(dir: &Path, socket: &Path) -> PathBuf {
        let path = dir.join("fake-connector");
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\necho 'rdlt-connector|1|0|0|{}'\nexec sleep 30\n",
                socket.display()
            ),
        )
        .expect("the fake script writes");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("the fake script becomes executable");
        path
    }

    /// Abort mid-dial: the advertised socket is bound but NEVER
    /// accepted, so the h2 handshake pends forever and the abort lands
    /// inside the dial. The abandoning caller's reap_parked must
    /// unlink the socket file and leave nothing parked — the
    /// every-abandonment-path unlink guarantee.
    #[tokio::test]
    async fn an_attach_aborted_mid_dial_leaves_no_socket_file_or_child() {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("held.sock");
        let _listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let bin = fake_connector(dir.path(), &socket);

        let slot = ChildSlot::default();
        let task = {
            let slot = slot.clone();
            tokio::spawn(async move {
                WireProbe::attach(
                    &bin,
                    Role::Source,
                    &serde_json::json!({}),
                    MAX_FRAME_BYTES as u64,
                    &slot,
                )
                .await
            })
        };
        // Wait until the attach has parsed the handshake line (the
        // socket joins the parked state exactly then) — the abort must
        // land inside the dial, not before the spawn.
        for _ in 0..500 {
            if slot.lock().expect("lock").socket.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            slot.lock().expect("lock").socket.is_some(),
            "the attach must reach the dial within the polling window"
        );
        task.abort();
        let _ = task.await;
        reap_parked(&slot).await;

        assert!(
            !socket.exists(),
            "the advertised socket file must be unlinked on abandonment"
        );
        let parked = slot.lock().expect("lock");
        assert!(
            parked.child.is_none() && parked.socket.is_none(),
            "nothing stays parked after the reap"
        );
    }

    /// A rogue advertising a REGULAR file's path in its handshake line
    /// must not commission the certifier to delete it — rogue
    /// connectors are this tool's explicit subject, so the advertised
    /// path is squarely adversarial. Both unlink seats are judged: the
    /// shared reap and the spawned probe's drop. (The runtime's `Guard`
    /// guards the identical operation; the certifier's seats ride the
    /// same lstat-is-socket rule.)
    #[tokio::test]
    async fn a_rogue_advertising_a_regular_file_does_not_get_it_deleted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let precious = dir.path().join("precious.txt");
        std::fs::write(&precious, b"not a socket").expect("the file writes");

        let slot = ChildSlot::default();
        slot.lock().expect("lock").park_socket(precious.clone());
        reap_parked(&slot).await;
        assert!(
            precious.exists(),
            "reap_parked must not unlink a regular file"
        );

        drop(SpawnedConnector {
            slot: slot.clone(),
            socket: precious.clone(),
        });
        assert!(
            precious.exists(),
            "SpawnedConnector::drop must not unlink a regular file"
        );
    }
}
