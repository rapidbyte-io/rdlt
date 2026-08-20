//! The source role: one server answers both halves of the wire protocol
//! a source implements — `Connector` (handshake, check, spec) and
//! `SourceService` (streams, read) — over one [`SourceConnector`] shell.
//!
//! One handshake populates the shell (the config document validated
//! through the same [`Shell::from_value`] gate a connector's own tests
//! build the shell with);
//! every RPC before that handshake, and every handshake attempt after
//! it, is refused. [`run`] is what a spawned connector process runs;
//! [`run_on`] is the seam under it — bind at an explicit path without
//! printing anything, so a test can drive the very listener `run` would
//! have started.

use std::future::Future as _;
use std::io::Write as _;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use rdlt_connector::arrow::RecordBatch;
use rdlt_connector::channel::{
    ByteReceiver, ByteSender, ByteSized, ChannelClosed, PushPayload, RecordsIn,
};
use rdlt_connector::error::SourceError;
use rdlt_connector::source::Source as _;
use rdlt_connector::{channel, gate, source};
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::source_service_server::{SourceService, SourceServiceServer};
use rdlt_connector_protocol::proto::{
    self, CheckReply, CheckRequest, Classification, ErrorFrame, HandshakeReply, HandshakeRequest,
    StreamList, StreamsReply, StreamsRequest, check_reply, read_frame, streams_reply,
};
use rdlt_connector_protocol::{MAX_FRAME_BYTES, PROTOCOL_VERSION};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

use super::{gate as serve_gate, wire};
use crate::config::Document;
use crate::source::{Shell, SourceConnector};

/// Serve-side channel budget for one `Read` call — bounds how far the
/// connector's producer can get ahead of this process's own forwarding
/// loop before it parks, exactly like an in-process `Source::read`
/// caller. IN-CONNECTOR-BUFFER ONLY: unrelated to gRPC/h2 flow control
/// between this process and whatever dials it, and unrelated to the
/// engine's own read budget on the far side of that wire.
///
/// Public but hidden from docs: not API — exposed so the integration pin
/// derives its admission ceiling from this constant rather than
/// restating it as a literal that could silently drift.
#[doc(hidden)]
pub const READ_CHANNEL_BUDGET: usize = 8 * 1024 * 1024;

/// Budget on the frame channel one `Read` call forwards into — caps the
/// BYTES of already-encoded `ReadFrame`s QUEUED between the forwarding
/// loop and the gRPC response stream while the CLIENT stalls reading
/// (the connector's producer parks behind it, against
/// [`READ_CHANNEL_BUDGET`] on the SPI push channel).
///
/// THE WORST-CASE SUM for a spawned source's in-flight read memory:
/// this budget (32 MiB) of encoded frames queued for the wire;
/// [`READ_CHANNEL_BUDGET`] (8 MiB) of SPI pushes queued behind them; the
/// ONE push the forwarding loop holds in hand while parked — as large
/// as the connector made it (the wire refuses above `MAX_FRAME_BYTES`,
/// 64 MiB), momentarily near TWICE that for an Arrow push whose decoded
/// batch and encoded IPC bytes coexist while [`read_frame_of`] runs; and
/// whatever tonic has already pulled for the wire, deliberately NOT
/// covered — each frame's permit drops at the handover, nothing on this
/// side can know when tonic frees the bytes, and h2 flow control paces
/// that hold (measured at one frame with the peer stalled).
///
/// It counts BYTES because counting frames priced them all the same: a
/// count bound of 16 frames let a source producing ~10 MiB frames
/// plateau near 500 MB with no knob that reached it. 32 MiB is four
/// times the read channel, an order of magnitude below that, and above
/// any single frame a source is expected to produce — so the
/// at-least-one admission rule stays the exception. No operator knob,
/// deliberately: one honest constant beats a dial nobody has a number
/// for; the door, should a workload need one, is a `with_*` builder on
/// the entry points, never a config key.
///
/// Public but hidden from docs: not API — exposed so the integration pin
/// derives its admission ceiling from this constant rather than
/// restating it as a literal that could silently drift.
#[doc(hidden)]
pub const BYTE_FRAME_BUDGET: usize = 32 * 1024 * 1024;

/// Secondary message cap on the frame channel: the byte budget prices a
/// zero-byte frame at nothing, so without a message cap payload-free
/// frames (an empty push, a terminal `ErrorFrame`) could queue without
/// limit while never touching the budget.
const FRAME_MESSAGE_CAPACITY: usize = 64;

/// A served source admits at most this many concurrent `Read` RPCs —
/// the engine's default stream cap (`DEFAULT_MAX_STREAMS_PER_SOURCE`,
/// mirrored here rather than imported: the sdk does not depend on the
/// engine), since the host reads a source's streams concurrently, one
/// `Read` per stream. A host that raises its per-source stream cap past
/// 1024 over a served source will have its extra concurrent reads
/// refused `RESOURCE_EXHAUSTED`. The ceiling is a bound on a runaway
/// client, not on the host's honest budget: per-read budgets
/// ([`BYTE_FRAME_BUDGET`] + [`READ_CHANNEL_BUDGET`] + one push in hand)
/// keep any single read bounded, and this bounds their count. The
/// RETAINED-REQUEST term the count multiplies: each admitted read
/// holds its post-cap spec (identifiers length-gated, collections
/// count-capped) plus its resume cursor. The cursor is bounded in BOTH
/// dimensions — 4 MiB of arriving bytes, and
/// a bounded node count once parsed — so what a read
/// retains is its payload plus a few megabytes of structure, not the
/// tens of millions of nodes a compact document of the same byte size
/// would otherwise become. The product this ceiling multiplies is
/// therefore bounded, and the house names its numbers: at this ceiling
/// the retained cursors across a saturated connector come to a few
/// gigabytes rather than the tens the byte ceiling alone allowed. Judging the node COUNT leaves the cursor the opaque
/// document its contract says it is: no value is inspected, only
/// counted.
pub const MAX_CONCURRENT_READS: usize = 1024;

/// The role a source's handshake must be asked for.
const EXPECTED_ROLE: &str = "source";

// ---- the frame channel -----------------------------------------------------
//
// The SPI's byte-budgeted channel over ENCODED frames: a permit that
// travels with the frame and releases when tonic takes it, a secondary
// message cap, and at-least-one admission so an over-budget frame passes
// alone rather than deadlocking. What the sdk adds is the tonic-facing
// `Stream` over the receiver and a hang-up signal the forwarding loop
// can observe while parked on the connector's pushes.

/// One frame with the cost the budget charges it: the payload it
/// carries. The protobuf envelope is a handful of bytes and is not
/// modelled — the budget bounds the data a stalled reader lets pile up,
/// and every byte that can pile up is in one of the payload arms. An
/// `ErrorFrame` is terminal, tiny, and priced at nothing so it can
/// always reach a client whose budget is spent; a transport `Status`
/// ends the stream and carries no payload.
struct Frame(Result<proto::ReadFrame, Status>);

impl ByteSized for Frame {
    fn byte_size(&self) -> usize {
        match &self.0 {
            Ok(frame) => match &frame.frame {
                Some(read_frame::Frame::RawJson(bytes))
                | Some(read_frame::Frame::ArrowIpc(bytes))
                | Some(read_frame::Frame::CheckpointCursorJson(bytes)) => bytes.len(),
                Some(read_frame::Frame::Error(_)) | None => 0,
            },
            Err(_) => 0,
        }
    }
}

/// The response stream is gone: the client hung up, or the stream
/// errored out from under this task. The forwarding loop turns it into
/// the SPI's closed-channel cancellation for the connector's producer.
#[derive(Debug)]
struct StreamGone;

/// The sending half the forwarding loop holds.
struct FrameSender {
    frames: ByteSender<Frame>,
    gone: watch::Receiver<bool>,
}

impl FrameSender {
    /// Resolves once the response stream has been dropped — what lets
    /// the forwarding loop learn of a hang-up while it is parked on the
    /// connector's next push rather than on a send.
    async fn stream_gone(&self) {
        let mut gone = self.gone.clone();
        if *gone.borrow() {
            return;
        }
        let _ = gone.changed().await;
    }

    /// Send one frame, parking until its bytes fit inside the budget and
    /// a message slot is free.
    async fn send(&self, frame: Result<proto::ReadFrame, Status>) -> Result<(), StreamGone> {
        self.frames
            .send(Frame(frame))
            .await
            .map_err(|ChannelClosed| StreamGone)
    }
}

/// The `Read` response stream: frames in the order the forwarding loop
/// sent them, each releasing its budget as tonic pulls it.
struct FrameStream {
    frames: ByteReceiver<Frame>,
    gone: watch::Sender<bool>,
    /// The `Read`'s admission permit under [`MAX_CONCURRENT_READS`],
    /// held for the response stream's lifetime — released when tonic
    /// drops the stream, however the read ended. `None` for the
    /// pre-admission error streams, which hold nothing.
    _admission: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Stream for FrameStream {
    type Item = Result<proto::ReadFrame, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // A fresh `recv` future per poll: the SPI receiver exposes only
        // the async form, and tokio's channel receive is cancel-safe, so
        // dropping the future on `Pending` loses nothing. Taking the
        // value out of its permit is what releases the budget — exactly
        // when tonic takes the frame for the wire.
        let recv = std::pin::pin!(self.frames.recv());
        recv.poll(cx)
            .map(|next| next.map(|permitted| permitted.into_value().0))
    }
}

impl Drop for FrameStream {
    /// A client hang-up drops this stream. Closing the receiver closes
    /// both the message queue and the byte-budget semaphore, so a sender
    /// parked on EITHER wait observes the hang-up; the watch tells a
    /// forwarding loop parked on the connector's pushes.
    fn drop(&mut self) {
        let _ = self.gone.send(true);
        self.frames.close();
    }
}

fn frame_channel(
    byte_budget: usize,
    message_capacity: usize,
    admission: Option<tokio::sync::OwnedSemaphorePermit>,
) -> (FrameSender, FrameStream) {
    let (frames_tx, frames_rx) = channel::bytes(byte_budget, message_capacity);
    let (gone_tx, gone_rx) = watch::channel(false);
    (
        FrameSender {
            frames: frames_tx,
            gone: gone_rx,
        },
        FrameStream {
            frames: frames_rx,
            gone: gone_tx,
            _admission: admission,
        },
    )
}

/// The gRPC surface over one [`SourceConnector`]. `shell` is empty until
/// a handshake succeeds; `Arc` because the `Read` RPC hands a clone to a
/// spawned task that outlives the request.
struct SourceServer<C: SourceConnector> {
    shell: OnceLock<Arc<Shell<C>>>,
    /// The process-wide `Read` admission ceiling ([`MAX_CONCURRENT_READS`]
    /// permits): every served connection shares it.
    read_admission: Arc<tokio::sync::Semaphore>,
    /// The process-wide ceiling on concurrent `Check`/`Streams` calls
    /// ([`wire::MAX_CONCURRENT_PROBES`]): both run the connector's own
    /// code, and neither had an admission bound of its own.
    probe_admission: Arc<tokio::sync::Semaphore>,
}

impl<C: SourceConnector> SourceServer<C> {
    fn new() -> Self {
        Self {
            shell: OnceLock::new(),
            read_admission: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_READS)),
            probe_admission: Arc::new(tokio::sync::Semaphore::new(wire::MAX_CONCURRENT_PROBES)),
        }
    }

    /// The shell, once a handshake has populated it — every RPC but
    /// `Handshake` itself and the config-free `Spec` needs this.
    fn shell(&self) -> Result<&Arc<Shell<C>>, Status> {
        self.shell
            .get()
            .ok_or_else(|| Status::failed_precondition(wire::HANDSHAKE_NOT_COMPLETED))
    }
}

impl<C: SourceConnector> wire::HandshakeShell for Shell<C> {
    type Error = <C::Config as Document>::Error;

    fn from_config(value: serde_json::Value) -> Result<Self, Self::Error> {
        Shell::<C>::from_value(value)
    }

    fn connector_id(&self) -> &'static str {
        C::NAME
    }

    fn connector_version(&self) -> &'static str {
        C::VERSION
    }

    fn spec_json(&self) -> Vec<u8> {
        serde_json::to_vec(&self.spec()).expect("a ConnectorSpec serializes to JSON infallibly")
    }

    fn capabilities_json(&self) -> Vec<u8> {
        // Capabilities are a DESTINATION concern (merge/replace/widen
        // support) — a source has none to advertise.
        Vec::new()
    }
}

/// Flatten a classified [`SourceError`] into the wire's [`ErrorFrame`]:
/// the classification as the enum, the INNER cause's text as the
/// message, and the rate-limit hint when there is one.
///
/// `ErrorFrame.message` is the CAUSE text, never the SPI's `Display`
/// frame: the receiving client renders the classification frame exactly
/// once on reconstruction, and a foreign server authoring its own cause
/// text cannot know rdlt's spellings anyway. Nothing is lost — context
/// is already folded into the inner cause. The wildcard arm is required
/// (`SourceError` is `#[non_exhaustive]` from outside its crate): an
/// unknown classification lands FATAL with the full rendered `Display`
/// as its message — a fallback that may double-frame on reconstruction,
/// preferred over dropping text.
fn source_error_frame(error: &SourceError) -> ErrorFrame {
    let (classification, message, retry_after) = match error {
        SourceError::Transient(cause) => (Classification::Transient, cause.to_string(), None),
        SourceError::RateLimited {
            retry_after,
            source,
        } => (
            Classification::RateLimited,
            source.to_string(),
            *retry_after,
        ),
        SourceError::Fatal(cause) => (Classification::Fatal, cause.to_string(), None),
        _ => (Classification::Fatal, error.to_string(), None),
    };
    wire::error_frame(classification, message, retry_after)
}

#[tonic::async_trait]
impl<C: SourceConnector> Connector for SourceServer<C> {
    async fn handshake(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeReply>, Status> {
        Ok(wire::handshake(
            &self.shell,
            EXPECTED_ROLE,
            request.into_inner(),
        ))
    }

    async fn check(&self, _request: Request<CheckRequest>) -> Result<Response<CheckReply>, Status> {
        // Admission BEFORE the connector's own code runs: what a check
        // costs is the connector's business, and unbounded concurrent
        // ones are the caller's choice, not the connector's.
        let _probe = Arc::clone(&self.probe_admission)
            .try_acquire_owned()
            .map_err(|_| wire::probes_exhausted())?;
        let shell = self.shell()?;
        let outcome = match shell.check().await {
            Ok(()) => check_reply::Outcome::Ok(proto::Empty {}),
            Err(error) => check_reply::Outcome::Error(source_error_frame(&error)),
        };
        Ok(Response::new(CheckReply {
            outcome: Some(outcome),
        }))
    }

    async fn spec(
        &self,
        _request: Request<proto::SpecRequest>,
    ) -> Result<Response<proto::SpecReply>, Status> {
        // Static identity: exempt from the pre-handshake refusal BY
        // CONSTRUCTION, since it never calls `shell()`.
        Ok(wire::spec_reply(C::NAME, C::VERSION, C::config_schema()))
    }
}

/// The declaration a `Streams` reply carries, joined under THE FRAMING
/// RULE (the proto field's contract): one JSON document per line,
/// single `\n` joins, no trailing newline, empty = zero streams. JSON
/// cannot carry a raw newline inside a string, so the join is
/// unambiguous for the client's line-wise gates.
///
/// The gates run HERE, before the join builds: a served connector that
/// would emit a reply the wire cannot carry must learn it from its own
/// refusal rather than by building gigabytes and watching the encode
/// cap reject the frame. Three bounds, each answering a different way
/// to be too big: the COUNT (the SPI's one ceiling, which the dialing
/// side holds the same reply to), each LINE against the document
/// bound, and the running TOTAL against the frame — because a thousand
/// individually-legal lines still make a blob no frame carries, and
/// discovering that at the encode cap means having built it.
fn declaration_jsonl(streams: &[source::StreamSpec]) -> Result<Vec<u8>, String> {
    if streams.len() > gate::MAX_DECLARED_STREAM_SPECS {
        return Err(format!(
            "this connector declares {} streams — over the {} the wire admits",
            streams.len(),
            gate::MAX_DECLARED_STREAM_SPECS
        ));
    }
    // Joined in place: one copy, and the total is known as it grows
    // rather than after a second one.
    let mut blob: Vec<u8> = Vec::new();
    for stream in streams {
        let line = serde_json::to_vec(stream).expect("a StreamSpec serializes to JSON infallibly");
        gate::refuse_oversized_document("a declared stream spec", &line)?;
        let delimiter = usize::from(!blob.is_empty());
        if blob.len() + delimiter + line.len() > MAX_FRAME_BYTES {
            return Err(format!(
                "this connector's declaration exceeds the {MAX_FRAME_BYTES}-byte frame the \
                 wire carries"
            ));
        }
        if delimiter == 1 {
            blob.push(b'\n');
        }
        blob.extend_from_slice(&line);
    }
    Ok(blob)
}

#[tonic::async_trait]
impl<C: SourceConnector> SourceService for SourceServer<C> {
    async fn streams(
        &self,
        _request: Request<StreamsRequest>,
    ) -> Result<Response<StreamsReply>, Status> {
        // Admission BEFORE the connector enumerates: see `check`.
        let _probe = Arc::clone(&self.probe_admission)
            .try_acquire_owned()
            .map_err(|_| wire::probes_exhausted())?;
        let shell = self.shell()?;
        let outcome = match shell.streams().await {
            Ok(streams) => match declaration_jsonl(&streams) {
                Ok(stream_specs_jsonl) => {
                    streams_reply::Outcome::Ok(StreamList { stream_specs_jsonl })
                }
                Err(message) => streams_reply::Outcome::Error(wire::error_frame(
                    Classification::Fatal,
                    message,
                    None,
                )),
            },
            Err(error) => streams_reply::Outcome::Error(source_error_frame(&error)),
        };
        Ok(Response::new(StreamsReply {
            outcome: Some(outcome),
        }))
    }

    type ReadStream = FrameStream;

    async fn read(
        &self,
        request: Request<proto::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let shell = Arc::clone(self.shell()?);
        // Admission BEFORE any per-read allocation: a read past the
        // ceiling is a protocol-state refusal (a `Status`, like the
        // destination's one-session ceiling), not a connector outcome.
        let admission = Arc::clone(&self.read_admission)
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted(format!(
                    "{MAX_CONCURRENT_READS} concurrent reads per connector process — the \
                     ceiling is reached"
                ))
            })?;
        let request = request.into_inner();

        // A request document that fails its gate answers INSIDE the
        // response stream — first and only frame a terminal FATAL
        // `ErrorFrame`, never a `Status`: an undecodable payload is a
        // connector-outcome refusal, not a protocol-state violation.
        // The size gates run BEFORE each parse: a compact document
        // materializes as an untyped `Value` at many times its wire
        // size, and both documents are RETAINED for the read's lifetime.
        // The cursor's bound is the cursor contract's own — tighter than
        // the config ceiling, and the same constant the client enforces
        // pre-send, so the two ends cannot disagree.
        if let Err(message) =
            gate::refuse_oversized_document("stream_spec_json", &request.stream_spec_json)
        {
            return Ok(error_stream(message).await);
        }
        let stream_spec: source::StreamSpec =
            match serde_json::from_slice(&request.stream_spec_json) {
                Ok(spec) => spec,
                Err(error) => {
                    return Ok(error_stream(format!(
                        "invalid stream_spec_json: {}",
                        gate::describe_parse_error(&error)
                    ))
                    .await);
                }
            };
        // The spec's identifiers — name, key fields, cursor field,
        // type-hint keys — are RETAINED for the read's lifetime and
        // quoted by connector refusals, so each rides the same wire
        // identifier ceiling the session seats hold theirs to. The
        // document ceiling above bounds the whole spec; this bounds any
        // ONE identifier, which a single multi-MiB name would otherwise
        // pass through.
        if let Err(message) = serve_gate::refuse_oversized_spec_identifiers(&stream_spec) {
            return Ok(error_stream(message).await);
        }
        let since = match &request.since_cursor_json {
            None => None,
            Some(bytes) if bytes.len() as u64 > gate::MAX_CURSOR_BYTES => {
                return Ok(error_stream(wire::oversized_cursor(
                    "since_cursor_json",
                    bytes.len(),
                    gate::MAX_CURSOR_BYTES,
                ))
                .await);
            }
            Some(bytes) => match serde_json::from_slice::<serde_json::Value>(bytes) {
                Ok(value) => {
                    // The byte ceiling above bounded what ARRIVED; this
                    // bounds what the arrival BECAME, the dimension a
                    // compact document expands worst — and it judges
                    // only the count, never a value, so the cursor
                    // stays opaque.
                    if let Err(message) =
                        serve_gate::refuse_dense_cursor("since_cursor_json", &value)
                    {
                        return Ok(error_stream(message).await);
                    }
                    Some(rdlt_connector::core::cursor::Cursor::new(value))
                }
                Err(error) => {
                    return Ok(error_stream(format!(
                        "invalid since_cursor_json: {}",
                        gate::describe_parse_error(&error)
                    ))
                    .await);
                }
            },
        };

        let (out, records_in) = channel::records(READ_CHANNEL_BUDGET);
        let read_request = source::ReadRequest::new(stream_spec, since, out);

        let read_task: JoinHandle<Result<(), SourceError>> =
            tokio::spawn(async move { shell.read(read_request).await });

        let (frame_tx, frame_rx) =
            frame_channel(BYTE_FRAME_BUDGET, FRAME_MESSAGE_CAPACITY, Some(admission));

        tokio::spawn(forward_read_frames(
            records_in,
            frame_tx,
            read_task,
            read_frame_of,
        ));

        Ok(Response::new(frame_rx))
    }
}

/// The forwarding loop between the connector's SPI pushes and the gRPC
/// response stream: translate every push through `encode`, park on the
/// frame channel's byte budget, and wind the read task down on every
/// exit path — client hang-up, encode failure, or the connector's own
/// end of stream.
///
/// `encode` is a parameter, not a hardcoded call, because its failure
/// arm cannot be reached from data: a well-formed batch encodes its own
/// schema infallibly, so the suite injects a failing encoder to drive
/// the encode-failure interleavings deterministically.
async fn forward_read_frames(
    mut records_in: RecordsIn,
    frame_tx: FrameSender,
    read_task: JoinHandle<Result<(), SourceError>>,
    encode: fn(PushPayload) -> Result<proto::ReadFrame, String>,
) {
    // The proto calls the Error frame TERMINAL: once one is on the
    // stream nothing may follow it — in particular the read task's own
    // eventual `Err` (its push observed the closed channel) must not
    // append a second "terminal" frame behind the first.
    let mut terminal_sent = false;
    let mut abort_reader = false;
    loop {
        let push = tokio::select! {
            push = records_in.recv() => push,
            () = frame_tx.stream_gone() => {
                records_in.close();
                abort_reader = true;
                break;
            }
        };
        let Some(push) = push else {
            break;
        };
        let frame = match encode(push.payload) {
            Ok(frame) => frame,
            Err(message) => {
                // A batch that failed to encode must not just vanish —
                // silently dropping it makes a truncated read look like
                // a clean end of stream. Send ONE terminal error frame,
                // then close the SPI channel exactly like a client
                // hang-up: the read task winds down via Break.
                let frame = proto::ReadFrame {
                    frame: Some(read_frame::Frame::Error(wire::error_frame(
                        Classification::Fatal,
                        message,
                        None,
                    ))),
                };
                let _ = frame_tx.send(Ok(frame)).await;
                terminal_sent = true;
                abort_reader = true;
                records_in.close();
                break;
            }
        };
        if frame_tx.send(Ok(frame)).await.is_err() {
            // The client hung up: closing BOTH halves of the SPI channel
            // — the message queue and the byte-budget semaphore a
            // producer may be parked on — turns that into the Break the
            // connector's next push observes.
            records_in.close();
            abort_reader = true;
            break;
        }
    }

    if abort_reader {
        read_task.abort();
    }

    match read_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) if !terminal_sent => {
            let frame = proto::ReadFrame {
                frame: Some(read_frame::Frame::Error(source_error_frame(&error))),
            };
            let _ = frame_tx.send(Ok(frame)).await;
        }
        // A terminal ErrorFrame is already on the stream — the encode
        // failure it reported is the diagnosis; the read task's
        // follow-on error is downstream noise.
        Ok(Err(_)) => {}
        // Same gate: the abort this loop itself requested turns the read
        // task's completion into a cancelled JoinError — transport noise
        // behind a diagnosis already delivered.
        Err(join_error) if !terminal_sent => {
            let _ = frame_tx
                .send(Err(Status::internal(format!(
                    "connector read task did not complete: {join_error}"
                ))))
                .await;
        }
        Err(_) => {}
    }
}

/// An already-terminated `Read` response stream whose first and only
/// frame is a terminal FATAL [`ErrorFrame`] carrying `message` — what a
/// request-decode failure answers with. One message slot and no byte
/// budget: an `ErrorFrame` is priced at nothing, so the enqueue cannot
/// park.
async fn error_stream(message: String) -> Response<FrameStream> {
    let (frame_tx, frame_rx) = frame_channel(0, 1, None);
    let frame = proto::ReadFrame {
        frame: Some(read_frame::Frame::Error(wire::error_frame(
            Classification::Fatal,
            message,
            None,
        ))),
    };
    frame_tx
        .send(Ok(frame))
        .await
        .expect("a fresh channel with capacity 1 accepts its one frame");
    Response::new(frame_rx)
}

/// One SPI push, translated to its wire shape — the payload picks the
/// oneof arm; nothing here inspects the connector or the request. `Err`
/// only for the Arrow arm, the one fallible step (the caller turns it
/// into a terminal `ErrorFrame` rather than a panic).
fn read_frame_of(payload: PushPayload) -> Result<proto::ReadFrame, String> {
    let frame = match payload {
        PushPayload::RawJson(bytes) => read_frame::Frame::RawJson(bytes.to_vec()),
        PushPayload::Arrow(batch) => read_frame::Frame::ArrowIpc(encode_arrow_ipc(&batch)?),
        PushPayload::Checkpoint(cursor) => read_frame::Frame::CheckpointCursorJson(
            serde_json::to_vec(cursor.as_value()).expect("a Cursor's value serializes infallibly"),
        ),
    };
    Ok(proto::ReadFrame { frame: Some(frame) })
}

/// One Arrow batch as an IPC *stream* (not the `File` container — no
/// footer, a schema message followed by one record-batch message,
/// exactly what a single-batch push needs). Writing into an in-memory
/// `Vec` fails only on a schema/batch mismatch the connector itself
/// produced — an `expect()` would turn that into a panicked task
/// indistinguishable, from the client's side, from a clean end of
/// stream.
fn encode_arrow_ipc(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), batch.schema_ref())
        .map_err(|error| format!("opening an arrow ipc stream writer: {error}"))?;
    writer
        .write(batch)
        .map_err(|error| format!("writing an arrow ipc record batch: {error}"))?;
    writer
        .into_inner()
        .map_err(|error| format!("closing an arrow ipc stream writer: {error}"))
}

/// Bind at an explicit path and return the [`Line`] a spawning host
/// would read from stdout, plus a handle for the serving task — WITHOUT
/// printing anything. [`run`] is this at a self-minted temp path plus
/// the printing a spawned connector process must do; this is the seam a
/// test drives directly.
///
/// Both gRPC services are wired to the SAME `SourceServer` instance
/// (`from_arc`, not two independent `new`s) — they share one
/// handshake-populated shell, so a `Streams` or `Read` call sees the
/// config a prior `Handshake` validated. `max_decoding_message_size` on
/// BOTH: tonic's 4 MiB default receive cap is below what one legitimate
/// frame may carry.
pub async fn run_on<C: SourceConnector>(
    path: impl AsRef<Path>,
) -> Result<(Line, JoinHandle<Result<(), wire::Error>>), wire::Error> {
    let path = path.as_ref();
    let listener = wire::bind(path)?;
    let incoming = UnixListenerStream::new(listener);

    let server = Arc::new(SourceServer::<C>::new());
    let serving = tonic::transport::Server::builder()
        .add_service(
            ConnectorServer::from_arc(Arc::clone(&server))
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_FRAME_BYTES),
        )
        .add_service(
            SourceServiceServer::from_arc(server)
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_FRAME_BYTES),
        )
        .serve_with_incoming(incoming);

    let handle = tokio::spawn(async move { serving.await.map_err(wire::Error::Serve) });

    Ok((
        Line {
            socket_path: path.to_path_buf(),
            protocol_min: PROTOCOL_VERSION,
            protocol_max: PROTOCOL_VERSION,
        },
        handle,
    ))
}

/// Turn a [`SourceConnector`] into an out-of-process protocol server:
/// bind a fresh Unix domain socket in a private per-process directory
/// under the system temp directory, print the handshake line on stdout
/// (flushed — the spawning host is reading a pipe, not a TTY), then
/// serve until the process is killed.
pub async fn run<C: SourceConnector>() -> Result<(), wire::Error> {
    let (line, handle) = run_on::<C>(wire::socket_path()?).await?;

    // `writeln!`, not `println!`: a spawning host that exits or never
    // reads its child's stdout leaves this write facing a broken pipe,
    // and `println!` panics on an IO error rather than surfacing one.
    let mut stdout = std::io::stdout();
    writeln!(stdout, "{}", line.render()).map_err(wire::Error::Stdout)?;
    stdout.flush().map_err(wire::Error::Stdout)?;

    handle.await.map_err(wire::Error::Join)?
}

#[cfg(test)]
mod tests {
    //! The frame channel's sdk-side properties, driven directly: what
    //! each frame costs the budget, and that a dropped stream reaches a
    //! parked sender and ends the stream at a terminal frame. The
    //! admission rule itself is the SPI channel's, pinned there; the
    //! wire-level consequence — what a STALLED reader lets pile up end
    //! to end — is pinned in `tests/cases/test_serve_source.rs`.
    use std::time::Duration;

    use tokio_stream::StreamExt as _;

    use super::*;

    /// The declaration is bounded in THREE ways, and this is the one a
    /// per-item gate cannot give: a thousand individually-legal lines
    /// still make a blob no frame carries. Discovering that at the
    /// encode cap would mean having built it.
    #[test]
    fn a_declaration_too_large_for_a_frame_refuses_before_it_is_built() {
        // Lines just under the document ceiling, enough of them to pass
        // the frame: every one legal alone, the total impossible.
        let wide = "w".repeat((rdlt_connector::gate::MAX_DOCUMENT_BYTES as usize) - 1024);
        let streams: Vec<source::StreamSpec> = (0..16)
            .map(|i| source::StreamSpec::new(format!("{wide}{i}")))
            .collect();
        let refusal = declaration_jsonl(&streams).expect_err("the total is past a frame");
        assert!(
            refusal.contains("frame"),
            "the refusal names what it could not fit: {refusal}"
        );
    }

    /// The declaration's gates run at the EMIT, not only at the
    /// client's decode: a connector that would declare more streams
    /// than the wire admits learns it from its own refusal instead of
    /// building the whole blob and watching the frame cap reject it.
    /// The count is the client's own, mirrored by value.
    #[test]
    fn the_declaration_refuses_more_streams_than_the_wire_admits() {
        let honest: Vec<source::StreamSpec> = (0..1024)
            .map(|i| source::StreamSpec::new(format!("s{i}")))
            .collect();
        let jsonl = declaration_jsonl(&honest).expect("1024 streams are admitted");
        assert_eq!(
            jsonl.iter().filter(|byte| **byte == b'\n').count(),
            1023,
            "the join is one newline between documents, none trailing"
        );

        let flood: Vec<source::StreamSpec> = (0..1025)
            .map(|i| source::StreamSpec::new(format!("s{i}")))
            .collect();
        let refusal = declaration_jsonl(&flood).expect_err("1025 streams are refused");
        assert!(
            refusal.contains("1025") && refusal.contains("1024"),
            "the refusal names the count and the ceiling: {refusal}"
        );
    }

    /// Zero streams is the empty blob, the framing rule's own
    /// stated case — not an error, and not a single empty line.
    #[test]
    fn a_source_declaring_nothing_emits_an_empty_blob() {
        assert!(
            declaration_jsonl(&[])
                .expect("zero streams are admitted")
                .is_empty(),
            "empty means zero streams"
        );
    }

    /// Every wait that must SUCCEED is bounded: the failure mode under
    /// test is a hang, and an unbounded await would report it as a
    /// timeout of the whole suite rather than a named failure.
    const BOUND: Duration = Duration::from_secs(5);

    /// Long enough that a send which is going to complete has, short
    /// enough to keep the suite quick.
    const PARKED: Duration = Duration::from_millis(50);

    /// Roomy enough that the message cap can never fire first — these
    /// tests are about the BYTE budget.
    const ROOMY: usize = 256;

    fn frame_of(bytes: usize) -> Result<proto::ReadFrame, Status> {
        Ok(proto::ReadFrame {
            frame: Some(read_frame::Frame::RawJson(vec![b'x'; bytes])),
        })
    }

    fn error_frame_of(message: &str) -> Result<proto::ReadFrame, Status> {
        Ok(proto::ReadFrame {
            frame: Some(read_frame::Frame::Error(wire::error_frame(
                Classification::Fatal,
                message.to_string(),
                None,
            ))),
        })
    }

    #[test]
    fn a_frame_costs_the_payload_it_carries() {
        assert_eq!(Frame(frame_of(1234)).byte_size(), 1234);
        assert_eq!(
            Frame(Ok(proto::ReadFrame {
                frame: Some(read_frame::Frame::ArrowIpc(vec![0; 77])),
            }))
            .byte_size(),
            77
        );
        assert_eq!(
            Frame(Ok(proto::ReadFrame {
                frame: Some(read_frame::Frame::CheckpointCursorJson(vec![0; 9])),
            }))
            .byte_size(),
            9
        );
        // Terminal, tiny, and deliberately free.
        assert_eq!(Frame(error_frame_of("boom")).byte_size(), 0);
        assert_eq!(Frame(Ok(proto::ReadFrame { frame: None })).byte_size(), 0);
        assert_eq!(Frame(Err(Status::internal("gone"))).byte_size(), 0);
    }

    #[tokio::test]
    async fn a_payload_free_frame_passes_a_fully_spent_budget() {
        // The terminal ErrorFrame is the diagnosis a client is waiting
        // for; gating it on a budget the stalled client itself spent
        // would hide exactly the failures that matter most.
        let (sender, mut stream) = frame_channel(100, ROOMY, None);
        sender
            .send(frame_of(100))
            .await
            .expect("exactly the budget");
        tokio::time::timeout(BOUND, sender.send(error_frame_of("induced")))
            .await
            .expect("a payload-free frame must not wait on the budget")
            .expect("the stream is still there");
        assert_eq!(
            Frame(Ok(stream.next().await.unwrap().unwrap())).byte_size(),
            100
        );
        assert!(matches!(
            stream.next().await.unwrap().unwrap().frame,
            Some(read_frame::Frame::Error(_))
        ));
    }

    /// The encode-failure arm ends the stream AT its terminal
    /// ErrorFrame even when the connector's read task is still running:
    /// the loop aborts that task, the abort surfaces as a `JoinError`,
    /// and the join arm must NOT append a transport `Status` behind the
    /// terminal frame. The encoder is injected because a well-formed
    /// batch cannot make the production encoder fail.
    #[tokio::test]
    async fn an_encode_failure_while_the_reader_runs_ends_the_stream_at_the_terminal_frame() {
        fn refuse(_: PushPayload) -> Result<proto::ReadFrame, String> {
            Err("induced encode failure".to_string())
        }
        let (mut out, records_in) = channel::records(1 << 20);
        // A task parked forever stands in for a connector mid-read, so
        // the loop's abort turns its completion into a cancelled
        // JoinError — exactly the interleaving under test.
        let read_task: tokio::task::JoinHandle<Result<(), SourceError>> =
            tokio::spawn(async { std::future::pending().await });
        out.rows([serde_json::json!({"n": 1})])
            .await
            .expect("the push is admitted");

        let (frame_tx, mut stream) = frame_channel(1 << 20, ROOMY, None);
        let loop_task = tokio::spawn(forward_read_frames(records_in, frame_tx, read_task, refuse));

        let first = tokio::time::timeout(BOUND, stream.next())
            .await
            .expect("the terminal frame arrives")
            .expect("the stream is not empty")
            .expect("a frame, not a transport Status");
        match first.frame {
            Some(read_frame::Frame::Error(error)) => {
                assert_eq!(error.message, "induced encode failure");
                assert_eq!(error.classification, Classification::Fatal as i32);
            }
            other => panic!("expected the terminal ErrorFrame, got {other:?}"),
        }
        let after = tokio::time::timeout(BOUND, stream.next())
            .await
            .expect("the stream ends rather than hanging");
        assert!(
            after.is_none(),
            "a transport Status followed the terminal ErrorFrame: {after:?}"
        );
        tokio::time::timeout(BOUND, loop_task)
            .await
            .expect("the forwarding loop winds down")
            .expect("the forwarding loop does not panic");
        drop(out);
    }

    #[tokio::test]
    async fn dropping_the_stream_wakes_a_sender_parked_on_the_budget() {
        // A client hang-up must reach a sender waiting on the
        // SEMAPHORE, not just one waiting for a message slot —
        // otherwise the forwarding loop never learns to cancel the
        // connector, and the read task hangs on forever.
        let (sender, stream) = frame_channel(8, ROOMY, None);
        sender
            .send(frame_of(8))
            .await
            .expect("the first frame fits");
        let parked = tokio::spawn(async move { sender.send(frame_of(8)).await });
        tokio::time::sleep(PARKED).await; // let it park
        drop(stream);
        let result = tokio::time::timeout(BOUND, parked)
            .await
            .expect("the hang-up must wake the parked sender")
            .expect("task joins");
        assert!(
            result.is_err(),
            "the woken sender is told the stream is gone, so the read can be cancelled"
        );
    }

    /// The hang-up signal reaches a forwarding loop parked on the
    /// connector's NEXT push, not only one parked on a send.
    #[tokio::test]
    async fn dropping_the_stream_resolves_stream_gone() {
        let (sender, stream) = frame_channel(8, ROOMY, None);
        let parked = tokio::spawn(async move { sender.stream_gone().await });
        tokio::time::sleep(PARKED).await;
        drop(stream);
        tokio::time::timeout(BOUND, parked)
            .await
            .expect("the hang-up resolves the wait")
            .expect("task joins");
    }
}
