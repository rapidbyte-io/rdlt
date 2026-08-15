//! The source half of `serve()`: `SourceServer` answers both halves of
//! the wire protocol a source connector implements — `Connector`
//! (handshake, check) and `SourceService` (streams, read) — over one
//! [`SourceConnector`] shell.
//!
//! One handshake populates the shell (config document validated the
//! same way an in-process embedder would validate it, through
//! [`Shell::from_value`]); every RPC before that handshake, and every
//! handshake attempt after it, is refused. [`source`] is what a spawned
//! connector process actually runs; [`serve_on`] is the seam under it —
//! bind at an explicit path without printing anything, so a test can
//! drive the very listener `source` would have started, without stdout
//! capture.

use std::io::Write as _;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use rdlt_connector::{PushPayload, Source as _, SourceError, records_channel};
use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::source_service_server::{SourceService, SourceServiceServer};
use rdlt_connector_protocol::proto::{
    self, CheckReply, CheckRequest, Classification, ErrorFrame, HandshakeReply, HandshakeRequest,
    StreamList, StreamsReply, StreamsRequest, check_reply, read_frame, streams_reply,
};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

use super::common::{self, ServeError};
use crate::config::Document;
use crate::source::{Shell, SourceConnector};

/// Serve-side channel budget for one `Read` call — bounds how far the
/// connector's producer can get ahead of this process's own forwarding
/// loop before it parks, exactly like an in-process `Source::read`
/// caller. It is IN-CONNECTOR-BUFFER ONLY: unrelated to gRPC/h2 flow
/// control between this process and whatever dials it (see
/// `serve/common.rs`), and unrelated to the engine's own read budget,
/// which governs the far side of that wire starting at 039's adapter.
///
/// Public but hidden from docs: not API — exposed so the integration
/// pin derives its admission ceiling from this constant rather than
/// restating it as a literal that could silently drift.
#[doc(hidden)]
pub const READ_CHANNEL_BUDGET: usize = 8 * 1024 * 1024;

/// Budget on the frame channel one `Read` call forwards into — caps the
/// BYTES of already-encoded `ReadFrame`s QUEUED between the forwarding
/// loop and the gRPC response stream while the CLIENT stalls reading
/// (the connector's producer parks behind it, against
/// [`READ_CHANNEL_BUDGET`]'s byte budget on the SPI push channel).
///
/// THE WORST-CASE SUM for a spawned source's in-flight read memory —
/// authoritative; the operator docs and `BatchPolicy::every_bytes`'s
/// doc both delegate the arithmetic here:
///
/// - this budget (32 MiB) of encoded frames queued for the wire;
/// - [`READ_CHANNEL_BUDGET`] (8 MiB) of SPI pushes queued behind them;
/// - the ONE push the forwarding loop holds in hand while parked on
///   this budget — as large as the connector made it (the wire refuses
///   above `common::MAX_FRAME_BYTES`, 64 MiB), and momentarily near
///   TWICE that for an Arrow push, whose decoded batch and encoded IPC
///   bytes coexist while [`read_frame_of`] runs;
/// - whatever tonic has already pulled for the wire, which this budget
///   deliberately does NOT cover: each frame's permit drops at the
///   handover (nothing on this side can know when tonic frees the
///   bytes), and h2 flow control is what paces that hold — the
///   admission pin measured it at one frame with the peer stalled.
///
/// IT COUNTS BYTES BECAUSE COUNTING FRAMES PRICED THEM ALL THE SAME.
/// This channel was bounded at 16 already-encoded frames regardless of
/// size, which for a postgres source's ~10 MiB frames measured a ~500 MB
/// out-of-box plateau (043's operator finding) — and no engine-side knob
/// reached it: `batch_policy.every_bytes` is the ENGINE's accumulate
/// cadence, not this buffer, so an operator watching a spawned
/// connector's memory had nothing to turn. A byte budget prices a
/// 64-byte checkpoint and an 8 MiB batch as what they are.
///
/// 32 MiB (D-046-1): four times the read channel, an order of
/// magnitude below that old worst case, and above any single frame a
/// source in this tree produces — so the at-least-one rule below stays
/// the exception it is meant to be rather than the normal path.
///
/// NO OPERATOR KNOB, deliberately (YAGNI): one honest constant beats a
/// dial nobody has a number for. Should a real workload ever need one,
/// the door is a `with_*` builder on the serve entry points — not a
/// config key, which would put a memory bound in the same document as
/// connection details and make it part of the frozen config vocabulary.
///
/// Public but hidden from docs: not API — exposed so the integration
/// pin derives its admission ceiling from this constant rather than
/// restating it as a literal that could silently drift.
#[doc(hidden)]
pub const BYTE_FRAME_BUDGET: usize = 32 * 1024 * 1024;

/// Secondary message cap on the frame channel, for the same reason the
/// SPI's records channel keeps one: the byte budget prices a zero-byte
/// message at nothing, so without a message cap frames carrying no
/// payload (an empty push, a terminal `ErrorFrame`) could queue without
/// limit while never touching the budget.
const FRAME_MESSAGE_CAPACITY: usize = 64;

/// The role a source's handshake must be asked for — the mirrored
/// spelling lives on the destination side (`serve::destination`'s own
/// `EXPECTED_ROLE`).
const EXPECTED_ROLE: &str = "source";

// ---- the byte-budgeted frame channel ---------------------------------------
//
// The admission rule is the SPI's (`rdlt_connector::channel`): a budget
// in bytes, a permit that travels WITH the value and releases when the
// value is taken, a secondary message cap, and at-least-one admission so
// an over-budget value still passes rather than deadlocking. The SPI
// owns that rule for pushes; this is the same rule over ENCODED frames,
// written here rather than reused because the receiving half must be a
// `Stream` for tonic to poll, and the SPI's `ByteReceiver` exposes only
// an `async fn recv`. Any fix to the rule belongs in both places.

/// One encoded frame plus the budget it was admitted under. The permit
/// rides WITH the frame and drops the moment [`FrameStream`] hands the
/// frame to tonic, so the budget bounds QUEUED frames only: bytes
/// tonic has pulled for the wire are the transport's, deliberately
/// unbudgeted — nothing on this side of the handover can know when
/// tonic frees them, and h2 flow control is what paces that hold.
/// [`BYTE_FRAME_BUDGET`]'s worst-case sum names the transport term.
struct BudgetedFrame {
    frame: Result<proto::ReadFrame, Status>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// The response stream is gone: the client hung up, or the stream
/// errored out from under this task. The forwarding loop turns it into
/// the SPI's closed-channel cancellation for the connector's producer.
#[derive(Debug)]
struct StreamGone;

/// The sending half the forwarding loop holds.
struct FrameSender {
    frames: mpsc::Sender<BudgetedFrame>,
    budget: Arc<Semaphore>,
    budget_total: usize,
    gone: watch::Receiver<bool>,
}

impl FrameSender {
    async fn stream_gone(&self) {
        let mut gone = self.gone.clone();
        if *gone.borrow() {
            return;
        }
        let _ = gone.changed().await;
    }

    /// Send one frame, parking until its encoded bytes fit inside the
    /// budget (and a message slot is free).
    ///
    /// A frame larger than the ENTIRE budget must still pass — refusing
    /// it, or waiting on permits that cannot exist, would deadlock a
    /// legal read — so its request is capped at the budget total and it
    /// degrades to "wait for the whole budget, then go alone". The
    /// `u32` saturation matters only for a budget above `u32::MAX`
    /// (semaphore permits are `u32`), where a saturated request is
    /// still the same drain-everything request.
    async fn send(&self, frame: Result<proto::ReadFrame, Status>) -> Result<(), StreamGone> {
        let bytes = match &frame {
            Ok(frame) => frame_bytes(frame),
            // A transport `Status` carries diagnostic text and ends the
            // stream; there is no payload to budget.
            Err(_) => 0,
        };
        let requested = bytes.min(self.budget_total).try_into().unwrap_or(u32::MAX);
        // Zero-byte frames skip the semaphore entirely — the message cap
        // is what bounds them. Acquiring zero permits would also
        // succeed; skipping states the intent, and keeps such a frame
        // passing even against a fully spent budget.
        let permit = if requested > 0 {
            Some(
                Arc::clone(&self.budget)
                    .acquire_many_owned(requested)
                    .await
                    .map_err(|_| StreamGone)?,
            )
        } else {
            None
        };
        self.frames
            .send(BudgetedFrame {
                frame,
                _permit: permit,
            })
            .await
            .map_err(|_| StreamGone)
    }
}

/// The `Read` response stream: frames in the order the forwarding loop
/// sent them, each releasing its budget as it is pulled.
struct FrameStream {
    frames: mpsc::Receiver<BudgetedFrame>,
    budget: Arc<Semaphore>,
    gone: watch::Sender<bool>,
}

impl Stream for FrameStream {
    type Item = Result<proto::ReadFrame, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Taking `frame` out of the wrapper drops the permit with it:
        // the budget releases exactly when tonic takes the frame for
        // the wire, not when the forwarding loop sent it.
        self.frames
            .poll_recv(cx)
            .map(|budgeted| budgeted.map(|budgeted| budgeted.frame))
    }
}

impl Drop for FrameStream {
    /// A client hang-up drops this stream, and a sender PARKED on the
    /// byte budget is waiting on the SEMAPHORE — a wait the message
    /// queue's own closure says nothing about. Closing both makes the
    /// hang-up an event the forwarding loop observes from EITHER wait,
    /// exactly as the SPI's `ByteReceiver::close` closes both halves of
    /// a records channel.
    ///
    /// It is deliberately explicit rather than left to emerge: dropping
    /// the receiver also drops the frames still queued, releasing their
    /// permits, which would eventually let a parked sender acquire and
    /// then fail on the closed queue instead — so a test cannot tell
    /// the two apart, and removing these lines leaves the suite green.
    /// That equivalence rests on when a dropped receiver releases
    /// buffered messages, which is tokio's business and not a promise
    /// this cancellation path should be built on: the connector's read
    /// task hanging forever is the failure at the other end of it.
    fn drop(&mut self) {
        let _ = self.gone.send(true);
        self.frames.close();
        self.budget.close();
    }
}

/// A frame channel: `byte_budget` caps the encoded bytes in flight,
/// `message_capacity` is the secondary cap that keeps payload-free
/// frames from queueing without limit.
fn frame_channel(byte_budget: usize, message_capacity: usize) -> (FrameSender, FrameStream) {
    let budget = Arc::new(Semaphore::new(byte_budget));
    let (frames_tx, frames_rx) = mpsc::channel(message_capacity);
    let (gone_tx, gone_rx) = watch::channel(false);
    (
        FrameSender {
            frames: frames_tx,
            budget: Arc::clone(&budget),
            budget_total: byte_budget,
            gone: gone_rx,
        },
        FrameStream {
            frames: frames_rx,
            budget,
            gone: gone_tx,
        },
    )
}

/// What a frame costs the budget: the payload it carries. The protobuf
/// envelope around that payload is a handful of bytes on top and is
/// deliberately not modelled — the budget bounds the data a stalled
/// reader lets pile up, and every byte that can actually pile up is in
/// one of these arms. An `ErrorFrame` is terminal, tiny, and priced at
/// nothing so it can always reach a client whose budget is spent.
fn frame_bytes(frame: &proto::ReadFrame) -> usize {
    match &frame.frame {
        Some(read_frame::Frame::RawJson(bytes)) => bytes.len(),
        Some(read_frame::Frame::ArrowIpc(bytes)) => bytes.len(),
        Some(read_frame::Frame::CheckpointCursorJson(bytes)) => bytes.len(),
        Some(read_frame::Frame::Error(_)) | None => 0,
    }
}

/// The gRPC surface over one [`SourceConnector`]. `shell` is empty until
/// a handshake succeeds; `Arc` (not a bare `Shell<C>`) because the `Read`
/// RPC hands a clone to a spawned task that outlives the request.
struct SourceServer<C: SourceConnector> {
    shell: OnceLock<Arc<Shell<C>>>,
}

impl<C: SourceConnector> SourceServer<C> {
    fn new() -> Self {
        Self {
            shell: OnceLock::new(),
        }
    }

    /// The shell, once handshake has populated it — every RPC but
    /// `Handshake` itself and the config-free `Spec` needs this.
    fn shell(&self) -> Result<&Arc<Shell<C>>, Status> {
        self.shell
            .get()
            .ok_or_else(|| Status::failed_precondition(common::HANDSHAKE_NOT_COMPLETED))
    }
}

/// What [`common::handshake`] needs from this shell — see
/// [`common::HandshakeShell`] for why this lives per-module rather than
/// on `Shell<C>` itself.
impl<C: SourceConnector> common::HandshakeShell for Shell<C> {
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
        // Deliberately empty: the proto's own field doc names
        // capabilities as a DESTINATION concern (merge/replace/widen
        // support) — a source has none to advertise.
        Vec::new()
    }
}

/// Flatten a classified [`SourceError`] into the wire's [`ErrorFrame`]:
/// the classification as the enum, the INNER cause's text as the
/// message, and the rate-limit hint when there is one.
///
/// THE CONTRACT (039 fix round 1): `ErrorFrame.message` is the CAUSE
/// text; classification travels only as the enum; the receiving client
/// renders the classification frame exactly once on reconstruction.
/// Shipping `error.to_string()` here — as this function first did —
/// put the SPI `Display` frame ON the wire, and a client mapping the
/// frame back to a `SourceError` then framed it again ("fatal source
/// error: fatal source error: …" — the 026 double-frame class, end to
/// end). The third-party argument decides which side owns the frame: a
/// foreign server authors its own cause text and cannot know rdlt's
/// SPI `Display` spellings, so the wire carries causes, never frames.
/// Nothing is lost — `SourceError::context` already folds context into
/// the inner cause string.
///
/// The wildcard arm is required: `SourceError` is `#[non_exhaustive]`
/// from OUTSIDE its defining crate, which this crate is. A future
/// classification this match has not been taught about lands FATAL
/// rather than failing to compile a shipped server — and, with no way
/// to reach an unknown variant's inner cause, it keeps the full
/// rendered `Display` as its message: a safe fallback that may
/// double-frame on reconstruction, preferred over dropping text.
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
    common::error_frame(classification, message, retry_after)
}

#[tonic::async_trait]
impl<C: SourceConnector> Connector for SourceServer<C> {
    async fn handshake(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeReply>, Status> {
        Ok(common::handshake(
            &self.shell,
            EXPECTED_ROLE,
            request.into_inner(),
        ))
    }

    async fn check(&self, _request: Request<CheckRequest>) -> Result<Response<CheckReply>, Status> {
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
        // Config-free static identity — the schema command's path (039):
        // answered from `C::NAME`/`C::VERSION`/`C::config_schema()`
        // alone, exempted from the pre-handshake refusal BY
        // CONSTRUCTION since it never calls `shell()`.
        Ok(common::spec_reply(C::NAME, C::VERSION, C::config_schema()))
    }
}

#[tonic::async_trait]
impl<C: SourceConnector> SourceService for SourceServer<C> {
    async fn streams(
        &self,
        _request: Request<StreamsRequest>,
    ) -> Result<Response<StreamsReply>, Status> {
        let shell = self.shell()?;
        let outcome = match shell.streams().await {
            Ok(streams) => {
                let stream_spec_json = streams
                    .iter()
                    .map(|stream| {
                        serde_json::to_vec(stream)
                            .expect("a StreamSpec serializes to JSON infallibly")
                    })
                    .collect();
                streams_reply::Outcome::Ok(StreamList { stream_spec_json })
            }
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
        let request = request.into_inner();

        // A request payload that fails to decode answers INSIDE the
        // response stream — first and only frame a terminal FATAL
        // `ErrorFrame` — never as a `Status` (038 review round 1, B2):
        // the twice-recorded Status-vs-ErrorFrame rule (serve/mod.rs;
        // the protocol crate's README) allows exactly two refusal
        // shapes, and an undecodable payload is a connector-outcome
        // refusal like the destination side's `*_json` decode
        // refusals, not a protocol-state violation.
        let stream_spec = match serde_json::from_slice(&request.stream_spec_json) {
            Ok(spec) => spec,
            Err(error) => {
                return Ok(error_stream(format!("invalid stream_spec_json: {error}")));
            }
        };
        let since = match &request.since_cursor_json {
            None => None,
            Some(bytes) => match serde_json::from_slice::<serde_json::Value>(bytes) {
                Ok(value) => Some(rdlt_connector::Cursor::new(value)),
                Err(error) => {
                    return Ok(error_stream(format!("invalid since_cursor_json: {error}")));
                }
            },
        };

        let (out, records_in) = records_channel(READ_CHANNEL_BUDGET);
        let read_request = rdlt_connector::ReadRequest::new(stream_spec, since, out);

        let read_task: JoinHandle<Result<(), SourceError>> =
            tokio::spawn(async move { shell.read(read_request).await });

        let (frame_tx, frame_rx) = frame_channel(BYTE_FRAME_BUDGET, FRAME_MESSAGE_CAPACITY);

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
/// `encode` is the push-to-frame translation ([`read_frame_of`] in
/// production). It is a parameter, not a hardcoded call, because its
/// failure arm cannot be reached from data: a well-formed batch encodes
/// its own schema infallibly, so the suite injects a failing encoder to
/// drive the encode-failure interleavings deterministically.
async fn forward_read_frames(
    mut records_in: rdlt_connector::RecordsIn,
    frame_tx: FrameSender,
    read_task: JoinHandle<Result<(), SourceError>>,
    encode: fn(PushPayload) -> Result<proto::ReadFrame, String>,
) {
    // Whether the encode-failure arm below has already emitted
    // its ErrorFrame. The proto calls the Error frame TERMINAL,
    // so once one is on the stream nothing may follow it — in
    // particular the read task's own eventual `Err` (its push
    // observed the closed channel and the connector may return
    // an error rather than `Ok`) must not append a second
    // "terminal" frame behind the first.
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
                // An Arrow batch that failed to encode must not
                // just vanish: silently dropping it here would
                // make a truncated read look identical to a
                // clean end of stream to whatever is on the
                // other end. Send ONE terminal error frame
                // instead, then close the SPI channel exactly
                // like a client hang-up below — the connector's
                // read task winds down via Break rather than
                // continuing to push into a channel nobody
                // drains.
                let frame = proto::ReadFrame {
                    frame: Some(read_frame::Frame::Error(common::error_frame(
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
            // The client hung up (or the stream errored out from
            // under us): closing BOTH halves of the SPI channel
            // — the message queue and the byte-budget semaphore
            // a producer may be parked on — turns that into the
            // Break the connector's next push observes, per the
            // SPI's closed-channel-is-cancellation contract.
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
        // A terminal ErrorFrame is already on the stream — the
        // encode failure it reported is the diagnosis; the read
        // task's follow-on error is downstream noise.
        Ok(Err(_)) => {}
        // Gated exactly like the arm above: when the encode-failure arm
        // already ended the stream with the protocol's TERMINAL
        // ErrorFrame, the abort this loop itself requested turns the
        // read task's completion into a cancelled JoinError — transport
        // noise behind a diagnosis already delivered, and nothing may
        // follow the terminal frame.
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
/// request-decode failure answers with (see the comment inside
/// `SourceServer::read` for why this is a frame, not a `Status`).
fn error_stream(message: String) -> Response<FrameStream> {
    // One terminal frame and nothing else, so the channel needs one
    // message slot and no byte budget at all: an `ErrorFrame` is priced
    // at nothing (see [`frame_bytes`]), which is what lets this be
    // placed synchronously — the enqueue below cannot park.
    let (frame_tx, frame_rx) = frame_channel(0, 1);
    let frame = proto::ReadFrame {
        frame: Some(read_frame::Frame::Error(common::error_frame(
            Classification::Fatal,
            message,
            None,
        ))),
    };
    frame_tx
        .frames
        .try_send(BudgetedFrame {
            frame: Ok(frame),
            _permit: None,
        })
        .expect("a fresh channel with capacity 1 accepts its one frame");
    Response::new(frame_rx)
}

/// One SPI push, translated to its wire shape — the payload picks the
/// oneof arm; nothing here inspects the connector or the request.
///
/// `Err` only for the Arrow arm: encoding is the one fallible step in
/// this translation (the caller turns it into a terminal `ErrorFrame`
/// rather than a panic — see the forwarding loop above).
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
/// exactly what a single-batch push needs): the format
/// [`rdlt_connector::PushPayload::Arrow`]'s wire counterpart names.
///
/// Writing into an in-memory `Vec` fails only on a schema/batch mismatch
/// the connector itself produced — an `expect()` would turn that into a
/// panicked task indistinguishable, from the client's side, from a
/// clean end of stream. Rendered as a plain `String`: the caller wraps
/// it in a terminal `ErrorFrame`, which only needs text.
fn encode_arrow_ipc(batch: &rdlt_connector::RecordBatch) -> Result<Vec<u8>, String> {
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
/// printing anything. [`source`] is this at a self-minted temp path,
/// with the printing a spawned connector process must do; this is the
/// seam a test drives directly, against the very listener `source` would
/// have started.
///
/// Both gRPC services ([`Connector`] and [`SourceService`]) are wired to
/// the SAME `SourceServer` instance (`from_arc`, not two independent
/// `new`s) — they share one handshake-populated shell, so a `Streams` or
/// `Read` call sees the config a prior `Handshake` validated.
pub async fn serve_on<C: SourceConnector>(
    path: impl AsRef<Path>,
) -> Result<(Line, JoinHandle<Result<(), ServeError>>), ServeError> {
    let path = path.as_ref();
    let listener = common::bind_uds(path)?;
    let incoming = UnixListenerStream::new(listener);

    let server = Arc::new(SourceServer::<C>::new());
    // `max_decoding_message_size` on BOTH services: tonic's 4 MiB
    // default receive cap is below what one legitimate frame may carry
    // — see `common::MAX_FRAME_BYTES`'s own doc.
    let serving = tonic::transport::Server::builder()
        .add_service(
            ConnectorServer::from_arc(Arc::clone(&server))
                .max_decoding_message_size(common::MAX_FRAME_BYTES)
                .max_encoding_message_size(common::MAX_FRAME_BYTES),
        )
        .add_service(
            SourceServiceServer::from_arc(server)
                .max_decoding_message_size(common::MAX_FRAME_BYTES)
                .max_encoding_message_size(common::MAX_FRAME_BYTES),
        )
        .serve_with_incoming(incoming);

    let handle = tokio::spawn(async move { serving.await.map_err(ServeError::Serve) });

    Ok((
        Line {
            socket_path: path.to_path_buf(),
            proto_min: PROTOCOL_VERSION,
            proto_max: PROTOCOL_VERSION,
        },
        handle,
    ))
}

/// Turn a [`SourceConnector`] into an out-of-process protocol server:
/// bind a fresh Unix domain socket in a private per-process directory
/// under the system temp directory, print the handshake line on stdout
/// (flushed — the spawning host is reading a pipe, not a TTY), then
/// serve until the process is killed.
pub async fn source<C: SourceConnector>() -> Result<(), ServeError> {
    let (line, handle) = serve_on::<C>(common::temp_socket_path()?).await?;

    // `writeln!`, not `println!`: a spawning host that exits (or never
    // reads its child's stdout — a misconfigured pipe) leaves this
    // write facing a broken pipe, and `println!` panics on an IO error
    // rather than surfacing one. `ServeError::Stdout` already exists
    // for exactly this write, so both the line and the flush that
    // follows report through it instead.
    let mut stdout = std::io::stdout();
    writeln!(stdout, "{}", line.render()).map_err(ServeError::Stdout)?;
    stdout.flush().map_err(ServeError::Stdout)?;

    handle.await.map_err(ServeError::Join)?
}

#[cfg(test)]
mod tests {
    //! The frame channel's admission rule, driven directly: the budget
    //! is in BYTES, an over-budget frame still passes alone, and a
    //! parked sender learns when the stream goes away. The wire-level
    //! consequence — what a STALLED reader lets pile up end to end —
    //! is pinned separately in `tests/cases/test_serve_source.rs`.
    use std::time::Duration;

    use tokio_stream::StreamExt as _;

    use super::*;

    /// Every wait that must SUCCEED is bounded: the failure mode under
    /// test is a hang, and an unbounded await would report it as a
    /// timeout of the whole suite rather than a named failure.
    const BOUND: Duration = Duration::from_secs(5);

    /// Long enough that a send which is going to complete has, short
    /// enough to keep the suite quick — the same bounded-negative idiom
    /// the SPI channel's own budget tests use.
    const PARKED: Duration = Duration::from_millis(50);

    /// Roomy enough that the message cap can never fire first — these
    /// tests are about the BYTE budget, and a message-count stall would
    /// be a different mechanism passing for it.
    const ROOMY: usize = 256;

    fn frame_of(bytes: usize) -> Result<proto::ReadFrame, Status> {
        Ok(proto::ReadFrame {
            frame: Some(read_frame::Frame::RawJson(vec![b'x'; bytes])),
        })
    }

    fn error_frame_of(message: &str) -> Result<proto::ReadFrame, Status> {
        Ok(proto::ReadFrame {
            frame: Some(read_frame::Frame::Error(common::error_frame(
                Classification::Fatal,
                message.to_string(),
                None,
            ))),
        })
    }

    #[test]
    fn frame_bytes_prices_every_payload_arm_by_what_it_carries() {
        assert_eq!(frame_bytes(&frame_of(1234).unwrap()), 1234);
        assert_eq!(
            frame_bytes(&proto::ReadFrame {
                frame: Some(read_frame::Frame::ArrowIpc(vec![0; 77])),
            }),
            77
        );
        assert_eq!(
            frame_bytes(&proto::ReadFrame {
                frame: Some(read_frame::Frame::CheckpointCursorJson(vec![0; 9])),
            }),
            9
        );
        // Terminal, tiny, and deliberately free — see `frame_bytes`.
        assert_eq!(frame_bytes(&error_frame_of("boom").unwrap()), 0);
        assert_eq!(frame_bytes(&proto::ReadFrame { frame: None }), 0);
    }

    #[tokio::test]
    async fn the_budget_counts_bytes_not_frames() {
        let (sender, mut stream) = frame_channel(100, ROOMY);
        // Fifty small frames pass — a count bound of 16 (what this
        // channel used to carry) would have parked at the seventeenth.
        for _ in 0..50 {
            sender.send(frame_of(2)).await.expect("small frames fit");
        }
        for _ in 0..50 {
            stream
                .next()
                .await
                .expect("the queued frames")
                .expect("a frame, not a status");
        }
        // …while the second of two large frames parks until the first is
        // pulled for the wire.
        sender
            .send(frame_of(80))
            .await
            .expect("the first large frame");
        let parked = sender.send(frame_of(80));
        tokio::pin!(parked);
        tokio::select! {
            _ = &mut parked => panic!("the byte budget did not park the second large frame"),
            _ = tokio::time::sleep(PARKED) => {}
        }
        stream
            .next()
            .await
            .expect("the first large frame")
            .expect("a frame, not a status");
        tokio::time::timeout(BOUND, parked)
            .await
            .expect("pulling a frame releases its budget")
            .expect("the stream is still there");
    }

    #[tokio::test]
    async fn a_frame_larger_than_the_whole_budget_passes_alone() {
        // Degrades to drain-the-budget rather than waiting on permits
        // that cannot exist — refusing it would deadlock a legal read.
        let (sender, mut stream) = frame_channel(16, ROOMY);
        tokio::time::timeout(BOUND, sender.send(frame_of(1_000_000)))
            .await
            .expect("an over-budget frame must pass rather than park forever")
            .expect("the stream is still there");
        // ALONE: nothing else is admitted beside it until it is pulled.
        let beside = tokio::time::timeout(PARKED, sender.send(frame_of(16))).await;
        assert!(
            beside.is_err(),
            "an over-budget frame holds the whole budget while it waits to be pulled"
        );
        assert_eq!(
            frame_bytes(&stream.next().await.expect("the big frame").expect("ok")),
            1_000_000
        );
        tokio::time::timeout(BOUND, sender.send(frame_of(16)))
            .await
            .expect("the budget released with the big frame")
            .expect("the stream is still there");
    }

    #[tokio::test]
    async fn a_payload_free_frame_passes_a_fully_spent_budget() {
        // The terminal ErrorFrame is the diagnosis a client is waiting
        // for; gating it on a budget the stalled client itself spent
        // would hide exactly the failures that matter most.
        let (sender, mut stream) = frame_channel(100, ROOMY);
        sender
            .send(frame_of(100))
            .await
            .expect("exactly the budget");
        tokio::time::timeout(BOUND, sender.send(error_frame_of("induced")))
            .await
            .expect("a payload-free frame must not wait on the budget")
            .expect("the stream is still there");
        assert_eq!(frame_bytes(&stream.next().await.unwrap().unwrap()), 100);
        assert!(matches!(
            stream.next().await.unwrap().unwrap().frame,
            Some(read_frame::Frame::Error(_))
        ));
    }

    /// The encode-failure arm ends the stream AT its terminal
    /// ErrorFrame even when the connector's read task is still running:
    /// the loop aborts that task, the abort surfaces as a `JoinError`,
    /// and the join arm must NOT append a transport `Status` behind the
    /// terminal frame — the proto's contract is that nothing follows
    /// it. The encoder is injected because a well-formed batch cannot
    /// make the production encoder fail (see [`forward_read_frames`]).
    #[tokio::test]
    async fn an_encode_failure_while_the_reader_runs_ends_the_stream_at_the_terminal_frame() {
        fn refuse(_: PushPayload) -> Result<proto::ReadFrame, String> {
            Err("induced encode failure".to_string())
        }
        let (mut out, records_in) = records_channel(1 << 20);
        // The reader is STILL RUNNING when the encode fails: a task
        // parked forever stands in for a connector mid-read, so the
        // loop's abort turns its completion into a cancelled JoinError —
        // exactly the interleaving under test.
        let read_task: tokio::task::JoinHandle<Result<(), rdlt_connector::SourceError>> =
            tokio::spawn(async { std::future::pending().await });
        out.rows([serde_json::json!({"n": 1})])
            .await
            .expect("the push is admitted");

        let (frame_tx, mut stream) = frame_channel(1 << 20, ROOMY);
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
        // Nothing may follow the terminal frame: the aborted read
        // task's JoinError must not become a trailing Status.
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
        // connector, and the read task hangs on forever. This pins the
        // OUTCOME, not the mechanism: `FrameStream::drop`'s own comment
        // records that the semaphore close it performs is belt to the
        // queue-drop's braces, so this test stays green if that line is
        // removed.
        let (sender, stream) = frame_channel(8, ROOMY);
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
}
