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
use std::sync::{Arc, OnceLock};

use rdlt_connector::{PushPayload, Source as _, SourceError, records_channel};
use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::source_service_server::{SourceService, SourceServiceServer};
use rdlt_connector_protocol::proto::{
    self, CheckReply, CheckRequest, Classification, ErrorFrame, HandshakeReply, HandshakeRequest,
    StreamList, StreamsReply, StreamsRequest, check_reply, read_frame, streams_reply,
};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
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
const READ_CHANNEL_BUDGET: usize = 8 * 1024 * 1024;

/// Bound on the frame channel one `Read` call forwards into — caps how
/// many already-encoded `ReadFrame`s can sit unread while the CLIENT
/// stalls reading its stream before the forwarding loop parks (and the
/// connector's producer parks behind it, against
/// [`READ_CHANNEL_BUDGET`]'s byte budget). Not a throughput budget:
/// headroom. 16 — the destination side's `REPLY_CHANNEL_BUDGET`
/// (`serve::destination`) cross-cites this channel as its sizing
/// precedent, so the two figures move together or not at all.
const FRAME_CHANNEL_BUDGET: usize = 16;

/// The role a source's handshake must be asked for — the mirrored
/// spelling lives on the destination side (`serve::destination`'s own
/// `EXPECTED_ROLE`).
const EXPECTED_ROLE: &str = "source";

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
    /// `Handshake` itself needs this.
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
/// classification, the error's own rendered text (the classification
/// frame included — `SourceError`'s `Display` already carries it, so a
/// receiver on the other end of the wire sees exactly what an in-process
/// caller's `.to_string()` would have shown), and the rate-limit hint
/// when there is one.
///
/// The wildcard arm is required: `SourceError` is `#[non_exhaustive]`
/// from OUTSIDE its defining crate, which this crate is. A future
/// classification this match has not been taught about lands FATAL
/// rather than failing to compile a shipped server.
fn source_error_frame(error: &SourceError) -> ErrorFrame {
    let (classification, retry_after) = match error {
        SourceError::Transient(_) => (Classification::Transient, None),
        SourceError::RateLimited { retry_after, .. } => (Classification::RateLimited, *retry_after),
        SourceError::Fatal(_) => (Classification::Fatal, None),
        _ => (Classification::Fatal, None),
    };
    common::error_frame(classification, error.to_string(), retry_after)
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

    type ReadStream = ReceiverStream<Result<proto::ReadFrame, Status>>;

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

        let (out, mut records_in) = records_channel(READ_CHANNEL_BUDGET);
        let read_request = rdlt_connector::ReadRequest::new(stream_spec, since, out);

        let read_task: JoinHandle<Result<(), SourceError>> =
            tokio::spawn(async move { shell.read(read_request).await });

        let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(FRAME_CHANNEL_BUDGET);

        tokio::spawn(async move {
            // Whether the encode-failure arm below has already emitted
            // its ErrorFrame. The proto calls the Error frame TERMINAL,
            // so once one is on the stream nothing may follow it — in
            // particular the read task's own eventual `Err` (its push
            // observed the closed channel and the connector may return
            // an error rather than `Ok`) must not append a second
            // "terminal" frame behind the first.
            let mut terminal_sent = false;
            loop {
                let Some(push) = records_in.recv().await else {
                    break;
                };
                let frame = match read_frame_of(push.payload) {
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
                    break;
                }
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
                Err(join_error) => {
                    let _ = frame_tx
                        .send(Err(Status::internal(format!(
                            "connector read task did not complete: {join_error}"
                        ))))
                        .await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(frame_rx)))
    }
}

/// An already-terminated `Read` response stream whose first and only
/// frame is a terminal FATAL [`ErrorFrame`] carrying `message` — what a
/// request-decode failure answers with (see the comment inside
/// `SourceServer::read` for why this is a frame, not a `Status`).
fn error_stream(message: String) -> Response<ReceiverStream<Result<proto::ReadFrame, Status>>> {
    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(1);
    let frame = proto::ReadFrame {
        frame: Some(read_frame::Frame::Error(common::error_frame(
            Classification::Fatal,
            message,
            None,
        ))),
    };
    frame_tx
        .try_send(Ok(frame))
        .expect("a fresh channel with capacity 1 accepts its one frame");
    Response::new(ReceiverStream::new(frame_rx))
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
                .max_decoding_message_size(common::MAX_FRAME_BYTES),
        )
        .add_service(
            SourceServiceServer::from_arc(server)
                .max_decoding_message_size(common::MAX_FRAME_BYTES),
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
/// bind a fresh Unix domain socket in the system temp directory, print
/// the handshake line on stdout (flushed — the spawning host is reading
/// a pipe, not a TTY), then serve until the process is killed.
pub async fn source<C: SourceConnector>() -> Result<(), ServeError> {
    let (line, handle) = serve_on::<C>(common::temp_socket_path()).await?;

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
