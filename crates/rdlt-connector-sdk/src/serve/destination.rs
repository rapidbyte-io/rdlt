//! The destination half of `serve()`: [`DestinationServer`] answers both
//! halves of the wire protocol a destination connector implements —
//! `Connector` (handshake, check) and `DestinationService` (one bidi
//! stream) — over one [`Shell`], the same sdk shell an in-process
//! embedder uses.
//!
//! Unlike the source side's per-RPC calls, `DestinationService` has
//! exactly one RPC — `OpenSession` — and it IS the session: a bidi
//! stream where every [`proto::SessionRequest`] frame maps to one call
//! on the [`Shell`]'s `Box<dyn LoadSession>`, in order, for as long as
//! the stream stays open. Going through `Shell::open` (never
//! reimplementing the choreography here) is what carries the sdk's
//! `Session<B>` enforcement onto the wire for free: write-before-ensure
//! refusal, and the clause-D3 replay check that runs inside `commit`
//! before anything republishes.
//!
//! That also explains why the wire's `ExistingReceipt` and `Replay`
//! frames — mirroring [`crate::destination::Backend`]'s finer-grained
//! primitives, not `LoadSession`'s — get trivial answers here: this
//! server only ever holds a `Box<dyn LoadSession>`, which does not
//! expose a standalone receipt lookup or replay hook, so a wire client
//! asking for either gets the honest "not tracked at this layer" answer
//! (`ExistingReceipt` always replies `None`; `Replay` is a no-op reply)
//! rather than a second, parallel implementation of what `commit`
//! already does correctly. A client wanting the real answer sends
//! `Publish`, which IS `LoadSession::commit`, replay and all.
//!
//! `OpenContext::part_events` is the other place this server departs
//! from a plain request/reply shape: the listener is a SYNC callback,
//! so any part it reports while a session method is in flight is
//! already sitting in the unbounded channel by the time that `await`
//! returns. Draining that channel — forwarding every queued part as its
//! own `PartClosedEvent` reply — immediately BEFORE sending the reply
//! for the request that (may have) produced them is what makes the
//! interleave order deterministic: a `part_closed` notification always
//! precedes the reply of the call that emitted it, never races it.

use std::io::Write as _;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use rdlt_connector::core::{CommitMeta, LoadId, PipelineId, TableName, TableSchema, WriteMode};
use rdlt_connector::{
    Destination, DestinationError, LoadSession, OpenContext, PartCloseReason, PartClosed,
};
use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::destination_service_server::{
    DestinationService, DestinationServiceServer,
};
use rdlt_connector_protocol::proto::{
    self, CheckReply, CheckRequest, Classification, ErrorFrame, HandshakeOk, HandshakeReply,
    HandshakeRequest, PartClosedEvent, Published, ReceiptReply, SessionReply, SessionRequest,
    StateReply, check_reply, handshake_reply, session_reply, session_request,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

use super::common::{self, ServeError};
use crate::destination::{DestinationConnector, Shell};

/// Bound on the reply channel one session forwards into — the same
/// order of magnitude as the source side's read-frame channel (16),
/// chosen for the same reason: a bidi session is request/reply-paced by
/// its own client, so this is headroom for the part-event interleave,
/// not a throughput budget.
const REPLY_CHANNEL_BUDGET: usize = 16;

/// The role a destination's handshake must be asked for — mirrors
/// `EXPECTED_ROLE` on the source side.
const EXPECTED_ROLE: &str = "destination";

/// Every role the protocol currently defines — see the source side's
/// identical constant for why this is separate from `EXPECTED_ROLE`.
const KNOWN_ROLES: [&str; 2] = ["source", "destination"];

/// The gRPC surface over one [`DestinationConnector`]. `shell` is empty
/// until a handshake succeeds; `Arc` because `OpenSession` hands a clone
/// to a spawned task that outlives the request.
struct DestinationServer<C: DestinationConnector> {
    shell: OnceLock<Arc<Shell<C>>>,
}

impl<C: DestinationConnector> DestinationServer<C> {
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
            .ok_or_else(|| Status::failed_precondition("handshake has not completed"))
    }
}

fn refuse_handshake(message: impl Into<String>) -> Response<HandshakeReply> {
    Response::new(HandshakeReply {
        outcome: Some(handshake_reply::Outcome::Error(common::error_frame(
            Classification::Fatal,
            message,
            None,
        ))),
    })
}

/// Flatten a classified [`DestinationError`] into the wire's
/// [`ErrorFrame`] — see the source side's `source_error_frame` twin for
/// why the wildcard arm is required (`DestinationError` is
/// `#[non_exhaustive]` from outside its defining crate).
fn destination_error_frame(error: &DestinationError) -> ErrorFrame {
    let (classification, retry_after) = match error {
        DestinationError::Transient(_) => (Classification::Transient, None),
        DestinationError::RateLimited { retry_after, .. } => {
            (Classification::RateLimited, *retry_after)
        }
        DestinationError::Fatal(_) => (Classification::Fatal, None),
        _ => (Classification::Fatal, None),
    };
    common::error_frame(classification, error.to_string(), retry_after)
}

/// A malformed `*_json` payload on an otherwise well-formed request
/// frame: a FATAL refusal naming which field failed to decode and the
/// serde error verbatim. Not part of the frozen-spelling surface (no
/// test pins its exact text) — a client sending undecodable JSON is a
/// protocol-level bug in whatever built the frame, not a data outcome.
fn decode_error_reply(field: &str, error: impl std::fmt::Display) -> session_reply::Reply {
    session_reply::Reply::Error(common::error_frame(
        Classification::Fatal,
        format!("invalid {field}: {error}"),
        None,
    ))
}

/// The frozen refusal for any non-`Open` frame arriving before a
/// session exists.
fn refuse_before_open() -> session_reply::Reply {
    session_reply::Reply::Error(common::error_frame(
        Classification::Fatal,
        "the session's first frame must be Open",
        None,
    ))
}

/// One closed part, translated to its wire shape.
fn part_closed_event(part: PartClosed) -> PartClosedEvent {
    PartClosedEvent {
        table: part.table.as_str().to_string(),
        encoded_bytes: part.encoded_bytes,
        reason: part_close_reason_str(part.reason).to_string(),
    }
}

/// [`PartCloseReason`]'s wire spelling — the same snake_case rendering
/// its `Serialize` impl produces
/// (`rdlt_connector::destination::tests::part_events_serialize_with_the_core_twin_spelling`),
/// reproduced by hand here because the wire field is a plain `string`,
/// not a JSON document. The wildcard arm is required: the enum is
/// `#[non_exhaustive]` from outside its defining crate.
fn part_close_reason_str(reason: PartCloseReason) -> &'static str {
    match reason {
        PartCloseReason::Target => "target",
        PartCloseReason::Time => "time",
        PartCloseReason::Budget => "budget",
        PartCloseReason::Commit => "commit",
        PartCloseReason::Schema => "schema",
        _ => "unknown",
    }
}

/// One Arrow IPC *stream*'s first (and only expected) record batch —
/// the `Write` frame's wire counterpart to the source side's
/// `encode_arrow_ipc`. Every failure mode (bytes that are not a valid
/// IPC stream at all, a stream with no batch, a batch that fails to
/// decode) collapses to the ONE frozen refusal: from the wire's
/// perspective these are indistinguishable "the client sent something
/// that isn't a decodable record batch" cases, not three separate
/// diagnoses worth telling apart.
fn decode_arrow_ipc(bytes: &[u8]) -> Result<rdlt_connector::RecordBatch, String> {
    arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .ok()
        .and_then(|mut reader| reader.next())
        .and_then(Result::ok)
        .ok_or_else(|| "write carried no decodable record batch".to_string())
}

#[tonic::async_trait]
impl<C: DestinationConnector> Connector for DestinationServer<C> {
    async fn handshake(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeReply>, Status> {
        let request = request.into_inner();

        if self.shell.get().is_some() {
            return Ok(refuse_handshake("handshake already completed"));
        }

        if request.expected_role != EXPECTED_ROLE {
            if !KNOWN_ROLES.contains(&request.expected_role.as_str()) {
                return Ok(refuse_handshake(format!(
                    "the handshake asked for role `{}`, which this connector does not recognize",
                    request.expected_role
                )));
            }
            return Ok(refuse_handshake(
                "this connector is a destination; the handshake asked for a source",
            ));
        }

        // See the source side's identical check for why this is `!=`
        // rather than an explicit range comparison.
        if request.protocol_version != PROTOCOL_VERSION {
            return Ok(refuse_handshake(format!(
                "protocol version {} is outside this connector's supported range [{PROTOCOL_VERSION}, {PROTOCOL_VERSION}]",
                request.protocol_version
            )));
        }

        let config: serde_json::Value = match serde_json::from_slice(&request.config_json) {
            Ok(config) => config,
            Err(error) => return Ok(refuse_handshake(format!("invalid config_json: {error}"))),
        };

        let shell = match Shell::<C>::from_value(config) {
            Ok(shell) => shell,
            Err(error) => return Ok(refuse_handshake(error.to_string())),
        };

        let spec = shell.spec();
        let spec_json =
            serde_json::to_vec(&spec).expect("a ConnectorSpec serializes to JSON infallibly");
        let capabilities_json = serde_json::to_vec(&shell.capabilities())
            .expect("DestinationCapabilities serializes to JSON infallibly");

        if self.shell.set(Arc::new(shell)).is_err() {
            // Lost a race against a concurrent handshake on the same
            // session — the same refusal either way.
            return Ok(refuse_handshake("handshake already completed"));
        }

        Ok(Response::new(HandshakeReply {
            outcome: Some(handshake_reply::Outcome::Ok(HandshakeOk {
                connector_id: C::NAME.to_string(),
                connector_version: C::VERSION.to_string(),
                spec_json,
                capabilities_json,
                // v0 hole, not an oversight — see the source side's
                // identical field for the reasoning.
                state_format_versions: Default::default(),
            })),
        }))
    }

    async fn check(&self, _request: Request<CheckRequest>) -> Result<Response<CheckReply>, Status> {
        let shell = self.shell()?;
        let outcome = match shell.check().await {
            Ok(()) => check_reply::Outcome::Ok(proto::Empty {}),
            Err(error) => check_reply::Outcome::Error(destination_error_frame(&error)),
        };
        Ok(Response::new(CheckReply {
            outcome: Some(outcome),
        }))
    }
}

/// Where one request frame's processing landed: keep driving the
/// session, or the session is over (a clean `Close`, a client hangup, a
/// transport error, or a reply the client is no longer around to
/// receive).
enum Step {
    Continue,
    End,
}

/// Send one reply. `false` means the client end has hung up (or the
/// stream errored out from under it) — callers fold that into
/// [`Step::End`], the bidi equivalent of the source side's
/// closed-response-stream cancellation.
async fn send(
    reply_tx: &mpsc::Sender<Result<SessionReply, Status>>,
    reply: session_reply::Reply,
) -> bool {
    reply_tx
        .send(Ok(SessionReply { reply: Some(reply) }))
        .await
        .is_ok()
}

/// Drain every part event already sitting in the channel — non-blocking
/// by construction (`try_recv`): the callback that filled it is
/// synchronous, so nothing more will arrive without another session
/// call running, and this must not itself become a point where the
/// session blocks. Forwards each as its own `PartClosedEvent` reply, in
/// the order the backend reported them. `false` means the client hung
/// up mid-drain.
async fn drain_parts(
    part_rx: &mut mpsc::UnboundedReceiver<PartClosed>,
    reply_tx: &mpsc::Sender<Result<SessionReply, Status>>,
) -> bool {
    while let Ok(part) = part_rx.try_recv() {
        if !send(
            reply_tx,
            session_reply::Reply::PartClosed(part_closed_event(part)),
        )
        .await
        {
            return false;
        }
    }
    true
}

/// Drain any part events a just-finished session call queued, THEN send
/// that call's own reply — the ordering the module doc promises. Folds
/// both steps' possible "client hung up" outcomes into [`Step`].
async fn finish(
    part_rx: &mut mpsc::UnboundedReceiver<PartClosed>,
    reply_tx: &mpsc::Sender<Result<SessionReply, Status>>,
    reply: session_reply::Reply,
) -> Step {
    if !drain_parts(part_rx, reply_tx).await {
        return Step::End;
    }
    if send(reply_tx, reply).await {
        Step::Continue
    } else {
        Step::End
    }
}

/// Handle one incoming frame against the session state machine —
/// `session` is `None` until an `Open` frame succeeds, then `Some` for
/// the rest of the stream's life.
async fn handle_frame<C: DestinationConnector>(
    shell: &Shell<C>,
    session: &mut Option<Box<dyn LoadSession>>,
    part_tx: &mpsc::UnboundedSender<PartClosed>,
    part_rx: &mut mpsc::UnboundedReceiver<PartClosed>,
    reply_tx: &mpsc::Sender<Result<SessionReply, Status>>,
    frame: SessionRequest,
) -> Step {
    if let Some(session_request::Request::Open(open)) = frame.request {
        if session.is_some() {
            let reply = session_reply::Reply::Error(common::error_frame(
                Classification::Fatal,
                "the session is already open",
                None,
            ));
            return finish(part_rx, reply_tx, reply).await;
        }
        let tx = part_tx.clone();
        let context = OpenContext::new(PipelineId::new(open.pipeline), LoadId::new(open.load_id))
            .with_part_events(Arc::new(move |part| {
                // The listener is a plain sync callback (`OpenContext`'s
                // own contract: "must never fail and never block") —
                // an unbounded channel is the correct shape for it:
                // advisory-volume telemetry, never awaited from inside
                // the callback itself.
                let _ = tx.send(part);
            }));
        let reply = match shell.open(context).await {
            Ok(opened) => {
                *session = Some(opened);
                session_reply::Reply::Opened(proto::Empty {})
            }
            Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
        };
        return finish(part_rx, reply_tx, reply).await;
    }

    let Some(session) = session.as_mut() else {
        return finish(part_rx, reply_tx, refuse_before_open()).await;
    };

    let reply = match frame.request {
        Some(session_request::Request::Open(_)) => unreachable!("handled above"),
        Some(session_request::Request::Ensure(ensure)) => {
            let schema = serde_json::from_slice::<TableSchema>(&ensure.table_schema_json);
            let mode = serde_json::from_slice::<WriteMode>(&ensure.write_mode_json);
            match (schema, mode) {
                (Ok(schema), Ok(mode)) => match session.ensure_table(&schema, &mode).await {
                    Ok(()) => session_reply::Reply::Ensured(proto::Empty {}),
                    Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                },
                (Err(error), _) => decode_error_reply("table_schema_json", error),
                (_, Err(error)) => decode_error_reply("write_mode_json", error),
            }
        }
        Some(session_request::Request::Write(write)) => match decode_arrow_ipc(&write.arrow_ipc) {
            Ok(batch) => match session.write(&TableName::new(write.table), batch).await {
                Ok(()) => session_reply::Reply::Written(proto::Empty {}),
                Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
            },
            Err(message) => session_reply::Reply::Error(common::error_frame(
                Classification::Fatal,
                message,
                None,
            )),
        },
        Some(session_request::Request::ExistingReceipt(_)) => {
            // See the module doc: `LoadSession` exposes no standalone
            // receipt lookup, so `None` is the honest answer this layer
            // can give without a second, parallel implementation of the
            // check `commit` already performs.
            session_reply::Reply::Receipt(ReceiptReply { receipt_json: None })
        }
        Some(session_request::Request::Replay(_)) => {
            // Same reasoning as `ExistingReceipt`: replay housekeeping
            // runs inside `commit`/`Publish`; a standalone `Replay`
            // frame is accepted but is a no-op at this layer.
            session_reply::Reply::Replayed(proto::Empty {})
        }
        Some(session_request::Request::Publish(publish)) => {
            match serde_json::from_slice::<CommitMeta>(&publish.commit_meta_json) {
                Ok(meta) => match session.commit(meta).await {
                    Ok(receipt) => session_reply::Reply::Published(Published {
                        receipt_json: serde_json::to_vec(&receipt)
                            .expect("a CommitReceipt serializes to JSON infallibly"),
                    }),
                    Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                },
                Err(error) => decode_error_reply("commit_meta_json", error),
            }
        }
        Some(session_request::Request::ReadState(read_state)) => {
            match session
                .read_state(&PipelineId::new(read_state.pipeline))
                .await
            {
                Ok(state) => session_reply::Reply::State(StateReply {
                    state_doc_json: state.map(|state| {
                        serde_json::to_vec(&state)
                            .expect("a StateDoc serializes to JSON infallibly")
                    }),
                }),
                Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
            }
        }
        Some(session_request::Request::Close(_)) => {
            let reply = match session.close().await {
                Ok(()) => session_reply::Reply::Closed(proto::Empty {}),
                Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
            };
            // `Close` ends the stream regardless of whether the reply
            // itself made it out — the session is over either way.
            let _ = finish(part_rx, reply_tx, reply).await;
            return Step::End;
        }
        None => session_reply::Reply::Error(common::error_frame(
            Classification::Fatal,
            "the session received a request frame with no payload",
            None,
        )),
    };
    finish(part_rx, reply_tx, reply).await
}

/// Run one session's request loop, from its `Open` to whatever ends it —
/// a clean `Close`, the client hanging up, or a transport error. Not a
/// method on [`DestinationServer`]: it outlives the single
/// `open_session` call that spawns it, so it owns its arguments outright
/// rather than borrowing `&self`.
async fn drive_session<C: DestinationConnector>(
    shell: Arc<Shell<C>>,
    mut incoming: Streaming<SessionRequest>,
    reply_tx: mpsc::Sender<Result<SessionReply, Status>>,
) {
    // Sync callback, advisory-volume telemetry (`OpenContext`'s own
    // doc): unbounded is correct here specifically because the sender
    // side never awaits and never blocks on backpressure — it is not a
    // general escape hatch from the byte-budget discipline the read
    // side observes.
    let (part_tx, mut part_rx) = mpsc::unbounded_channel::<PartClosed>();
    let mut session: Option<Box<dyn LoadSession>> = None;

    loop {
        // `biased`: a part event queued from a PREVIOUS iteration's
        // session call is forwarded before this iteration reads its
        // next request frame — the between-requests half of the
        // ordering guarantee. The within-one-request half (a part event
        // fired synchronously by the call THIS iteration is about to
        // run) is handled by `finish`'s explicit drain immediately
        // before that request's own reply; relying on this `select!`
        // alone for that half would race the next loop turn instead of
        // guaranteeing the order.
        tokio::select! {
            biased;
            Some(part) = part_rx.recv() => {
                if !send(&reply_tx, session_reply::Reply::PartClosed(part_closed_event(part))).await {
                    return;
                }
            }
            frame = incoming.message() => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    // The client closed the request half, or the
                    // transport errored reading it — either way there is
                    // no peer left to reply to.
                    Ok(None) | Err(_) => return,
                };
                match handle_frame(&shell, &mut session, &part_tx, &mut part_rx, &reply_tx, frame).await {
                    Step::Continue => {}
                    Step::End => return,
                }
            }
        }
    }
}

#[tonic::async_trait]
impl<C: DestinationConnector> DestinationService for DestinationServer<C> {
    type OpenSessionStream = ReceiverStream<Result<SessionReply, Status>>;

    async fn open_session(
        &self,
        request: Request<Streaming<SessionRequest>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let shell = Arc::clone(self.shell()?);
        let incoming = request.into_inner();
        let (reply_tx, reply_rx) = mpsc::channel(REPLY_CHANNEL_BUDGET);

        tokio::spawn(drive_session(shell, incoming, reply_tx));

        Ok(Response::new(ReceiverStream::new(reply_rx)))
    }
}

/// Bind at an explicit path and return the [`Line`] a spawning host
/// would read from stdout, plus a handle for the serving task — WITHOUT
/// printing anything. Mirrors the source side's `serve_on` — see there
/// for why this is the seam tests drive rather than [`destination`]
/// itself.
///
/// Both gRPC services ([`Connector`] and [`DestinationService`]) are
/// wired to the SAME [`DestinationServer`] instance — they share one
/// handshake-populated shell, so `OpenSession` sees the config a prior
/// `Handshake` validated.
pub async fn serve_on<C: DestinationConnector>(
    path: impl AsRef<Path>,
) -> Result<(Line, JoinHandle<Result<(), ServeError>>), ServeError> {
    let path = path.as_ref();
    let listener = common::bind_uds(path)?;
    let incoming = UnixListenerStream::new(listener);

    let server = Arc::new(DestinationServer::<C>::new());
    let serving = tonic::transport::Server::builder()
        .add_service(ConnectorServer::from_arc(Arc::clone(&server)))
        .add_service(DestinationServiceServer::from_arc(server))
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

/// Turn a [`DestinationConnector`] into an out-of-process protocol
/// server: bind a fresh Unix domain socket in the system temp
/// directory, print the handshake line on stdout (flushed — the
/// spawning host is reading a pipe, not a TTY), then serve until the
/// process is killed. Mirrors the source side's `source` entry point.
pub async fn destination<C: DestinationConnector>() -> Result<(), ServeError> {
    let (line, handle) = serve_on::<C>(common::temp_socket_path()).await?;

    let mut stdout = std::io::stdout();
    writeln!(stdout, "{}", line.render()).map_err(ServeError::Stdout)?;
    stdout.flush().map_err(ServeError::Stdout)?;

    handle.await.map_err(ServeError::Join)?
}
