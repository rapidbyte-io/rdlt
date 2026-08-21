//! The destination role: one server answers both halves of the wire
//! protocol a destination implements — `Connector` (handshake, check,
//! spec) and `DestinationService` (the `OpenSession` bidi stream) —
//! driving the connector's raw [`Backend`] directly, not a
//! `LoadSession` wrapper, so every wire frame reaches a REAL backend
//! method rather than a stub.
//!
//! ONE long-lived bidirectional stream IS the session: it mirrors a
//! [`Backend`]'s own lifetime (a stream reset is the session's crash
//! class; a client half-close is its orderly end). Every frame
//! (`Ensure`/`Write`/`ExistingReceipt`/`Replay`/`Publish`/`ReadState`/
//! `Close`) maps 1:1 onto its own [`Backend`] method — the wire speaks
//! the real exactly-once grammar, not a collapsed `commit`.
//!
//! The choreography splits along the trust boundary this server sits
//! on. [`WriteGuard`] (write-before-ensure, open-once) is enforced HERE,
//! directly against the frames as they arrive, because a bidi stream
//! carrying client-supplied ORDER never trusts that order. The commit
//! choreography (`existing_receipt` → `replay` → `publish`) is NOT
//! refereed here: each of those frames reaches its own `Backend` method
//! independently and the CALLER decides which to send next — the client
//! crate's remote-backend adapter reconstructs it over the same
//! `Session<B>` type [`Destination::open`] composes for free. So a foreign
//! client CAN send `Publish` twice for one `(load_id, commit_seq)`
//! without ever asking `ExistingReceipt`, and the ONLY thing that saves
//! exactly-once is the destination's own durable receipt guard inside
//! `Backend::publish` — the certifier drives exactly that sequence over
//! the wire and demands a replay or a refusal, never a fresh mint.
//!
//! `OpenContext::part_events` is a SYNC callback, so any part it
//! reports while a `Backend` call is in flight is already sitting in
//! the unbounded channel by the time that call's `await` returns.
//! Draining that channel immediately BEFORE sending the reply for the
//! call that produced it is what the ordering promise covers: every
//! part already queued when a call returns precedes that call's own
//! reply. A part a buffering backend fires from a task this server
//! never awaited carries no such promise; it arrives as its own
//! `PartClosedEvent` reply as soon as the request loop next turns.
//!
//! One live session per served listener: a second concurrent
//! `OpenSession` is refused outright, `Status::failed_precondition`,
//! frozen wording `one session per connector process` — [`run`], the
//! only entry a spawned process runs, opens exactly one listener per
//! process, so the two ceilings coincide as shipped. Deliberate:
//! loosening it later is additive; a backend's own per-session staging
//! guard is defense in depth BEHIND this ceiling, not a replacement.
//! There is no idle timeout: a stalled client holds the slot until the
//! provider supervising the connector process evicts it.
//!
//! [`run`] is what a spawned connector process runs; [`run_on`] is the
//! seam under it — bind at an explicit path without printing anything,
//! so a test can drive the very listener [`run`] would have started.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use rdlt_connector::arrow::RecordBatch;
use rdlt_connector::core::commit::{CommitMeta, CommitReceipt, WriteMode};
use rdlt_connector::core::id::{LoadId, PipelineId, TableName};
use rdlt_connector::core::schema::TableSchema;
use rdlt_connector::core::state::StateDoc;
use rdlt_connector::destination::{Destination, OpenContext, PartCloseReason, PartClosed};
use rdlt_connector::error::DestinationError;
use rdlt_connector::gate;
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::destination_service_server::{
    DestinationService, DestinationServiceServer,
};
use rdlt_connector_protocol::proto::{
    self, CheckReply, CheckRequest, Classification, ErrorFrame, HandshakeReply, HandshakeRequest,
    PartClosedEvent, Published, ReceiptReply, SessionReply, SessionRequest, StateReply,
    session_reply, session_request,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::wire::FrameCapped;
use super::{gate as serve_gate, wire};
use crate::config::Document;
use crate::destination::{Backend, DestinationConnector, Shell, WriteGuard};

/// Bound on the reply channel one session forwards into: how many
/// already-produced replies — request replies and forwarded part events
/// alike — can sit unread while the CLIENT stalls reading its own
/// stream. A COUNT, unlike the source side's byte-bounded frame
/// channel, because reply production is client-paced: a reply exists
/// only because the client sent the frame that asked for it, so queued
/// replies can never outnumber the requests the client chose to send
/// before reading, plus whatever part events those calls emitted. What
/// a count does not bound is bytes: `StateReply` carries the pipeline's
/// whole state document. That document is held to the wire's document
/// ceiling on the way OUT as well as in, so the worst case held here
/// is 16 documents of that bound — 128 MiB, reachable
/// only by a client that pipelines 16 `ReadState` frames and reads
/// none of the replies. The
/// bulk data path (`Write` frames carrying Arrow IPC) travels the OTHER
/// way and never touches this channel.
const REPLY_CHANNEL_BUDGET: usize = 16;

/// The role a destination's handshake must be asked for.
const EXPECTED_ROLE: &str = "destination";

/// The gRPC surface over one [`DestinationConnector`]. `shell` is empty
/// until a handshake succeeds; `Arc` because `OpenSession` hands a clone
/// to a spawned task that outlives the request. `session_active` is the
/// one-session-per-process ceiling.
struct DestinationServer<C: DestinationConnector> {
    shell: OnceLock<Arc<Shell<C>>>,
    session_active: Arc<AtomicBool>,
    /// The process-wide ceiling on concurrent calls that run the
    /// connector's own code — `Handshake` and `Check` here
    /// ([`wire::MAX_CONCURRENT_CONNECTOR_CALLS`]). The session seat has
    /// the one-session slot; this is the other door.
    connector_admission: Arc<tokio::sync::Semaphore>,
}

impl<C: DestinationConnector> DestinationServer<C> {
    fn new() -> Self {
        Self {
            shell: OnceLock::new(),
            session_active: Arc::new(AtomicBool::new(false)),
            connector_admission: Arc::new(tokio::sync::Semaphore::new(
                wire::MAX_CONCURRENT_CONNECTOR_CALLS,
            )),
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

impl<C: DestinationConnector> wire::HandshakeShell for Shell<C> {
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
        // A destination's capabilities ARE the host's planning input
        // (merge/replace/widen support): the wire field carries them.
        serde_json::to_vec(&self.capabilities())
            .expect("Capabilities serializes to JSON infallibly")
    }

    fn state_format_versions_json(&self) -> Vec<u8> {
        // The state-doc ceiling, declared automatically: this build can
        // resume exactly what its SDK understands, and the client's
        // `ReadState` seat refuses a persisted document newer than the
        // declaration (037: refuse before extraction, never reset).
        serde_json::to_vec(&std::collections::BTreeMap::from([(
            rdlt_connector::core::state::STATE_DOC_FORMAT_KIND,
            rdlt_connector::core::state::STATE_FORMAT_VERSION,
        )]))
        .expect("a one-kind map serializes to JSON infallibly")
    }
}

/// Flatten a classified [`DestinationError`] into the wire's
/// [`ErrorFrame`] — the shared construction behind
/// [`wire::error_frame_of`]; this alias names the role at its call sites.
fn destination_error_frame(error: &DestinationError) -> ErrorFrame {
    wire::error_frame_of(error)
}

/// A FATAL refusal reply carrying `message` — the shape every in-session
/// refusal this server itself decides takes.
fn refuse(message: impl Into<String>) -> session_reply::Reply {
    session_reply::Reply::Error(wire::error_frame(Classification::Fatal, message, None))
}

/// Decode one inbound session document: the document ceiling FIRST, on
/// the raw bytes, bounding the parse's own materialization; then the
/// parse, its failure rendered by KIND and location alone (serde's
/// verbatim `Display` can quote the parsed value back over the wire).
/// The `invalid {field}: …` prefix is a frozen wire spelling.
fn decode_document<T: serde::de::DeserializeOwned>(
    field: &str,
    bytes: &[u8],
) -> Result<T, session_reply::Reply> {
    gate::refuse_oversized_document(field, bytes).map_err(refuse)?;
    serde_json::from_slice::<T>(bytes).map_err(|error| {
        refuse(format!(
            "invalid {field}: {}",
            gate::describe_parse_error(&error)
        ))
    })
}

/// One closed part, translated to its wire shape.
fn part_closed_event(part: PartClosed) -> PartClosedEvent {
    PartClosedEvent {
        table: part.table.as_str().to_string(),
        encoded_bytes: part.encoded_bytes,
        reason: part_close_reason_str(part.reason),
    }
}

/// [`PartCloseReason`]'s wire spelling, taken DIRECTLY from its own
/// `Serialize` impl rather than a hand-maintained match table that
/// could drift from the SPI's spelling silently. A unit-variant enum
/// with `#[serde(rename_all = "snake_case")]` always serializes to a
/// plain JSON string; the `Debug` fallback only fires if a FUTURE
/// variant serializes to something else, and its PascalCase spelling
/// can never collide with a real snake_case reason.
fn part_close_reason_str(reason: PartCloseReason) -> String {
    match serde_json::to_value(reason) {
        Ok(serde_json::Value::String(spelling)) => spelling,
        _ => format!("{reason:?}"),
    }
}

/// One Arrow IPC *stream*'s exactly-one record batch — the `Write`
/// frame's payload. Bytes that are not a valid IPC stream, a stream with
/// no batch message, or a stream whose SECOND message fails to decode
/// all collapse to the ONE frozen prefix (`write carried no decodable
/// record batch`) with the underlying arrow cause appended, so no leg
/// silently drops the diagnostic; a stream carrying a SECOND, DECODABLE
/// batch gets its own distinct refusal, because silently taking only
/// the first would drop every row after it. The decode discipline
/// itself — belt, framing pre-pass, width and row caps, one-batch rule,
/// field walk — is the SPI's one shared seat; what lives here is the
/// seat's vocabulary and this side's LENGTH-ONLY field-name rule (the
/// family rule above: serve gates are length-only at the wire).
fn decode_arrow_ipc(bytes: &[u8]) -> Result<RecordBatch, String> {
    const REFUSAL: &str = "write carried no decodable record batch";
    gate::decode_one_batch_ipc(
        bytes,
        REFUSAL,
        "write carried more than one record batch; a Write frame is exactly one batch",
        |message| message,
        &mut refuse_oversized_field_name,
    )
}

/// A write batch's field names through the wire identifier ceiling,
/// length-only: these names are retained by the session and reach
/// backend error text exactly like any other inbound identifier, and
/// the family rule holds — content escaping belongs to the display
/// renders on the side that displays.
fn refuse_oversized_field_name(name: &str) -> Result<(), String> {
    if name.len() > gate::MAX_WIRE_IDENTIFIER_BYTES {
        return Err(format!(
            "a write field name of {} bytes exceeds the {}-byte wire identifier \
             ceiling — refused at the wire boundary",
            name.len(),
            gate::MAX_WIRE_IDENTIFIER_BYTES
        ));
    }
    Ok(())
}

/// The state a `ReadState` answers with, held to the wire's document
/// ceiling on the way OUT as well as on the way in. A backend whose
/// retained state has outgrown what the wire admits cannot ship it: the
/// reply would build at frame scale and the reply channel would hold as
/// many of them as the client pipelined. Refusing names the seat
/// instead — a state document past the ceiling means the store grew it
/// by some path other than this wire, which is a defect to see, not to
/// stream.
fn state_reply(state: Option<StateDoc>) -> Result<StateReply, String> {
    let state_doc_json = state
        .map(|state| serde_json::to_vec(&state).expect("a StateDoc serializes to JSON infallibly"));
    if let Some(bytes) = state_doc_json.as_deref() {
        gate::refuse_oversized_document("state_doc_json", bytes)?;
    }
    Ok(StateReply { state_doc_json })
}

#[tonic::async_trait]
impl<C: DestinationConnector> Connector for DestinationServer<C> {
    async fn handshake(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeReply>, Status> {
        // Admission BEFORE the connector's own code runs (see
        // [`wire::admitted`] for why): then the shared handshake
        // choreography.
        wire::admitted(&self.connector_admission, async {
            Ok::<_, Status>(wire::handshake(
                &self.shell,
                EXPECTED_ROLE,
                request.into_inner(),
            ))
        })
        .await
    }

    async fn check(&self, _request: Request<CheckRequest>) -> Result<Response<CheckReply>, Status> {
        wire::admitted(&self.connector_admission, async {
            match self.shell() {
                Ok(shell) => Ok(Response::new(wire::check_reply_of(shell.check().await))),
                Err(status) => Err(status),
            }
        })
        .await
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
/// synchronous, so nothing more will arrive without another `Backend`
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

/// Drain any part events a just-finished `Backend` call queued, THEN
/// send that call's own reply — the ordering the module doc promises.
/// Folds both steps' possible "client hung up" outcomes into [`Step`].
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

/// The per-session mutable state [`handle_frame`] threads through —
/// one struct because `guard`/`backend`/`closed` are facets of the SAME
/// session, not independent plumbing.
struct SessionState<C: DestinationConnector> {
    guard: WriteGuard,
    /// `None` until an `Open` frame succeeds, then `Some` for the rest
    /// of the stream's life.
    backend: Option<C::Backend>,
    /// Set by the explicit `Close` arm so [`drive_session`]'s
    /// best-effort cleanup does not run `Backend::close` a second time.
    closed: bool,
}

impl<C: DestinationConnector> SessionState<C> {
    fn new() -> Self {
        Self {
            guard: WriteGuard::new(),
            backend: None,
            closed: false,
        }
    }
}

/// Handle one incoming frame against the raw [`Backend`] and its
/// [`WriteGuard`]. Every frame maps to its OWN `Backend` method call;
/// only the write-before-ensure/open-once guard is refereed here.
async fn handle_frame<C: DestinationConnector>(
    shell: &Shell<C>,
    state: &mut SessionState<C>,
    part_tx: &mpsc::UnboundedSender<PartClosed>,
    part_rx: &mut mpsc::UnboundedReceiver<PartClosed>,
    reply_tx: &mpsc::Sender<Result<SessionReply, Status>>,
    frame: SessionRequest,
) -> Step {
    if let Some(session_request::Request::Open(open)) = frame.request {
        // The guard is checked BEFORE attempting `connect` and marked
        // open ONLY after `connect` SUCCEEDS, so a failed `Open` is
        // legal to retry on the same stream — an eager mark would turn
        // a Transient connect failure into a permanent refusal.
        let reply = if state.guard.is_open() {
            refuse("a session accepts at most one Open frame, and it must be first")
        } else {
            if let Err(reply) =
                serve_gate::refuse_oversized_identifier("pipeline id", &open.pipeline).and_then(
                    |()| serve_gate::refuse_oversized_identifier("load id", &open.load_id),
                )
            {
                return finish(part_rx, reply_tx, reply).await;
            }
            let tx = part_tx.clone();
            let context =
                OpenContext::new(PipelineId::new(open.pipeline), LoadId::new(open.load_id))
                    .with_part_events(Arc::new(move |part| {
                        // A sync callback that must never fail or
                        // block: an unbounded channel is its shape.
                        let _ = tx.send(part);
                    }));
            match shell.connect(&context).await {
                Ok(opened) => {
                    state.backend = Some(opened);
                    state.guard.mark_open();
                    session_reply::Reply::Opened(proto::Empty {})
                }
                Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
            }
        };
        return finish(part_rx, reply_tx, reply).await;
    }

    let guard = &mut state.guard;
    let Some(backend) = state.backend.as_mut() else {
        return finish(
            part_rx,
            reply_tx,
            refuse("the session's first frame must be Open"),
        )
        .await;
    };

    let reply = match frame.request {
        Some(session_request::Request::Open(_)) => unreachable!("handled above"),
        Some(session_request::Request::Ensure(ensure)) => {
            let schema =
                decode_document::<TableSchema>("table_schema_json", &ensure.table_schema_json);
            let mode = decode_document::<WriteMode>("write_mode_json", &ensure.write_mode_json);
            match (schema, mode) {
                (Ok(schema), Ok(mode)) => {
                    // Every identifier the schema and mode carry is
                    // retained by the session or reaches a backend's
                    // error text — the same ceiling at each.
                    let identifiers_ok = serve_gate::refuse_ensure_counts(&schema, &mode)
                        .and_then(|()| {
                            serve_gate::refuse_oversized_identifier(
                                "table name",
                                schema.table.as_str(),
                            )
                        })
                        .and_then(|()| {
                            schema.parent.iter().try_for_each(|parent| {
                                serve_gate::refuse_oversized_identifier(
                                    "parent table name",
                                    parent.parent.as_str(),
                                )
                            })
                        })
                        .and_then(|()| schema.columns.iter().try_for_each(serve_gate::gate_column))
                        .and_then(|()| match &mode {
                            WriteMode::Merge { key } => key.iter().try_for_each(|column| {
                                serve_gate::refuse_oversized_identifier("merge key column", column)
                            }),
                            _ => Ok(()),
                        });
                    match identifiers_ok {
                        Err(reply) => reply,
                        Ok(()) => match backend.ensure_table(&schema, &mode).await {
                            Ok(()) => {
                                guard.ensure(schema.table.clone());
                                session_reply::Reply::Ensured(proto::Empty {})
                            }
                            Err(error) => {
                                session_reply::Reply::Error(destination_error_frame(&error))
                            }
                        },
                    }
                }
                (Err(reply), _) | (_, Err(reply)) => reply,
            }
        }
        Some(session_request::Request::Write(write)) => {
            let table = TableName::new(write.table);
            match serve_gate::refuse_oversized_identifier("table name", table.as_str()).and_then(
                |()| {
                    guard.check_write(&table).map_err(|error| {
                        session_reply::Reply::Error(destination_error_frame(&error))
                    })
                },
            ) {
                Err(reply) => reply,
                Ok(()) => match decode_arrow_ipc(&write.arrow_ipc) {
                    Ok(batch) => match backend.write(&table, batch).await {
                        Ok(()) => session_reply::Reply::Written(proto::Empty {}),
                        Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                    },
                    Err(message) => refuse(message),
                },
            }
        }
        Some(session_request::Request::ExistingReceipt(existing)) => {
            match serve_gate::refuse_oversized_identifier("load id", &existing.load_id) {
                Err(reply) => reply,
                Ok(()) => {
                    let load_id = LoadId::new(existing.load_id);
                    match backend
                        .existing_receipt(&load_id, existing.commit_seq)
                        .await
                    {
                        Ok(receipt) => session_reply::Reply::Receipt(ReceiptReply {
                            receipt_json: receipt.map(|receipt| {
                                serde_json::to_vec(&receipt)
                                    .expect("a CommitReceipt serializes to JSON infallibly")
                            }),
                        }),
                        Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                    }
                }
            }
        }
        Some(session_request::Request::Replay(replay)) => {
            let meta = decode_document::<CommitMeta>("commit_meta_json", &replay.commit_meta_json);
            let receipt = decode_document::<CommitReceipt>("receipt_json", &replay.receipt_json);
            match (meta, receipt) {
                // The decoded receipt's load id is wire-authored
                // identity a backend's refusals quote — gated exactly
                // like ExistingReceipt's, so the two receipt seats
                // cannot drift. The decoded META walks the shared
                // gate: every identifier it carries, sub-maps
                // included, rides the ceiling Open and ReadState hold
                // theirs to.
                (Ok(meta), Ok(receipt)) => {
                    match serve_gate::gate_commit_meta(&meta).and_then(|()| {
                        serve_gate::refuse_oversized_identifier("load id", receipt.load_id.as_str())
                    }) {
                        Err(reply) => reply,
                        Ok(()) => match backend.replay(&meta, &receipt).await {
                            Ok(()) => session_reply::Reply::Replayed(proto::Empty {}),
                            Err(error) => {
                                session_reply::Reply::Error(destination_error_frame(&error))
                            }
                        },
                    }
                }
                (Err(reply), _) | (_, Err(reply)) => reply,
            }
        }
        Some(session_request::Request::Publish(publish)) => {
            match decode_document::<CommitMeta>("commit_meta_json", &publish.commit_meta_json) {
                // The meta walks the shared gate — every identifier
                // it carries, sub-maps included — exactly as its
                // Replay twin does.
                Ok(meta) => match serve_gate::gate_commit_meta(&meta) {
                    Err(reply) => reply,
                    Ok(()) => match backend.publish(meta).await {
                        Ok(receipt) => session_reply::Reply::Published(Published {
                            receipt_json: serde_json::to_vec(&receipt)
                                .expect("a CommitReceipt serializes to JSON infallibly"),
                        }),
                        Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                    },
                },
                Err(reply) => reply,
            }
        }
        Some(session_request::Request::ReadState(read_state)) => {
            match serve_gate::refuse_oversized_identifier("pipeline id", &read_state.pipeline) {
                Err(reply) => reply,
                Ok(()) => match backend
                    .read_state(&PipelineId::new(read_state.pipeline))
                    .await
                {
                    Ok(state) => match state_reply(state) {
                        Ok(reply) => session_reply::Reply::State(reply),
                        Err(message) => session_reply::Reply::Error(wire::error_frame(
                            Classification::Fatal,
                            message,
                            None,
                        )),
                    },
                    Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                },
            }
        }
        Some(session_request::Request::Close(_)) => {
            let reply = match backend.close().await {
                Ok(()) => session_reply::Reply::Closed(proto::Empty {}),
                Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
            };
            // The explicit close ran — `drive_session`'s best-effort
            // abandoned-session cleanup must not run it AGAIN.
            state.closed = true;
            let _ = finish(part_rx, reply_tx, reply).await;
            return Step::End;
        }
        None => refuse("the session received a request frame with no payload"),
    };
    finish(part_rx, reply_tx, reply).await
}

/// Releases [`DestinationServer::session_active`] on drop — covers
/// EVERY [`drive_session`] exit path uniformly, so the one-session
/// ceiling can never leak stuck-active from a codepath that forgot to
/// release it by hand. `open_session` acquires the slot BEFORE spawning
/// `drive_session`, which then owns it for the session's life.
struct SessionSlot(Arc<AtomicBool>);

impl Drop for SessionSlot {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Run one session's request loop, from its `Open` to whatever ends it —
/// a clean `Close`, the client hanging up, or a transport error — then
/// best-effort close the backend on EVERY LOOP EXIT that is not the
/// explicit `Close` frame, per the close contract (called exactly once
/// whenever the session ends; best-effort on a failure path), so an
/// abandoned session does not leak whatever the backend opened.
///
/// "Every loop exit" is narrower than "every way this function can
/// stop running": `Backend::close` is `async` and Rust has no async
/// `Drop`, so this cleanup is a plain `if` after the `loop` rather than
/// destructor-based like `SessionSlot`. It does NOT run if something
/// outside aborts the task while it is parked mid-`select!` — nothing in
/// this crate holds such an abort handle, so the gap is real but
/// unreachable from inside this codebase.
async fn drive_session<C: DestinationConnector>(
    shell: Arc<Shell<C>>,
    mut incoming: Streaming<SessionRequest>,
    reply_tx: mpsc::Sender<Result<SessionReply, Status>>,
    _slot: SessionSlot,
) {
    // Unbounded because the sender is a sync callback that never
    // awaits: advisory-volume telemetry, not an escape from the
    // byte-budget discipline the read side observes.
    let (part_tx, mut part_rx) = mpsc::unbounded_channel::<PartClosed>();
    let mut state = SessionState::<C>::new();

    loop {
        // `biased`: a part event queued from a PREVIOUS iteration's
        // `Backend` call is forwarded before this iteration reads its
        // next request frame — the between-requests half of the
        // ordering guarantee (`finish`'s drain is the within-request
        // half). Racing against `incoming.message()` relies on tonic's
        // `Streaming::message` being cancel-safe: it decodes into a
        // buffer owned by the `Streaming` value, not the returned
        // future, so dropping the losing branch never discards a
        // partially-decoded frame.
        tokio::select! {
            biased;
            Some(part) = part_rx.recv() => {
                if !send(&reply_tx, session_reply::Reply::PartClosed(part_closed_event(part))).await {
                    break;
                }
            }
            frame = incoming.message() => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    // The client closed the request half, or the
                    // transport errored reading it — either way there is
                    // no peer left to reply to.
                    Ok(None) | Err(_) => break,
                };
                // The panic belt: a `Backend` that panics mid-call is a
                // connector defect, and an uncontained unwind would end
                // this task with the client's stream simply gone and
                // `close` never run. Contained, the panic becomes a typed
                // internal error on the stream and the best-effort close
                // below still runs.
                let step = CatchUnwind(std::pin::pin!(handle_frame(
                    &shell,
                    &mut state,
                    &part_tx,
                    &mut part_rx,
                    &reply_tx,
                    frame,
                )))
                .await;
                match step {
                    Ok(Step::Continue) => {}
                    Ok(Step::End) => break,
                    Err(payload) => {
                        let _ = reply_tx
                            .send(Err(Status::internal(format!(
                                "the connector's backend panicked while handling the request: {}",
                                gate::panic_text(payload.as_ref())
                            ))))
                            .await;
                        break;
                    }
                }
            }
        }
    }

    // Best-effort close on EVERY exit path above except the explicit
    // `Close` frame (which already ran it and set `closed`) — contained
    // the same way, since a backend that panicked once may panic again.
    if !state.closed
        && let Some(backend) = state.backend.as_mut()
    {
        let _ = CatchUnwind(std::pin::pin!(backend.close())).await;
    }
}

/// A future whose panics are contained: polls the pinned inner future
/// under `catch_unwind`, resolving to `Err(payload)` the moment a poll
/// unwinds; a panicked future is never polled again. Over a `Pin<&mut
/// F>` (`std::pin::pin!` at the call site) so the wrapper is `Unpin`
/// and needs no projection.
struct CatchUnwind<'a, F>(std::pin::Pin<&'a mut F>);

impl<F: std::future::Future> std::future::Future for CatchUnwind<'_, F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.0.as_mut();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(std::task::Poll::Ready(output)) => std::task::Poll::Ready(Ok(output)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(payload) => std::task::Poll::Ready(Err(payload)),
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

        // The one-session-per-process ceiling — refuse a second
        // concurrent `OpenSession` outright, before spawning anything.
        if self
            .session_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Status::failed_precondition(
                "one session per connector process",
            ));
        }
        let slot = SessionSlot(Arc::clone(&self.session_active));

        let incoming = request.into_inner();
        let (reply_tx, reply_rx) = mpsc::channel(REPLY_CHANNEL_BUDGET);

        tokio::spawn(drive_session(shell, incoming, reply_tx, slot));

        Ok(Response::new(ReceiverStream::new(reply_rx)))
    }
}

/// Bind at an explicit path and return the [`Line`] a spawning host
/// would read from stdout, plus a handle for the serving task — WITHOUT
/// printing anything; the seam tests drive rather than [`run`] itself.
/// The bind/serve/line scaffold is `wire::bind_and_serve`; what lives
/// here is the role's service wiring: both gRPC services on the SAME
/// `DestinationServer` instance — they share one handshake-populated
/// shell, so `OpenSession` sees the config a prior `Handshake`
/// validated.
pub async fn run_on<C: DestinationConnector>(
    path: impl AsRef<Path>,
) -> Result<(Line, JoinHandle<Result<(), wire::Error>>), wire::Error> {
    wire::bind_and_serve(path.as_ref(), |incoming| async move {
        let server = Arc::new(DestinationServer::<C>::new());
        tonic::transport::Server::builder()
            .add_service(ConnectorServer::from_arc(Arc::clone(&server)).frame_capped())
            .add_service(DestinationServiceServer::from_arc(server).frame_capped())
            .serve_with_incoming(incoming)
            .await
    })
    .await
}

/// Turn a [`DestinationConnector`] into an out-of-process protocol
/// server: bind a fresh Unix domain socket in a private per-process
/// directory under the system temp directory, print the handshake line
/// on stdout, then serve until the process is killed.
pub async fn run<C: DestinationConnector>() -> Result<(), wire::Error> {
    let (line, handle) = run_on::<C>(wire::socket_path()?).await?;
    wire::announce(&line)?;
    handle.await.map_err(wire::Error::Join)?
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Egress rides the ingress ceiling: a state document the wire
    /// would refuse on the way in cannot leave on the way out either.
    /// The honest document passes untouched; the overgrown one refuses
    /// naming its seat, instead of building at frame scale and being
    /// held as many times over as the client pipelined `ReadState`.
    #[test]
    fn a_state_document_past_the_ceiling_refuses_instead_of_shipping() {
        let mut doc = StateDoc::new(rdlt_connector::core::id::PipelineId::new("p"), "test");
        assert!(
            state_reply(Some(doc.clone())).is_ok(),
            "an honest state document ships"
        );
        assert!(state_reply(None).is_ok(), "no state is not a refusal");

        // Grown past the ceiling by its own contents — the shape a
        // store that outgrew the wire would actually present.
        doc.engine_version = "v".repeat(gate::MAX_DOCUMENT_BYTES as usize + 1);
        let refusal = state_reply(Some(doc)).expect_err("an overgrown document refuses");
        assert!(
            refusal.contains("state_doc_json"),
            "the refusal names the seat: {refusal}"
        );
    }

    /// [`part_close_reason_str`]'s whole point: it must never drift from
    /// [`PartCloseReason`]'s own `Serialize`. All FIVE variants, not a
    /// sample.
    #[test]
    fn part_close_reason_str_matches_the_types_own_serde_spelling() {
        for reason in [
            PartCloseReason::Target,
            PartCloseReason::Time,
            PartCloseReason::Budget,
            PartCloseReason::Commit,
            PartCloseReason::Schema,
        ] {
            let serde_spelling = serde_json::to_value(reason)
                .expect("PartCloseReason serializes")
                .as_str()
                .expect("a string variant")
                .to_string();
            assert_eq!(
                part_close_reason_str(reason),
                serde_spelling,
                "part_close_reason_str diverged from PartCloseReason's own Serialize for {reason:?}"
            );
        }
    }

    /// A fuzz-found 160-byte reproducer, served as a Write: the pinned
    /// property is the TYPED refusal — this input refuses at the
    /// framing pre-pass (its declared framing is already over the
    /// frame's end): the pre-pass first, the belt for what the pre-pass
    /// cannot see.
    #[test]
    fn a_crafted_write_is_a_typed_decode_refusal_never_an_escape() {
        const REPRO: [u8; 160] = [
            0xff, 0xff, 0xff, 0xff, 0x78, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x0a, 0x00, 0x0c, 0x00, 0x06, 0x00, 0x05, 0x00, 0x08, 0x00, 0x0a, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x04, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x08, 0x00, 0x08, 0x00, 0x00, 0x00,
            0x04, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x14, 0x00, 0x00, 0x00, 0x10, 0x00, 0x14, 0x00, 0x08, 0x00, 0x06, 0x00, 0x07, 0x00,
            0x0c, 0x00, 0x00, 0x00, 0x10, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
            0x10, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x69, 0x64, 0x00, 0x00, 0x08, 0x00, 0x0c, 0x00,
            0x08, 0x00, 0x07, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x40, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x29, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
            0x88, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00,
            0x16, 0x00, 0x06, 0x00, 0x05, 0x00,
        ];

        let error = decode_arrow_ipc(&REPRO).expect_err("crafted bytes refuse typed");
        assert!(
            error.starts_with("write carried no decodable record batch: "),
            "the seat's refusal vocabulary, never an escape: {error}"
        );
    }

    /// The panic belt cannot contain the DECLARED-length arms — a 4-byte
    /// word declaring ~2 GiB of metadata makes arrow's reader
    /// commit-and-zero the size before discovering the bytes are
    /// missing. The pre-pass must refuse first; the refusal spelling is
    /// the pre-pass's own, which is the structural proof the reader
    /// (and its allocation) never ran.
    #[test]
    fn a_write_declaring_a_huge_metadata_length_refuses_before_arrow_allocates() {
        let mut frame = vec![0xff, 0xff, 0xff, 0xff];
        frame.extend_from_slice(&0x7fff_fff0_i32.to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);
        let error = decode_arrow_ipc(&frame).expect_err("an overdeclared write refuses typed");
        assert_eq!(
            error,
            "write carried no decodable record batch: a declared metadata length of \
             2147483632 bytes exceeds the 24-byte frame"
        );
    }
}
