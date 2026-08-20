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

use std::io::Write as _;
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
use rdlt_connector::{channel, gate};
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::destination_service_server::{
    DestinationService, DestinationServiceServer,
};
use rdlt_connector_protocol::proto::{
    self, CheckReply, CheckRequest, Classification, ErrorFrame, HandshakeReply, HandshakeRequest,
    PartClosedEvent, Published, ReceiptReply, SessionReply, SessionRequest, StateReply,
    check_reply, session_reply, session_request,
};
use rdlt_connector_protocol::{MAX_FRAME_BYTES, PROTOCOL_VERSION};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

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
/// is 16 documents of that bound — a few tens of megabytes, reachable
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
    /// The process-wide ceiling on concurrent `Check` calls
    /// ([`wire::MAX_CONCURRENT_PROBES`]): a check runs the connector's
    /// own code, and had no admission bound of its own. The session
    /// seat has the one-session slot; this is the other door.
    probe_admission: Arc<tokio::sync::Semaphore>,
}

impl<C: DestinationConnector> DestinationServer<C> {
    fn new() -> Self {
        Self {
            shell: OnceLock::new(),
            session_active: Arc::new(AtomicBool::new(false)),
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
}

/// Flatten a classified [`DestinationError`] into the wire's
/// [`ErrorFrame`]: the classification as the enum, the INNER cause's
/// text as the message (never the SPI's `Display` frame — the receiving
/// client renders the classification exactly once on reconstruction),
/// the rate-limit hint when there is one. The wildcard arm is required
/// (`DestinationError` is `#[non_exhaustive]` from outside its crate)
/// and keeps the full `Display` rather than dropping text.
fn destination_error_frame(error: &DestinationError) -> ErrorFrame {
    let (classification, message, retry_after) = match error {
        DestinationError::Transient(cause) => (Classification::Transient, cause.to_string(), None),
        DestinationError::RateLimited {
            retry_after,
            source,
        } => (
            Classification::RateLimited,
            source.to_string(),
            *retry_after,
        ),
        DestinationError::Fatal(cause) => (Classification::Fatal, cause.to_string(), None),
        _ => (Classification::Fatal, error.to_string(), None),
    };
    wire::error_frame(classification, message, retry_after)
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
/// the first would drop every row after it.
fn decode_arrow_ipc(bytes: &[u8]) -> Result<RecordBatch, String> {
    contained_decode(|| decode_arrow_ipc_erring(bytes))
}

/// The seat's panic belt: `catch_unwind` contains PANICS only. The
/// declared-length ABORT class is closed by the framing pre-pass inside
/// `decode_arrow_ipc_erring` — the two defenses are disjoint.
fn contained_decode<T>(work: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
        Ok(decoded) => decoded,
        Err(payload) => Err(format!(
            "write carried no decodable record batch: the Arrow decoder panicked: {}",
            // A panic payload is attacker-adjacent text; the shared
            // rendering bounds it.
            gate::panic_text(payload.as_ref())
        )),
    }
}

/// The greatest number of columns one wire batch may declare — a
/// schema message spends only forty-odd flatbuffer bytes per field, so
/// field COUNT is the cheapest per-byte expansion an Arrow frame
/// offers, and each field becomes an owned `Field` and an array. A
/// table this wide is already past what any destination models
/// honestly.
///
/// This bounds WIDTH only. Rows are bounded separately, so the two
/// together admit a rectangle of up to ~4.1e9 cells — well above the
/// engine's own per-batch cell budget, which is the host's to enforce
/// on its own side. The wire's job here is to keep either dimension
/// from being spent as an allocation lever; it is deliberately not a
/// cell budget, and calling it one would overstate it.
const MAX_RECORD_BATCH_COLUMNS: usize = 4096;

fn decode_arrow_ipc_erring(bytes: &[u8]) -> Result<RecordBatch, String> {
    const REFUSAL: &str = "write carried no decodable record batch";

    // The framing pre-pass: the panic belt catches arrow's unwinds, but
    // the DECLARED-length arms — a 2 GiB metadata memset, a `bodyLength`
    // allocation whose failure ABORTS — are neither panics nor errors,
    // so the declarations are held against the frame's real bytes
    // before the reader runs.
    gate::refuse_overdeclared_framing(bytes).map_err(|reason| format!("{REFUSAL}: {reason}"))?;
    // Arrow's error text is client-authored at frame scale — a `Field`
    // render carries the field's whole metadata map — so every appended
    // cause goes through the bounded diagnostic render, the client
    // decode mirror's exact treatment.
    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| {
        format!(
            "{REFUSAL}: {}",
            gate::render_diagnostic(&error.to_string(), 256)
        )
    })?;
    // Width is judged on the SCHEMA, before a batch is pulled: by the
    // time a `RecordBatch` exists arrow has built every `Field` AND
    // every array, which is the allocation this cap exists to prevent.
    // The schema message is read by `try_new` above, so the count is
    // known here.
    let declared_columns = reader.schema().fields().len();
    if declared_columns > MAX_RECORD_BATCH_COLUMNS {
        return Err(format!(
            "{REFUSAL}: the batch declares {declared_columns} columns, over the {}-column \
             wire cap — column count is bounded separately from encoded bytes",
            MAX_RECORD_BATCH_COLUMNS
        ));
    }
    let first = match reader.next() {
        Some(Ok(batch)) => batch,
        Some(Err(error)) => {
            return Err(format!(
                "{REFUSAL}: {}",
                gate::render_diagnostic(&error.to_string(), 256)
            ));
        }
        None => return Err(REFUSAL.to_string()),
    };
    // Row count is the memory dimension the framing pre-pass cannot
    // see — Null and run-end-encoded columns carry millions of rows in
    // almost no body bytes, and the batch goes straight to the
    // connector's own backend.
    if first.num_rows() > channel::MAX_RECORD_BATCH_ROWS {
        return Err(format!(
            "{REFUSAL}: the batch carries {} rows, over the {}-row wire cap — row count is \
             bounded separately from encoded bytes",
            first.num_rows(),
            channel::MAX_RECORD_BATCH_ROWS
        ));
    }
    match reader.next() {
        Some(Ok(_)) => Err(
            "write carried more than one record batch; a Write frame is exactly one batch"
                .to_string(),
        ),
        Some(Err(error)) => Err(format!(
            "{REFUSAL}: {}",
            gate::render_diagnostic(&error.to_string(), 256)
        )),
        None => Ok(first),
    }
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
            Err(error) => check_reply::Outcome::Error(destination_error_frame(&error)),
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
///
/// Both gRPC services are wired to the SAME `DestinationServer`
/// instance — they share one handshake-populated shell, so
/// `OpenSession` sees the config a prior `Handshake` validated.
/// `max_decoding_message_size` on BOTH: tonic's 4 MiB default receive
/// cap is below what one legitimate `Write` frame may carry.
pub async fn run_on<C: DestinationConnector>(
    path: impl AsRef<Path>,
) -> Result<(Line, JoinHandle<Result<(), wire::Error>>), wire::Error> {
    let path = path.as_ref();
    let listener = wire::bind(path)?;
    let incoming = UnixListenerStream::new(listener);

    let server = Arc::new(DestinationServer::<C>::new());
    let serving = tonic::transport::Server::builder()
        .add_service(
            ConnectorServer::from_arc(Arc::clone(&server))
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_FRAME_BYTES),
        )
        .add_service(
            DestinationServiceServer::from_arc(server)
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

/// Turn a [`DestinationConnector`] into an out-of-process protocol
/// server: bind a fresh Unix domain socket in a private per-process
/// directory under the system temp directory, print the handshake line
/// on stdout (flushed — the spawning host is reading a pipe, not a
/// TTY), then serve until the process is killed.
pub async fn run<C: DestinationConnector>() -> Result<(), wire::Error> {
    let (line, handle) = run_on::<C>(wire::socket_path()?).await?;

    let mut stdout = std::io::stdout();
    writeln!(stdout, "{}", line.render()).map_err(wire::Error::Stdout)?;
    stdout.flush().map_err(wire::Error::Stdout)?;

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

    /// The belt pinned DIRECTLY (synthetic, because every live input that
    /// once reached it now refuses earlier at the pre-pass): an unwind
    /// inside the decode is classified as a typed refusal, never an
    /// escape.
    #[test]
    fn a_decoder_panic_is_contained_as_a_typed_refusal() {
        let error = contained_decode::<()>(|| panic!("crafted metadata"))
            .expect_err("the unwind is contained");
        assert!(error.contains("Arrow decoder panicked"), "{error}");
        assert!(error.contains("crafted metadata"), "{error}");
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

    /// The body-length sibling: a real one-batch stream whose
    /// record-batch message is patched to declare a ~2 GiB body — the
    /// allocation whose failure ABORTS, which no belt contains. The
    /// patch locates `bodyLength` structurally (root table offset →
    /// vtable → the field's slot), self-verified against the decoded
    /// value before patching.
    #[test]
    fn a_write_declaring_a_huge_body_length_refuses_before_arrow_allocates() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .expect("batch");
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &schema).expect("writer");
        writer.write(&batch).expect("write");
        let mut bytes = writer.into_inner().expect("finish");
        assert!(
            decode_arrow_ipc(&bytes).is_ok(),
            "the unpatched stream decodes"
        );

        // Walk to the second message (the record batch); patch its
        // declared bodyLength.
        let (meta_start, meta_end) = {
            let mut pos = 0usize;
            let mut spans = Vec::new();
            while spans.len() < 2 {
                assert_eq!(&bytes[pos..pos + 4], [0xff; 4], "continuation marker");
                let meta_len =
                    i32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().expect("4 bytes"))
                        as usize;
                let start = pos + 8;
                spans.push((start, start + meta_len));
                let body = usize::try_from(
                    arrow::ipc::root_as_message(&bytes[start..start + meta_len])
                        .expect("valid metadata")
                        .bodyLength(),
                )
                .expect("non-negative body");
                pos = start + meta_len + body;
            }
            spans[1]
        };
        let meta = &bytes[meta_start..meta_end];
        let root = u32::from_le_bytes(meta[0..4].try_into().expect("4 bytes")) as i64;
        let soffset = i32::from_le_bytes(
            meta[root as usize..root as usize + 4]
                .try_into()
                .expect("4 bytes"),
        ) as i64;
        let vtable = usize::try_from(root - soffset).expect("in-bounds vtable");
        let slot =
            u16::from_le_bytes(meta[vtable + 10..vtable + 12].try_into().expect("2 bytes")) as i64;
        assert_ne!(slot, 0, "bodyLength is present in the message");
        let field_pos = meta_start + usize::try_from(root + slot).expect("in-bounds field");
        bytes[field_pos..field_pos + 8].copy_from_slice(&0x7fff_fff0_i64.to_le_bytes());

        let frame_len = bytes.len();
        let error = decode_arrow_ipc(&bytes).expect_err("an overdeclared body refuses typed");
        assert_eq!(
            error,
            format!(
                "write carried no decodable record batch: a declared body length of \
                 2147483632 bytes exceeds the {frame_len}-byte frame"
            )
        );
    }
}
