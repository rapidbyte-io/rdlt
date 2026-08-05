//! The destination half of `serve()`: `DestinationServer` answers both
//! halves of the wire protocol a destination connector implements —
//! `Connector` (handshake, check) and `DestinationService` (the
//! `OpenSession` bidi stream) — driving the connector's raw [`Backend`]
//! directly, NOT a [`rdlt_connector::LoadSession`] wrapper (038 T5
//! review, ADR D5: an earlier version of this module wrapped
//! `Shell::open`'s `Box<dyn LoadSession>`, which made the wire's
//! `ExistingReceipt`/`Replay` frames inert stubs rather than real
//! answers — the design doc's amendment records the reversal).
//!
//! ONE long-lived bidirectional stream IS the session: it mirrors a
//! [`Backend`]'s own lifetime (a stream reset is the session's crash
//! class; a client half-close is its orderly end). Every wire frame
//! (`Ensure`/`Write`/`ExistingReceipt`/`Replay`/`Publish`/`ReadState`/
//! `Close`) maps 1:1 onto its own [`Backend`] method — the wire speaks
//! the REAL exactly-once grammar, not a collapsed `commit`.
//!
//! The choreography splits along the trust boundary this server sits
//! on. [`crate::destination::WriteGuard`] (write-before-ensure,
//! open-once) is enforced HERE, directly against the frames as they
//! arrive, because a bidi stream carrying client-supplied ORDER never
//! trusts that order — the same rule an in-process caller gets for free
//! by construction (one [`crate::destination::Session`] per
//! `Destination::open` call) has to be policed by hand against a wire
//! client that might get it wrong. The D3 commit choreography
//! (`existing_receipt` → `replay` → `publish`) is NOT enforced here at
//! all: each of those three frames reaches its own `Backend` method
//! independently, and the CALLER decides which to send next — an
//! in-process embedder never sees this layer (it gets the choreography
//! for free from `Session::commit`), and 039's remote-backend adapter
//! will reconstruct it client-side over the SAME `Session<B>` generic,
//! reusing it by identical type rather than reimplementing it against
//! the wire. A foreign client that gets the choreography wrong — for
//! instance, sending `Publish` twice for one `(load_id, commit_seq)`
//! without ever asking `ExistingReceipt` first — is not this server's
//! problem to referee: see [`crate::destination::Backend::existing_receipt`]'s
//! own doc for why a transactional backend keeps its own durable guard
//! as defense in depth, independent of whatever choreography a caller
//! was supposed to run ("Backends whose receipts live in the same
//! transaction as their publish keep their internal guard too; this is
//! the protocol fast path").
//!
//! LOUD, because it is easy to read past: v0's wire literally does not
//! sequence commit frames. A foreign client CAN send `Publish` twice for
//! the same `(load_id, commit_seq)` without ever asking `ExistingReceipt`
//! first, and nothing in THIS SERVER stops it — see the paragraph just
//! above, this server does not referee frame order. The ONLY thing that
//! saves exactly-once here is the destination's OWN durable receipt
//! guard inside `Backend::publish`; a shipped `Backend` that does not
//! keep one is wire-reachably double-publishable. This is a RECORDED
//! gap, not a silently accepted one (038 T5 review round 2, F-4): the
//! sdk's own black-box conformance kit never drives `Backend` directly
//! today — it only exercises the in-process `Session<B>` path, where the
//! choreography above IS enforced by the caller — so nothing in this
//! codebase currently proves a shipped `Backend` actually keeps that
//! guard when reached this way. Feature 040's conformance kit needs a
//! Backend-direct D3-companion clause: drive `Publish` twice over the
//! WIRE with no `ExistingReceipt`/`Replay` in between and assert the
//! second either replays the first receipt or is refused — never
//! silently re-applies.
//!
//! `OpenContext::part_events` is the other place this server departs
//! from a plain request/reply shape: the listener is a SYNC callback,
//! so any part it reports while a `Backend` call is in flight is
//! already sitting in the unbounded channel by the time that call's
//! `await` returns. Draining that channel immediately BEFORE sending
//! the reply for the call that (may have) produced it is what the
//! ordering promise actually covers: every part already queued when a
//! call returns precedes that call's own reply. An asynchronously
//! emitted part — one a buffering backend fires from a task this server
//! never directly awaited — carries no such promise; it simply arrives
//! as its own `PartClosedEvent` reply as soon as the request loop next
//! turns (the `biased` `select!` in `drive_session`), which may land
//! before, after, or interleaved with any particular request's reply.
//!
//! v0 allows exactly ONE live session per served listener
//! (`DestinationServer::session_active` — one `DestinationServer` per
//! call to [`serve_on`]) — a second concurrent `OpenSession` is refused
//! outright, `Status::failed_precondition`, frozen wording `one session
//! per connector process`. The wording says "process" rather than
//! "listener" because [`destination`], the only entry a spawned
//! connector process actually runs, opens exactly one listener per
//! process — so "per served listener" and "per connector process" name
//! the SAME ceiling as shipped; a caller embedding [`serve_on`] directly
//! (as every test in this crate does) could in principle stand up more
//! than one listener in one process, and the frozen text does not
//! promise anything about that case. This is a deliberate ceiling, not
//! a discovered limitation: loosening it later (one listener serving
//! several concurrent sessions) is additive; shipping against the
//! current single-session assumption and tightening it later would not
//! be. A real backend's own per-session staging guard (the file
//! connector's S6 lease is the precedent) is the defense-in-depth
//! BEHIND this ceiling, not a replacement for it — refusing the RPC up
//! front means no backend ever has to detect a second concurrent
//! session on its own.
//!
//! v0 has no idle timeout (038 T5 review round 2, F-8): `OpenSession`
//! spawns one task per pipeline session, and a stalled or hung client
//! holds the one-session slot above indefinitely — nothing in this
//! layer evicts it. 039's provider (whatever supervises the connector
//! process) owns liveness — heartbeats, process-level timeouts,
//! restart-on-hang — not this layer.
//!
//! [`destination`] is what a spawned connector process actually runs;
//! [`serve_on`] is the seam under it, mirroring
//! [`super::source::serve_on`] — bind at an explicit path without
//! printing anything, so a test can drive the very listener
//! [`destination`] would have started.

use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use rdlt_connector::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{Destination, DestinationError, OpenContext, PartCloseReason, PartClosed};
use rdlt_connector_protocol::PROTOCOL_VERSION;
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
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

use super::common::{self, ServeError};
use crate::config::Document;
use crate::destination::{Backend, DestinationConnector, Shell, WriteGuard};

/// Bound on the reply channel one session forwards into. Every reply on
/// it is triggered by ONE client frame (request/reply-paced, not a
/// throughput stream) except `PartClosedEvent`s: those QUEUE in the
/// unbounded `part_tx`/`part_rx` pair, but each still traverses this
/// SAME bounded channel as its own reply when forwarded. So the bound
/// caps how many already-produced replies — request replies and
/// forwarded part events alike — can sit unread while the CLIENT stalls
/// reading its own stream; a telemetry burst beyond it parks in the
/// unbounded pair (the forwarding loop parking with it) rather than
/// growing this channel, so this is not a throughput budget. 16, the
/// same order of magnitude as the source side's read-frame channel, for
/// the same reason: headroom.
const REPLY_CHANNEL_BUDGET: usize = 16;

/// The role a destination's handshake must be asked for — mirrors
/// `EXPECTED_ROLE` on the source side.
const EXPECTED_ROLE: &str = "destination";

/// The gRPC surface over one [`DestinationConnector`]. `shell` is empty
/// until a handshake succeeds; `Arc` because `OpenSession` hands a clone
/// to a spawned task that outlives the request. `session_active` is F5's
/// one-session-per-process ceiling — see the module doc.
struct DestinationServer<C: DestinationConnector> {
    shell: OnceLock<Arc<Shell<C>>>,
    session_active: Arc<AtomicBool>,
}

impl<C: DestinationConnector> DestinationServer<C> {
    fn new() -> Self {
        Self {
            shell: OnceLock::new(),
            session_active: Arc::new(AtomicBool::new(false)),
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
impl<C: DestinationConnector> common::HandshakeShell for Shell<C> {
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
        // Unlike a source's empty capabilities, a destination's ARE the
        // host's planning input (merge/replace/widen support) — the
        // proto field doc names this explicitly.
        serde_json::to_vec(&self.capabilities())
            .expect("DestinationCapabilities serializes to JSON infallibly")
    }
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
        reason: part_close_reason_str(part.reason),
    }
}

/// [`PartCloseReason`]'s wire spelling, taken DIRECTLY from its own
/// `Serialize` impl rather than reproduced by hand (038 T5 review, F7):
/// a hand-maintained match table drifted from the SPI's own spelling
/// silently, with a wildcard arm swallowing the mismatch as "unknown" —
/// indistinguishable from a genuinely new variant. A unit-variant enum
/// with `#[serde(rename_all = "snake_case")]` always serializes to a
/// plain JSON string; the `Debug`-formatted fallback only fires if a
/// FUTURE variant somehow serializes to something else (e.g. a struct
/// variant would serialize as a JSON object), and its PascalCase
/// spelling can never collide with a real snake_case reason.
fn part_close_reason_str(reason: PartCloseReason) -> String {
    match serde_json::to_value(reason) {
        Ok(serde_json::Value::String(spelling)) => spelling,
        _ => format!("{reason:?}"),
    }
}

/// One Arrow IPC *stream*'s exactly-one record batch — the `Write`
/// frame's wire counterpart to the source side's `encode_arrow_ipc`; the
/// proto's own comment on `Write` states the one-batch rule. Bytes that
/// are not a valid IPC stream, a stream with no batch message, or a
/// stream whose SECOND message fails to decode all collapse to the ONE
/// frozen prefix (`write carried no decodable record batch`, 038 T5
/// review's own quoted spelling) with the underlying arrow cause
/// appended — the same frozen-prefix-plus-cause discipline
/// [`decode_error_reply`] uses for the `*_json` fields, so none of these
/// three legs is the one place that silently drops the diagnostic
/// detail (038 T5 review round 2, item 4: an earlier version folded the
/// "second message present but corrupt" case into the multi-batch
/// refusal below, discarding its cause — a genuinely different failure
/// than "there IS a decodable second batch", which alone gets the
/// multi-batch spelling). A stream carrying a SECOND, DECODABLE batch
/// message gets its own, distinct refusal (038 T5 review, F3: silently
/// taking only the first batch would drop every row after it —
/// measured as the defect this refusal exists to prevent, not a
/// hypothetical).
fn decode_arrow_ipc(bytes: &[u8]) -> Result<rdlt_connector::RecordBatch, String> {
    const REFUSAL: &str = "write carried no decodable record batch";

    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| format!("{REFUSAL}: {error}"))?;
    let first = match reader.next() {
        Some(Ok(batch)) => batch,
        Some(Err(error)) => return Err(format!("{REFUSAL}: {error}")),
        None => return Err(REFUSAL.to_string()),
    };
    match reader.next() {
        Some(Ok(_)) => Err(
            "write carried more than one record batch; a Write frame is exactly one batch"
                .to_string(),
        ),
        Some(Err(error)) => Err(format!("{REFUSAL}: {error}")),
        None => Ok(first),
    }
}

#[tonic::async_trait]
impl<C: DestinationConnector> Connector for DestinationServer<C> {
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
        // Config-free static identity — the schema command's path (039):
        // answered from `C::NAME`/`C::VERSION`/`C::config_schema()`
        // alone, exempted from the pre-handshake refusal BY
        // CONSTRUCTION since it never calls `shell()`.
        Ok(common::spec_reply(C::NAME, C::VERSION, C::config_schema()))
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
/// bundled into one struct (rather than three separate `&mut` params)
/// to keep that function's arity under clippy's `too_many_arguments`
/// bar; grouping them is also the honest shape, since `guard`/`backend`/
/// `closed` all describe facets of the SAME one session, not
/// independent pieces of plumbing.
struct SessionState<C: DestinationConnector> {
    guard: WriteGuard,
    /// `None` until an `Open` frame succeeds, then `Some` for the rest
    /// of the stream's life.
    backend: Option<C::Backend>,
    /// Set by the explicit `Close` arm so [`drive_session`]'s F2
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
/// [`WriteGuard`] (bundled in `state`). Every frame maps to its OWN
/// `Backend` method call (ADR D5): `ExistingReceipt`/`Replay`/`Publish`
/// each reach `Backend::existing_receipt`/`replay`/`publish` directly
/// rather than a collapsed `commit` — see the module doc for why this
/// server does not referee the D3 choreography those three imply, only
/// the write-before-ensure/open-once guard.
async fn handle_frame<C: DestinationConnector>(
    shell: &Shell<C>,
    state: &mut SessionState<C>,
    part_tx: &mpsc::UnboundedSender<PartClosed>,
    part_rx: &mut mpsc::UnboundedReceiver<PartClosed>,
    reply_tx: &mpsc::Sender<Result<SessionReply, Status>>,
    frame: SessionRequest,
) -> Step {
    if let Some(session_request::Request::Open(open)) = frame.request {
        // 038 T5 review round 2, item 2: the guard is checked BEFORE
        // attempting `connect`, and marked open ONLY after `connect`
        // SUCCEEDS — never eagerly. An eager mark would consume the
        // guard's one Open on a merely Transient connect failure,
        // leaving `opened == true` with no backend and no way to retry
        // on the same stream: every later frame (including a retrying
        // `Open`) would then refuse, downgrading a retryable failure to
        // one that is not. With the check-then-mark-on-success split, a
        // failed `Open` is legal to retry on the same stream — the
        // guard never learns anything happened until it actually did.
        let reply = if state.guard.is_open() {
            // Frozen-as-shipped shape difference: this refusal renders
            // through `DestinationError`'s `Display` — the client sees
            // the "fatal destination error: " prefix — while its
            // sibling refusals (`refuse_before_open`, the decode
            // refusals) are bare `common::error_frame` messages with no
            // prefix. Both spellings are pinned by the serve suite; do
            // not "harmonize" them.
            session_reply::Reply::Error(destination_error_frame(&DestinationError::fatal(
                "a session accepts at most one Open frame, and it must be first",
            )))
        } else {
            let tx = part_tx.clone();
            let context =
                OpenContext::new(PipelineId::new(open.pipeline), LoadId::new(open.load_id))
                    .with_part_events(Arc::new(move |part| {
                        // The listener is a plain sync callback
                        // (`OpenContext`'s own contract: "must never
                        // fail and never block") — an unbounded
                        // channel is the correct shape for it:
                        // advisory-volume telemetry, never awaited
                        // from inside the callback itself.
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
        return finish(part_rx, reply_tx, refuse_before_open()).await;
    };

    let reply = match frame.request {
        Some(session_request::Request::Open(_)) => unreachable!("handled above"),
        Some(session_request::Request::Ensure(ensure)) => {
            let schema = serde_json::from_slice::<TableSchema>(&ensure.table_schema_json);
            let mode = serde_json::from_slice::<WriteMode>(&ensure.write_mode_json);
            match (schema, mode) {
                (Ok(schema), Ok(mode)) => match backend.ensure_table(&schema, &mode).await {
                    Ok(()) => {
                        guard.ensure(schema.table.clone());
                        session_reply::Reply::Ensured(proto::Empty {})
                    }
                    Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                },
                (Err(error), _) => decode_error_reply("table_schema_json", error),
                (_, Err(error)) => decode_error_reply("write_mode_json", error),
            }
        }
        Some(session_request::Request::Write(write)) => {
            let table = TableName::new(write.table);
            match guard.check_write(&table) {
                Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                Ok(()) => match decode_arrow_ipc(&write.arrow_ipc) {
                    Ok(batch) => match backend.write(&table, batch).await {
                        Ok(()) => session_reply::Reply::Written(proto::Empty {}),
                        Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                    },
                    Err(message) => session_reply::Reply::Error(common::error_frame(
                        Classification::Fatal,
                        message,
                        None,
                    )),
                },
            }
        }
        Some(session_request::Request::ExistingReceipt(existing)) => {
            // ADR D5: this touches `Backend::existing_receipt` directly
            // — a REAL lookup, not a stub. The D3 choreography deciding
            // whether to follow this with `Replay` or `Publish` is NOT
            // this server's job: it lives in the CALLER's `Session<B>`
            // (the client-side `Session<RemoteBackend>` 039 builds), the
            // SAME generic type the in-process path composes. A foreign
            // client that gets the choreography wrong — e.g. double-
            // publishing one `(load_id, commit_seq)` — is caught by the
            // destination's own DURABLE receipt guard, not by this
            // server refereeing wire order: see
            // `Backend::existing_receipt`'s own doc — "Backends whose
            // receipts live in the same transaction as their publish
            // keep their internal guard too; this is the protocol fast
            // path."
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
        Some(session_request::Request::Replay(replay)) => {
            let meta = serde_json::from_slice::<CommitMeta>(&replay.commit_meta_json);
            let receipt = serde_json::from_slice::<CommitReceipt>(&replay.receipt_json);
            match (meta, receipt) {
                (Ok(meta), Ok(receipt)) => match backend.replay(&meta, &receipt).await {
                    Ok(()) => session_reply::Reply::Replayed(proto::Empty {}),
                    Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
                },
                (Err(error), _) => decode_error_reply("commit_meta_json", error),
                (_, Err(error)) => decode_error_reply("receipt_json", error),
            }
        }
        Some(session_request::Request::Publish(publish)) => {
            match serde_json::from_slice::<CommitMeta>(&publish.commit_meta_json) {
                Ok(meta) => match backend.publish(meta).await {
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
            match backend
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
            let reply = match backend.close().await {
                Ok(()) => session_reply::Reply::Closed(proto::Empty {}),
                Err(error) => session_reply::Reply::Error(destination_error_frame(&error)),
            };
            // The explicit close ran — `drive_session`'s best-effort
            // abandoned-session cleanup (F2) must not run it AGAIN.
            state.closed = true;
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

/// Releases [`DestinationServer::session_active`] on drop — covers
/// EVERY [`drive_session`] exit path (a clean `Close`, a client hangup,
/// a transport error, a reply the client can no longer receive)
/// uniformly, so F5's one-session ceiling can never leak stuck-active
/// from a codepath that forgot to release it by hand. `open_session`
/// acquires the slot BEFORE spawning `drive_session`, which then owns
/// it for the rest of the session's life.
struct SessionSlot(Arc<AtomicBool>);

impl Drop for SessionSlot {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Run one session's request loop, from its `Open` to whatever ends it —
/// a clean `Close`, the client hanging up, or a transport error — then
/// (F2, 038 T5 review) best-effort close the backend on EVERY LOOP EXIT
/// that is NOT the explicit `Close` frame. `LoadSession::close`'s
/// contract (unchanged for a raw `Backend`): "Called exactly once
/// whenever the session ends", and on a failure/cancellation path the
/// caller invokes it best-effort, ignoring its error — the second
/// half deliberately unquoted, since it condenses the contract's own
/// longer sentence. Before this fix, an abandoned session (a
/// client that vanishes mid-`Write`) leaked whatever the backend opened,
/// because nothing but the explicit `Close` arm ever called it — the
/// 037 US2 T7 leak class, reopened for the wire.
///
/// "Every loop exit" is deliberately narrower than "every way this
/// function can stop running" (038 T5 review round 2, item 3): this
/// cleanup is a plain `if` after the `loop`, not `Drop`-based like
/// `SessionSlot` below, because `Backend::close` is `async` and Rust has
/// no async `Drop` — there is no safe way to run it from a destructor.
/// So it runs on every path THIS function's own `loop` takes to a
/// `break` (client hangup, transport error, a reply the client can no
/// longer receive), but NOT if something outside this function ever
/// aborts the task `drive_session` runs in (`JoinHandle::abort`) while
/// it is parked mid-`select!` — that would skip straight to the
/// destructor phase, and only synchronous state (like `SessionSlot`'s
/// atomic release just below) survives that. Nothing in this crate
/// currently holds such an abort handle (`open_session` spawns and
/// discards it), so the gap is real but currently unreachable from
/// inside this codebase — recorded rather than silently covered by
/// wording that would overclaim what a non-async destructor can do.
async fn drive_session<C: DestinationConnector>(
    shell: Arc<Shell<C>>,
    mut incoming: Streaming<SessionRequest>,
    reply_tx: mpsc::Sender<Result<SessionReply, Status>>,
    _slot: SessionSlot,
) {
    // Sync callback, advisory-volume telemetry (`OpenContext`'s own
    // doc): unbounded is correct here specifically because the sender
    // side never awaits and never blocks on backpressure — it is not a
    // general escape hatch from the byte-budget discipline the read
    // side observes.
    let (part_tx, mut part_rx) = mpsc::unbounded_channel::<PartClosed>();
    let mut state = SessionState::<C>::new();

    loop {
        // `biased`: a part event queued from a PREVIOUS iteration's
        // `Backend` call is forwarded before this iteration reads its
        // next request frame — the between-requests half of the
        // ordering guarantee. The within-one-request half (a part event
        // fired synchronously by the call THIS iteration is about to
        // run) is handled by `finish`'s explicit drain immediately
        // before that request's own reply.
        //
        // Racing `part_rx.recv()` against `incoming.message()` here
        // relies on tonic 0.14.6's `Streaming::message` being
        // cancel-safe: it decodes into a buffer owned by `&mut self`
        // (the `Streaming` value), not by the returned future, so
        // dropping the LOSING branch's future on any given `select!`
        // iteration (as happens to whichever branch does not win) never
        // discards a partially-decoded frame — the next `.message()`
        // call resumes cleanly. This reliance is deliberate, not
        // incidental: a non-cancel-safe read here would need its own
        // buffering to race safely against `part_rx`.
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
                match handle_frame(&shell, &mut state, &part_tx, &mut part_rx, &reply_tx, frame).await {
                    Step::Continue => {}
                    Step::End => break,
                }
            }
        }
    }

    // F2: best-effort close on EVERY exit path above except the
    // explicit `Close` frame (which already ran it and set `closed`).
    if !state.closed
        && let Some(backend) = state.backend.as_mut()
    {
        let _ = backend.close().await;
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

        // F5: v0's one-session-per-process ceiling — refuse a second
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
/// printing anything. Mirrors the source side's `serve_on` — see there
/// for why this is the seam tests drive rather than [`destination`]
/// itself.
///
/// Both gRPC services ([`Connector`] and [`DestinationService`]) are
/// wired to the SAME `DestinationServer` instance — they share one
/// handshake-populated shell, so `OpenSession` sees the config a prior
/// `Handshake` validated.
pub async fn serve_on<C: DestinationConnector>(
    path: impl AsRef<Path>,
) -> Result<(Line, JoinHandle<Result<(), ServeError>>), ServeError> {
    let path = path.as_ref();
    let listener = common::bind_uds(path)?;
    let incoming = UnixListenerStream::new(listener);

    let server = Arc::new(DestinationServer::<C>::new());
    // `max_decoding_message_size` on BOTH services: tonic's 4 MiB
    // default receive cap is below what one legitimate `Write` frame
    // may carry — see `common::MAX_FRAME_BYTES`'s own doc.
    let serving = tonic::transport::Server::builder()
        .add_service(
            ConnectorServer::from_arc(Arc::clone(&server))
                .max_decoding_message_size(common::MAX_FRAME_BYTES),
        )
        .add_service(
            DestinationServiceServer::from_arc(server)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// [`part_close_reason_str`]'s whole point: it must never drift from
    /// [`PartCloseReason`]'s own `Serialize` — the 030 paraphrase class
    /// (a hand-copied spelling silently diverging from the type it
    /// mirrors). All FIVE variants, not a sample.
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
}
