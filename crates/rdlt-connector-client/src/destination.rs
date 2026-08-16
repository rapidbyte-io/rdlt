//! [`Destination`] and [`Backend`] — the SPI write seam over the wire:
//! an SPI [`rdlt_connector::Destination`] whose sessions run the sdk's
//! D3 exactly-once choreography CLIENT-side, over a [`Backend`] whose
//! every method is a frame on the `OpenSession` bidi stream.
//!
//! The layering is the whole design (038 T5 review, ADR D5): the wire
//! mirrors the sdk's backend seam
//! ([`Backend`](rdlt_connector_sdk::destination::Backend)) 1:1
//! (`Ensure`/`Write`/`ExistingReceipt`/`Replay`/
//! `Publish`/`ReadState`/`Close`, each frame one method), and the D3
//! commit choreography (`existing_receipt` → `replay` → `publish`) is
//! NOT reimplemented against the wire —
//! [`Destination::open`](rdlt_connector::Destination::open)
//! boxes the sdk's own [`Session`]`::new(Backend)`, the SAME
//! generic type the sdk's serving shell composes, so the choreography
//! runs here by identical code. The server does not referee that ordering
//! (the serve module doc's trust split); its [`WriteGuard`] refusals
//! arrive as ordinary `ErrorFrame` replies and map to typed errors like
//! any other classified failure.
//!
//! [`WriteGuard`]: rdlt_connector_sdk::destination::WriteGuard
//!
//! One bidi stream IS the session: request/reply paced, with
//! `PartClosedEvent` notifications interleaved. Each [`Backend`] call
//! sends its frame and reads replies until its own tagged reply
//! resolves it, forwarding every part event it passes into the
//! [`OpenContext::part_events`] callback — the SPI telemetry seam
//! restored across the wire. Because the serving side drains queued
//! parts BEFORE a call's own reply, every part a backend reported
//! during a call reaches the callback before that call returns here.
//!
//! Abandonment (a session dropped without `close`): the sdk's
//! [`Session`] adds no drop-time behavior of its own — dropping it
//! drops the backend, and dropping a [`Backend`] drops the
//! request sender, which ends the RPC's request stream. That IS the
//! wire's abandonment signal: the serving side observes the stream end
//! and best-effort closes the real backend itself (its F2 cleanup).
//! Nothing here blocks in `Drop` — the client sends no frame on
//! abandonment, matching what the in-process `Session` honestly does.

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use rdlt_connector::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{
    ConnectorSpec, DestinationCapabilities, DestinationError, LoadSession, OpenContext,
    PartCloseReason, PartClosed, PartEventFn, RecordBatch,
};
use rdlt_connector_protocol::proto::{self, session_reply, session_request};
use rdlt_connector_sdk::destination::Session;
use tokio::sync::mpsc;
use tonic::Streaming;
use tonic::transport::Channel;

use crate::error::FromWire;
use crate::{error, gate, handshake, wire};

/// The frozen fatal for a reply stream that ends while a call is still
/// waiting on its reply — the session is over, whoever ended it.
const SESSION_ENDED: &str = "the connector session ended before replying";

/// An SPI [`rdlt_connector::Destination`] over the wire: the dialed
/// channel plus the handshake's cached spec AND capabilities — both
/// answered synchronously, with no RPC left to make. Constructed only
/// through [`Destination::connect`] — there is no way to hold one whose
/// identity was not verified (D-039-2).
#[derive(Debug)]
pub struct Destination {
    channel: Channel,
    spec: ConnectorSpec,
    capabilities: DestinationCapabilities,
    deadline: Duration,
}

impl Destination {
    /// Dial `socket_path` (the engine budget paces the wire — see
    /// [`wire::dial`]) and run the [`handshake::Role::Destination`]
    /// handshake, verifying the connector against `expected`. Returns
    /// the adapter AND the full [`handshake::Outcome`], mirroring
    /// [`Source::connect`](crate::source::Source::connect).
    pub async fn connect(
        socket_path: &Path,
        engine_budget_bytes: u64,
        config: &serde_json::Value,
        expected: &handshake::Requirement,
    ) -> Result<(Destination, handshake::Outcome), error::Error> {
        let (channel, outcome) = handshake::establish(
            socket_path,
            engine_budget_bytes,
            config,
            handshake::Role::Destination,
            expected,
        )
        .await?;
        // The proto pins `capabilities_json` non-empty for destinations
        // — a destination handshake without one is a wire the protocol
        // does not define, refused rather than defaulted (an all-false
        // sheet would silently plan away merge support).
        let capabilities = outcome.capabilities.ok_or_else(|| {
            error::Error::Protocol("the destination handshake carried no capabilities".to_string())
        })?;
        Ok((
            Destination {
                channel,
                spec: outcome.spec.clone(),
                capabilities,
                deadline: expected.rpc_deadline,
            },
            outcome,
        ))
    }

    /// Open the raw wire [`Backend`] directly — the lower layer
    /// [`Destination::open`](rdlt_connector::Destination::open)
    /// composes [`Session`] on top of, mirroring
    /// the sdk's `Shell::connect` split: nothing here enforces
    /// write-before-ensure or the D3 choreography (the SERVER's guard
    /// still polices frame order on its side of the trust boundary).
    /// Sends `Open{pipeline, load_id}` and awaits `Opened`; a refused
    /// connect arrives as the Open frame's `ErrorFrame` reply.
    pub async fn open_backend(&self, context: &OpenContext) -> Result<Backend, DestinationError> {
        // Capacity 1 is enough by construction: the session is
        // request/reply paced — every send awaits its reply before the
        // next, so a slot is always free.
        let (requests, feed) = mpsc::channel::<proto::SessionRequest>(1);
        let mut client = wire::destination_client(self.channel.clone());
        let replies = wire::bounded(
            self.deadline,
            wire::Operation::Reply,
            client.open_session(tokio_stream::wrappers::ReceiverStream::new(feed)),
        )
        .await
        .map_err(DestinationError::fatal_error)?
        .map_err(DestinationError::transport)?
        .into_inner();

        let mut backend = Backend {
            requests,
            replies,
            part_events: context.part_events.clone(),
            deadline: self.deadline,
        };
        let reply = backend
            .call(session_request::Request::Open(proto::Open {
                pipeline: context.pipeline.as_str().to_string(),
                load_id: context.load_id.as_str().to_string(),
            }))
            .await?;
        match reply {
            session_reply::Reply::Opened(_) => Ok(backend),
            other => Err(unexpected_reply("Open", &other)),
        }
    }
}

#[async_trait]
impl rdlt_connector::Destination for Destination {
    /// The handshake's cached document — no RPC (see the source half's
    /// `spec`, same cache rationale).
    fn spec(&self) -> ConnectorSpec {
        self.spec.clone()
    }

    async fn check(&self) -> Result<(), DestinationError> {
        wire::check(&self.channel, self.deadline).await
    }

    /// The handshake-cached sheet, synchronously — the trait's own
    /// shape ("the host plans from this and does not re-verify at
    /// runtime") already forbids an RPC here, and the cache is what
    /// makes the answer honest rather than a default.
    fn capabilities(&self) -> DestinationCapabilities {
        self.capabilities
    }

    async fn open(&self, context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        Ok(Box::new(Session::new(self.open_backend(&context).await?)))
    }
}

/// A reply whose variant is not the one `method`'s frame is defined to
/// receive — a protocol violation, not a data outcome. Not a frozen
/// spelling: a conforming server never produces it.
fn unexpected_reply(method: &str, reply: &session_reply::Reply) -> DestinationError {
    // The shared escape over a BOUNDED Debug rendering: `Debug` escapes
    // control bytes but leaves the inventory's Lo-category fillers raw,
    // and a wrong-variant reply can carry a workload-sized document — a
    // diagnostic line is not a firehose, so the render truncates at a
    // char boundary. The cap is the RAW prefix: the escape can expand
    // each control char to ~10 bytes (`\u{10ffff}`), so the worst-case
    // message is ~10× the cap — bounded and inert, an order below the
    // workload-sized render it replaced.
    const REPLY_RENDER_CAP: usize = 2048;
    let debug = format!("{reply:?}");
    let bounded = if debug.len() <= REPLY_RENDER_CAP {
        gate::escape(&debug).into_owned()
    } else {
        let mut cut = REPLY_RENDER_CAP;
        while !debug.is_char_boundary(cut) {
            cut -= 1;
        }
        format!(
            "{}…[truncated from {} bytes]",
            gate::escape(&debug[..cut]),
            debug.len()
        )
    };
    DestinationError::protocol(format!(
        "the connector answered {method} with an unexpected reply: {bounded}"
    ))
}

/// One batch as an Arrow IPC *stream* carrying exactly one record-batch
/// message — the `Write` frame's encoder seat of the proto's one-batch
/// rule (a multi-batch write is several `Write` frames, one batch
/// each; `Session` hands this method one batch at a time, so the rule
/// holds by construction here). Failure is a schema/batch mismatch the
/// CALLER produced — rendered as text for a fatal, like the serve
/// side's `encode_arrow_ipc`.
fn encode_one_batch(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), batch.schema_ref())
        .map_err(|error| format!("opening an arrow ipc stream writer: {error}"))?;
    writer
        .write(batch)
        .map_err(|error| format!("writing an arrow ipc record batch: {error}"))?;
    writer
        .into_inner()
        .map_err(|error| format!("closing an arrow ipc stream writer: {error}"))
}

/// The wire backend: one bidi session, each method one frame and its
/// tagged reply. The public face is the sdk's
/// [`Backend`](rdlt_connector_sdk::destination::Backend) trait —
/// construction goes through [`Destination::open_backend`] alone, so no
/// backend exists whose `Open` the server did not accept.
///
/// An `ErrorFrame` reply maps back through the one `FromWire` mapping and
/// the session stays usable (matching serve semantics — the refusal
/// was a reply, not a stream end); the stream ending mid-await is the
/// frozen `SESSION_ENDED` fatal; a transport `Status` is fatal
/// safe-loud with the transport named.
pub struct Backend {
    requests: mpsc::Sender<proto::SessionRequest>,
    replies: Streaming<proto::SessionReply>,
    /// The SPI telemetry seam, carried across the wire: interleaved
    /// `PartClosedEvent` replies land here.
    part_events: Option<PartEventFn>,
    /// The requirement's RPC deadline, bounding each reply await.
    deadline: Duration,
}

impl std::fmt::Debug for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("part_events", &self.part_events.is_some())
            .finish_non_exhaustive()
    }
}

impl Backend {
    /// Forward one wire part event into the SPI callback. An unknown
    /// reason spelling (a NEWER server's variant this build does not
    /// know) skips the event rather than panicking or inventing a
    /// reason: part events are advisory telemetry, and a lossy skip is
    /// the honest degradation for a vocabulary gap. A table name
    /// carrying control characters is NOT a vocabulary gap — a table
    /// name is filesystem-adjacent and travels into host telemetry, so
    /// it rides the identifier gate and refuses typed like a declared
    /// stream name, before the event can reach the callback.
    fn forward_part(&self, event: proto::PartClosedEvent) -> Result<(), DestinationError> {
        gate::identifier("table name", &event.table).map_err(DestinationError::fatal)?;
        let Some(listener) = &self.part_events else {
            return Ok(());
        };
        let reason = serde_json::Value::String(event.reason);
        let Ok(reason) = serde_json::from_value::<PartCloseReason>(reason) else {
            return Ok(());
        };
        listener(PartClosed::new(
            TableName::new(event.table),
            event.encoded_bytes,
            reason,
        ));
        Ok(())
    }

    /// Send one frame and read replies until its own tagged reply
    /// resolves it: `part_closed` notifications forward to the callback
    /// and the loop continues; an `ErrorFrame` resolves as the typed
    /// error (the session stays usable); any other reply resolves as
    /// itself for the caller to match.
    ///
    /// Each reply await is bounded by the RPC deadline, and the bound
    /// is per QUIET INTERVAL: every reply that arrives — a `part_closed`
    /// included — starts the next wait's clock afresh, so a publish
    /// that legitimately reports many parts before its own reply never
    /// trips it, while a session that goes silent mid-call (a flood
    /// followed by silence included) always fails typed. What the
    /// deadline deliberately does NOT bound: a rogue that keeps
    /// flooding `part_closed` without ever resolving the call keeps
    /// this loop spinning for as long as it keeps sending — memory
    /// stays bounded (one reply in flight), and the spin ends when the
    /// host cancels or drops the session; a total-duration bound here
    /// would instead fail legitimate long publishes.
    async fn call(
        &mut self,
        request: session_request::Request,
    ) -> Result<session_reply::Reply, DestinationError> {
        // A failed send means the server side of the session is gone.
        // Deliberately not an error of its own: fall through to the
        // reply stream, whose terminal state carries the real diagnosis
        // (a clean end → the frozen SESSION_ENDED, a broken transport →
        // its Status). The send is deadline-bounded like every other
        // await here: a window-starving rogue that stops draining
        // requests would otherwise wedge this await BEFORE the bounded
        // reply read ever starts — the elapse reports as the reply
        // that never came, which is what the caller observes either
        // way.
        match wire::bounded(
            self.deadline,
            wire::Operation::Reply,
            self.requests.send(proto::SessionRequest {
                request: Some(request),
            }),
        )
        .await
        {
            Ok(_sent_or_gone) => {}
            Err(timeout) => return Err(DestinationError::fatal_error(timeout)),
        }
        loop {
            let next = wire::bounded(
                self.deadline,
                wire::Operation::Reply,
                self.replies.message(),
            )
            .await
            .map_err(DestinationError::fatal_error)?;
            let reply = match next {
                Ok(Some(proto::SessionReply { reply: Some(reply) })) => reply,
                Ok(Some(proto::SessionReply { reply: None })) => {
                    return Err(DestinationError::protocol(
                        "a session reply carried no payload".to_string(),
                    ));
                }
                Ok(None) => return Err(DestinationError::fatal(SESSION_ENDED)),
                Err(status) => return Err(DestinationError::transport(status)),
            };
            match reply {
                session_reply::Reply::PartClosed(event) => self.forward_part(event)?,
                session_reply::Reply::Error(frame) => {
                    return Err(DestinationError::from_frame(&frame));
                }
                resolved => return Ok(resolved),
            }
        }
    }
}

#[async_trait]
impl rdlt_connector_sdk::destination::Backend for Backend {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        let reply = self
            .call(session_request::Request::Ensure(proto::Ensure {
                table_schema_json: serde_json::to_vec(schema)
                    .expect("a TableSchema serializes to JSON infallibly"),
                write_mode_json: serde_json::to_vec(mode)
                    .expect("a WriteMode serializes to JSON infallibly"),
            }))
            .await?;
        match reply {
            session_reply::Reply::Ensured(_) => Ok(()),
            other => Err(unexpected_reply("Ensure", &other)),
        }
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let arrow_ipc = encode_one_batch(&batch).map_err(DestinationError::fatal)?;
        let reply = self
            .call(session_request::Request::Write(proto::Write {
                table: table.as_str().to_string(),
                arrow_ipc,
            }))
            .await?;
        match reply {
            session_reply::Reply::Written(_) => Ok(()),
            other => Err(unexpected_reply("Write", &other)),
        }
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        let reply = self
            .call(session_request::Request::ExistingReceipt(
                proto::ExistingReceipt {
                    load_id: load_id.as_str().to_string(),
                    commit_seq,
                },
            ))
            .await?;
        match reply {
            session_reply::Reply::Receipt(receipt) => receipt
                .receipt_json
                .map(|bytes| {
                    serde_json::from_slice::<CommitReceipt>(&bytes).map_err(|error| {
                        DestinationError::protocol(format!(
                            "undecodable receipt_json in a session reply: {}",
                            rdlt_connector::json::describe_parse_error(&error)
                        ))
                    })
                })
                .transpose(),
            other => Err(unexpected_reply("ExistingReceipt", &other)),
        }
    }

    async fn replay(
        &mut self,
        meta: &CommitMeta,
        receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        let reply = self
            .call(session_request::Request::Replay(proto::Replay {
                commit_meta_json: serde_json::to_vec(meta)
                    .expect("a CommitMeta serializes to JSON infallibly"),
                receipt_json: serde_json::to_vec(receipt)
                    .expect("a CommitReceipt serializes to JSON infallibly"),
            }))
            .await?;
        match reply {
            session_reply::Reply::Replayed(_) => Ok(()),
            other => Err(unexpected_reply("Replay", &other)),
        }
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let reply = self
            .call(session_request::Request::Publish(proto::Publish {
                commit_meta_json: serde_json::to_vec(&meta)
                    .expect("a CommitMeta serializes to JSON infallibly"),
            }))
            .await?;
        match reply {
            session_reply::Reply::Published(published) => {
                serde_json::from_slice::<CommitReceipt>(&published.receipt_json).map_err(|error| {
                    DestinationError::protocol(format!(
                        "undecodable receipt_json in a session reply: {}",
                        rdlt_connector::json::describe_parse_error(&error)
                    ))
                })
            }
            other => Err(unexpected_reply("Publish", &other)),
        }
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let reply = self
            .call(session_request::Request::ReadState(proto::ReadState {
                pipeline: pipeline.as_str().to_string(),
            }))
            .await?;
        match reply {
            session_reply::Reply::State(state) => state
                .state_doc_json
                .map(|bytes| {
                    // `StateDoc` is a typed shell around UNTYPED cursor
                    // values, so this seat gets the document ceiling
                    // every untyped parse runs — BEFORE the parse whose
                    // materialization it bounds. An honest state
                    // document is summarized cursors measured in
                    // kilobytes; a multi-megabyte one embedded data.
                    gate::document("state_doc_json", &bytes).map_err(DestinationError::protocol)?;
                    let doc = serde_json::from_slice::<StateDoc>(&bytes).map_err(|error| {
                        DestinationError::protocol(format!(
                            "undecodable state_doc_json in a session reply: {}",
                            rdlt_connector::json::describe_parse_error(&error)
                        ))
                    })?;
                    // And every cursor it carries honors the cursor
                    // contract on its SERIALIZED form — the same
                    // re-serialized gate the checkpoint seat runs, so a
                    // cursor that parses inside the document ceiling
                    // cannot still inflate past the contract the WAL
                    // line cap is sized for. The stream names are
                    // connector identifiers and ride the identifier
                    // gate.
                    for (stream, cursor) in &doc.cursors {
                        gate::identifier("stream name", stream.as_str())
                            .map_err(DestinationError::protocol)?;
                        gate::cursor(cursor.as_value()).map_err(|reason| {
                            DestinationError::protocol(format!(
                                "the state document's cursor for `{}` violates \
                                 the cursor contract: {reason}",
                                // The shared escape: this refusal must not
                                // carry the bytes it judges.
                                gate::escape(stream.as_str())
                            ))
                        })?;
                    }
                    Ok(doc)
                })
                .transpose(),
            other => Err(unexpected_reply("ReadState", &other)),
        }
    }

    /// STRICT on the orderly path: sends `Close` and awaits `Closed`,
    /// so a backend whose close fails reports it (the reply is that
    /// failure's `ErrorFrame`). The best-effort half of the close
    /// contract needs no frame at all — see the module doc's
    /// abandonment paragraph.
    async fn close(&mut self) -> Result<(), DestinationError> {
        let reply = self
            .call(session_request::Request::Close(proto::Close {}))
            .await?;
        match reply {
            session_reply::Reply::Closed(_) => Ok(()),
            other => Err(unexpected_reply("Close", &other)),
        }
    }
}
