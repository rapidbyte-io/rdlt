//! Hand-driven rogue servers for the clause suites — each serves a
//! shape the sdk's serve half can never produce (a skewed identity, a
//! two-batch arrow frame, a client rendering inside a frame message, a
//! second session accepted, a slot never reclaimed, an incomplete
//! pre-handshake Spec reply, a read stream held open forever past its
//! scripted frames, a two-batch write accepted), which is the
//! whole point: certification demands every clause be PROVEN able to
//! fail, and only a server willing to violate the rules can prove it.
//! The 039 idiom (rdlt-connector-client's rogue fixture) promoted into
//! the certifier: raw tonic services on an in-process UDS — no spawn,
//! no built bin, so the rogue suites ride the bare (ungated) run.
//!
//! Test-only by construction (`#[cfg(test)]` at the `lib.rs` mod
//! declaration): nothing shipped can reach a rogue.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rdlt_connector::core::{CommitMeta, CommitReceipt, LoadId, TableSchema};
use rdlt_connector::{ConnectorSpec, StreamSpec};
use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::destination_service_server::{
    DestinationService, DestinationServiceServer,
};
use rdlt_connector_protocol::proto::source_service_server::{SourceService, SourceServiceServer};
use rdlt_connector_protocol::proto::{
    self, Classification, handshake_reply, read_frame, session_reply, session_request,
    streams_reply,
};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

/// Build one wire `ErrorFrame` — the rogues script their refusals with
/// it, including deliberately malformed ones built directly in tests.
pub(crate) fn error_frame(classification: Classification, message: &str) -> proto::ErrorFrame {
    proto::ErrorFrame {
        classification: classification as i32,
        message: message.to_string(),
        retry_after_ms: None,
    }
}

/// Wrap an `ErrorFrame` as a read-stream frame.
pub(crate) fn error_read_frame(frame: proto::ErrorFrame) -> proto::ReadFrame {
    proto::ReadFrame {
        frame: Some(read_frame::Frame::Error(frame)),
    }
}

/// One `arrow_ipc` read frame whose IPC stream carries `batches`
/// record batches — the sdk's own encoder writes exactly one by
/// construction, so a multi-batch frame NEEDS a rogue.
pub(crate) fn arrow_read_frame(batches: usize) -> proto::ReadFrame {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )
    .expect("a one-column batch builds");
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &schema)
            .expect("an IPC stream writer opens over a Vec");
        for _ in 0..batches {
            writer.write(&batch).expect("a batch writes");
        }
        writer.finish().expect("the IPC stream finishes");
    }
    proto::ReadFrame {
        frame: Some(read_frame::Frame::ArrowIpc(bytes)),
    }
}

/// One `arrow_ipc` read frame whose payload alone is one byte past the
/// protocol's frame ceiling ([`MAX_FRAME_BYTES`]), so the encoded wire
/// message (tag + length prefix + payload) lands past it too — the
/// certification bar's oversized-frame arm. The bytes are deliberately
/// NOT an Arrow stream: the claim under test is that the dial-side
/// decode cap refuses the frame at the transport, so no payload decode
/// ever runs. The sdk's serve half caps its own send size, so an
/// oversized frame NEEDS a rogue.
pub(crate) fn oversized_read_frame() -> proto::ReadFrame {
    proto::ReadFrame {
        frame: Some(read_frame::Frame::ArrowIpc(vec![0u8; MAX_FRAME_BYTES + 1])),
    }
}

/// One `raw_json` read frame — a boundary frame for the kill-matrix
/// rogue (K-S2 breaks on the first non-checkpoint frame).
pub(crate) fn json_read_frame() -> proto::ReadFrame {
    proto::ReadFrame {
        frame: Some(read_frame::Frame::RawJson(br#"{"id":1}"#.to_vec())),
    }
}

/// One `checkpoint_cursor_json` read frame — K-S3's boundary frame.
pub(crate) fn checkpoint_read_frame() -> proto::ReadFrame {
    proto::ReadFrame {
        frame: Some(read_frame::Frame::CheckpointCursorJson(b"{}".to_vec())),
    }
}

/// What the rogue's `Handshake` RPC answers.
pub(crate) enum HandshakeScript {
    /// Answer `HandshakeOk` with exactly these values — the skew
    /// rogues script disagreement between the spec document and the
    /// wire's reported identity, and the map rogues populate
    /// `state_format_versions`.
    Ok {
        connector_id: &'static str,
        connector_version: &'static str,
        spec_name: &'static str,
        spec_version: &'static str,
        state_format_versions: &'static [(&'static str, u32)],
    },
    /// Refuse the handshake with a FATAL error frame.
    Refuse { message: &'static str },
    /// Never answer at all — the silent-but-alive connector: the
    /// transport is up (the stack answers pings), the process lives,
    /// and the reply never comes. The SIGKILL matrix cannot produce
    /// this shape (a dead socket errors); only a deadline tells it
    /// from a slow connector, and the clause budget is that deadline.
    Silence,
}

impl HandshakeScript {
    /// The truthful script — id and spec agree — for rogues whose
    /// misbehavior lives elsewhere.
    pub(crate) fn truthful() -> Self {
        HandshakeScript::Ok {
            connector_id: "rogue",
            connector_version: "0.0.0",
            spec_name: "rogue",
            spec_version: "0.0.0",
            state_format_versions: &[],
        }
    }
}

/// A scripted source: handshake, declared streams, and two read
/// scripts — one for reads naming a DECLARED stream, one for anything
/// else (P6's induced refusal reads there).
pub(crate) struct RogueSource {
    pub(crate) handshake: HandshakeScript,
    /// The streams the `Streams` RPC declares.
    pub(crate) streams: Vec<StreamSpec>,
    /// Frames served when `Read` names a declared stream.
    pub(crate) read_declared: Vec<proto::ReadFrame>,
    /// Frames served when `Read` names anything else.
    pub(crate) read_undeclared: Vec<proto::ReadFrame>,
    /// When set, a read stream NEVER ends after its scripted frames —
    /// the sender is parked forever, so a client that outlives its
    /// connector observes silence, not a clean end. The kill matrix's
    /// window-exhaustion rogue (the sdk's serve half always terminates
    /// its streams).
    pub(crate) read_hold_open: bool,
}

#[tonic::async_trait]
impl Connector for RogueSource {
    async fn handshake(
        &self,
        _request: Request<proto::HandshakeRequest>,
    ) -> Result<Response<proto::HandshakeReply>, Status> {
        let outcome = match &self.handshake {
            HandshakeScript::Ok {
                connector_id,
                connector_version,
                spec_name,
                spec_version,
                state_format_versions,
            } => handshake_reply::Outcome::Ok(proto::HandshakeOk {
                connector_id: (*connector_id).to_string(),
                connector_version: (*connector_version).to_string(),
                spec_json: serde_json::to_vec(&ConnectorSpec::new(*spec_name, *spec_version))
                    .expect("a ConnectorSpec serializes to JSON infallibly"),
                capabilities_json: Vec::new(),
                state_format_versions: state_format_versions
                    .iter()
                    .map(|(kind, version)| ((*kind).to_string(), *version))
                    .collect(),
            }),
            HandshakeScript::Refuse { message } => {
                handshake_reply::Outcome::Error(error_frame(Classification::Fatal, message))
            }
            HandshakeScript::Silence => {
                std::future::pending::<()>().await;
                unreachable!("the silent rogue never answers")
            }
        };
        Ok(Response::new(proto::HandshakeReply {
            outcome: Some(outcome),
        }))
    }

    async fn check(
        &self,
        _request: Request<proto::CheckRequest>,
    ) -> Result<Response<proto::CheckReply>, Status> {
        Err(Status::unimplemented(
            "the rogue serves Handshake, Streams and Read alone",
        ))
    }

    async fn spec(
        &self,
        _request: Request<proto::SpecRequest>,
    ) -> Result<Response<proto::SpecReply>, Status> {
        Err(Status::unimplemented(
            "the rogue serves Handshake, Streams and Read alone",
        ))
    }
}

#[tonic::async_trait]
impl SourceService for RogueSource {
    async fn streams(
        &self,
        _request: Request<proto::StreamsRequest>,
    ) -> Result<Response<proto::StreamsReply>, Status> {
        let stream_spec_json = self
            .streams
            .iter()
            .map(|stream| {
                serde_json::to_vec(stream).expect("a StreamSpec serializes to JSON infallibly")
            })
            .collect();
        Ok(Response::new(proto::StreamsReply {
            outcome: Some(streams_reply::Outcome::Ok(proto::StreamList {
                stream_spec_json,
            })),
        }))
    }

    type ReadStream = ReceiverStream<Result<proto::ReadFrame, Status>>;

    async fn read(
        &self,
        request: Request<proto::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let declared = serde_json::from_slice::<StreamSpec>(&request.into_inner().stream_spec_json)
            .map(|spec| self.streams.iter().any(|stream| stream.name == spec.name))
            .unwrap_or(false);
        let frames = if declared {
            self.read_declared.clone()
        } else {
            self.read_undeclared.clone()
        };
        // Preload and drop the sender: the stream serves the scripted
        // frames and then ends — a clean end of stream. The hold-open
        // script instead parks the sender forever: frames, then
        // silence, never an end.
        let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(frames.len().max(1));
        for frame in frames {
            frame_tx
                .try_send(Ok(frame))
                .expect("a preloaded channel sized to its frames has capacity");
        }
        if self.read_hold_open {
            tokio::spawn(async move {
                let _held_forever = frame_tx;
                std::future::pending::<()>().await;
            });
        }
        Ok(Response::new(ReceiverStream::new(frame_rx)))
    }
}

/// Bind the rogue source at `path` (synchronously — no race with a
/// caller that dials right away) and serve until the returned task is
/// dropped.
pub(crate) fn serve_source(path: &Path, rogue: RogueSource) -> JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).expect("bind the rogue's socket");
    let incoming = UnixListenerStream::new(listener);
    let rogue = Arc::new(rogue);
    let serving = tonic::transport::Server::builder()
        .add_service(ConnectorServer::from_arc(Arc::clone(&rogue)))
        .add_service(SourceServiceServer::from_arc(rogue))
        .serve_with_incoming(incoming);
    tokio::spawn(async move {
        let _ = serving.await;
    })
}

/// A rogue answering the config-free pre-handshake `Spec` RPC with
/// whatever document it is scripted with — P4's designated rogue
/// serves an INCOMPLETE one (a blank name, a non-object
/// `config_schema`), shapes the sdk's serve half can never produce
/// because a served connector's spec is built from its own crate
/// constants. Handshake and Check refuse: the P4 probe is the Spec
/// fetch alone, and nothing else may reach this rogue.
pub(crate) struct RogueBlankSpec {
    /// The spec document the `Spec` RPC answers, as scripted.
    pub(crate) spec: ConnectorSpec,
}

#[tonic::async_trait]
impl Connector for RogueBlankSpec {
    async fn handshake(
        &self,
        _request: Request<proto::HandshakeRequest>,
    ) -> Result<Response<proto::HandshakeReply>, Status> {
        Err(Status::unimplemented("the rogue serves Spec alone"))
    }

    async fn check(
        &self,
        _request: Request<proto::CheckRequest>,
    ) -> Result<Response<proto::CheckReply>, Status> {
        Err(Status::unimplemented("the rogue serves Spec alone"))
    }

    async fn spec(
        &self,
        _request: Request<proto::SpecRequest>,
    ) -> Result<Response<proto::SpecReply>, Status> {
        Ok(Response::new(proto::SpecReply {
            spec_json: serde_json::to_vec(&self.spec)
                .expect("a ConnectorSpec serializes to JSON infallibly"),
        }))
    }
}

/// Bind the blank-spec rogue at `path` (synchronously) and serve until
/// the returned task is dropped — [`serve_source`]'s pre-handshake twin
/// for the P4 probe.
pub(crate) fn serve_spec(path: &Path, spec: ConnectorSpec) -> JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).expect("bind the rogue's socket");
    let incoming = UnixListenerStream::new(listener);
    let serving = tonic::transport::Server::builder()
        .add_service(ConnectorServer::new(RogueBlankSpec { spec }))
        .serve_with_incoming(incoming);
    tokio::spawn(async move {
        let _ = serving.await;
    })
}

/// How a rogue destination disciplines its one-session slot — each
/// variant a violation the sdk's own `SessionSlot` makes impossible.
pub(crate) enum SessionDiscipline {
    /// Accept EVERY `OpenSession` — no ceiling at all (P8's designated
    /// rogue).
    AcceptEverySession,
    /// Accept the FIRST `OpenSession` and refuse every later one with
    /// the ceiling status forever, even after the first session's
    /// stream ends — the slot is never reclaimed (P9's designated
    /// rogue).
    NeverReclaim,
}

/// A scripted destination serving `OpenSession` alone: `Open` answers
/// `Opened`, `Close` answers `Closed` and ends the stream, anything
/// else is refused in-stream — enough session for the P8/P9 probes to
/// hold and abandon.
pub(crate) struct RogueDestination {
    discipline: SessionDiscipline,
    opened: AtomicBool,
}

#[tonic::async_trait]
impl DestinationService for RogueDestination {
    type OpenSessionStream = ReceiverStream<Result<proto::SessionReply, Status>>;

    async fn open_session(
        &self,
        request: Request<Streaming<proto::SessionRequest>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        match self.discipline {
            SessionDiscipline::AcceptEverySession => {}
            SessionDiscipline::NeverReclaim => {
                if self.opened.swap(true, Ordering::AcqRel) {
                    return Err(Status::failed_precondition(
                        "one session per connector process",
                    ));
                }
            }
        }
        let mut requests = request.into_inner();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            while let Ok(Some(frame)) = requests.message().await {
                let reply = match frame.request {
                    Some(session_request::Request::Open(_)) => {
                        session_reply::Reply::Opened(proto::Empty {})
                    }
                    Some(session_request::Request::Close(_)) => {
                        let _ = reply_tx
                            .send(Ok(proto::SessionReply {
                                reply: Some(session_reply::Reply::Closed(proto::Empty {})),
                            }))
                            .await;
                        break;
                    }
                    _ => session_reply::Reply::Error(error_frame(
                        Classification::Fatal,
                        "the rogue serves Open and Close alone",
                    )),
                };
                if reply_tx
                    .send(Ok(proto::SessionReply { reply: Some(reply) }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(reply_rx)))
    }
}

/// Bind the rogue destination at `path` (synchronously) and serve
/// until the returned task is dropped — [`serve_source`]'s
/// write-direction twin.
pub(crate) fn serve_destination(path: &Path, discipline: SessionDiscipline) -> JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).expect("bind the rogue's socket");
    let incoming = UnixListenerStream::new(listener);
    let rogue = RogueDestination {
        discipline,
        opened: AtomicBool::new(false),
    };
    let serving = tonic::transport::Server::builder()
        .add_service(DestinationServiceServer::new(rogue))
        .serve_with_incoming(incoming);
    tokio::spawn(async move {
        let _ = serving.await;
    })
}

/// How the order-book rogue plays the session grammar — one variant
/// per deliberate violation the P10 probe must catch, plus the
/// conformant control that proves the probe's happy path completes
/// without a spawned bin.
#[derive(Clone, Copy)]
pub(crate) enum OrderBookScript {
    /// Keeps the whole grammar: refuses unordered writes, keeps
    /// receipts durable across sessions, answers `publish` of an
    /// already-committed load with the prior receipt.
    Conformant,
    /// Answers `written` to a write on a never-ensured table — the
    /// write-before-ensure refusal is missing.
    AcceptWriteBeforeEnsure,
    /// Answers `written` to a write whose arrow_ipc payload carries
    /// more than one record batch — the one-batch refusal is missing
    /// (P11's designated rogue; the sdk's serve half refuses such a
    /// frame itself, so a violation NEEDS a rogue).
    AcceptMultiBatchWrite,
    /// Scripts its induced write-before-ensure refusal with a client
    /// rendering baked into the message (`fatal destination error: `) —
    /// the frame carries a rendered classification where bare cause
    /// text belongs (P12's designated rogue).
    RenderedRefusal,
    /// Reports an existing receipt for every asked load, accepts
    /// `replay`, then ALSO accepts `publish` with a freshly minted
    /// receipt — the replay-vs-publish exclusivity is missing.
    PublishOnReplay,
    /// Never answers `close`: the session goes silent, the reply
    /// stream stays open — the probe can only time out.
    HangOnClose,
    /// Answers `closed`, then emits one `part_closed` event AFTER it —
    /// the one boundary a part event may never cross.
    PartEventAfterClose,
}

/// A scripted destination speaking the FULL session grammar (unlike
/// [`RogueDestination`], whose P8/P9 probes need only Open/Close).
/// Receipts live across sessions — the conformant script's durability —
/// and every `OpenSession` is accepted: P10 never probes the ceiling.
pub(crate) struct RogueOrderBook {
    script: OrderBookScript,
    /// The `(load_id, commit_seq)` pairs a `publish` committed, shared
    /// across sessions the way a real destination's receipt log is.
    published: Arc<Mutex<HashSet<(String, u64)>>>,
}

/// Does `bytes` decode as one Arrow IPC stream carrying MORE than one
/// record batch? Undecodable bytes are not the multi-batch case — the
/// conformant script refuses exactly the one violation P11 induces,
/// judged by the same counter the P5/P11 probes read the wire with.
fn multi_batch(bytes: &[u8]) -> bool {
    matches!(crate::wire::count_batches(bytes), Ok(count) if count > 1)
}

/// One serialized `CommitReceipt` for `(load, seq)` — the rogue's
/// receipts are built from the real type so the probe's JSON judgment
/// reads the shape a shipped connector would serve.
fn receipt_json(load: &str, seq: u64) -> Vec<u8> {
    serde_json::to_vec(&CommitReceipt {
        load_id: LoadId::new(load),
        commit_seq: seq,
    })
    .expect("a CommitReceipt serializes to JSON infallibly")
}

impl RogueOrderBook {
    /// Play one request frame by the script. `None` means "answer
    /// nothing" — the hang script's `close` arm.
    fn play(
        &self,
        ensured: &mut HashSet<String>,
        request: Option<session_request::Request>,
    ) -> Option<session_reply::Reply> {
        let published = || {
            self.published
                .lock()
                .expect("the rogue's receipt set lock is never poisoned")
        };
        Some(match request {
            Some(session_request::Request::Open(_)) => {
                session_reply::Reply::Opened(proto::Empty {})
            }
            Some(session_request::Request::Ensure(ensure)) => {
                if let Ok(schema) = serde_json::from_slice::<TableSchema>(&ensure.table_schema_json)
                {
                    ensured.insert(schema.table.as_str().to_string());
                }
                session_reply::Reply::Ensured(proto::Empty {})
            }
            Some(session_request::Request::Write(write)) => {
                if !matches!(self.script, OrderBookScript::AcceptWriteBeforeEnsure)
                    && !ensured.contains(&write.table)
                {
                    // The rendered-refusal script violates the frame's
                    // TEXT here, not the order book: the refusal still
                    // arrives, carrying a client rendering.
                    let message = match self.script {
                        OrderBookScript::RenderedRefusal => "fatal destination error: boom",
                        _ => "write before ensure_table",
                    };
                    session_reply::Reply::Error(error_frame(Classification::Fatal, message))
                } else if !matches!(self.script, OrderBookScript::AcceptMultiBatchWrite)
                    && multi_batch(&write.arrow_ipc)
                {
                    session_reply::Reply::Error(error_frame(
                        Classification::Fatal,
                        "one record batch per write frame",
                    ))
                } else {
                    session_reply::Reply::Written(proto::Empty {})
                }
            }
            Some(session_request::Request::ExistingReceipt(existing)) => {
                let known = matches!(self.script, OrderBookScript::PublishOnReplay)
                    || published().contains(&(existing.load_id.clone(), existing.commit_seq));
                session_reply::Reply::Receipt(proto::ReceiptReply {
                    receipt_json: known
                        .then(|| receipt_json(&existing.load_id, existing.commit_seq)),
                })
            }
            Some(session_request::Request::Replay(_)) => {
                session_reply::Reply::Replayed(proto::Empty {})
            }
            Some(session_request::Request::Publish(publish)) => {
                match serde_json::from_slice::<CommitMeta>(&publish.commit_meta_json) {
                    Ok(meta) => {
                        let receipt = if matches!(self.script, OrderBookScript::PublishOnReplay) {
                            // The fresh mint: a receipt the reported
                            // existing one never was — seq bumped.
                            receipt_json(meta.load_id.as_str(), meta.commit_seq + 1)
                        } else {
                            published()
                                .insert((meta.load_id.as_str().to_string(), meta.commit_seq));
                            receipt_json(meta.load_id.as_str(), meta.commit_seq)
                        };
                        session_reply::Reply::Published(proto::Published {
                            receipt_json: receipt,
                        })
                    }
                    Err(error) => session_reply::Reply::Error(error_frame(
                        Classification::Fatal,
                        &format!("invalid commit_meta_json: {error}"),
                    )),
                }
            }
            Some(session_request::Request::ReadState(_)) => {
                session_reply::Reply::State(proto::StateReply {
                    state_doc_json: None,
                })
            }
            Some(session_request::Request::Close(_)) => match self.script {
                OrderBookScript::HangOnClose => return None,
                _ => session_reply::Reply::Closed(proto::Empty {}),
            },
            None => session_reply::Reply::Error(error_frame(
                Classification::Fatal,
                "the session received a request frame with no payload",
            )),
        })
    }
}

#[tonic::async_trait]
impl DestinationService for RogueOrderBook {
    type OpenSessionStream = ReceiverStream<Result<proto::SessionReply, Status>>;

    async fn open_session(
        &self,
        request: Request<Streaming<proto::SessionRequest>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let rogue = RogueOrderBook {
            script: self.script,
            published: Arc::clone(&self.published),
        };
        let mut requests = request.into_inner();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            let mut ensured = HashSet::new();
            while let Ok(Some(frame)) = requests.message().await {
                let closing = matches!(frame.request, Some(session_request::Request::Close(_)));
                let Some(reply) = rogue.play(&mut ensured, frame.request) else {
                    // The hang script's Close: answer NOTHING and keep
                    // the stream open — the certifier must outlive it.
                    continue;
                };
                if reply_tx
                    .send(Ok(proto::SessionReply { reply: Some(reply) }))
                    .await
                    .is_err()
                {
                    break;
                }
                if closing {
                    if matches!(rogue.script, OrderBookScript::PartEventAfterClose) {
                        // The boundary violation: `closed` was answered
                        // and a part event follows it anyway.
                        let _ = reply_tx
                            .send(Ok(proto::SessionReply {
                                reply: Some(session_reply::Reply::PartClosed(
                                    proto::PartClosedEvent {
                                        table: "p10_order_book".to_string(),
                                        encoded_bytes: 1,
                                        reason: "commit".to_string(),
                                    },
                                )),
                            }))
                            .await;
                    }
                    // Reply (and any scripted violation) sent, stream
                    // ends.
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(reply_rx)))
    }
}

/// Bind the order-book rogue at `path` (synchronously) and serve until
/// the returned task is dropped — the P10 twin of
/// [`serve_destination`].
pub(crate) fn serve_order_book(path: &Path, script: OrderBookScript) -> JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).expect("bind the rogue's socket");
    let incoming = UnixListenerStream::new(listener);
    let rogue = RogueOrderBook {
        script,
        published: Arc::new(Mutex::new(HashSet::new())),
    };
    let serving = tonic::transport::Server::builder()
        .add_service(DestinationServiceServer::new(rogue))
        .serve_with_incoming(incoming);
    tokio::spawn(async move {
        let _ = serving.await;
    })
}
