//! Hand-driven rogue servers for the clause suites — each serves a
//! shape the sdk's serve half can never produce (a skewed identity, a
//! two-batch arrow frame, a client rendering inside a frame message, a
//! second session accepted, a slot never reclaimed), which is the
//! whole point: certification demands every clause be PROVEN able to
//! fail, and only a server willing to violate the rules can prove it.
//! The 039 idiom (rdlt-connector-client's rogue fixture) promoted into
//! the certifier: raw tonic services on an in-process UDS — no spawn,
//! no built bin, so the rogue suites ride the bare (ungated) run.
//!
//! Test-only by construction (`#[cfg(test)]` at the `lib.rs` mod
//! declaration): nothing shipped can reach a rogue.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rdlt_connector::{ConnectorSpec, StreamSpec};
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
        // frames and then ends — a clean end of stream.
        let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(frames.len().max(1));
        for frame in frames {
            frame_tx
                .try_send(Ok(frame))
                .expect("a preloaded channel sized to its frames has capacity");
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
