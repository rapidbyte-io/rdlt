//! Hand-driven servers that serve EXACTLY the frames a test scripts —
//! including shapes the sdk's serve half can never emit (its arrow
//! encoder writes exactly one batch per frame by construction; its
//! session refusals are `ErrorFrame` replies, never a raw `Status`
//! inside the stream), which is the whole point: the client's refusal
//! seats need a server willing to violate the rules, and the pacing
//! observation needs a producer with no in-connector byte budget
//! between it and the wire. Each `Connector` half answers the minimal
//! truthful handshake (`rogue`/`0.0.0`) so the client's `connect`
//! reaches the scripted RPC at all; everything else is deliberately
//! unimplemented.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rdlt_connector::{ConnectorSpec, DestinationCapabilities};
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::destination_service_server::{
    DestinationService, DestinationServiceServer,
};
use rdlt_connector_protocol::proto::source_service_server::{SourceService, SourceServiceServer};
use rdlt_connector_protocol::proto::{self, handshake_reply, read_frame, session_reply};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

/// What the rogue's `Read` RPC does.
#[derive(Debug, Clone)]
pub enum ReadScript {
    /// Send these frames verbatim, then end the stream cleanly.
    Frames(Vec<proto::ReadFrame>),
    /// Send these frames verbatim, then go SILENT — the stream never
    /// ends and no further frame arrives: the silent-but-alive shape
    /// the per-frame deadline exists to catch.
    FramesThenSilence(Vec<proto::ReadFrame>),
    /// Send these frames one by one, sleeping `interval` before each —
    /// the slow-but-flowing shape the per-frame deadline must NOT trip
    /// on, however long the whole stream takes. Ends cleanly.
    Drip {
        frames: Vec<proto::ReadFrame>,
        interval: std::time::Duration,
    },
    /// Send copies of `frame` for as long as the client keeps the RPC
    /// alive, adding the frame's payload size to `sent_bytes` after each
    /// send the response channel ACCEPTS — the pacing observer: an
    /// accepted send is a frame the transport was willing to take, so
    /// the counter reads how far flow control let the producer run.
    Blast {
        frame: proto::ReadFrame,
        sent_bytes: Arc<AtomicU64>,
    },
}

/// The payload bytes a scripted frame carries — what the blast counter
/// adds per accepted send.
fn frame_payload_bytes(frame: &proto::ReadFrame) -> u64 {
    match &frame.frame {
        Some(read_frame::Frame::RawJson(bytes))
        | Some(read_frame::Frame::ArrowIpc(bytes))
        | Some(read_frame::Frame::CheckpointCursorJson(bytes)) => bytes.len() as u64,
        _ => 0,
    }
}

/// The scripted server both gRPC services are wired to.
#[derive(Debug)]
pub struct Rogue {
    script: ReadScript,
}

#[tonic::async_trait]
impl Connector for Rogue {
    async fn handshake(
        &self,
        _request: Request<proto::HandshakeRequest>,
    ) -> Result<Response<proto::HandshakeReply>, Status> {
        // Minimal and truthful: whatever role/config the client asked
        // for is accepted — the rogue exists to serve scripted frames,
        // not to re-test the sdk's handshake gates.
        let spec = ConnectorSpec::new("rogue", "0.0.0");
        Ok(Response::new(proto::HandshakeReply {
            outcome: Some(handshake_reply::Outcome::Ok(proto::HandshakeOk {
                connector_id: "rogue".to_string(),
                connector_version: "0.0.0".to_string(),
                spec_json: serde_json::to_vec(&spec)
                    .expect("a ConnectorSpec serializes to JSON infallibly"),
                capabilities_json: Vec::new(),
                state_format_versions: Default::default(),
            })),
        }))
    }

    async fn check(
        &self,
        _request: Request<proto::CheckRequest>,
    ) -> Result<Response<proto::CheckReply>, Status> {
        Err(Status::unimplemented("the rogue serves Read alone"))
    }

    async fn spec(
        &self,
        _request: Request<proto::SpecRequest>,
    ) -> Result<Response<proto::SpecReply>, Status> {
        Err(Status::unimplemented("the rogue serves Read alone"))
    }
}

#[tonic::async_trait]
impl SourceService for Rogue {
    async fn streams(
        &self,
        _request: Request<proto::StreamsRequest>,
    ) -> Result<Response<proto::StreamsReply>, Status> {
        Err(Status::unimplemented("the rogue serves Read alone"))
    }

    type ReadStream = ReceiverStream<Result<proto::ReadFrame, Status>>;

    async fn read(
        &self,
        _request: Request<proto::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        match self.script.clone() {
            ReadScript::Frames(frames) => {
                // Preload and drop the sender: the stream serves the
                // scripted frames and then ends — a clean end of stream.
                let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(frames.len().max(1));
                for frame in frames {
                    frame_tx
                        .try_send(Ok(frame))
                        .expect("a preloaded channel sized to its frames has capacity");
                }
                Ok(Response::new(ReceiverStream::new(frame_rx)))
            }
            ReadScript::FramesThenSilence(frames) => {
                // Preload, then park the sender forever: the scripted
                // frames arrive and nothing ever follows — silence, not
                // a clean end.
                let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(frames.len().max(1));
                for frame in frames {
                    frame_tx
                        .try_send(Ok(frame))
                        .expect("a preloaded channel sized to its frames has capacity");
                }
                tokio::spawn(async move {
                    let _held_forever = frame_tx;
                    std::future::pending::<()>().await;
                });
                Ok(Response::new(ReceiverStream::new(frame_rx)))
            }
            ReadScript::Drip { frames, interval } => {
                let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(1);
                tokio::spawn(async move {
                    for frame in frames {
                        tokio::time::sleep(interval).await;
                        if frame_tx.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    // The sender drops here: a clean end of stream.
                });
                Ok(Response::new(ReceiverStream::new(frame_rx)))
            }
            ReadScript::Blast { frame, sent_bytes } => {
                // Capacity 1 so the counter tracks the TRANSPORT's
                // appetite, not this channel's own buffering: each send
                // completes only once tonic pulled the previous frame
                // toward the wire.
                let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(1);
                let size = frame_payload_bytes(&frame);
                tokio::spawn(async move {
                    // Ends when the client tears the RPC down (dropping
                    // its Streaming resets the stream and this channel's
                    // receiver drops with it).
                    while frame_tx.send(Ok(frame.clone())).await.is_ok() {
                        sent_bytes.fetch_add(size, Ordering::Relaxed);
                    }
                });
                Ok(Response::new(ReceiverStream::new(frame_rx)))
            }
        }
    }
}

/// Bind the rogue at `path` and serve until the returned task is
/// dropped — the test-side mirror of the sdk's `serve_on`, minus every
/// gate the sdk enforces.
pub fn serve(path: &Path, script: ReadScript) -> JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).expect("bind the rogue's socket");
    let incoming = UnixListenerStream::new(listener);
    let rogue = Arc::new(Rogue { script });
    let serving = tonic::transport::Server::builder()
        .add_service(ConnectorServer::from_arc(Arc::clone(&rogue)))
        .add_service(SourceServiceServer::from_arc(rogue))
        .serve_with_incoming(incoming);
    tokio::spawn(async move {
        let _ = serving.await;
    })
}

/// What the rogue destination's `OpenSession` does.
#[derive(Debug, Clone)]
pub enum SessionScript {
    /// Answer the `Open` frame with `Opened` (so the client's
    /// `open_backend` succeeds), then answer the NEXT request frame
    /// with `Err(Status)` INSIDE the reply stream — the mid-session
    /// transport failure the sdk's serve half never emits (its
    /// refusals are `ErrorFrame` replies and its endings are clean),
    /// which is exactly why the client reply loop's `Err` arm needs a
    /// rogue to reach it.
    OpenedThenStatus { code: tonic::Code, message: String },
    /// Answer the `Open` frame with `Opened`, consume the NEXT request
    /// frame, then before going SILENT emit `parts` interleaved
    /// `part_closed` events naming `table` — the stream never ends and
    /// the call's own reply never comes. With `parts: 0` this is the
    /// bare silent-but-alive session; with parts it proves a flood
    /// does not defeat the quiet-interval deadline: every event resets
    /// the clock, and the silence AFTER the flood still times out
    /// typed. A hostile `table` drives the wire edge's control-
    /// character refusal.
    OpenedThenPartsThenSilence { parts: u64, table: String },
}

/// The scripted destination server: a truthful handshake carrying a
/// capabilities sheet (the client refuses a destination handshake
/// without one), then the scripted session.
#[derive(Debug)]
pub struct RogueDestination {
    script: SessionScript,
    /// Overrides the handshake's capabilities sheet when `Some` — the
    /// 5M6 rogue declares an out-of-range `ident_rules.max_len`.
    capabilities: Option<DestinationCapabilities>,
}

#[tonic::async_trait]
impl Connector for RogueDestination {
    async fn handshake(
        &self,
        _request: Request<proto::HandshakeRequest>,
    ) -> Result<Response<proto::HandshakeReply>, Status> {
        let spec = ConnectorSpec::new("rogue", "0.0.0");
        Ok(Response::new(proto::HandshakeReply {
            outcome: Some(handshake_reply::Outcome::Ok(proto::HandshakeOk {
                connector_id: "rogue".to_string(),
                connector_version: "0.0.0".to_string(),
                spec_json: serde_json::to_vec(&spec)
                    .expect("a ConnectorSpec serializes to JSON infallibly"),
                capabilities_json: serde_json::to_vec(&self.capabilities.unwrap_or_default())
                    .expect("a capabilities sheet serializes to JSON infallibly"),
                state_format_versions: Default::default(),
            })),
        }))
    }

    async fn check(
        &self,
        _request: Request<proto::CheckRequest>,
    ) -> Result<Response<proto::CheckReply>, Status> {
        Err(Status::unimplemented("the rogue serves OpenSession alone"))
    }

    async fn spec(
        &self,
        _request: Request<proto::SpecRequest>,
    ) -> Result<Response<proto::SpecReply>, Status> {
        Err(Status::unimplemented("the rogue serves OpenSession alone"))
    }
}

#[tonic::async_trait]
impl DestinationService for RogueDestination {
    type OpenSessionStream = ReceiverStream<Result<proto::SessionReply, Status>>;

    async fn open_session(
        &self,
        request: Request<Streaming<proto::SessionRequest>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let script = self.script.clone();
        let mut requests = request.into_inner();
        // Capacity 1 matches the client's request/reply pacing: each
        // send below completes only once tonic pulled the previous
        // reply toward the wire.
        let (reply_tx, reply_rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            // Consume the Open frame and accept it, so the failure
            // lands MID-SESSION — inside an in-flight Backend call,
            // not at the open seat (a handler-level Err(Status)
            // surfaces trailers-only there; T4's record).
            let _ = requests.message().await;
            let _ = reply_tx
                .send(Ok(proto::SessionReply {
                    reply: Some(session_reply::Reply::Opened(proto::Empty {})),
                }))
                .await;
            let _ = requests.message().await;
            match script {
                SessionScript::OpenedThenStatus { code, message } => {
                    let _ = reply_tx.send(Err(Status::new(code, message))).await;
                }
                SessionScript::OpenedThenPartsThenSilence { parts, table } => {
                    for index in 0..parts {
                        let _ = reply_tx
                            .send(Ok(proto::SessionReply {
                                reply: Some(session_reply::Reply::PartClosed(
                                    proto::PartClosedEvent {
                                        table: table.clone(),
                                        encoded_bytes: index,
                                        reason: "commit".to_string(),
                                    },
                                )),
                            }))
                            .await;
                    }
                    let _held_forever = reply_tx;
                    std::future::pending::<()>().await;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(reply_rx)))
    }
}

/// Where the [`Mute`] connector goes silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MuteSeat {
    /// The handshake handler never answers — the wire is up, the
    /// connector is alive, and nothing ever comes back.
    Handshake,
    /// The handshake answers truthfully; `check` never does.
    Check,
}

/// A connector that is ALIVE but silent at the scripted seat: its
/// transport works (h2 setup completes, keep-alive pings are answered
/// by the stack), so nothing but a deadline can tell it from a slow
/// one — the exact shape the SIGKILL kill matrix cannot cover.
#[derive(Debug)]
pub struct Mute {
    seat: MuteSeat,
}

#[tonic::async_trait]
impl Connector for Mute {
    async fn handshake(
        &self,
        _request: Request<proto::HandshakeRequest>,
    ) -> Result<Response<proto::HandshakeReply>, Status> {
        if self.seat == MuteSeat::Handshake {
            std::future::pending::<()>().await;
        }
        let spec = ConnectorSpec::new("mute", "0.0.0");
        Ok(Response::new(proto::HandshakeReply {
            outcome: Some(handshake_reply::Outcome::Ok(proto::HandshakeOk {
                connector_id: "mute".to_string(),
                connector_version: "0.0.0".to_string(),
                spec_json: serde_json::to_vec(&spec)
                    .expect("a ConnectorSpec serializes to JSON infallibly"),
                capabilities_json: Vec::new(),
                state_format_versions: Default::default(),
            })),
        }))
    }

    async fn check(
        &self,
        _request: Request<proto::CheckRequest>,
    ) -> Result<Response<proto::CheckReply>, Status> {
        std::future::pending::<()>().await;
        unreachable!("the mute connector never answers check")
    }

    async fn spec(
        &self,
        _request: Request<proto::SpecRequest>,
    ) -> Result<Response<proto::SpecReply>, Status> {
        Err(Status::unimplemented("the mute connector serves silence"))
    }
}

/// A connector whose handshake reports exactly the identity a test
/// scripts — spec document built from the same values, so only the
/// wire-edge gates under test judge them.
#[derive(Debug)]
pub struct Identity {
    id: &'static str,
    version: &'static str,
}

#[tonic::async_trait]
impl Connector for Identity {
    async fn handshake(
        &self,
        _request: Request<proto::HandshakeRequest>,
    ) -> Result<Response<proto::HandshakeReply>, Status> {
        let spec = ConnectorSpec::new(self.id, self.version);
        Ok(Response::new(proto::HandshakeReply {
            outcome: Some(handshake_reply::Outcome::Ok(proto::HandshakeOk {
                connector_id: self.id.to_string(),
                connector_version: self.version.to_string(),
                spec_json: serde_json::to_vec(&spec)
                    .expect("a ConnectorSpec serializes to JSON infallibly"),
                capabilities_json: Vec::new(),
                state_format_versions: Default::default(),
            })),
        }))
    }

    async fn check(
        &self,
        _request: Request<proto::CheckRequest>,
    ) -> Result<Response<proto::CheckReply>, Status> {
        Err(Status::unimplemented("the identity rogue only handshakes"))
    }

    async fn spec(
        &self,
        _request: Request<proto::SpecRequest>,
    ) -> Result<Response<proto::SpecReply>, Status> {
        Err(Status::unimplemented("the identity rogue only handshakes"))
    }
}

/// Bind the identity rogue at `path` and serve until the returned task
/// is dropped.
pub fn serve_identity(path: &Path, id: &'static str, version: &'static str) -> JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).expect("bind the identity rogue's socket");
    let incoming = UnixListenerStream::new(listener);
    let serving = tonic::transport::Server::builder()
        .add_service(ConnectorServer::new(Identity { id, version }))
        .serve_with_incoming(incoming);
    tokio::spawn(async move {
        let _ = serving.await;
    })
}

/// Bind the mute connector at `path` and serve until the returned task
/// is dropped.
pub fn serve_mute(path: &Path, seat: MuteSeat) -> JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).expect("bind the mute's socket");
    let incoming = UnixListenerStream::new(listener);
    let serving = tonic::transport::Server::builder()
        .add_service(ConnectorServer::new(Mute { seat }))
        .serve_with_incoming(incoming);
    tokio::spawn(async move {
        let _ = serving.await;
    })
}

/// Bind the rogue destination at `path` and serve until the returned
/// task is dropped — [`serve`]'s destination twin.
pub fn serve_destination(path: &Path, script: SessionScript) -> JoinHandle<()> {
    serve_destination_with_capabilities(path, script, None)
}

/// [`serve_destination`] with a caller-declared capabilities sheet.
pub fn serve_destination_with_capabilities(
    path: &Path,
    script: SessionScript,
    capabilities: Option<DestinationCapabilities>,
) -> JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).expect("bind the rogue's socket");
    let incoming = UnixListenerStream::new(listener);
    let rogue = Arc::new(RogueDestination {
        script,
        capabilities,
    });
    let serving = tonic::transport::Server::builder()
        .add_service(ConnectorServer::from_arc(Arc::clone(&rogue)))
        .add_service(DestinationServiceServer::from_arc(rogue))
        .serve_with_incoming(incoming);
    tokio::spawn(async move {
        let _ = serving.await;
    })
}
