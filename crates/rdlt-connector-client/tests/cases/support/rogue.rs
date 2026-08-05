//! A hand-driven `SourceService` that serves EXACTLY the frames a test
//! scripts — including shapes the sdk's serve half can never emit (its
//! arrow encoder writes exactly one batch per frame by construction),
//! which is the whole point: the client's one-batch refusal seat needs
//! a server willing to violate the rule, and the pacing observation
//! needs a producer with no in-connector byte budget between it and
//! the wire. The `Connector` half answers the minimal truthful
//! handshake (`rogue`/`0.0.0`) so `RemoteSource::connect` reaches
//! `Read` at all; everything else is deliberately unimplemented.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rdlt_connector::ConnectorSpec;
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::source_service_server::{SourceService, SourceServiceServer};
use rdlt_connector_protocol::proto::{self, handshake_reply, read_frame};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status};

/// What the rogue's `Read` RPC does.
#[derive(Debug, Clone)]
pub enum ReadScript {
    /// Send these frames verbatim, then end the stream cleanly.
    Frames(Vec<proto::ReadFrame>),
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
