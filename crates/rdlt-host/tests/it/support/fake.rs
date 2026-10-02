//! A connector that answers the handshake and then breaks the protocol, as a faulty one would.

use std::pin::Pin;

use bytes::Bytes;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_connector::wire::v1;
use rdlt_wire::v1::connector_server::{Connector, ConnectorServer};
use tokio::net::UnixStream;
use tokio_stream::{Stream, StreamExt as _};
use tonic::{Request, Response, Status, Streaming};
use tower::ServiceExt as _;

/// How the fake breaks the protocol.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Fault {
    /// Its reads send a checkpoint answering a barrier the host never asked for.
    AnswersAhead,
    /// It answers each heartbeat with a sequence number the host never sent.
    EchoAhead,
    /// Its configuration answers as another connector than its handshake did.
    Impostor,
    /// Its reads send a schema that is no IPC message.
    GarbageSchema,
    /// Its reads send the schema message and the batch frames this makes.
    Sends(fn() -> (Bytes, Vec<rdlt_wire::IpcFrame>)),
    /// It never answers a heartbeat, and its checks never end.
    Silent,
    /// Its reads send the same schema epoch twice.
    StaleEpoch,
    /// Its handshake says its limits, but no dictionary limit among them.
    NoDictionaryLimit,
    /// Its reads know no barrier in their start, as a connector built before it could carry one,
    /// and have nothing to read: each answers a barrier its controls ask for before its first
    /// credit, and ends at that credit.
    Unstarted,
    /// It is a destination whose writes it answers, every interval, with a credit of the bytes
    /// given, and never with a flush's stats.
    Trickles(std::time::Duration, u64),
    /// It answers a discovery and a plan with this many empty entries, each two bytes on the wire.
    Bloats(usize),
}

/// A connector that breaks the protocol as its fault says.
#[derive(Debug)]
pub(crate) struct Fake(pub(crate) Fault);

type Answer<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The host's end of a socket whose other end serves `fake`.
pub(crate) fn serve_fake(fake: Fake) -> UnixStream {
    let (host, connector) = UnixStream::pair().expect("a socket pair");
    let service =
        ConnectorServer::new(fake).map_request(|request: http::Request<hyper::body::Incoming>| {
            request.map(tonic::body::Body::new)
        });
    tokio::spawn(
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .serve_connection(TokioIo::new(connector), TowerToHyperService::new(service)),
    );
    host
}

/// The fake's spec, as `id`, a destination where `destination`.
fn spec(id: &str, destination: bool) -> v1::ConnectorSpec {
    let capabilities = rdlt_connector::Capabilities::minimal();
    v1::ConnectorSpec {
        id: id.to_owned(),
        version: "0.0.0".to_owned(),
        roles: vec![if destination {
            v1::Role::Destination
        } else {
            v1::Role::Source
        } as i32],
        config_schema_json: "{}".to_owned(),
        source_capabilities: (!destination).then_some(v1::SourceCapabilities {}),
        destination_capabilities: destination.then(|| v1::Capabilities::from(&capabilities)),
    }
}

impl Fake {
    fn destination(&self) -> bool {
        matches!(self.0, Fault::Trickles(..))
    }
}

#[tonic::async_trait]
impl Connector for Fake {
    async fn handshake(
        &self,
        _: Request<v1::HandshakeRequest>,
    ) -> Result<Response<v1::HandshakeResponse>, Status> {
        Ok(Response::new(v1::HandshakeResponse {
            spec: Some(spec("test.fake", self.destination())),
            accepted_features: Vec::new(),
            limits: matches!(self.0, Fault::NoDictionaryLimit).then(|| v1::Limits {
                dictionary_bytes: 0,
                ..rdlt_wire::Limits::default().into()
            }),
        }))
    }

    async fn configure(
        &self,
        _: Request<v1::ConfigureRequest>,
    ) -> Result<Response<v1::ConfigureResponse>, Status> {
        let id = if matches!(self.0, Fault::Impostor) {
            "test.impostor"
        } else {
            "test.fake"
        };
        Ok(Response::new(v1::ConfigureResponse {
            spec: Some(spec(id, self.destination())),
        }))
    }

    async fn check(
        &self,
        _: Request<v1::CheckRequest>,
    ) -> Result<Response<v1::CheckResponse>, Status> {
        if matches!(self.0, Fault::Silent) {
            std::future::pending::<()>().await;
        }
        Ok(Response::new(v1::CheckResponse {}))
    }

    async fn discover(
        &self,
        _: Request<v1::DiscoverRequest>,
    ) -> Result<Response<v1::Catalog>, Status> {
        let Fault::Bloats(entries) = self.0 else {
            return Err(Status::unimplemented("discover"));
        };
        Ok(Response::new(v1::Catalog {
            streams: vec![v1::StreamSpec::default(); entries],
        }))
    }

    async fn plan(
        &self,
        _: Request<v1::PlanRequest>,
    ) -> Result<Response<v1::PlanResponse>, Status> {
        let Fault::Bloats(entries) = self.0 else {
            return Err(Status::unimplemented("plan"));
        };
        Ok(Response::new(v1::PlanResponse {
            starts: vec![v1::PartitionState::default(); entries],
            ..v1::PlanResponse::default()
        }))
    }

    type ReadStream = Answer<v1::ReadFrame>;

    async fn read(
        &self,
        request: Request<Streaming<v1::ReadControl>>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        if matches!(self.0, Fault::Unstarted) {
            return Ok(Response::new(Box::pin(answering(request.into_inner()))));
        }
        let schema = |ipc_schema| v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Schema(v1::SchemaFrame {
                schema_epoch: 1,
                ipc_schema,
            })),
        };
        let sent = if matches!(self.0, Fault::AnswersAhead) {
            let checkpoint = v1::CheckpointFrame {
                cursor: Some(v1::Cursor {
                    version: 1,
                    bytes: Bytes::from_static(b"c"),
                }),
                barrier: Some(u64::MAX),
            };
            vec![Ok(v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Checkpoint(checkpoint)),
            })]
        } else if matches!(self.0, Fault::StaleEpoch) {
            let arrow = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )]);
            let ipc = rdlt_wire::Encoder::default()
                .schema(&arrow)
                .expect("the schema encodes");
            vec![Ok(schema(ipc.clone())), Ok(schema(ipc))]
        } else if let Fault::Sends(frames) = self.0 {
            let (ipc, frames) = frames();
            let batches = frames.into_iter().map(|frame| v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Batch(v1::BatchFrame {
                    schema_epoch: 1,
                    kind: v1::BatchKind::Arrow as i32,
                    data_header: frame.header,
                    data_body: frame.body,
                })),
            });
            std::iter::once(schema(ipc))
                .chain(batches)
                .map(Ok)
                .collect()
        } else {
            vec![Ok(schema(Bytes::from_static(b"not an IPC message")))]
        };
        let frames = tokio_stream::iter(sent)
            .chain(tokio_stream::pending::<Result<v1::ReadFrame, Status>>());
        Ok(Response::new(Box::pin(frames)))
    }

    async fn read_acknowledged(
        &self,
        _: Request<v1::ReadAcknowledgedRequest>,
    ) -> Result<Response<v1::ReadAcknowledgedResponse>, Status> {
        Err(Status::unimplemented("read_acknowledged"))
    }

    type ReadPublishedStream = Answer<v1::ReadFrame>;

    async fn read_published(
        &self,
        _: Request<v1::ReadPublishedRequest>,
    ) -> Result<Response<Self::ReadPublishedStream>, Status> {
        Err(Status::unimplemented("read_published"))
    }

    async fn committed(
        &self,
        _: Request<v1::CommittedRequest>,
    ) -> Result<Response<v1::CommittedResponse>, Status> {
        Err(Status::unimplemented("committed"))
    }

    async fn open(
        &self,
        _: Request<v1::OpenRequest>,
    ) -> Result<Response<v1::OpenResponse>, Status> {
        if !self.destination() {
            return Err(Status::unimplemented("open"));
        }
        Ok(Response::new(v1::OpenResponse {
            session: 1,
            epoch: 1,
            state: Vec::new(),
        }))
    }

    async fn apply_schema(
        &self,
        _: Request<v1::ApplySchemaRequest>,
    ) -> Result<Response<v1::ApplySchemaResponse>, Status> {
        Err(Status::unimplemented("apply_schema"))
    }

    type WriteStream = Answer<v1::WriteAck>;

    async fn write(
        &self,
        request: Request<Streaming<v1::WriteFrame>>,
    ) -> Result<Response<Self::WriteStream>, Status> {
        let Fault::Trickles(every, bytes) = self.0 else {
            return Err(Status::unimplemented("write"));
        };
        // The frames are taken and dropped, so the transport's windows never fill.
        let mut frames = request.into_inner();
        tokio::spawn(async move { while let Some(Ok(_)) = frames.next().await {} });
        let (acks, sent) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                let credit = v1::WriteAck {
                    ack: Some(v1::write_ack::Ack::Credit(v1::Credit { bytes })),
                };
                if acks.send(Ok(credit)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(sent),
        )))
    }

    async fn commit(&self, _: Request<v1::CommitRequest>) -> Result<Response<v1::Receipt>, Status> {
        Err(Status::unimplemented("commit"))
    }

    async fn close(
        &self,
        _: Request<v1::CloseRequest>,
    ) -> Result<Response<v1::CloseResponse>, Status> {
        Err(Status::unimplemented("close"))
    }

    type HeartbeatStream = Answer<v1::Pong>;

    async fn heartbeat(
        &self,
        request: Request<Streaming<v1::Ping>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        if matches!(self.0, Fault::Silent) {
            return Ok(Response::new(Box::pin(tokio_stream::pending())));
        }
        let ahead = if matches!(self.0, Fault::EchoAhead) {
            1000
        } else {
            0
        };
        let pongs = request.into_inner().map(move |ping| {
            ping.map(|ping| v1::Pong {
                seq: ping.seq + ahead,
            })
        });
        Ok(Response::new(Box::pin(pongs)))
    }
}

/// A read with nothing to read: it answers a barrier `controls` ask for before its first credit
/// with a checkpoint, and ends at that credit.
fn answering(
    mut controls: Streaming<v1::ReadControl>,
) -> impl Stream<Item = Result<v1::ReadFrame, Status>> + Send {
    let (frames, sent) = tokio::sync::mpsc::channel(2);
    tokio::spawn(async move {
        let mut answered = Vec::new();
        while let Some(Ok(control)) = controls.next().await {
            match control.control {
                Some(v1::read_control::Control::Checkpoint(asked)) => {
                    answered.push(v1::read_frame::Frame::Checkpoint(v1::CheckpointFrame {
                        cursor: Some(v1::Cursor {
                            version: 1,
                            bytes: Bytes::new(),
                        }),
                        barrier: Some(asked.barrier),
                    }));
                }
                Some(v1::read_control::Control::Credit(_)) => break,
                _ => {}
            }
        }
        answered.push(v1::read_frame::Frame::Done(v1::Done {}));
        for frame in answered {
            let frame = v1::ReadFrame { frame: Some(frame) };
            if frames.send(Ok(frame)).await.is_err() {
                return;
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(sent)
}
