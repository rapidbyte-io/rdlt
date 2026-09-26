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
    /// It answers each heartbeat with a sequence number the host never sent.
    EchoAhead,
    /// Its reads send a schema that is no IPC message.
    GarbageSchema,
    /// It never answers a heartbeat, and its checks never end.
    Silent,
    /// Its reads send the same schema epoch twice.
    StaleEpoch,
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

#[tonic::async_trait]
impl Connector for Fake {
    async fn handshake(
        &self,
        _: Request<v1::HandshakeRequest>,
    ) -> Result<Response<v1::HandshakeResponse>, Status> {
        let spec = v1::ConnectorSpec {
            id: "test.fake".to_owned(),
            version: "0.0.0".to_owned(),
            roles: vec![v1::Role::Source as i32],
            config_schema_json: "{}".to_owned(),
            source_capabilities: Some(v1::SourceCapabilities {}),
            destination_capabilities: None,
        };
        Ok(Response::new(v1::HandshakeResponse {
            spec: Some(spec),
            accepted_features: Vec::new(),
            limits: None,
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
        Err(Status::unimplemented("discover"))
    }

    async fn plan(
        &self,
        _: Request<v1::PlanRequest>,
    ) -> Result<Response<v1::PlanResponse>, Status> {
        Err(Status::unimplemented("plan"))
    }

    type ReadStream = Answer<v1::ReadFrame>;

    async fn read(
        &self,
        _: Request<Streaming<v1::ReadControl>>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let schema = |ipc_schema| v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Schema(v1::SchemaFrame {
                schema_epoch: 1,
                ipc_schema,
            })),
        };
        let sent = if matches!(self.0, Fault::StaleEpoch) {
            let arrow = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )]);
            let ipc = rdlt_wire::Encoder::default().schema(&arrow);
            vec![Ok(schema(ipc.clone())), Ok(schema(ipc))]
        } else {
            vec![Ok(schema(Bytes::from_static(b"not an IPC message")))]
        };
        let frames = tokio_stream::iter(sent)
            .chain(tokio_stream::pending::<Result<v1::ReadFrame, Status>>());
        Ok(Response::new(Box::pin(frames)))
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
        Err(Status::unimplemented("open"))
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
        _: Request<Streaming<v1::WriteFrame>>,
    ) -> Result<Response<Self::WriteStream>, Status> {
        Err(Status::unimplemented("write"))
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
