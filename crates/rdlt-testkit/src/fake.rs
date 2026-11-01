//! A connector that answers the handshake and then breaks the protocol, as a faulty one would.

mod plane;
#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use parking_lot::Mutex;
use rdlt_connector::wire::v1;
use rdlt_wire::bounded::Window;
use rdlt_wire::plane::Router;
use rdlt_wire::prost::bytes::Bytes;
use rdlt_wire::tonic::{Request, Response, Status, Streaming};
use rdlt_wire::v1::connector_server::{Connector, ConnectorServer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UnixStream;
use tokio_stream::{Stream, StreamExt as _};
use tower::ServiceExt as _;

pub use plane::{Polled, mebibyte};

/// How the fake breaks the protocol.
#[derive(Clone, Copy, Debug)]
pub enum Fault {
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
    /// Its heartbeat call never opens, and its checks never end.
    Unopened,
    /// Its heartbeat call is refused, and its checks never end.
    RefusesHeartbeat,
    /// Its heartbeat call opens and it never reads a ping, its transport taking 16 bytes of
    /// any call before it reads them; its checks never end.
    Unread,
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
    Trickles(Duration, u64),
    /// It answers a discovery and a plan with this many empty entries, each two bytes on the wire.
    Bloats(usize),
    /// Every failure it answers with carries status details that are not base64.
    GarbledDetails,
    /// Its handshake answers as this changes it.
    Handshakes(fn(&mut v1::HandshakeResponse)),
    /// It is a destination whose opens answer with a state record of this key.
    Keys(fn() -> String),
    /// Its reads send this many schemas, each of the next epoch, and nothing else.
    Schemas(u64),
    /// Its reads send this many schemas, each of the next epoch and followed by a lag.
    Chatters(u64),
    /// Its reads send this many lags, and nothing else.
    Lags(u64),
    /// It is a destination whose configuration answers with identifier rules at their limits.
    WideRules,
    /// Its reads send the next of these frames for each credit the host grants, and keep each
    /// credit.
    Granted(fn() -> Vec<v1::ReadFrame>, &'static Granted),
    /// Its reads send frames of a mebibyte for as long as its transport takes them, whatever
    /// its credit, counting what the transport took.
    Floods(&'static Polled),
    /// It is a destination whose writes it answers with a credit of the bytes given, and whose
    /// frames it never reads.
    Hoards(u64),
}

/// The credits a fake's read was granted, in order.
pub type Granted = Mutex<Vec<u64>>;

/// A connector that breaks the protocol as its fault says.
#[derive(Debug)]
pub struct Fake(pub Fault);

type Answer<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Serves `fake` on `io` until the connection ends.
pub async fn serve<IO>(fake: Fake, io: IO)
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let garbles = matches!(fake.0, Fault::GarbledDetails);
    let unread = matches!(fake.0, Fault::Unread);
    let service = routed(fake)
        .map_request(|request: http::Request<hyper::body::Incoming>| {
            request.map(rdlt_wire::tonic::body::Body::new)
        })
        .map_response(
            move |mut response: http::Response<rdlt_wire::tonic::body::Body>| {
                let headers = response.headers_mut();
                if garbles && headers.contains_key("grpc-status") {
                    headers.insert(
                        "grpc-status-details-bin",
                        http::HeaderValue::from_static("!"),
                    );
                }
                response
            },
        );
    let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
    builder
        .timer(TokioTimer::new())
        // Hosts never read the date, and its text is the wall clock's, which would make two runs
        // of one simulated seed write header blocks of different lengths.
        .auto_date_header(false)
        // Pings end a connection whose host the network dropped silently, as a served
        // connector's do.
        .keep_alive_interval(Some(Duration::from_secs(5)))
        .keep_alive_timeout(Duration::from_secs(30));
    if unread {
        builder.initial_stream_window_size(16);
    }
    // A connection that fails ends, which is all a fake does about it.
    builder
        .serve_connection(TokioIo::new(io), TowerToHyperService::new(service))
        .await
        .ok();
}

/// The host's end of a socket whose other end serves `fake`.
pub fn socket(fake: Fake) -> UnixStream {
    let (host, connector) = UnixStream::pair().expect("a socket pair");
    tokio::spawn(serve(fake, connector));
    host
}

/// `fake`'s calls, each request held to the default limits, the data plane's served by it as
/// its generated calls are.
fn routed(fake: Fake) -> Router<ConnectorServer<Fake>, Fake> {
    let fake = Arc::new(fake);
    let limits = rdlt_wire::Limits::default();
    let window = Window::new(usize::MAX);
    Router::new(
        ConnectorServer::from_arc(Arc::clone(&fake)),
        fake,
        &limits,
        window,
    )
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
        destination_capabilities: destination.then(|| v1::Capabilities::from(&capabilities)),
    }
}

impl Fake {
    fn destination(&self) -> bool {
        matches!(
            self.0,
            Fault::Trickles(..) | Fault::Keys(_) | Fault::WideRules | Fault::Hoards(_)
        )
    }
}

#[rdlt_wire::tonic::async_trait]
impl Connector for Fake {
    async fn handshake(
        &self,
        _: Request<v1::HandshakeRequest>,
    ) -> Result<Response<v1::HandshakeResponse>, Status> {
        let mut answer = v1::HandshakeResponse {
            spec: Some(spec("test.fake", self.destination())),
            accepted_features: Vec::new(),
            limits: matches!(self.0, Fault::NoDictionaryLimit).then(|| v1::Limits {
                dictionary_bytes: 0,
                ..rdlt_wire::Limits::default().into()
            }),
            protocol_major: rdlt_wire::PROTOCOL_MAJOR,
        };
        if let Fault::Handshakes(change) = self.0 {
            change(&mut answer);
        }
        Ok(Response::new(answer))
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
        let mut configured = spec(id, self.destination());
        if let (Fault::WideRules, Some(capabilities)) =
            (self.0, configured.destination_capabilities.as_mut())
        {
            use rdlt_connector::limits::{
                MAX_RESERVED_BYTES, MAX_RESERVED_PREFIXES, MAX_RESERVED_WORDS,
            };
            // Each word or prefix as long as one may be.
            let word = |index: usize| {
                let mark = format!("w{index}_");
                let fill = "x".repeat(MAX_RESERVED_BYTES - mark.len());
                mark + &fill
            };
            let rules = capabilities.identifiers.as_mut().expect("identifier rules");
            rules.reserved = (0..MAX_RESERVED_WORDS).map(word).collect();
            rules.reserved_table_prefixes = (0..MAX_RESERVED_PREFIXES).map(word).collect();
        }
        Ok(Response::new(v1::ConfigureResponse {
            spec: Some(configured),
        }))
    }

    async fn check(
        &self,
        _: Request<v1::CheckRequest>,
    ) -> Result<Response<v1::CheckResponse>, Status> {
        if matches!(
            self.0,
            Fault::Silent | Fault::Unopened | Fault::RefusesHeartbeat | Fault::Unread
        ) {
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

    async fn read_acknowledged(
        &self,
        _: Request<v1::ReadAcknowledgedRequest>,
    ) -> Result<Response<v1::ReadAcknowledgedResponse>, Status> {
        Err(Status::unimplemented("read_acknowledged"))
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
        let state = match self.0 {
            Fault::Keys(key) => vec![v1::StateRecord {
                key: key(),
                value: Bytes::from_static(b"{}"),
            }],
            _ => Vec::new(),
        };
        Ok(Response::new(v1::OpenResponse {
            session: 1,
            epoch: 1,
            landed: None,
            state,
        }))
    }

    async fn apply_schema(
        &self,
        _: Request<v1::ApplySchemaRequest>,
    ) -> Result<Response<v1::ApplySchemaResponse>, Status> {
        Err(Status::unimplemented("apply_schema"))
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
        match self.0 {
            Fault::Unopened => std::future::pending::<()>().await,
            Fault::RefusesHeartbeat => return Err(Status::unavailable("no heartbeat")),
            Fault::Unread => {
                // The pings stay unread for as long as the call is open.
                let pending = tokio_stream::pending::<Result<v1::Pong, Status>>();
                let pongs = pending.map(move |pong| {
                    let _ = &request;
                    pong
                });
                return Ok(Response::new(Box::pin(pongs)));
            }
            _ => {}
        }
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
