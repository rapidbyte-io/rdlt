//! A source that keeps the protocol but for one fault, served over an opener in this process; for
//! the faults of a destination's frames, a destination too.

mod writes;

use std::future::Future;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::{Array, Int64Array, RecordBatch};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_connector::wire::{status, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rdlt_host::{Opener, Passing};
use rdlt_testkit::fake::encapsulated;
use rdlt_wire::bounded::Window;
use rdlt_wire::opener::{Accepting, Opening};
use rdlt_wire::route::{Router, Streams};
use rdlt_wire::stream::{Bounds, Call, End, FrameReader, Kind, Outgoing, Piped, Received, Rows};
use rdlt_wire::v1::connector_server::{Connector, ConnectorServer};
use rdlt_wire::{Limits, PROTOCOL_MAJOR, PUBLISHED};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::StreamExt as _;
use tonic::body::Body;
use tonic::{Request, Response, Status, Streaming};
use tower::ServiceExt as _;

/// The rule the fake breaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    /// It answers a handshake at any major version.
    AnyVersion,
    /// It refuses another major version, but not as unsupported.
    MistypedVersion,
    /// It answers its handshake without its limits.
    Limitless,
    /// It answers its handshake without the protocol version it speaks: no host connects to it,
    /// so no clause passes.
    Unversioned,
    /// It declares a limit of one row a batch, below the protocol's minimum: no host connects
    /// to it, so no clause passes.
    FewRows,
    /// It answers calls before its handshake or configuration, and a second handshake or
    /// configuration.
    Unordered,
    /// It accepts every feature a handshake offers, those it does not know among them.
    AcceptsAnyFeature,
    /// It refuses a handshake that offers a feature it does not know.
    RefusesUnknownFeatures,
    /// It answers a handshake as a destination, which it does not serve.
    EveryRole,
    /// It refuses a role it does not serve, but not as unsupported.
    MistypedRole,
    /// It takes a configuration beyond its limit.
    Unlimited,
    /// It answers each heartbeat with the next sequence number.
    EchoAhead,
    /// It opens a read whose start does not decode.
    Lenient,
    /// It declares configuration and cursor limits beyond any this host sends, which breaks no
    /// clause.
    Vast,
    /// It reads from a cursor beyond its limit.
    LenientCursor,
    /// It answers each heartbeat, and keeps its answers' stream open after the pings end, which
    /// breaks no clause.
    Lingering,
    /// It answers no heartbeat.
    Mute,
    /// It answers no handshake, though its connection lives on.
    Deaf,
    /// As a destination, it keeps to identifiers of 32 bytes, which breaks no clause.
    ShortNames,
    /// As a destination, it takes a batch it cannot decode, or one beyond its frame limit.
    LenientFrames,
    /// As a destination, it refuses a batch it cannot decode, or one beyond its frame limit,
    /// with a code of its own.
    MiscodedFrames,
    /// As a destination, it takes a batch whose buffers lie off their alignment, and refuses
    /// any other it cannot decode.
    MisalignedFrames,
    /// It reads back what it published, as a destination, but each read-back fails.
    ReadBackFails,
    /// It reads back what it published, without end.
    ReadBackEndless,
    /// It reads back what it published, and ends without its done frame.
    ReadBackUnfinished,
}

/// The clause each fault breaks.
pub(crate) const BROKEN: [(Fault, &str); 12] = [
    (Fault::AnyVersion, "P-HANDSHAKE"),
    (Fault::MistypedVersion, "P-HANDSHAKE"),
    (Fault::Limitless, "P-HANDSHAKE"),
    (Fault::MistypedRole, "P-ROLE"),
    (Fault::Unordered, "P-ORDER"),
    (Fault::EveryRole, "P-ROLE"),
    (Fault::Unlimited, "P-LIMITS"),
    (Fault::EchoAhead, "P-HEARTBEAT"),
    (Fault::Lenient, "P-MALFORMED"),
    (Fault::LenientCursor, "P-LIMITS"),
    (Fault::AcceptsAnyFeature, "P-HANDSHAKE"),
    // Only the handshake clause offers a feature no host defines.
    (Fault::RefusesUnknownFeatures, "P-HANDSHAKE"),
];

/// Its configuration limit, in bytes.
const CONFIG_BYTES: usize = 1024;

/// Its cursor limit, in bytes.
const CURSOR_BYTES: usize = 1024;

/// Its frame limit, in bytes: the least the protocol lets a peer set.
const FRAME_BYTES: u64 = rdlt_wire::limits::MIN_FRAME_BYTES;

/// A connection's fake, keeping the protocol but for `fault`.
pub(crate) struct Fake {
    fault: Fault,
    handshaken: AtomicBool,
    configured: AtomicBool,
}

type Answer<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

/// The limits the fake reads within: a frame a little over its frame limit is read, so the fake
/// refuses it by the limit.
fn reading() -> Limits {
    Limits {
        frame_bytes: 2 * FRAME_BYTES,
        ..Limits::default()
    }
}

/// The host's end of a new opener whose other end serves a fake with `fault`.
pub(crate) fn served(fault: Fault) -> std::io::Result<Box<dyn Opener>> {
    let (host, connector) = std::os::unix::net::UnixStream::pair()?;
    let accepting = Accepting::new(OwnedFd::from(connector))?;
    let fake = Arc::new(Fake {
        fault,
        handshaken: AtomicBool::new(false),
        configured: AtomicBool::new(false),
    });
    tokio::spawn(accept(fake, accepting));
    Ok(Box::new(Passing::new(OwnedFd::from(host))?))
}

/// Serves each stream `accepting` passes: the control connection's calls, and each data call.
async fn accept(fake: Arc<Fake>, accepting: Accepting) {
    while let Ok(Some((opening, fd))) = accepting.next().await {
        let socket = std::os::unix::net::UnixStream::from(fd);
        let Ok(socket) = socket
            .set_nonblocking(true)
            .and_then(|()| tokio::net::UnixStream::from_std(socket))
        else {
            continue;
        };
        match opening {
            Opening::Control => {
                tokio::spawn(control(Arc::clone(&fake), socket));
            }
            Opening::Data => {
                tokio::spawn(Arc::clone(&fake).stream(socket));
            }
        }
    }
}

/// Serves the fake's calls on its control connection, `socket`.
async fn control(fake: Arc<Fake>, socket: tokio::net::UnixStream) {
    let limits = reading();
    let server =
        ConnectorServer::from_arc(Arc::clone(&fake)).max_decoding_message_size(limits.largest());
    let window = Window::new(usize::MAX);
    let service = Router::new(server, fake, &limits, window)
        .map_request(|request: http::Request<hyper::body::Incoming>| request.map(Body::new));
    hyper::server::conn::http2::Builder::new(TokioExecutor::new())
        .timer(TokioTimer::new())
        .serve_connection(TokioIo::new(socket), TowerToHyperService::new(service))
        .await
        .ok();
}

/// The frame carrying `message`.
fn message(message: &impl rdlt_wire::prost::Message) -> Outgoing {
    Outgoing::message(None, message).expect("a message fits a head")
}

/// The frame answering `call`'s opening with `error`.
fn refusal(call: Call, error: &ConnectorError) -> Outgoing {
    let error = Some(v1::Error::from(error));
    match call {
        Call::Write => message(&v1::WriteAck {
            ack: error.map(v1::write_ack::Ack::Error),
        }),
        Call::Read | Call::ReadPublished => message(&v1::ReadFrame {
            frame: error.map(v1::read_frame::Frame::Error),
        }),
    }
}

/// The frame opening `call`.
fn opened(call: Call) -> Outgoing {
    Outgoing::new(Kind::Message { opens: Some(call) }, Vec::new()).expect("an empty frame")
}

/// Writes `frame` to `io`; whether the host took it.
async fn send<W: AsyncWrite + Unpin>(io: &mut W, mut frame: Outgoing) -> bool {
    frame.write(io).await.is_ok()
}

fn refused(kind: ConnectorErrorKind, code: &'static str) -> Status {
    status(&ConnectorError::new(kind, format!("refused: {code}")).with_code(code))
}

impl Fake {
    fn keeps(&self, fault: Fault) -> bool {
        self.fault != fault
    }

    /// Whether it reads back what it published: then it serves the destination role too.
    fn reads_back(&self) -> bool {
        matches!(
            self.fault,
            Fault::ReadBackFails | Fault::ReadBackEndless | Fault::ReadBackUnfinished
        )
    }

    /// Whether it serves the destination role too.
    fn writes(&self) -> bool {
        self.reads_back()
            || matches!(
                self.fault,
                Fault::ShortNames
                    | Fault::LenientFrames
                    | Fault::MiscodedFrames
                    | Fault::MisalignedFrames
            )
    }

    /// The kind of a refusal `mistyped` gets wrong.
    fn kind(&self, mistyped: Fault) -> ConnectorErrorKind {
        if self.keeps(mistyped) {
            ConnectorErrorKind::Unsupported
        } else {
            ConnectorErrorKind::Internal
        }
    }
}

impl Streams for Fake {
    fn serve(
        self: Arc<Self>,
        stream: Piped<Body>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(self.stream(stream))
    }
}

impl Fake {
    /// Serves the data call `io` carries, as the fault says.
    async fn stream<IO: AsyncRead + AsyncWrite + Send + 'static>(self: Arc<Self>, io: IO) {
        let (read, mut write) = tokio::io::split(io);
        let limits = reading();
        let opening = Bounds {
            batch: None,
            json: None,
            ..Call::Read.bounds(End::Host, &limits)
        };
        let mut frames = FrameReader::new(read, opening);
        let Ok(Some(first)) = frames.next().await else {
            return;
        };
        let Kind::Message { opens: Some(call) } = first.head.kind else {
            return;
        };
        frames.set_bounds(call.bounds(End::Host, &limits));
        match call {
            Call::Read => self.reading(&first, &mut write).await,
            Call::Write => writes::write(self.fault, &first, frames, &mut write).await,
            Call::ReadPublished => self.reading_back(&mut write).await,
        }
    }

    /// A read `opening` starts, as the fault says: eight log lines and its end.
    async fn reading<W: AsyncWrite + Unpin>(&self, opening: &Received, io: &mut W) {
        let start = opening.message::<v1::ReadStart>(Call::Read.opening(), 1 << 20);
        let refused = match start {
            Err(_) if self.keeps(Fault::Lenient) => Some(
                ConnectorError::new(ConnectorErrorKind::Internal, "refused: invalid_message")
                    .with_code("invalid_message"),
            ),
            Ok(start)
                if start.cursor.as_ref().map_or(0, |cursor| cursor.bytes.len()) > CURSOR_BYTES
                    && self.keeps(Fault::LenientCursor) =>
            {
                Some(ConnectorError::data("refused: limit_exceeded").with_code("limit_exceeded"))
            }
            _ => None,
        };
        if let Some(refused) = refused {
            send(io, refusal(Call::Read, &refused)).await;
            return;
        }
        if !send(io, opened(Call::Read)).await {
            return;
        }
        for line in 0..8 {
            let log = v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Log(v1::LogFrame {
                    level: v1::LogLevel::Info as i32,
                    message: format!("line {line}"),
                })),
            };
            if !send(io, message(&log)).await {
                return;
            }
        }
        let done = v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Done(v1::Done {})),
        };
        send(io, message(&done)).await;
    }

    /// A read-back of what it published, as the fault says.
    async fn reading_back<W: AsyncWrite + Unpin>(&self, io: &mut W) {
        let mut encoder = rdlt_wire::Encoder::default();
        let rows: Arc<dyn Array> = Arc::new(Int64Array::from(vec![7; 64 * 1024]));
        let batch = RecordBatch::try_from_iter([("id", rows)]).expect("a batch");
        let schema = message(&v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Schema(v1::SchemaFrame {
                schema_epoch: 1,
                ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
            })),
        });
        let data = encoder.batch(&batch).expect("the batch encodes").remove(0);
        let body = encapsulated(&data);
        let rows = || {
            let kind = Kind::Batch {
                rows: Rows::Arrow,
                dictionary: false,
                tag: 1,
            };
            Outgoing::new(kind, vec![body.clone()]).expect("a batch fits a head")
        };
        let endless = match self.fault {
            Fault::ReadBackEndless => true,
            Fault::ReadBackUnfinished => false,
            _ => {
                let refused = ConnectorError::new(
                    ConnectorErrorKind::Transient,
                    "refused: the store is unreachable",
                );
                send(io, refusal(Call::ReadPublished, &refused)).await;
                return;
            }
        };
        if !send(io, opened(Call::ReadPublished)).await || !send(io, schema).await {
            return;
        }
        while send(io, rows()).await && endless {}
    }
}

/// The fake's spec.
fn spec() -> v1::ConnectorSpec {
    v1::ConnectorSpec {
        id: "test.fake".to_owned(),
        version: "0.0.0".to_owned(),
        roles: vec![v1::Role::Source as i32],
        config_schema_json: "{}".to_owned(),
        destination_capabilities: None,
    }
}

#[tonic::async_trait]
impl Connector for Fake {
    async fn handshake(
        &self,
        request: Request<v1::HandshakeRequest>,
    ) -> Result<Response<v1::HandshakeResponse>, Status> {
        if !self.keeps(Fault::Deaf) {
            std::future::pending::<()>().await;
        }
        let request = request.into_inner();
        let unknown = request.features.iter().any(|feature| feature != PUBLISHED);
        if unknown && !self.keeps(Fault::RefusesUnknownFeatures) {
            return Err(refused(ConnectorErrorKind::Unsupported, "feature"));
        }
        if request.protocol_major != PROTOCOL_MAJOR && self.keeps(Fault::AnyVersion) {
            return Err(refused(
                self.kind(Fault::MistypedVersion),
                "protocol_version",
            ));
        }
        if self.handshaken.swap(true, Ordering::SeqCst) && self.keeps(Fault::Unordered) {
            return Err(refused(ConnectorErrorKind::Internal, "handshake_repeated"));
        }
        if request.role != v1::Role::Source as i32 && self.keeps(Fault::EveryRole) && !self.writes()
        {
            return Err(refused(self.kind(Fault::MistypedRole), "role"));
        }
        let (config_bytes, cursor_bytes) = if self.keeps(Fault::Vast) {
            (CONFIG_BYTES as u64, CURSOR_BYTES as u64)
        } else {
            (u64::MAX - 1, u64::MAX - 1)
        };
        let limits = v1::Limits {
            config_bytes,
            cursor_bytes,
            frame_bytes: FRAME_BYTES,
            batch_rows: u64::from(!self.keeps(Fault::FewRows)),
            ..Limits::default().into()
        };
        Ok(Response::new(v1::HandshakeResponse {
            spec: Some(spec()),
            accepted_features: if !self.keeps(Fault::AcceptsAnyFeature) {
                request.features.clone()
            } else if self.reads_back()
                && request.features.iter().any(|feature| feature == PUBLISHED)
            {
                vec![PUBLISHED.to_owned()]
            } else {
                Vec::new()
            },
            limits: self.keeps(Fault::Limitless).then_some(limits),
            protocol_major: if self.keeps(Fault::Unversioned) {
                PROTOCOL_MAJOR
            } else {
                0
            },
        }))
    }

    async fn configure(
        &self,
        request: Request<v1::ConfigureRequest>,
    ) -> Result<Response<v1::ConfigureResponse>, Status> {
        if !self.handshaken.load(Ordering::SeqCst) && self.keeps(Fault::Unordered) {
            return Err(refused(ConnectorErrorKind::Internal, "no_handshake"));
        }
        if self.configured.swap(true, Ordering::SeqCst) && self.keeps(Fault::Unordered) {
            return Err(refused(ConnectorErrorKind::Internal, "configure_repeated"));
        }
        if request.into_inner().config_json.len() > CONFIG_BYTES && self.keeps(Fault::Unlimited) {
            return Err(refused(ConnectorErrorKind::Data, "limit_exceeded"));
        }
        Ok(Response::new(v1::ConfigureResponse { spec: Some(spec()) }))
    }

    async fn check(
        &self,
        _: Request<v1::CheckRequest>,
    ) -> Result<Response<v1::CheckResponse>, Status> {
        if self.keeps(Fault::Unordered) {
            if !self.handshaken.load(Ordering::SeqCst) {
                return Err(refused(ConnectorErrorKind::Internal, "no_handshake"));
            }
            if !self.configured.load(Ordering::SeqCst) {
                return Err(refused(ConnectorErrorKind::Internal, "not_configured"));
            }
        }
        Ok(Response::new(v1::CheckResponse {}))
    }

    async fn discover(
        &self,
        _: Request<v1::DiscoverRequest>,
    ) -> Result<Response<v1::Catalog>, Status> {
        Ok(Response::new(v1::Catalog {
            streams: vec![v1::StreamSpec {
                name: Some(v1::StreamName {
                    namespace: None,
                    name: "rows".to_owned(),
                }),
                ..v1::StreamSpec::default()
            }],
        }))
    }

    async fn plan(
        &self,
        _: Request<v1::PlanRequest>,
    ) -> Result<Response<v1::PlanResponse>, Status> {
        Ok(Response::new(v1::PlanResponse {
            partitions: vec!["whole".to_owned()],
            phase: None,
            starts: Vec::new(),
            unbounded: Vec::new(),
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
        Ok(Response::new(v1::OpenResponse {
            session: 1,
            epoch: 1,
            state: Vec::new(),
        }))
    }

    async fn apply_schema(
        &self,
        request: Request<v1::ApplySchemaRequest>,
    ) -> Result<Response<v1::ApplySchemaResponse>, Status> {
        let created = request
            .into_inner()
            .change
            .and_then(|change| change.change)
            .and_then(|change| match change {
                v1::table_change::Change::Create(create) => create.table,
                _ => None,
            });
        let longest = created.map_or(0, |table| table.name.len());
        if longest > 32 && !self.keeps(Fault::ShortNames) {
            return Err(refused(ConnectorErrorKind::Data, "identifier"));
        }
        Ok(Response::new(v1::ApplySchemaResponse {}))
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
        let ahead = u64::from(!self.keeps(Fault::EchoAhead));
        let pongs = request.into_inner().map(move |ping| {
            ping.map(|ping| v1::Pong {
                seq: ping.seq + ahead,
            })
        });
        let answers: Answer<v1::Pong> = match self.fault {
            Fault::Lingering => Box::pin(pongs.chain(tokio_stream::pending())),
            Fault::Mute => Box::pin(tokio_stream::pending()),
            _ => Box::pin(pongs),
        };
        Ok(Response::new(answers))
    }
}
