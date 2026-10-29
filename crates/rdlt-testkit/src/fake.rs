//! A connector that answers the handshake and then breaks the protocol, as a faulty one would.

#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use parking_lot::Mutex;
use rdlt_connector::wire::v1;
use rdlt_wire::bounded::Window;
use rdlt_wire::prost::bytes::Bytes;
use rdlt_wire::route::{Router, Streams};
use rdlt_wire::stream::{Call, End, FrameReader, Kind, Outgoing, Piped, Rows};
use rdlt_wire::tonic::body::Body;
use rdlt_wire::tonic::{Request, Response, Status, Streaming};
use rdlt_wire::v1::connector_server::{Connector, ConnectorServer};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt as _};
use tower::ServiceExt as _;

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
    /// It is a destination whose writes take every frame, and never answer a flush.
    Unflushed,
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
    /// Its reads send this many schemas, each of the next epoch and followed by a log line.
    Chatters(u64),
    /// Its reads send this many log lines, and nothing else.
    Logs(u64),
    /// It is a destination whose configuration answers with identifier rules at their limits.
    WideRules,
    /// Its reads send frames of a mebibyte for as long as their stream takes them, counting what
    /// it took.
    Floods(&'static Polled),
    /// It is a destination whose writes it opens, and whose frames it never reads.
    Hoards,
    /// Its reads send a frame's head of no kind a frame has.
    BadHead,
    /// Its reads send a batch frame's head and half its body, then nothing more.
    Stalls,
}

/// What a flooding read's transport took of it, and when last.
#[derive(Debug)]
pub struct Polled {
    bytes: AtomicU64,
    last: Mutex<Option<Instant>>,
}

impl Polled {
    /// A count of nothing taken.
    #[must_use]
    #[expect(
        clippy::new_without_default,
        reason = "a test keeps one in a static, which only a const constructor builds"
    )]
    pub const fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            last: Mutex::new(None),
        }
    }

    fn took(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).expect("a frame's length fits");
        self.bytes.fetch_add(bytes, Ordering::SeqCst);
        *self.last.lock() = Some(Instant::now());
    }

    /// Bytes: the frames the transport took.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::SeqCst)
    }

    /// Waits until the transport has taken a frame and then none for `quiet`.
    pub async fn settled(&self, quiet: Duration) {
        loop {
            tokio::time::sleep(quiet / 4).await;
            let last = *self.last.lock();
            if last.is_some_and(|last| last.elapsed() >= quiet) {
                return;
            }
        }
    }
}

/// The schema of a column of ids, and a batch frame's body of a mebibyte of them.
pub fn mebibyte() -> (Bytes, Bytes) {
    let values: Vec<i64> = (0..131_072).collect();
    let column = Arc::new(arrow_array::Int64Array::from(values));
    let batch = arrow_array::RecordBatch::try_from_iter([("id", column as _)]).expect("a batch");
    let mut encoder = rdlt_wire::Encoder::default();
    let schema = encoder.schema(&batch.schema()).expect("the schema encodes");
    let mut frames = encoder.batch(&batch).expect("the batch encodes");
    let frame = frames.pop().expect("one frame");
    (schema, encapsulated(&frame))
}

/// The body of a batch frame carrying `frame`: its encapsulated IPC message.
pub fn encapsulated(frame: &rdlt_wire::IpcFrame) -> Bytes {
    let mut bytes = Vec::with_capacity(8 + frame.header.len() + frame.body.len());
    bytes.extend_from_slice(&[0xff; 4]);
    let len = i32::try_from(frame.header.len()).expect("a header's length fits");
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(&frame.header);
    bytes.extend_from_slice(&frame.body);
    Bytes::from(bytes)
}

/// The frame of a read's message `frame`.
fn read_message(frame: v1::read_frame::Frame) -> Outgoing {
    let message = v1::ReadFrame { frame: Some(frame) };
    Outgoing::message(None, &message).expect("a fake's message fits a head")
}

/// A batch frame of epoch 1, its body `body`.
fn batch_frame(body: Bytes) -> Outgoing {
    let kind = Kind::Batch {
        rows: Rows::Arrow,
        dictionary: false,
        tag: 1,
    };
    Outgoing::new(kind, vec![body]).expect("a fake's batch fits a head")
}

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
    // Dropped once the connection ends, which ends every data call it still serves.
    let (connected, ended) = tokio::sync::watch::channel(());
    let service = routed(fake, ended)
        .map_request(|request: http::Request<hyper::body::Incoming>| request.map(Body::new))
        .map_response(move |mut response: http::Response<Body>| {
            let headers = response.headers_mut();
            if garbles && headers.contains_key("grpc-status") {
                headers.insert(
                    "grpc-status-details-bin",
                    http::HeaderValue::from_static("!"),
                );
            }
            response
        });
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
    drop(connected);
}

/// The host's end of a socket whose other end serves `fake`.
pub fn socket(fake: Fake) -> UnixStream {
    let (host, connector) = UnixStream::pair().expect("a socket pair");
    tokio::spawn(serve(fake, connector));
    host
}

/// `fake`'s calls, each request held to the default limits, its data calls' streams served by it
/// as its generated calls are, each until `ended` tells its connection has.
fn routed(
    fake: Fake,
    ended: tokio::sync::watch::Receiver<()>,
) -> Router<ConnectorServer<Fake>, Streamed> {
    let fake = Arc::new(fake);
    let limits = rdlt_wire::Limits::default();
    let window = Window::new(usize::MAX);
    let streamed = Arc::new(Streamed {
        fake: Arc::clone(&fake),
        ended,
    });
    Router::new(ConnectorServer::from_arc(fake), streamed, &limits, window)
}

/// A fake's data calls, each ended once its connection has.
struct Streamed {
    fake: Arc<Fake>,
    /// Closed once the connection has ended.
    ended: tokio::sync::watch::Receiver<()>,
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

/// A frame of a log line.
fn logged() -> v1::read_frame::Frame {
    v1::read_frame::Frame::Log(v1::LogFrame {
        level: v1::LogLevel::Info as i32,
        message: "chatter".to_owned(),
    })
}

/// The IPC schema of one column of ids.
fn ids() -> Bytes {
    let arrow = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]);
    rdlt_wire::Encoder::default()
        .schema(&arrow)
        .expect("the schema encodes")
}

impl Fake {
    fn destination(&self) -> bool {
        matches!(
            self.0,
            Fault::Unflushed | Fault::Keys(_) | Fault::WideRules | Fault::Hoards
        )
    }

    /// The frames a read sends, as the fault says.
    fn read_frames(&self) -> Vec<Outgoing> {
        let schema = |ipc_schema| {
            read_message(v1::read_frame::Frame::Schema(v1::SchemaFrame {
                schema_epoch: 1,
                ipc_schema,
            }))
        };
        if matches!(self.0, Fault::AnswersAhead) {
            let checkpoint = v1::CheckpointFrame {
                cursor: Some(v1::Cursor {
                    version: 1,
                    bytes: Bytes::from_static(b"c"),
                }),
                barrier: Some(u64::MAX),
            };
            vec![read_message(v1::read_frame::Frame::Checkpoint(checkpoint))]
        } else if matches!(self.0, Fault::StaleEpoch) {
            vec![schema(ids()), schema(ids())]
        } else if let Fault::Schemas(count) | Fault::Chatters(count) = self.0 {
            let chatty = matches!(self.0, Fault::Chatters(_));
            (1..=count)
                .flat_map(|epoch| {
                    let schema = v1::read_frame::Frame::Schema(v1::SchemaFrame {
                        schema_epoch: epoch,
                        ipc_schema: ids(),
                    });
                    std::iter::once(schema).chain(chatty.then(logged))
                })
                .map(read_message)
                .collect()
        } else if let Fault::Logs(count) = self.0 {
            (0..count).map(|_| read_message(logged())).collect()
        } else if let Fault::Sends(frames) = self.0 {
            let (ipc, frames) = frames();
            let batches = frames.iter().map(|frame| batch_frame(encapsulated(frame)));
            std::iter::once(schema(ipc)).chain(batches).collect()
        } else {
            vec![schema(Bytes::from_static(b"not an IPC message"))]
        }
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

impl Streams for Streamed {
    fn serve(
        self: Arc<Self>,
        stream: Piped<Body>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(async move {
            let mut ended = self.ended.clone();
            tokio::select! {
                biased;
                _ = ended.changed() => {}
                () = self.fake.stream(stream) => {}
            }
        })
    }
}

/// A data stream's reader on the fake's end, and what it writes to.
type Reading = FrameReader<tokio::io::ReadHalf<Piped<Body>>>;
type Writing = tokio::io::WriteHalf<Piped<Body>>;

impl Fake {
    /// Serves the data call `stream` carries, as the fault says.
    async fn stream(&self, stream: Piped<Body>) {
        let (read, mut write) = tokio::io::split(stream);
        let Some((call, frames)) = opened(read).await else {
            return;
        };
        let served = matches!(
            (call, self.0),
            (Call::Write, Fault::Hoards | Fault::Unflushed) | (Call::Read, _)
        );
        let mut answer = if served {
            Outgoing::new(Kind::Message { opens: Some(call) }, Vec::new()).expect("an empty frame")
        } else {
            refusal(call)
        };
        if answer.write(&mut write).await.is_err() || !served {
            return;
        }
        match self.0 {
            // The frames stay unread for as long as the write is open.
            Fault::Hoards => std::future::pending::<()>().await,
            // The frames are taken and dropped, so the stream's buffers never fill.
            Fault::Unflushed => drain(frames).await,
            Fault::Floods(polled) => {
                tokio::join!(drain(frames), flood(polled, &mut write));
            }
            Fault::BadHead => bad_head(&mut write).await,
            Fault::Stalls => stall(&mut write).await,
            _ => {
                let sending = async {
                    for mut frame in self.read_frames() {
                        if frame.write(&mut write).await.is_err() {
                            return;
                        }
                    }
                    std::future::pending::<()>().await;
                };
                tokio::join!(drain(frames), sending);
            }
        }
    }
}

/// The call a data stream opens, and its frames after the opening; `None` where it opens none.
async fn opened(read: tokio::io::ReadHalf<Piped<Body>>) -> Option<(Call, Reading)> {
    let limits = rdlt_wire::Limits::default();
    let opening = rdlt_wire::stream::Bounds {
        batch: None,
        json: None,
        ..Call::Read.bounds(End::Host, &limits)
    };
    let mut frames = FrameReader::new(read, opening);
    let Ok(Some(first)) = frames.next().await else {
        return None;
    };
    let Kind::Message { opens: Some(call) } = first.head.kind else {
        return None;
    };
    frames.set_bounds(call.bounds(End::Host, &limits));
    Some((call, frames))
}

/// The answer refusing `call`, which the fake serves none of.
fn refusal(call: Call) -> Outgoing {
    let refused = v1::Error {
        kind: v1::ErrorKind::Unsupported as i32,
        message: "the fake serves no such call".to_owned(),
        ..v1::Error::default()
    };
    match call {
        Call::Write => Outgoing::message(
            None,
            &v1::WriteAck {
                ack: Some(v1::write_ack::Ack::Error(refused)),
            },
        ),
        Call::Read | Call::ReadPublished => Outgoing::message(
            None,
            &v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Error(refused)),
            },
        ),
    }
    .expect("an error fits a head")
}

/// Takes `frames` to their end, dropping each.
async fn drain(mut frames: Reading) {
    while let Ok(Some(_)) = frames.next().await {}
}

/// Sends frames of a mebibyte for as long as the stream takes them, counting them in `polled`.
async fn flood(polled: &Polled, write: &mut Writing) {
    let (schema, batch) = mebibyte();
    let schema = v1::read_frame::Frame::Schema(v1::SchemaFrame {
        schema_epoch: 1,
        ipc_schema: schema,
    });
    if read_message(schema).write(write).await.is_err() {
        return;
    }
    loop {
        let mut frame = batch_frame(batch.clone());
        let len = frame.len();
        if frame.write(write).await.is_err() {
            return;
        }
        polled.took(len);
    }
}

/// Sends a head of a kind no frame has, then nothing more.
async fn bad_head(write: &mut Writing) {
    let mut head = [0_u8; rdlt_wire::stream::HEAD_BYTES];
    head[4] = 9;
    if write.write_all(&head).await.is_ok() {
        std::future::pending::<()>().await;
    }
}

/// Sends a batch frame's head and half its body, then nothing more.
async fn stall(write: &mut Writing) {
    let (_, batch) = mebibyte();
    let mut frame = batch_frame(batch);
    let half = rdlt_wire::stream::HEAD_BYTES + frame.len() / 2;
    let mut sent = 0;
    while sent < half {
        let chunk = rdlt_wire::prost::bytes::Buf::chunk(&frame);
        let take = chunk.len().min(half - sent);
        if write.write_all(&chunk[..take]).await.is_err() {
            return;
        }
        rdlt_wire::prost::bytes::Buf::advance(&mut frame, take);
        sent += take;
    }
    std::future::pending::<()>().await;
}
