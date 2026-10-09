//! A source that keeps the protocol but for one fault, served over a socket in this process; for
//! the faults of a destination's frames, a destination too.

mod writes;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::{Array, Int64Array, RecordBatch};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_connector::wire::{status, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rdlt_host::Stream;
use rdlt_wire::bounded::Window;
use rdlt_wire::plane::{Incoming, Plane, Router, Serving};
use rdlt_wire::v1::connector_server::{Connector, ConnectorServer};
use rdlt_wire::{Limits, PROTOCOL_MAJOR, PUBLISHED};
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
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
    /// It reads a read that begins with credit.
    Lenient,
    /// Its reads send every frame, whatever the credit.
    Greedy,
    /// Its reads send every frame, whatever the credit, a little over a second apart.
    Paced,
    /// Its reads send a frame for each credit it is granted, however little that is.
    Eager,
    /// Its reads wait for credit, or for the fourth message that grants any, whichever is first.
    Patient,
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
    /// It reads back what it published, as a destination, but each read-back fails.
    ReadBackFails,
    /// It reads back what it published, without end.
    ReadBackEndless,
    /// It reads back what it published, and ends without its done frame.
    ReadBackUnfinished,
}

/// The clause each fault breaks.
pub(crate) const BROKEN: [(Fault, &str); 16] = [
    (Fault::AnyVersion, "P-HANDSHAKE"),
    (Fault::MistypedVersion, "P-HANDSHAKE"),
    (Fault::Limitless, "P-HANDSHAKE"),
    (Fault::MistypedRole, "P-ROLE"),
    (Fault::Unordered, "P-ORDER"),
    (Fault::EveryRole, "P-ROLE"),
    (Fault::Unlimited, "P-LIMITS"),
    (Fault::EchoAhead, "P-HEARTBEAT"),
    (Fault::Lenient, "P-MALFORMED"),
    (Fault::Greedy, "P-CREDIT"),
    (Fault::Paced, "P-CREDIT"),
    (Fault::Eager, "P-CREDIT"),
    (Fault::Patient, "P-CREDIT"),
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

/// The host's end of a new socket whose other end serves a fake with `fault`.
pub(crate) fn served(fault: Fault) -> std::io::Result<Box<dyn Stream>> {
    let (host, connector) = tokio::net::UnixStream::pair()?;
    let fake = Arc::new(Fake {
        fault,
        handshaken: AtomicBool::new(false),
        configured: AtomicBool::new(false),
    });
    // A message a little over its frame limit is read, so the fake refuses it by the limit.
    let limits = Limits {
        frame_bytes: 2 * FRAME_BYTES,
        ..Limits::default()
    };
    let server =
        ConnectorServer::from_arc(Arc::clone(&fake)).max_decoding_message_size(limits.largest());
    let window = Window::new(usize::MAX);
    let service = Router::new(server, fake, &limits, window).map_request(
        |request: http::Request<hyper::body::Incoming>| request.map(tonic::body::Body::new),
    );
    tokio::spawn(
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .serve_connection(TokioIo::new(connector), TowerToHyperService::new(service)),
    );
    Ok(Box::new(host))
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
                Fault::ShortNames | Fault::LenientFrames | Fault::MiscodedFrames
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

impl Plane for Fake {
    fn write(&self, frames: Incoming<v1::WriteFrame>) -> Serving<'_, v1::WriteAck> {
        Box::pin(async move { Ok(writes::write(self.fault, frames)) })
    }

    fn read(&self, controls: Incoming<v1::ReadControl>) -> Serving<'_, v1::ReadFrame> {
        Box::pin(self.reading(controls))
    }

    fn read_published(&self, _: v1::ReadPublishedRequest) -> Serving<'_, v1::ReadFrame> {
        Box::pin(async { self.reading_back() })
    }
}

impl Fake {
    /// A read the host controls with `controls`, as the fault says.
    async fn reading(
        &self,
        mut controls: Incoming<v1::ReadControl>,
    ) -> Result<Answer<v1::ReadFrame>, Status> {
        use v1::read_control::Control;
        let first = controls
            .message()
            .await?
            .and_then(|control| control.control);
        if !matches!(first, Some(Control::Start(_))) && self.keeps(Fault::Lenient) {
            return Err(refused(ConnectorErrorKind::Internal, "invalid_message"));
        }
        if let Some(Control::Start(start)) = &first {
            let cursor = start.cursor.as_ref().map_or(0, |cursor| cursor.bytes.len());
            if cursor > CURSOR_BYTES && self.keeps(Fault::LenientCursor) {
                return Err(refused(ConnectorErrorKind::Data, "limit_exceeded"));
            }
        }
        let paced = !self.keeps(Fault::Paced);
        let greedy = !self.keeps(Fault::Greedy) || paced;
        let eager = !self.keeps(Fault::Eager);
        let patient = !self.keeps(Fault::Patient);
        let (frames, answer) = mpsc::channel(64);
        tokio::spawn(async move {
            let log = |line: usize| v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Log(v1::LogFrame {
                    level: v1::LogLevel::Info as i32,
                    message: format!("line {line}"),
                })),
            };
            let (mut credit, mut granted_times): (i64, u32) = (0, 0);
            for line in 0..8 {
                if paced && line > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
                }
                while credit <= 0 && !greedy {
                    match controls.message().await {
                        Ok(Some(v1::ReadControl {
                            control: Some(Control::Credit(granted)),
                        })) => {
                            credit += i64::try_from(granted.bytes).unwrap_or(i64::MAX);
                            granted_times += 1;
                            if eager || (patient && granted_times >= 4) {
                                credit = credit.max(1);
                            }
                        }
                        Ok(Some(_)) => {}
                        _ => return,
                    }
                }
                credit -= 16;
                if frames.send(Ok(log(line))).await.is_err() {
                    return;
                }
            }
            let done = v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Done(v1::Done {})),
            };
            frames.send(Ok(done)).await.ok();
        });
        Ok(Box::pin(ReceiverStream::new(answer)) as Answer<_>)
    }

    /// A read-back of what it published, as the fault says.
    fn reading_back(&self) -> Result<Answer<v1::ReadFrame>, Status> {
        use v1::read_frame::Frame;
        let frame = |frame| Ok(v1::ReadFrame { frame: Some(frame) });
        let mut encoder = rdlt_wire::Encoder::default();
        let rows: Arc<dyn Array> = Arc::new(Int64Array::from(vec![7; 64 * 1024]));
        let batch = RecordBatch::try_from_iter([("id", rows)]).expect("a batch");
        let schema = Frame::Schema(v1::SchemaFrame {
            schema_epoch: 1,
            ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
        });
        let data = encoder.batch(&batch).expect("the batch encodes").remove(0);
        let rows = Frame::Batch(v1::BatchFrame {
            schema_epoch: 1,
            kind: v1::BatchKind::Arrow as i32,
            data_header: data.header,
            data_body: data.body,
        });
        let frames: Answer<v1::ReadFrame> = match self.fault {
            Fault::ReadBackEndless => Box::pin(tokio_stream::iter([frame(schema)]).chain(
                tokio_stream::iter(std::iter::repeat_with(move || frame(rows.clone()))),
            )),
            Fault::ReadBackUnfinished => Box::pin(tokio_stream::iter([frame(schema), frame(rows)])),
            _ => {
                return Err(status(&ConnectorError::new(
                    ConnectorErrorKind::Transient,
                    "refused: the store is unreachable",
                )));
            }
        };
        Ok(frames)
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
