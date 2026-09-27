//! A source that keeps the protocol but for one fault, served over a socket in this process.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::{Array, Int64Array, RecordBatch};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_connector::wire::{status, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rdlt_host::Stream;
use rdlt_wire::v1::connector_server::{Connector, ConnectorServer};
use rdlt_wire::{PROTOCOL_MAJOR, PUBLISHED};
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
    /// It answers calls before its handshake, and a second handshake.
    Unordered,
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
    /// It declares a configuration limit beyond any this host sends, which breaks no clause.
    Vast,
    /// It reads back what it published, as a destination, but each read-back fails.
    ReadBackFails,
    /// It reads back what it published, without end.
    ReadBackEndless,
    /// It reads back what it published, and ends without its done frame.
    ReadBackUnfinished,
}

/// The clause each fault breaks.
pub(crate) const BROKEN: [(Fault, &str); 10] = [
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
];

/// Its configuration limit, in bytes.
const CONFIG_BYTES: usize = 1024;

/// A connection's fake, keeping the protocol but for `fault`.
pub(crate) struct Fake {
    fault: Fault,
    handshaken: AtomicBool,
}

type Answer<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

/// The host's end of a new socket whose other end serves a fake with `fault`.
pub(crate) fn served(fault: Fault) -> std::io::Result<Box<dyn Stream>> {
    let (host, connector) = tokio::net::UnixStream::pair()?;
    let fake = Fake {
        fault,
        handshaken: AtomicBool::new(false),
    };
    let service =
        ConnectorServer::new(fake).map_request(|request: http::Request<hyper::body::Incoming>| {
            request.map(tonic::body::Body::new)
        });
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

    /// The kind of a refusal `mistyped` gets wrong.
    fn kind(&self, mistyped: Fault) -> ConnectorErrorKind {
        if self.keeps(mistyped) {
            ConnectorErrorKind::Unsupported
        } else {
            ConnectorErrorKind::Internal
        }
    }
}

#[tonic::async_trait]
impl Connector for Fake {
    async fn handshake(
        &self,
        request: Request<v1::HandshakeRequest>,
    ) -> Result<Response<v1::HandshakeResponse>, Status> {
        let request = request.into_inner();
        if request.protocol_major != PROTOCOL_MAJOR && self.keeps(Fault::AnyVersion) {
            return Err(refused(
                self.kind(Fault::MistypedVersion),
                "protocol_version",
            ));
        }
        if self.handshaken.swap(true, Ordering::SeqCst) && self.keeps(Fault::Unordered) {
            return Err(refused(ConnectorErrorKind::Internal, "handshake_repeated"));
        }
        if request.config_json.len() > CONFIG_BYTES && self.keeps(Fault::Unlimited) {
            return Err(refused(ConnectorErrorKind::Data, "limit_exceeded"));
        }
        if request.role != v1::Role::Source as i32
            && self.keeps(Fault::EveryRole)
            && !self.reads_back()
        {
            return Err(refused(self.kind(Fault::MistypedRole), "role"));
        }
        let config_bytes = if self.keeps(Fault::Vast) {
            CONFIG_BYTES as u64
        } else {
            u64::MAX - 1
        };
        let limits = v1::Limits {
            config_bytes,
            ..rdlt_wire::Limits::default().into()
        };
        Ok(Response::new(v1::HandshakeResponse {
            spec: Some(v1::ConnectorSpec {
                id: "test.fake".to_owned(),
                version: "0.0.0".to_owned(),
                roles: vec![v1::Role::Source as i32],
                config_schema_json: "{}".to_owned(),
                source_capabilities: Some(v1::SourceCapabilities {}),
                destination_capabilities: None,
            }),
            accepted_features: if self.reads_back()
                && request.features.iter().any(|feature| feature == PUBLISHED)
            {
                vec![PUBLISHED.to_owned()]
            } else {
                Vec::new()
            },
            limits: self.keeps(Fault::Limitless).then_some(limits),
        }))
    }

    async fn check(
        &self,
        _: Request<v1::CheckRequest>,
    ) -> Result<Response<v1::CheckResponse>, Status> {
        if !self.handshaken.load(Ordering::SeqCst) && self.keeps(Fault::Unordered) {
            return Err(refused(ConnectorErrorKind::Internal, "no_handshake"));
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
        }))
    }

    type ReadStream = Answer<v1::ReadFrame>;

    async fn read(
        &self,
        request: Request<Streaming<v1::ReadControl>>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        use v1::read_control::Control;
        let mut controls = request.into_inner();
        let first = controls
            .message()
            .await?
            .and_then(|control| control.control);
        if !matches!(first, Some(Control::Start(_))) && self.keeps(Fault::Lenient) {
            return Err(refused(ConnectorErrorKind::Internal, "invalid_message"));
        }
        let greedy = !self.keeps(Fault::Greedy);
        let (frames, answer) = mpsc::channel(64);
        tokio::spawn(async move {
            let log = |line: usize| v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Log(v1::LogFrame {
                    level: v1::LogLevel::Info as i32,
                    message: format!("line {line}"),
                })),
            };
            let mut credit: i64 = 0;
            for line in 0..8 {
                while credit <= 0 && !greedy {
                    match controls.next().await {
                        Some(Ok(v1::ReadControl {
                            control: Some(Control::Credit(granted)),
                        })) => {
                            credit += i64::try_from(granted.bytes).unwrap_or(i64::MAX);
                        }
                        Some(Ok(_)) => {}
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
        Ok(Response::new(Box::pin(ReceiverStream::new(answer))))
    }

    type ReadPublishedStream = Answer<v1::ReadFrame>;

    async fn read_published(
        &self,
        _: Request<v1::ReadPublishedRequest>,
    ) -> Result<Response<Self::ReadPublishedStream>, Status> {
        use v1::read_frame::Frame;
        let frame = |frame| Ok(v1::ReadFrame { frame: Some(frame) });
        let mut encoder = rdlt_wire::Encoder::default();
        let rows: Arc<dyn Array> = Arc::new(Int64Array::from(vec![7; 64 * 1024]));
        let batch = RecordBatch::try_from_iter([("id", rows)]).expect("a batch");
        let schema = Frame::Schema(v1::SchemaFrame {
            schema_epoch: 1,
            ipc_schema: encoder.schema(&batch.schema()),
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
        Ok(Response::new(frames))
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
        let ahead = u64::from(!self.keeps(Fault::EchoAhead));
        let pongs = request.into_inner().map(move |ping| {
            ping.map(|ping| v1::Pong {
                seq: ping.seq + ahead,
            })
        });
        Ok(Response::new(Box::pin(pongs)))
    }
}
