//! Messages sent as raw bytes, as no well-behaved peer builds them: an answer of the fake
//! connector to one method or to its data calls, and a request of a raw client.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf as _, BufMut as _, Bytes};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_testkit::fake::{Fake, Fault};
use rdlt_wire::bounded::Window;
use rdlt_wire::route::{Router, Streams};
use rdlt_wire::stream::{Call, End, FrameReader, Kind, Outgoing, Piped};
use rdlt_wire::v1::connector_server::ConnectorServer;
use tokio::net::UnixStream;
use tonic::Status;
use tonic::body::Body;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tower::ServiceExt as _;

/// A body of one gRPC message, prefix and all, and the trailers of a call that succeeded.
struct Raw(Option<Bytes>, bool);

/// `payload` as a gRPC message, its prefix before it.
fn framed(payload: &[u8]) -> Bytes {
    let mut message = Vec::with_capacity(payload.len() + 5);
    message.put_u8(0);
    message.put_u32(u32::try_from(payload.len()).expect("a payload of a message"));
    message.extend_from_slice(payload);
    message.into()
}

impl http_body::Body for Raw {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        if let Some(message) = self.0.take() {
            return Poll::Ready(Some(Ok(http_body::Frame::data(message))));
        }
        if std::mem::take(&mut self.1) {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
            return Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))));
        }
        Poll::Ready(None)
    }
}

/// The host's end of a socket whose other end serves the fake connector breaking the protocol
/// as `fault` says, but for `method`, which it answers with a message of `payload`.
pub(crate) fn answering(fault: Fault, method: &'static str, payload: &[u8]) -> UnixStream {
    let (host, connector) = UnixStream::pair().expect("a socket pair");
    // Framed now, so what the host holds of it is the host's own.
    let payload = framed(payload);
    let server = ConnectorServer::new(Fake(fault));
    let service = tower::service_fn(move |request: http::Request<hyper::body::Incoming>| {
        let mut server = server.clone();
        let payload = payload.clone();
        async move {
            if request.uri().path().ends_with(&format!("/{method}")) {
                let mut response = http::Response::new(Body::new(Raw(Some(payload), true)));
                response.headers_mut().insert(
                    "content-type",
                    http::HeaderValue::from_static("application/grpc"),
                );
                return Ok::<_, std::convert::Infallible>(response);
            }
            tower::Service::call(&mut server, request.map(Body::new)).await
        }
    });
    tokio::spawn(
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .serve_connection(TokioIo::new(connector), TowerToHyperService::new(service)),
    );
    host
}

/// Sends and takes messages as the bytes they are.
#[derive(Clone, Copy, Debug, Default)]
struct Bytewise;

impl Codec for Bytewise {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = Bytewise;
    type Decoder = Bytewise;

    fn encoder(&mut self) -> Self::Encoder {
        Bytewise
    }

    fn decoder(&mut self) -> Self::Decoder {
        Bytewise
    }
}

impl Encoder for Bytewise {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, buffer: &mut EncodeBuf<'_>) -> Result<(), Status> {
        buffer.put(item);
        Ok(())
    }
}

impl Decoder for Bytewise {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, buffer: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(buffer.copy_to_bytes(buffer.remaining())))
    }
}

/// Calls `method` of the connector served on the other end of `io` with a message of `payload`.
pub(crate) async fn called(io: UnixStream, method: &str, payload: Bytes) -> Result<Bytes, Status> {
    let mut client = tonic::client::Grpc::new(super::raw_channel(io).await);
    client
        .ready()
        .await
        .map_err(|error| Status::unknown(error.to_string()))?;
    let path = format!("/rdlt.connector.v1.Connector/{method}")
        .parse()
        .expect("a path");
    client
        .unary(tonic::Request::new(payload), path, Bytewise)
        .await
        .map(tonic::Response::into_inner)
}

/// Answers each data call by opening it and sending one message frame of `payload`, then waits
/// for its host to go.
struct Answering(Bytes);

impl Streams for Answering {
    fn serve(self: Arc<Self>, stream: Piped<Body>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let (read, mut write) = tokio::io::split(stream);
            let limits = rdlt_wire::Limits::default();
            let opening = rdlt_wire::stream::Bounds {
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
            let opened = Outgoing::new(Kind::Message { opens: Some(call) }, Vec::new());
            let message = Outgoing::new(Kind::Message { opens: None }, vec![self.0.clone()]);
            for frame in [opened, message] {
                let mut frame = frame.expect("a frame fits a head");
                if frame.write(&mut write).await.is_err() {
                    return;
                }
            }
            while let Ok(Some(_)) = frames.next().await {}
        })
    }
}

/// The host's end of a socket whose other end serves the fake connector breaking the protocol
/// as `fault` says, but for its data calls, each of which it opens and answers with a message
/// frame of `payload`.
pub(crate) fn answering_streams(fault: Fault, payload: &[u8]) -> UnixStream {
    let (host, connector) = UnixStream::pair().expect("a socket pair");
    let limits = rdlt_wire::Limits::default();
    let server = ConnectorServer::new(Fake(fault)).max_decoding_message_size(limits.largest());
    let streams = Arc::new(Answering(Bytes::copy_from_slice(payload)));
    let window = Window::new(usize::MAX);
    let service = Router::new(server, streams, &limits, window)
        .map_request(|request: http::Request<hyper::body::Incoming>| request.map(Body::new));
    tokio::spawn(
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .serve_connection(TokioIo::new(connector), TowerToHyperService::new(service)),
    );
    host
}
