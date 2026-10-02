//! Messages sent as raw bytes, as no well-behaved peer builds them: an answer of the fake
//! connector to one method, and a request of a raw client.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf as _, BufMut as _, Bytes};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_wire::v1::connector_server::ConnectorServer;
use tokio::net::UnixStream;
use tonic::Status;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

use super::fake::{Fake, Fault};

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
                let mut response =
                    http::Response::new(tonic::body::Body::new(Raw(Some(payload), true)));
                response.headers_mut().insert(
                    "content-type",
                    http::HeaderValue::from_static("application/grpc"),
                );
                return Ok::<_, std::convert::Infallible>(response);
            }
            tower::Service::call(&mut server, request.map(tonic::body::Body::new)).await
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

/// Calls `method`, a streaming method, of the connector served on the other end of `io` with one
/// message of `payload`, and returns how its first answer ends.
pub(crate) async fn streamed(
    io: UnixStream,
    method: &str,
    payload: Bytes,
) -> Result<Bytes, Status> {
    let mut client = tonic::client::Grpc::new(super::raw_channel(io).await);
    client
        .ready()
        .await
        .map_err(|error| Status::unknown(error.to_string()))?;
    let path = format!("/rdlt.connector.v1.Connector/{method}")
        .parse()
        .expect("a path");
    let messages = tokio_stream::iter([payload]);
    let mut answers = client
        .streaming(tonic::Request::new(messages), path, Bytewise)
        .await?
        .into_inner();
    answers
        .message()
        .await?
        .ok_or_else(|| Status::unknown("the call ended"))
}
