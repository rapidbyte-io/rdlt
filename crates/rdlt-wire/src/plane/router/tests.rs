use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http_body::{Body as _, Frame};
use tokio::sync::mpsc;
use tonic::body::Body;
use tonic::codegen::http::{self, HeaderValue};
use tonic::codegen::{BoxFuture, Service, tokio_stream};
use tonic::{Code, Status};

use super::{Answers, Plane, Router, Serving};
use crate::bounded::Window;
use crate::limits::{HANDSHAKE_BYTES, Limits, MIN_FRAME_BYTES};
use crate::plane::{Incoming, WRITE};
use crate::testing::{Feed, fed};
use crate::v1;

/// What a write's reader came to, message by message.
type Read = Result<Option<v1::WriteFrame>, Code>;

/// A plane whose writes are read to their end, each outcome told, and answered with a credit;
/// or refused before any answer.
enum Writes {
    Read(mpsc::UnboundedSender<Read>),
    Refused,
}

impl Plane for Writes {
    fn write(&self, mut frames: Incoming<v1::WriteFrame>) -> Serving<'_, v1::WriteAck> {
        Box::pin(async move {
            let Self::Read(told) = self else {
                return Err(Status::unimplemented("no write"));
            };
            let told = told.clone();
            tokio::spawn(async move {
                loop {
                    let read = frames.message().await.map_err(|status| status.code());
                    let last = !matches!(read, Ok(Some(_)));
                    if told.send(read).is_err() || last {
                        return;
                    }
                }
            });
            let credit = v1::WriteAck {
                ack: Some(v1::write_ack::Ack::Credit(v1::Credit { bytes: 1 })),
            };
            Ok(Box::pin(tokio_stream::iter([Ok(credit)])) as Answers<_>)
        })
    }
}

/// A service answering each request with its own body, its path in a header.
#[derive(Clone)]
struct Echo;

/// A service never ready.
#[derive(Clone)]
struct Busy;

impl Service<http::Request<Body>> for Busy {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<http::Response<Body>, Infallible>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Infallible>> {
        std::task::Poll::Pending
    }

    fn call(&mut self, _: http::Request<Body>) -> Self::Future {
        unreachable!("a service never ready is never called")
    }
}

impl Service<http::Request<Body>> for Echo {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<http::Response<Body>, Infallible>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Infallible>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let path = HeaderValue::from_str(request.uri().path()).unwrap();
        let mut answer = http::Response::new(request.into_body());
        answer.headers_mut().insert("x-path", path);
        Box::pin(async move { Ok(answer) })
    }
}

/// Limits whose largest message is a frame of the least size.
fn limits() -> Limits {
    Limits {
        frame_bytes: MIN_FRAME_BYTES,
        catalog_bytes: 1 << 20,
        state_bytes: 1 << 20,
        config_bytes: 1 << 20,
        cursor_bytes: 1 << 20,
        schema_bytes: 1 << 20,
        ..Limits::default()
    }
}

/// A router over `plane` and [`Echo`], within [`limits`] and `window`.
fn router(plane: Writes, window: Window) -> Router<Echo, Writes> {
    Router::new(Echo, Arc::new(plane), &limits(), window)
}

/// A request to `path` whose body is fed by what it returns.
fn request(path: &str) -> (Feed, http::Request<Body>) {
    let (feed, body) = fed();
    let request = http::Request::post(path)
        .header("content-type", "application/grpc")
        .body(body)
        .unwrap();
    (feed, request)
}

/// The prefix of a message declaring `length` bytes, and the message's first byte.
fn begun(length: usize) -> Bytes {
    let mut begun = vec![0];
    begun.extend(u32::try_from(length).unwrap().to_be_bytes());
    begun.push(0x0a);
    Bytes::from(begun)
}

/// What `future` comes to, failing the test where that takes ten seconds.
async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("the wait is bounded")
}

/// Every frame `body` passes on until it ends or fails.
async fn drained(mut body: Body) -> Vec<Result<Frame<Bytes>, Code>> {
    let mut frames = Vec::new();
    while let Some(frame) =
        std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await
    {
        let failed = frame.is_err();
        frames.push(frame.map_err(|status| status.code()));
        if failed {
            break;
        }
    }
    frames
}

#[tokio::test]
async fn a_write_s_frame_beyond_its_wire_bound_is_refused_before_the_plane_reads_it() {
    let (told, mut reads) = mpsc::unbounded_channel();
    let mut router = router(Writes::Read(told), Window::new(usize::MAX));
    let (feed, request) = request(WRITE);
    let beyond = limits().decoding(crate::limits::Class::Data) + 1;
    feed.send(Frame::data(begun(beyond))).unwrap();
    let answer = within(router.call(request)).await.unwrap();
    assert_eq!(answer.status(), http::StatusCode::OK);
    assert_eq!(within(reads.recv()).await, Some(Err(Code::OutOfRange)));
}

#[tokio::test]
async fn a_write_is_answered_with_its_answers_and_trailers_of_success() {
    let (told, mut reads) = mpsc::unbounded_channel();
    let mut router = router(Writes::Read(told), Window::new(usize::MAX));
    let (feed, request) = request(WRITE);
    let flush = v1::WriteFrame {
        frame: Some(v1::write_frame::Frame::Flush(v1::Unit {})),
    };
    feed.send(Frame::data(
        super::super::Chained::chunks(flush.clone()).head,
    ))
    .unwrap();
    drop(feed);
    let answer = within(router.call(request)).await.unwrap();
    assert_eq!(answer.headers()["content-type"], "application/grpc");
    assert_eq!(within(reads.recv()).await, Some(Ok(Some(flush))));
    assert_eq!(within(reads.recv()).await, Some(Ok(None)));
    let frames = within(drained(answer.into_body())).await;
    assert_eq!(frames.len(), 2, "{frames:?}");
    let trailers = frames[1].as_ref().unwrap().trailers_ref().unwrap();
    assert_eq!(Status::from_header_map(trailers).unwrap().code(), Code::Ok);
}

#[tokio::test]
async fn a_write_refused_before_any_answer_is_answered_trailers_only() {
    let mut router = router(Writes::Refused, Window::new(usize::MAX));
    let (_feed, request) = request(WRITE);
    let answer = within(router.call(request)).await.unwrap();
    let status = Status::from_header_map(answer.headers()).unwrap();
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(answer.body().is_end_stream(), "no body");
}

#[tokio::test]
async fn a_write_naming_a_compression_is_refused_before_the_plane_hears_of_it() {
    let (told, mut reads) = mpsc::unbounded_channel();
    let mut router = router(Writes::Read(told), Window::new(usize::MAX));
    let (_feed, mut request) = request(WRITE);
    request
        .headers_mut()
        .insert("grpc-encoding", HeaderValue::from_static("gzip"));
    let answer = within(router.call(request)).await.unwrap();
    let status = Status::from_header_map(answer.headers()).unwrap();
    assert_eq!(status.code(), Code::Unimplemented);
    drop(router);
    assert_eq!(within(reads.recv()).await, None, "the plane heard nothing");
}

#[tokio::test]
async fn every_other_call_reaches_the_service_within_its_class_s_bound() {
    let (told, _reads) = mpsc::unbounded_channel();
    let mut router = router(Writes::Read(told), Window::new(usize::MAX));
    let path = "/rdlt.connector.v1.Connector/Handshake";
    let (feed, request) = request(path);
    let beyond = usize::try_from(HANDSHAKE_BYTES).unwrap() + 1;
    feed.send(Frame::data(begun(beyond))).unwrap();
    let answer = within(router.call(request)).await.unwrap();
    assert_eq!(answer.headers()["x-path"], path);
    let frames = within(drained(answer.into_body())).await;
    assert!(matches!(frames[..], [Err(Code::OutOfRange)]), "{frames:?}");
}

#[tokio::test]
async fn writes_still_arriving_hold_the_window_and_the_rest_wait() {
    let length = limits().decoding(crate::limits::Class::Data) - 5;
    let message = 5 + length;
    let window = Window::new(4 * message);
    let (told, mut reads) = mpsc::unbounded_channel();
    let mut router = router(Writes::Read(told), window.clone());
    let mut feeds = Vec::new();
    for _ in 0..5 {
        let (feed, request) = request(WRITE);
        feed.send(Frame::data(begun(length))).unwrap();
        feeds.push(feed);
        tokio::spawn(router.call(request));
    }
    for _ in 0..1_000 {
        if window.taken() == 4 * message {
            break;
        }
        tokio::task::yield_now().await;
    }
    // Four writes take room for a whole frame each, and the fifth what is left as it waits.
    assert_eq!(window.taken(), 4 * message);
    assert!(reads.try_recv().is_err(), "no frame is passed on");
    drop(feeds);
    for _ in 0..5 {
        assert_eq!(
            reads.recv().await,
            Some(Err(Code::Internal)),
            "each ends within its frame"
        );
    }
    for _ in 0..1_000 {
        if window.taken() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(window.taken(), 0, "every write's room is given back");
}

#[test]
fn the_router_is_ready_when_the_service_beside_the_plane_is() {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let (told, _reads) = mpsc::unbounded_channel();
    let window = Window::new(usize::MAX);
    let mut busy = Router::new(
        Busy,
        Arc::new(Writes::Read(told)),
        &limits(),
        window.clone(),
    );
    assert!(busy.poll_ready(&mut context).is_pending());
    let mut ready = router(Writes::Refused, window);
    assert!(ready.poll_ready(&mut context).is_ready());
}
