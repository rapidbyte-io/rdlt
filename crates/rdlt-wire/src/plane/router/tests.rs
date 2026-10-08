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
use crate::plane::{Chained, Incoming, READ, READ_PUBLISHED, WRITE};
use crate::testing::{Feed, fed};
use crate::v1;

/// What the plane heard of a call.
#[derive(Debug, PartialEq)]
enum Heard {
    /// What a write's reader came to, frame by frame.
    Write(Result<Option<v1::WriteFrame>, Code>),
    /// What a read's reader came to, control by control.
    Read(Result<Option<v1::ReadControl>, Code>),
    /// A read-back's request.
    ReadBack(v1::ReadPublishedRequest),
}

/// A plane whose calls' requests are read to their end, each outcome told, and answered with a
/// credit, or a read's frame; or refused before any answer.
enum Calls {
    Heard(mpsc::UnboundedSender<Heard>),
    Refused,
}

/// Reads `incoming` to its end on a task of its own, telling each outcome as `heard` makes it.
fn told<M: prost::Message + Default + 'static>(
    told: &mpsc::UnboundedSender<Heard>,
    mut incoming: Incoming<M>,
    heard: fn(Result<Option<M>, Code>) -> Heard,
) {
    let told = told.clone();
    tokio::spawn(async move {
        loop {
            let read = incoming.message().await.map_err(|status| status.code());
            let last = !matches!(read, Ok(Some(_)));
            if told.send(heard(read)).is_err() || last {
                return;
            }
        }
    });
}

/// A read's frame of a batch.
fn read_frame() -> v1::ReadFrame {
    v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Batch(v1::BatchFrame {
            schema_epoch: 1,
            kind: v1::BatchKind::Arrow as i32,
            data_header: Bytes::from_static(b"h"),
            data_body: Bytes::from_static(b"body"),
        })),
    }
}

impl Plane for Calls {
    fn write(&self, frames: Incoming<v1::WriteFrame>) -> Serving<'_, v1::WriteAck> {
        Box::pin(async move {
            let Self::Heard(heard) = self else {
                return Err(Status::unimplemented("no write"));
            };
            told(heard, frames, Heard::Write);
            let credit = v1::WriteAck {
                ack: Some(v1::write_ack::Ack::Credit(v1::Credit { bytes: 1 })),
            };
            Ok(Box::pin(tokio_stream::iter([Ok(credit)])) as Answers<_>)
        })
    }

    fn read(&self, controls: Incoming<v1::ReadControl>) -> Serving<'_, v1::ReadFrame> {
        Box::pin(async move {
            let Self::Heard(heard) = self else {
                return Err(Status::unimplemented("no read"));
            };
            told(heard, controls, Heard::Read);
            Ok(Box::pin(tokio_stream::iter([Ok(read_frame())])) as Answers<_>)
        })
    }

    fn read_published(&self, request: v1::ReadPublishedRequest) -> Serving<'_, v1::ReadFrame> {
        Box::pin(async move {
            let Self::Heard(heard) = self else {
                return Err(Status::unimplemented("no read-back"));
            };
            heard.send(Heard::ReadBack(request)).ok();
            Ok(Box::pin(tokio_stream::iter([Ok(read_frame())])) as Answers<_>)
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
fn router(plane: Calls, window: Window) -> Router<Echo, Calls> {
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
    let mut router = router(Calls::Heard(told), Window::new(usize::MAX));
    let (feed, request) = request(WRITE);
    let beyond = limits().decoding(crate::limits::Class::Data) + 1;
    feed.send(Frame::data(begun(beyond))).unwrap();
    let answer = within(router.call(request)).await.unwrap();
    assert_eq!(answer.status(), http::StatusCode::OK);
    assert_eq!(
        within(reads.recv()).await,
        Some(Heard::Write(Err(Code::OutOfRange)))
    );
}

#[tokio::test]
async fn a_write_is_answered_with_its_answers_and_trailers_of_success() {
    let (told, mut reads) = mpsc::unbounded_channel();
    let mut router = router(Calls::Heard(told), Window::new(usize::MAX));
    let (feed, request) = request(WRITE);
    let flush = v1::WriteFrame {
        frame: Some(v1::write_frame::Frame::Flush(v1::Unit {})),
    };
    feed.send(Frame::data(Chained::chunks(flush.clone()).head))
        .unwrap();
    drop(feed);
    let answer = within(router.call(request)).await.unwrap();
    assert_eq!(answer.headers()["content-type"], "application/grpc");
    assert_eq!(
        within(reads.recv()).await,
        Some(Heard::Write(Ok(Some(flush))))
    );
    assert_eq!(within(reads.recv()).await, Some(Heard::Write(Ok(None))));
    let frames = within(drained(answer.into_body())).await;
    assert_eq!(frames.len(), 2, "{frames:?}");
    let trailers = frames[1].as_ref().unwrap().trailers_ref().unwrap();
    assert_eq!(Status::from_header_map(trailers).unwrap().code(), Code::Ok);
}

#[tokio::test]
async fn a_write_refused_before_any_answer_is_answered_trailers_only() {
    let mut router = router(Calls::Refused, Window::new(usize::MAX));
    let (_feed, request) = request(WRITE);
    let answer = within(router.call(request)).await.unwrap();
    let status = Status::from_header_map(answer.headers()).unwrap();
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(answer.body().is_end_stream(), "no body");
}

#[tokio::test]
async fn a_data_plane_call_naming_a_compression_is_refused_before_the_plane_hears_of_it() {
    for path in [WRITE, READ, READ_PUBLISHED] {
        let (told, mut reads) = mpsc::unbounded_channel();
        let mut router = router(Calls::Heard(told), Window::new(usize::MAX));
        let (_feed, mut request) = request(path);
        request
            .headers_mut()
            .insert("grpc-encoding", HeaderValue::from_static("gzip"));
        let answer = within(router.call(request)).await.unwrap();
        let status = Status::from_header_map(answer.headers()).unwrap();
        assert_eq!(status.code(), Code::Unimplemented, "{path}");
        drop(router);
        assert_eq!(within(reads.recv()).await, None, "the plane heard nothing");
    }
}

#[tokio::test]
async fn a_read_s_control_beyond_its_wire_bound_is_refused_before_the_plane_reads_it() {
    let (told, mut reads) = mpsc::unbounded_channel();
    let mut router = router(Calls::Heard(told), Window::new(usize::MAX));
    let (feed, request) = request(READ);
    let beyond = limits().decoding(crate::limits::Class::Cursor) + 1;
    feed.send(Frame::data(begun(beyond))).unwrap();
    let answer = within(router.call(request)).await.unwrap();
    assert_eq!(answer.status(), http::StatusCode::OK);
    let heard = within(reads.recv()).await;
    assert_eq!(heard, Some(Heard::Read(Err(Code::OutOfRange))));
}

#[tokio::test]
async fn a_read_is_answered_with_its_frames_and_trailers_of_success() {
    let (told, mut reads) = mpsc::unbounded_channel();
    let mut router = router(Calls::Heard(told), Window::new(usize::MAX));
    let (feed, request) = request(READ);
    let start = v1::ReadControl {
        control: Some(v1::read_control::Control::Start(v1::ReadStart {
            partition: "p".to_owned(),
            ..v1::ReadStart::default()
        })),
    };
    feed.send(Frame::data(Chained::chunks(start.clone()).head))
        .unwrap();
    drop(feed);
    let answer = within(router.call(request)).await.unwrap();
    assert_eq!(answer.headers()["content-type"], "application/grpc");
    let heard = within(reads.recv()).await;
    assert_eq!(heard, Some(Heard::Read(Ok(Some(start)))));
    let heard = within(reads.recv()).await;
    assert_eq!(heard, Some(Heard::Read(Ok(None))));
    let frames = within(drained(answer.into_body())).await;
    assert_eq!(frames.len(), 3, "the batch's head and body, then trailers");
    let sent: Vec<u8> = frames[..2]
        .iter()
        .flat_map(|frame| frame.as_ref().unwrap().data_ref().unwrap().to_vec())
        .collect();
    let mut expected = vec![0];
    let length = u32::try_from(prost::Message::encoded_len(&read_frame())).unwrap();
    expected.extend(length.to_be_bytes());
    expected.extend(prost::Message::encode_to_vec(&read_frame()));
    assert_eq!(sent, expected);
    let trailers = frames[2].as_ref().unwrap().trailers_ref().unwrap();
    assert_eq!(Status::from_header_map(trailers).unwrap().code(), Code::Ok);
}

/// A request to read back the table `name`.
fn read_back(name: &str) -> v1::ReadPublishedRequest {
    v1::ReadPublishedRequest {
        table: Some(v1::TableRef {
            name: name.to_owned(),
            ..v1::TableRef::default()
        }),
    }
}

#[tokio::test]
async fn a_read_back_reaches_the_plane_as_its_one_request() {
    let (told, mut reads) = mpsc::unbounded_channel();
    let mut router = router(Calls::Heard(told), Window::new(usize::MAX));
    let (feed, request) = request(READ_PUBLISHED);
    feed.send(Frame::data(Chained::chunks(read_back("t")).head))
        .unwrap();
    drop(feed);
    let answer = within(router.call(request)).await.unwrap();
    assert!(
        Status::from_header_map(answer.headers()).is_none(),
        "answered"
    );
    let heard = within(reads.recv()).await;
    assert_eq!(heard, Some(Heard::ReadBack(read_back("t"))));
    let frames = within(drained(answer.into_body())).await;
    assert_eq!(frames.len(), 3, "a frame's head and body, then trailers");
}

#[tokio::test]
async fn a_read_back_without_its_request_is_refused_before_the_plane_hears_of_it() {
    let (told, mut reads) = mpsc::unbounded_channel();
    let mut router = router(Calls::Heard(told), Window::new(usize::MAX));
    let (feed, request) = request(READ_PUBLISHED);
    drop(feed);
    let answer = within(router.call(request)).await.unwrap();
    let status = Status::from_header_map(answer.headers()).unwrap();
    assert_eq!(status.code(), Code::Internal);
    assert!(answer.body().is_end_stream(), "no body");
    drop(router);
    assert_eq!(within(reads.recv()).await, None, "the plane heard nothing");
}

#[tokio::test]
async fn a_read_or_read_back_refused_before_any_answer_is_answered_trailers_only() {
    for path in [READ, READ_PUBLISHED] {
        let mut router = router(Calls::Refused, Window::new(usize::MAX));
        let (feed, request) = request(path);
        feed.send(Frame::data(Chained::chunks(read_back("t")).head))
            .unwrap();
        drop(feed);
        let answer = within(router.call(request)).await.unwrap();
        let status = Status::from_header_map(answer.headers()).unwrap();
        assert_eq!(status.code(), Code::Unimplemented, "{path}");
        assert!(answer.body().is_end_stream(), "no body");
    }
}

/// A read-back tonic's own server serves, telling the request it was called with.
struct Served(mpsc::UnboundedSender<v1::ReadPublishedRequest>);

impl tonic::server::ServerStreamingService<v1::ReadPublishedRequest> for Served {
    type Response = v1::ReadFrame;
    type ResponseStream = Answers<v1::ReadFrame>;
    type Future = BoxFuture<tonic::Response<Answers<v1::ReadFrame>>, Status>;

    fn call(&mut self, request: tonic::Request<v1::ReadPublishedRequest>) -> Self::Future {
        self.0.send(request.into_inner()).ok();
        Box::pin(async {
            let answers = Box::pin(tokio_stream::empty()) as Answers<_>;
            Ok(tonic::Response::new(answers))
        })
    }
}

/// What the plane's reader, and tonic's server, come to of a read-back's request of `sent`: the
/// request, or the code of the status that refuses it.
async fn read_backs(sent: &[Result<Bytes, Code>]) -> [Result<v1::ReadPublishedRequest, Code>; 2] {
    let body = || {
        let (feed, body) = fed();
        for each in sent {
            match each {
                Ok(data) => feed.send(Frame::data(data.clone())),
                Err(code) => feed.fail(Status::new(*code, "failed")),
            }
            .unwrap();
        }
        let limits = limits();
        let bounds = crate::bounded::Bounds::of(
            &limits,
            crate::limits::Class::of_request("ReadPublished"),
            crate::scan::request("ReadPublished"),
        );
        crate::bounded::Bounded::new(body, bounds, None)
    };
    let plane = Incoming::<v1::ReadPublishedRequest>::request(body())
        .unary()
        .await
        .map_err(|status| status.code());
    let (heard, mut requests) = mpsc::unbounded_channel();
    let codec = tonic_prost::ProstCodec::<v1::ReadFrame, v1::ReadPublishedRequest>::default();
    let answer = tonic::server::Grpc::new(codec)
        .server_streaming(Served(heard), http::Request::new(body()))
        .await;
    let tonic = match Status::from_header_map(answer.headers()) {
        Some(refused) => Err(refused.code()),
        None => Ok(requests.try_recv().expect("tonic called the service")),
    };
    [plane, tonic]
}

#[tokio::test]
async fn a_read_back_s_request_is_read_as_tonic_s_server_reads_it() {
    let one = Chained::chunks(read_back("t")).head;
    let other = Chained::chunks(read_back("u")).head;
    let mut compressed = one.to_vec();
    compressed[0] = 1;
    let cases: [(&str, Vec<Result<Bytes, Code>>); 7] = [
        ("one request", vec![Ok(one.clone())]),
        ("in pieces", vec![Ok(one.slice(..3)), Ok(one.slice(3..))]),
        ("none", vec![]),
        ("two", vec![Ok(one.clone()), Ok(other)]),
        ("cut short", vec![Ok(one.slice(..one.len() - 1))]),
        ("compressed", vec![Ok(Bytes::from(compressed))]),
        (
            "failing after its request",
            vec![Ok(one.clone()), Err(Code::Unavailable)],
        ),
    ];
    for (case, sent) in cases {
        let [plane, tonic] = read_backs(&sent).await;
        assert_eq!(plane, tonic, "{case}");
    }
}

#[tokio::test]
async fn every_other_call_reaches_the_service_within_its_class_s_bound() {
    let (told, _reads) = mpsc::unbounded_channel();
    let mut router = router(Calls::Heard(told), Window::new(usize::MAX));
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
    let mut router = router(Calls::Heard(told), window.clone());
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
            Some(Heard::Write(Err(Code::Internal))),
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
        Arc::new(Calls::Heard(told)),
        &limits(),
        window.clone(),
    );
    assert!(busy.poll_ready(&mut context).is_pending());
    let mut ready = router(Calls::Refused, window);
    assert!(ready.poll_ready(&mut context).is_ready());
}
