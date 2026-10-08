//! Every way a call ends, read by [`Incoming`] and by tonic's own client and `Streaming`, which
//! must agree: the host classes a failure by its code.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::Frame;
use prost::Message;
use tonic::codec::Streaming;
use tonic::codegen::http::{self, HeaderMap, HeaderValue, StatusCode};
use tonic::codegen::{Service, tokio_stream};
use tonic::{Code, Status};

use super::reads::{done, json, read_answer, read_batch, read_request};
use super::{Incoming, answered, batch_frame, credit, prefixed, requested, trailers};
use crate::bounded::Bounded;
use crate::testing::fed;
use crate::v1;

/// What a body is fed.
#[derive(Clone, Debug)]
enum Fed {
    Data(Vec<u8>),
    Trailers(HeaderMap),
    /// The body fails with a status of this code, as a stream reset or a connection gone does.
    Fails(Code),
    /// The body fails with an error of the transport's own.
    FailsInIo,
}

/// A body fed `fed`, which then ends.
fn body(events: &[Fed]) -> tonic::body::Body {
    let (feed, body) = fed();
    for each in events {
        match each {
            Fed::Data(data) => feed.send(Frame::data(Bytes::from(data.clone()))),
            Fed::Trailers(trailers) => feed.send(Frame::trailers(trailers.clone())),
            Fed::Fails(code) => feed.fail(Status::new(*code, "the stream failed")),
            Fed::FailsInIo => feed.fail(Status::from_error(Box::new(std::io::Error::other(
                "the connection was reset",
            )))),
        }
        .unwrap();
    }
    body
}

/// What reading a call came to: its messages, then its end, or the code of its failure.
type Read<M> = (Vec<M>, Result<(), Code>);

/// Reads `incoming` until it ends or fails.
async fn read<M: Message + Default>(mut incoming: Incoming<M>) -> Read<M> {
    let mut messages = Vec::new();
    loop {
        match incoming.message().await {
            Ok(Some(message)) => messages.push(message),
            Ok(None) => return (messages, Ok(())),
            Err(status) => return (messages, Err(status.code())),
        }
    }
}

/// Reads tonic's `streaming` until it ends or fails.
async fn read_tonic<M>(mut streaming: Streaming<M>) -> Read<M> {
    let mut messages = Vec::new();
    loop {
        match streaming.message().await {
            Ok(Some(message)) => messages.push(message),
            Ok(None) => return (messages, Ok(())),
            Err(status) => return (messages, Err(status.code())),
        }
    }
}

/// A transport answering its one call with an answer made beforehand.
struct Answering(Mutex<Option<http::Response<Bounded>>>);

impl Service<http::Request<tonic::body::Body>> for Answering {
    type Response = http::Response<Bounded>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Request<tonic::body::Body>) -> Self::Future {
        let answer = self.0.lock().unwrap().take().expect("one call");
        Box::pin(async move { Ok(answer) })
    }
}

/// An answer of HTTP status `code` and `headers`, its body fed `fed` and held as `bounded`
/// holds it.
fn answer(
    bounded: fn(tonic::body::Body) -> Bounded,
    code: StatusCode,
    headers: &HeaderMap,
    fed: &[Fed],
) -> http::Response<Bounded> {
    let mut answer = http::Response::new(bounded(body(fed)));
    *answer.status_mut() = code;
    answer.headers_mut().extend(headers.clone());
    answer
}

/// How the plane and tonic's client each read an answer of `M` to the call at `path`, of
/// `code`, `headers` and `fed`, held as `bounded` holds it.
async fn answers_to<M: Message + Default + Send + 'static>(
    (path, bounded): (&'static str, fn(tonic::body::Body) -> Bounded),
    code: StatusCode,
    headers: &HeaderMap,
    fed: &[Fed],
) -> (Read<M>, Read<M>) {
    let plane = match Incoming::answer(answer(bounded, code, headers, fed)) {
        Ok(incoming) => read(incoming).await,
        Err(status) => (Vec::new(), Err(status.code())),
    };
    let transport = Answering(Mutex::new(Some(answer(bounded, code, headers, fed))));
    let mut client = tonic::client::Grpc::new(transport);
    let codec = tonic_prost::ProstCodec::<v1::Unit, M>::default();
    let requests = tokio_stream::iter(Vec::<v1::Unit>::new());
    let tonic = match client
        .streaming(tonic::Request::new(requests), path.parse().unwrap(), codec)
        .await
    {
        Ok(streaming) => read_tonic(streaming.into_inner()).await,
        Err(status) => (Vec::new(), Err(status.code())),
    };
    (plane, tonic)
}

/// How the plane and tonic's client each read a write's answer of `code`, `headers` and `fed`.
async fn answers(
    code: StatusCode,
    headers: &HeaderMap,
    fed: &[Fed],
) -> (Read<v1::WriteAck>, Read<v1::WriteAck>) {
    answers_to((super::super::WRITE, answered), code, headers, fed).await
}

/// Headers of an answer under way.
fn grpc() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/grpc"));
    headers
}

#[tokio::test]
async fn an_answer_ends_as_tonic_s_client_reads_it() {
    let acks = || {
        [credit(1), credit(2)]
            .iter()
            .map(|ack| Fed::Data(prefixed(ack)))
            .collect::<Vec<_>>()
    };
    let ending = |end: Fed| [acks(), vec![end]].concat();
    let mut cases = vec![
        (
            "trailers of success",
            ending(Fed::Trailers(trailers(&Status::ok("")))),
        ),
        ("no trailers", acks()),
        ("trailers without a status", ending(Fed::Trailers(grpc()))),
        ("a stream reset", ending(Fed::Fails(Code::Cancelled))),
        ("a stream refused", ending(Fed::Fails(Code::Unavailable))),
        ("a connection gone", ending(Fed::Fails(Code::Internal))),
        ("an io error", ending(Fed::FailsInIo)),
        (
            "a message cut short",
            [acks(), vec![Fed::Data(prefixed(&credit(3))[..4].to_vec())]].concat(),
        ),
        (
            "a compressed message",
            vec![Fed::Data([&[1][..], &prefixed(&credit(3))[1..]].concat())],
        ),
        (
            "a message the scan cannot walk",
            vec![Fed::Data(crate::testing::message(&[0x0a, 0x05]))],
        ),
        (
            // An error whose message is no UTF-8.
            "a message that does not decode",
            vec![Fed::Data(crate::testing::message(&[
                0x1a, 0x03, 0x12, 0x01, 0xff,
            ]))],
        ),
    ];
    for code in [Code::Unavailable, Code::Cancelled, Code::ResourceExhausted] {
        let status = Status::new(code, "failed");
        cases.push((
            "trailers of a failure",
            ending(Fed::Trailers(trailers(&status))),
        ));
    }
    for (case, fed) in cases {
        let (plane, tonic) = answers(StatusCode::OK, &grpc(), &fed).await;
        assert_eq!(plane, tonic, "{case}");
    }
}

#[tokio::test]
async fn an_answer_s_headers_end_it_as_tonic_s_client_reads_them() {
    let fed = [
        Fed::Data(prefixed(&credit(1))),
        Fed::Trailers(trailers(&Status::internal("late"))),
    ];
    let mut cases = Vec::new();
    for status in [
        Status::ok(""),
        Status::unimplemented("no write"),
        Status::unavailable("busy"),
    ] {
        let mut headers = grpc();
        headers.extend(trailers(&status));
        cases.push((
            format!("trailers only: {:?}", status.code()),
            StatusCode::OK,
            headers,
        ));
    }
    for code in [
        StatusCode::BAD_REQUEST,
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::GATEWAY_TIMEOUT,
        StatusCode::IM_A_TEAPOT,
    ] {
        cases.push((format!("HTTP {code}"), code, grpc()));
    }
    let mut compressed = grpc();
    compressed.insert("grpc-encoding", HeaderValue::from_static("gzip"));
    cases.push(("compressed".to_owned(), StatusCode::OK, compressed));
    let mut identity = grpc();
    identity.insert("grpc-encoding", HeaderValue::from_static("identity"));
    cases.push(("not compressed".to_owned(), StatusCode::OK, identity));
    for (case, code, headers) in cases {
        let ended = [Fed::Data(prefixed(&credit(1)))];
        for fed in [&fed[..], &ended] {
            let (plane, tonic) = answers(code, &headers, fed).await;
            assert_eq!(plane, tonic, "{case}");
        }
    }
}

/// How the plane and tonic's `Streaming` each read a request of `M` fed `fed`, held as
/// `bounded` holds it.
async fn requests_of<M: Message + Default + Send + 'static>(
    bounded: fn(tonic::body::Body) -> Bounded,
    fed: &[Fed],
) -> (Read<M>, Read<M>) {
    let plane = read(Incoming::request(bounded(body(fed)))).await;
    let decoder =
        tonic::codec::Codec::decoder(&mut tonic_prost::ProstCodec::<v1::Unit, M>::default());
    let streaming = Streaming::new_request(decoder, bounded(body(fed)), None, None);
    (plane, read_tonic(streaming).await)
}

/// How the plane and tonic's `Streaming` each read a write's request fed `fed`.
async fn requests(fed: &[Fed]) -> (Read<v1::WriteFrame>, Read<v1::WriteFrame>) {
    requests_of(requested, fed).await
}

#[tokio::test]
async fn a_request_ends_as_tonic_s_server_reads_it() {
    let frames = || {
        [batch_frame(1, b"h", &[5; 300]), batch_frame(2, b"", b"")]
            .iter()
            .map(|frame| Fed::Data(prefixed(frame)))
            .collect::<Vec<_>>()
    };
    let ending = |end: Fed| [frames(), vec![end]].concat();
    let cases = [
        ("its client's end", frames()),
        (
            "cancelled by its client",
            ending(Fed::Fails(Code::Cancelled)),
        ),
        (
            "trailers",
            ending(Fed::Trailers(trailers(&Status::internal("odd")))),
        ),
        ("a stream refused", ending(Fed::Fails(Code::Unavailable))),
        ("a connection gone", ending(Fed::Fails(Code::Internal))),
        ("an io error", ending(Fed::FailsInIo)),
        (
            "a message cut short",
            [
                frames(),
                vec![Fed::Data(
                    prefixed(&batch_frame(3, b"", b"b"))[..7].to_vec(),
                )],
            ]
            .concat(),
        ),
        (
            "cancelled within a message",
            [
                frames(),
                vec![
                    Fed::Data(prefixed(&batch_frame(3, b"", b"b"))[..7].to_vec()),
                    Fed::Fails(Code::Cancelled),
                ],
            ]
            .concat(),
        ),
        (
            "a compressed message",
            vec![Fed::Data(
                [&[1][..], &prefixed(&batch_frame(3, b"", b"b"))[1..]].concat(),
            )],
        ),
    ];
    for (case, fed) in cases {
        let (plane, tonic) = requests(&fed).await;
        assert_eq!(plane, tonic, "{case}");
    }
}

#[tokio::test]
async fn a_read_s_answer_ends_as_tonic_s_client_reads_it() {
    let frames = || {
        [
            read_batch(1, b"h", &[6; 2_000]),
            json(br#"[{"a":1}]"#),
            read_batch(1, b"", b""),
        ]
        .iter()
        .map(|frame| Fed::Data(prefixed(frame)))
        .collect::<Vec<_>>()
    };
    let ending = |end: Vec<Fed>| [frames(), end].concat();
    let ok = || Fed::Trailers(trailers(&Status::ok("")));
    let mut cases = vec![
        (
            "done, then trailers of success",
            ending(vec![Fed::Data(prefixed(&done())), ok()]),
        ),
        (
            "done, then no trailers",
            ending(vec![Fed::Data(prefixed(&done()))]),
        ),
        ("no done, trailers of success", ending(vec![ok()])),
        ("a stream reset", ending(vec![Fed::Fails(Code::Cancelled)])),
        (
            "a connection gone",
            ending(vec![Fed::Fails(Code::Internal)]),
        ),
        ("an io error", ending(vec![Fed::FailsInIo])),
        (
            "a frame cut short",
            ending(vec![Fed::Data(
                prefixed(&read_batch(2, b"", b"b"))[..8].to_vec(),
            )]),
        ),
    ];
    for code in [
        Code::Internal,
        Code::Unavailable,
        Code::OutOfRange,
        Code::Unknown,
    ] {
        let failed = Fed::Trailers(trailers(&Status::new(code, "the read failed")));
        cases.push(("a failed read", ending(vec![failed])));
    }
    let read = (super::super::READ, read_answer as fn(_) -> _);
    for (case, fed) in cases {
        let (plane, tonic) = answers_to::<v1::ReadFrame>(read, StatusCode::OK, &grpc(), &fed).await;
        assert_eq!(plane, tonic, "{case}");
    }
    let mut refused = grpc();
    refused.extend(trailers(&Status::unimplemented("no read-back")));
    let read_back = (super::super::READ_PUBLISHED, read_answer as fn(_) -> _);
    let (plane, tonic) =
        answers_to::<v1::ReadFrame>(read_back, StatusCode::OK, &refused, &[]).await;
    assert_eq!(plane, tonic, "a read-back refused trailers-only");
    assert_eq!(plane.1, Err(Code::Unimplemented));
}

/// `body` held to the bounds of a read's controls.
fn controls(body: tonic::body::Body) -> Bounded {
    read_request(body, "Read")
}

#[tokio::test]
async fn a_read_s_controls_end_as_tonic_s_server_reads_them() {
    let credit = |bytes| v1::ReadControl {
        control: Some(v1::read_control::Control::Credit(v1::Credit { bytes })),
    };
    let sent = || {
        [credit(1), credit(2)]
            .iter()
            .map(|control| Fed::Data(prefixed(control)))
            .collect::<Vec<_>>()
    };
    let ending = |end: Fed| [sent(), vec![end]].concat();
    let cases = [
        ("its client's end", sent()),
        (
            "cancelled by its client",
            ending(Fed::Fails(Code::Cancelled)),
        ),
        ("a connection gone", ending(Fed::Fails(Code::Internal))),
        (
            "a control cut short",
            ending(Fed::Data(prefixed(&credit(3))[..6].to_vec())),
        ),
        (
            "a compressed control",
            vec![Fed::Data([&[1][..], &prefixed(&credit(3))[1..]].concat())],
        ),
    ];
    for (case, fed) in cases {
        let (plane, tonic) = requests_of::<v1::ReadControl>(controls, &fed).await;
        assert_eq!(plane, tonic, "{case}");
    }
}
