use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::Body as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::codegen::http;

use super::{Router, Streams};
use crate::bounded::Window;
use crate::limits::Limits;
use crate::stream::{DATA, Piped};

/// Runs `work` on a runtime of its own.
fn running<F: Future>(work: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime builds")
        .block_on(work)
}

/// Serves each data stream by answering what it reads, in capitals, until it ends.
struct Shouting;

impl Streams for Shouting {
    fn serve(self: Arc<Self>, mut stream: Piped<Body>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let mut read = Vec::new();
            stream
                .read_to_end(&mut read)
                .await
                .expect("the stream reads");
            read.make_ascii_uppercase();
            stream.write_all(&read).await.expect("the answer is taken");
            stream.shutdown().await.expect("the stream ends");
        })
    }
}

/// Answers every call with nothing, noting each path and what its body yielded first.
#[derive(Clone, Default)]
struct Noting(Arc<Mutex<Vec<Noted>>>);

/// A call's path, and whether its body's first frame read.
type Noted = (String, Option<bool>);

impl Service<http::Request<Body>> for Noting {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<Body>, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let noted = Arc::clone(&self.0);
        Box::pin(async move {
            let path = request.uri().path().to_owned();
            let mut body = request.into_body();
            let first = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context))
                .await
                .map(|frame| frame.is_ok());
            noted
                .lock()
                .expect("the notes are whole")
                .push((path, first));
            Ok(http::Response::new(Body::empty()))
        })
    }
}

/// The bytes of `body`, to its end.
async fn collected(mut body: Body) -> Vec<u8> {
    let mut all = Vec::new();
    while let Some(frame) =
        std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await
    {
        if let Ok(data) = frame.expect("the body reads").into_data() {
            all.extend_from_slice(&data);
        }
    }
    all
}

fn router(noting: &Noting) -> Router<Noting, Shouting> {
    let limits = Limits::default();
    Router::new(
        noting.clone(),
        Arc::new(Shouting),
        &limits,
        Window::new(limits.largest()),
    )
}

#[test]
fn a_data_call_s_stream_is_served_by_its_streams_and_answered_in_its_answer_s_body() {
    running(async {
        let noting = Noting::default();
        let mut router = router(&noting);
        let body = Body::new(full(b"a frame's bytes"));
        let mut request = http::Request::new(body);
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = http::Uri::from_static(DATA);
        let answer = router.call(request).await.expect("the call is answered");
        assert_eq!(answer.status(), http::StatusCode::OK);
        assert_eq!(collected(answer.into_body()).await, b"A FRAME'S BYTES");
        assert!(
            noting.0.lock().expect("the notes are whole").is_empty(),
            "the service saw no data call"
        );
    });
}

#[test]
fn every_other_call_goes_to_the_service_its_request_held_to_its_bounds() {
    running(async {
        let noting = Noting::default();
        let mut router = router(&noting);
        // A message whose prefix declares more than a check's request may hold.
        let mut message = vec![0];
        message.extend(u32::MAX.to_be_bytes());
        let mut request = http::Request::new(Body::new(full(&message)));
        *request.method_mut() = http::Method::POST;
        *request.uri_mut() = http::Uri::from_static("/rdlt.connector.v1.Connector/Check");
        router.call(request).await.expect("the call is answered");
        let noted = noting.0.lock().expect("the notes are whole").clone();
        assert_eq!(
            noted,
            [("/rdlt.connector.v1.Connector/Check".to_owned(), Some(false))],
            "the service saw the call, its message refused"
        );
    });
}

/// A body of `bytes`, whole.
fn full(bytes: &[u8]) -> Full {
    Full(Some(Bytes::copy_from_slice(bytes)))
}

/// A body of one chunk.
struct Full(Option<Bytes>);

impl http_body::Body for Full {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, tonic::Status>>> {
        Poll::Ready(self.0.take().map(|bytes| Ok(http_body::Frame::data(bytes))))
    }
}
