//! What a byte kept of a call's transport frame keeps alive once the call and its connection
//! are gone.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body, Frame};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_wire::flow::Transport;
use rdlt_wire::limits::TRANSPORT_FRAME_BYTES;
use tokio::net::UnixStream;

use crate::HEAP;

/// Bytes: the answer's body, a call's stream window of it.
const BODY: usize = 4 << 20;

/// A body of one chunk, or of none.
struct Once(Option<Bytes>);

impl Body for Once {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|chunk| Ok(Frame::data(chunk))))
    }
}

/// Bytes the heap holds once a call's first transport frame was read, `keep` bytes of it kept,
/// and the call, both ends of the connection and their tasks are gone, beyond what it held
/// before.
async fn left(keep: usize) -> usize {
    let transport = Transport::default();
    let before = HEAP.current_usage();
    let (host, connector) = UnixStream::pair().expect("a socket pair");
    let answer = tower::service_fn(|_: http::Request<hyper::body::Incoming>| async {
        let chunk = Bytes::from(vec![7_u8; BODY]);
        Ok::<_, Infallible>(http::Response::new(Once(Some(chunk))))
    });
    let serving = tokio::spawn(
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .initial_stream_window_size(transport.stream_window)
            .initial_connection_window_size(transport.connection_window)
            .max_frame_size(transport.max_frame)
            .serve_connection(TokioIo::new(connector), TowerToHyperService::new(answer)),
    );
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .timer(TokioTimer::new())
        .initial_stream_window_size(transport.stream_window)
        .initial_connection_window_size(transport.connection_window)
        .max_frame_size(transport.max_frame)
        .handshake::<_, Once>(TokioIo::new(host))
        .await
        .expect("the client handshakes");
    let connecting = tokio::spawn(connection);
    let response = sender
        .send_request(http::Request::new(Once(None)))
        .await
        .expect("the call is answered");
    let mut body = response.into_body();
    let frame = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context))
        .await
        .expect("a frame arrives")
        .expect("the frame arrives whole");
    let data = frame.into_data().expect("a frame of data");
    let kept = data.slice(..keep);
    drop((data, body, sender));
    connecting.abort();
    serving.abort();
    let _connected = connecting.await;
    let _served = serving.await;
    let left = HEAP.current_usage().saturating_sub(before);
    drop(kept);
    left
}

/// Bytes `left(keep)` comes to, failing the test where a call over a socket pair takes longer
/// than a minute.
async fn bounded(keep: usize) -> usize {
    tokio::time::timeout(Duration::from_secs(60), left(keep))
        .await
        .expect("a call over a socket pair ends within a minute")
}

#[tokio::test]
async fn a_byte_kept_of_a_transport_frame_keeps_alive_no_more_than_the_frame() {
    // The first call leaves what the runtime allocates once, so it is not measured.
    bounded(0).await;
    let without = bounded(0).await;
    let with = bounded(1).await;
    let frame = usize::try_from(TRANSPORT_FRAME_BYTES).unwrap();
    assert!(
        with.saturating_sub(without) <= frame + (64 << 10),
        "a byte kept {with} bytes alive, {without} without it"
    );
}
