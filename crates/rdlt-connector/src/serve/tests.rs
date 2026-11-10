use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rdlt_wire::Limits;

use super::{Served, serve_connection};

/// A request body with nothing in it.
struct Nothing;

impl http_body::Body for Nothing {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
        Poll::Ready(None)
    }
}

#[tokio::test(start_paused = true)]
async fn a_served_response_carries_no_date_from_the_wall_clock() {
    let (host, connector) = tokio::io::duplex(64 * 1024);
    tokio::spawn(serve_connection(
        Arc::new(Served::default()),
        connector,
        Limits::default(),
    ));
    let exchange = async {
        let (mut sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(host))
                .await
                .expect("the connection handshakes");
        tokio::spawn(connection);
        let request = http::Request::post("http://connector/rdlt.v1.Unknown/Call")
            .header("content-type", "application/grpc")
            .body(Nothing)
            .expect("a request");
        sender
            .send_request(request)
            .await
            .expect("the response arrives")
    };
    let response = tokio::time::timeout(std::time::Duration::from_secs(10), exchange)
        .await
        .expect("the connector answers");
    assert!(
        response.headers().get(http::header::DATE).is_none(),
        "{:?}",
        response.headers()
    );
}

#[tokio::test(start_paused = true)]
async fn a_host_that_closes_before_its_preface_ends_the_connection_cleanly() {
    use tokio::io::AsyncReadExt as _;

    let (mut host, connector) = tokio::io::duplex(64 * 1024);
    let serving = tokio::spawn(serve_connection(
        Arc::new(Served::default()),
        connector,
        Limits::default(),
    ));
    // The connector's settings, which it sends before it reads anything.
    let mut settings = [0; 1024];
    let read = tokio::time::timeout(std::time::Duration::from_secs(10), host.read(&mut settings))
        .await
        .expect("the connector writes first")
        .expect("the host reads");
    assert!(read > 0, "the connector sent its settings");
    drop(host);
    let served = tokio::time::timeout(std::time::Duration::from_secs(10), serving)
        .await
        .expect("the connection ends")
        .expect("serving does not panic");
    assert!(served.is_ok(), "{served:?}");
}
