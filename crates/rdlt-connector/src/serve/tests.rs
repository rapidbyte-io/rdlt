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
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(host))
            .await
            .expect("the connection handshakes");
    tokio::spawn(connection);
    let request = http::Request::post("http://connector/rdlt.v1.Unknown/Call")
        .header("content-type", "application/grpc")
        .body(Nothing)
        .expect("a request");
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        sender.send_request(request),
    )
    .await
    .expect("the connector answers")
    .expect("the response arrives");
    assert!(
        response.headers().get(http::header::DATE).is_none(),
        "{:?}",
        response.headers()
    );
}
