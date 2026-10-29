use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rdlt_wire::Limits;

use super::{Opened, Opening, ServeError, Served, serve_connection, serve_streams};

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

/// Streams passed in turn from a list, each `Err` the opener's failure; then none, for ever.
struct Listed(std::vec::IntoIter<std::io::Result<Option<(Opening, tokio::io::DuplexStream)>>>);

impl Opened for Listed {
    type Stream = tokio::io::DuplexStream;

    async fn next(&mut self) -> std::io::Result<Option<(Opening, tokio::io::DuplexStream)>> {
        match self.0.next() {
            Some(next) => next,
            None => std::future::pending().await,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_connection_whose_opener_fails_ends_with_its_failure() {
    let (_host, control) = tokio::io::duplex(64 * 1024);
    let failed = std::io::Error::other("a stream passed with its descriptors cut short");
    let opened = Listed(vec![Ok(Some((Opening::Control, control))), Err(failed)].into_iter());
    let serving = serve_streams(
        Arc::new(Served::default()),
        opened,
        Limits::default(),
        std::future::pending(),
    );
    let served = tokio::time::timeout(std::time::Duration::from_secs(10), serving)
        .await
        .expect("the connection ends");
    assert!(matches!(served, Err(ServeError::Opener(_))), "{served:?}");
}
