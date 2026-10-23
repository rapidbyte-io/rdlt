use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo};
use rdlt_wire::tonic::body::Body;

use super::{Fake, Fault, Polled, serve};

#[tokio::test(start_paused = true)]
async fn a_fake_answers_without_the_wall_clocks_date() {
    let (host, connector) = tokio::io::duplex(64 * 1024);
    tokio::spawn(serve(Fake(Fault::GarbageSchema), connector));
    let (mut sender, connection) = tokio::time::timeout(
        Duration::from_secs(5),
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(host)),
    )
    .await
    .expect("the fake handshakes in time")
    .expect("the fake speaks HTTP/2");
    tokio::spawn(connection);
    let request = http::Request::post("http://fake/rdlt.connector.v1.Connector/Check")
        .header("content-type", "application/grpc")
        .body(Body::empty())
        .expect("a request");
    let answered = tokio::time::timeout(Duration::from_secs(5), sender.send_request(request))
        .await
        .expect("the check is answered in time")
        .expect("the check is answered");
    assert!(
        answered.headers().get(http::header::DATE).is_none(),
        "{:?}",
        answered.headers()
    );
}

#[tokio::test(start_paused = true)]
async fn a_flood_settles_by_the_clock_of_the_runtime_it_runs_on() {
    static POLLED: Polled = Polled::new();
    POLLED.took(8);
    tokio::time::timeout(
        Duration::from_secs(1),
        POLLED.settled(Duration::from_millis(200)),
    )
    .await
    .expect("the paused clock passes the quiet period");
    assert_eq!(POLLED.bytes(), 8);
}
