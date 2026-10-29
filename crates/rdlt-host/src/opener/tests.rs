use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::Role;
use rdlt_wire::opener::Accepting;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{Opener as _, Opening, Passing};
use crate::remote::{Connection, DEADLINE_EXCEEDED, Deadlines, Options};

/// An opener's two ends: the host's, and the connector's, which takes the streams passed.
fn opener() -> (Passing, Accepting) {
    let (host, connector) = std::os::unix::net::UnixStream::pair().expect("a socket pair");
    let passing = Passing::new(OwnedFd::from(host)).expect("the host's end registers");
    let accepting =
        Accepting::new(OwnedFd::from(connector)).expect("the connector's end registers");
    (passing, accepting)
}

/// `fd`, a stream passed to the connector, as the connector reads and writes it.
fn stream(fd: OwnedFd) -> tokio::net::UnixStream {
    let stream = std::os::unix::net::UnixStream::from(fd);
    stream
        .set_nonblocking(true)
        .expect("the stream goes non-blocking");
    tokio::net::UnixStream::from_std(stream).expect("the stream registers")
}

#[tokio::test]
async fn each_stream_reaches_the_connector_as_what_it_was_opened_as_and_the_host_keeps_one_end() {
    let (passing, accepting) = opener();
    for opening in [Opening::Control, Opening::Data, Opening::Data] {
        let mut host = passing.open(opening).await.expect("the stream opens");
        let (passed, fd) = tokio::time::timeout(Duration::from_secs(5), accepting.next())
            .await
            .expect("the stream is passed in time")
            .expect("the opener reads")
            .expect("a stream was passed");
        assert_eq!(passed, opening);
        let mut connector = stream(fd);
        host.write_all(b"ping").await.expect("the host writes");
        let mut read = [0; 4];
        connector
            .read_exact(&mut read)
            .await
            .expect("the connector reads");
        assert_eq!(&read, b"ping");
        // The connector's end closed, the host's reads end: no copy of it stayed in the host.
        drop(connector);
        let ended = tokio::time::timeout(Duration::from_secs(5), host.read(&mut read)).await;
        assert_eq!(ended.expect("the stream ends").expect("it reads"), 0);
    }
}

#[tokio::test]
async fn an_opener_whose_connector_is_gone_opens_nothing() {
    let (passing, accepting) = opener();
    drop(accepting);
    let opened = tokio::time::timeout(Duration::from_secs(5), passing.open(Opening::Control))
        .await
        .expect("the opening ends");
    assert!(opened.is_err(), "a stream was opened to no connector");
}

#[tokio::test(start_paused = true)]
async fn a_connector_that_never_takes_its_streams_fails_the_connection_within_its_deadline() {
    let (passing, accepting) = opener();
    let options = Options {
        deadlines: Deadlines {
            connect: Duration::from_secs(5),
            ..Deadlines::default()
        },
        ..Options::default()
    };
    let config = serde_json::json!({});
    let connecting = Connection::connect_opened(Arc::new(passing), Role::Source, &config, options);
    let failed = tokio::time::timeout(Duration::from_secs(60), connecting)
        .await
        .expect("the connection fails by its deadline")
        .expect_err("no connector answered");
    assert_eq!(failed.code(), Some(DEADLINE_EXCEEDED), "{failed}");
    drop(accepting);
}
