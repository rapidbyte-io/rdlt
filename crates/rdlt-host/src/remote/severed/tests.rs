use std::io::{ErrorKind, IoSlice};

use tokio::io::{AsyncReadExt as _, AsyncWrite as _, AsyncWriteExt as _};
use tokio_util::sync::CancellationToken;

use super::Severed;
use crate::sink::Sink;

#[tokio::test]
async fn a_live_transport_passes_every_call_to_its_stream() {
    for vectored in [true, false] {
        let mut severed = Severed::new(Sink::new(vectored), CancellationToken::new());
        assert_eq!(severed.is_write_vectored(), vectored);
        let buffers = [IoSlice::new(b"one "), IoSlice::new(b"two")];
        let written = severed
            .write_vectored(&buffers)
            .await
            .expect("the stream writes");
        severed
            .write_all(b" three")
            .await
            .expect("the stream writes");
        severed.flush().await.expect("the stream flushes");
        severed.shutdown().await.expect("the stream shuts down");
        let read = severed.read(&mut [0; 8]).await.expect("the stream reads");
        assert_eq!((written, read), (7, 0));
        assert_eq!(severed.io.written, b"one two three");
        assert_eq!((severed.io.flushes, severed.io.shutdowns), (1, 1));
    }
}

#[tokio::test]
async fn an_ended_transport_fails_every_call_without_reaching_its_stream() {
    let cut = CancellationToken::new();
    let mut severed = Severed::new(Sink::new(true), cut.clone());
    cut.cancel();
    let buffers = [IoSlice::new(b"one")];
    let failures = [
        severed.read(&mut [0; 8]).await.map(drop),
        severed.write(b"one").await.map(drop),
        severed.write_vectored(&buffers).await.map(drop),
        severed.flush().await,
        severed.shutdown().await,
    ];
    for failure in failures {
        let error = failure.expect_err("an ended transport fails");
        assert_eq!(error.kind(), ErrorKind::ConnectionAborted, "{error}");
    }
    assert!(severed.io.written.is_empty());
    assert_eq!((severed.io.flushes, severed.io.shutdowns), (0, 0));
}

#[tokio::test]
async fn a_read_waiting_on_its_stream_fails_once_the_connection_ends() {
    let cut = CancellationToken::new();
    let (_peer, stream) = tokio::io::duplex(64);
    let mut severed = Severed::new(stream, cut.clone());
    let reading = tokio::spawn(async move { severed.read(&mut [0; 8]).await });
    tokio::task::yield_now().await;
    cut.cancel();
    let error = reading
        .await
        .expect("the read ends")
        .expect_err("the read fails");
    assert_eq!(error.kind(), ErrorKind::ConnectionAborted, "{error}");
}
