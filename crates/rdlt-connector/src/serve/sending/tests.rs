use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, DuplexStream};

use super::Sending;

const SEND: Duration = Duration::from_secs(10);

#[tokio::test(start_paused = true)]
async fn a_write_its_peer_takes_nothing_of_fails_at_the_send_wait() {
    let (connection, mut peer) = tokio::io::duplex(1024);
    let mut sending = Sending::within(connection, SEND);
    // The pipe takes what it holds, and then the peer takes nothing.
    sending.write_all(&[0; 1024]).await.unwrap();
    let started = tokio::time::Instant::now();
    let error = sending.write_all(&[0; 1024]).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(started.elapsed(), SEND);
    let mut taken = [0; 1024];
    peer.read_exact(&mut taken).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_peer_that_takes_a_little_at_a_time_keeps_its_writes_going() {
    let (connection, mut peer) = tokio::io::duplex(1024);
    let mut sending = Sending::within(connection, SEND);
    let taking = tokio::spawn(async move {
        let mut taken = vec![0; 8 * 1024];
        for chunk in taken.chunks_mut(512) {
            tokio::time::sleep(SEND / 2).await;
            peer.read_exact(chunk).await.unwrap();
        }
        taken.len()
    });
    // Each write waits less than the send wait for the peer to take some.
    sending.write_all(&[1; 8 * 1024]).await.unwrap();
    sending.flush().await.unwrap();
    assert_eq!(taking.await.unwrap(), 8 * 1024);
}

/// A pipe that says whether it writes vectored, and whose flushes never end, as a connection's
/// do once its host takes nothing.
struct Held {
    pipe: DuplexStream,
    vectored: bool,
}

impl AsyncRead for Held {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.pipe).poll_read(context, buffer)
    }
}

impl AsyncWrite for Held {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.pipe).poll_write(context, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.pipe).poll_shutdown(context)
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }
}

#[tokio::test(start_paused = true)]
async fn a_flush_its_host_takes_nothing_of_fails_at_the_send_wait() {
    let (_host, pipe) = tokio::io::duplex(64);
    let inner = Held {
        pipe,
        vectored: false,
    };
    let mut sending = Sending::within(inner, SEND);
    let started = tokio::time::Instant::now();
    let error = tokio::time::timeout(SEND * 2, sending.flush())
        .await
        .expect("the flush ends once the send wait passes")
        .expect_err("the send wait passes");
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(started.elapsed(), SEND);
}

#[test]
fn writes_are_vectored_as_the_connection_writes_them() {
    for vectored in [true, false] {
        let (_host, pipe) = tokio::io::duplex(64);
        let inner = Held { pipe, vectored };
        assert_eq!(Sending::within(inner, SEND).is_write_vectored(), vectored);
    }
}
