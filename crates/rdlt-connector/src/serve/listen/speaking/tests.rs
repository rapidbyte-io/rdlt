use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, DuplexStream};

use super::{PREFACE, Speaking};

const WITHIN: Duration = Duration::from_secs(10);

#[tokio::test(start_paused = true)]
async fn a_host_that_sends_part_of_its_preface_in_time_fails_to_read_once_the_time_passes() {
    let (mut host, inner) = tokio::io::duplex(64);
    let mut speaking = Speaking::within(inner, WITHIN);
    host.write_all(&[b'P'; PREFACE - 1])
        .await
        .expect("the host writes");
    // Into a buffer part filled already, as the protocol's reads may be: only what arrives counts.
    let mut storage = [0; 2 * PREFACE];
    let mut buffer = tokio::io::ReadBuf::new(&mut storage);
    buffer.put_slice(&[b'x'; PREFACE / 2]);
    std::future::poll_fn(|context| Pin::new(&mut speaking).poll_read(context, &mut buffer))
        .await
        .expect("what arrived reads");
    assert_eq!(buffer.filled().len(), PREFACE / 2 + PREFACE - 1);
    let mut rest = [0; PREFACE];
    let failed = speaking.read(&mut rest).await.expect_err("the time passes");
    assert_eq!(failed.kind(), std::io::ErrorKind::TimedOut);
}

#[tokio::test(start_paused = true)]
async fn a_host_that_sent_its_preface_in_pieces_may_then_go_quiet() {
    let (mut host, inner) = tokio::io::duplex(64);
    let mut speaking = Speaking::within(inner, WITHIN);
    let mut buffer = [0; PREFACE];
    for piece in [&[b'P'; 10][..], &[b'P'; PREFACE - 10][..]] {
        host.write_all(piece).await.expect("the host writes");
        let read = speaking.read(&mut buffer).await.expect("the piece reads");
        assert_eq!(read, piece.len());
    }
    let late = async {
        tokio::time::sleep(WITHIN * 3).await;
        host.write_all(b"frames").await.expect("the host writes");
        host
    };
    let (read, mut host) = tokio::join!(speaking.read(&mut buffer), late);
    assert_eq!(read.expect("a quiet host is not dropped"), 6);
    speaking
        .write_all(b"answer")
        .await
        .expect("the connector writes");
    let written = std::io::IoSlice::new(b" more");
    assert_eq!(
        speaking
            .write_vectored(&[written])
            .await
            .expect("the connector writes"),
        5
    );
    speaking.shutdown().await.expect("the connector shuts down");
    let mut answered = Vec::new();
    host.read_to_end(&mut answered)
        .await
        .expect("the host reads to the end");
    assert_eq!(answered, b"answer more");
}

/// A pipe that says whether it writes vectored, and counts its flushes.
struct Counted {
    pipe: DuplexStream,
    vectored: bool,
    flushes: u32,
}

impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.pipe).poll_read(context, buffer)
    }
}

impl AsyncWrite for Counted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.pipe).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.flushes += 1;
        Pin::new(&mut self.pipe).poll_flush(context)
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

#[tokio::test]
async fn writes_are_vectored_and_flushed_as_the_connection_writes_them() {
    for vectored in [true, false] {
        let (_host, pipe) = tokio::io::duplex(64);
        let inner = Counted {
            pipe,
            vectored,
            flushes: 0,
        };
        let mut speaking = Speaking::within(inner, WITHIN);
        assert_eq!(speaking.is_write_vectored(), vectored);
        speaking.flush().await.expect("the connector flushes");
        assert_eq!(speaking.inner.flushes, 1);
    }
}
