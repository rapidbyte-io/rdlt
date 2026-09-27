use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use super::Rewound;

#[tokio::test]
async fn the_bytes_read_ahead_are_read_again_first_however_small_the_reads() {
    let (mut peer, inner) = tokio::io::duplex(64);
    peer.write_all(b" and the rest")
        .await
        .expect("the peer writes");
    drop(peer);
    let mut rewound = Rewound::new(b"first bytes".to_vec(), inner);
    let mut read = Vec::new();
    let mut piece = [0; 3];
    loop {
        let count = rewound.read(&mut piece).await.expect("the stream reads");
        if count == 0 {
            break;
        }
        read.extend_from_slice(&piece[..count]);
    }
    assert_eq!(read, b"first bytes and the rest");
}

/// A stream that takes every write whole, vectored or not, says whether it writes vectored, and
/// counts its flushes and shutdowns.
struct Sink {
    written: Vec<u8>,
    vectored: bool,
    flushes: u32,
    shutdowns: u32,
}

impl AsyncRead for Sink {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        _buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Sink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.written.extend_from_slice(buffer);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.flushes += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdowns += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let mut count = 0;
        for buffer in buffers {
            self.written.extend_from_slice(buffer);
            count += buffer.len();
        }
        Poll::Ready(Ok(count))
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }
}

#[tokio::test]
async fn writes_reach_the_stream_vectored_as_it_writes_them() {
    for vectored in [true, false] {
        let mut rewound = Rewound::new(
            Vec::new(),
            Sink {
                written: Vec::new(),
                vectored,
                flushes: 0,
                shutdowns: 0,
            },
        );
        assert_eq!(rewound.is_write_vectored(), vectored);
        let buffers = [
            std::io::IoSlice::new(b"one "),
            std::io::IoSlice::new(b"two"),
        ];
        let written = rewound
            .write_vectored(&buffers)
            .await
            .expect("the stream writes");
        rewound
            .write_all(b" three")
            .await
            .expect("the stream writes");
        rewound.flush().await.expect("the stream flushes");
        rewound.shutdown().await.expect("the stream shuts down");
        assert_eq!(written, 7);
        assert_eq!(rewound.inner.written, b"one two three");
        assert_eq!((rewound.inner.flushes, rewound.inner.shutdowns), (1, 1));
    }
}
