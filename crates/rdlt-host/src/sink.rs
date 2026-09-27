//! A stream for testing the streams that wrap one.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite};

/// A stream that takes every write whole, vectored or not, says whether it writes vectored, and
/// counts its flushes and shutdowns.
pub(crate) struct Sink {
    pub(crate) written: Vec<u8>,
    vectored: bool,
    pub(crate) flushes: u32,
    pub(crate) shutdowns: u32,
}

impl Sink {
    /// An empty sink, writing vectored when `vectored` says so.
    pub(crate) fn new(vectored: bool) -> Self {
        Self {
            written: Vec::new(),
            vectored,
            flushes: 0,
            shutdowns: 0,
        }
    }
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
