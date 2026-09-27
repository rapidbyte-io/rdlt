//! A stream whose first bytes were read ahead, and are read again.

#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// `inner`, read from `first` onwards: the bytes read ahead come first.
pub(crate) struct Rewound<S> {
    first: Vec<u8>,
    /// How many of `first` were read again.
    taken: usize,
    inner: S,
}

impl<S> Rewound<S> {
    pub(crate) fn new(first: Vec<u8>, inner: S) -> Self {
        Self {
            first,
            taken: 0,
            inner,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Rewound<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        let left = &this.first[this.taken..];
        if left.is_empty() {
            return Pin::new(&mut this.inner).poll_read(context, buffer);
        }
        let count = left.len().min(buffer.remaining());
        buffer.put_slice(&left[..count]);
        this.taken += count;
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Rewound<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}
