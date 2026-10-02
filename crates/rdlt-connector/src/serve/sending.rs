//! A connection whose host must keep taking what is sent to it.
//!
//! A host that vanished, its network partitioned and its close lost, or that holds its window
//! shut, takes nothing: once the transport's buffers fill, every write waits, HTTP/2's own pings
//! among them, and nothing else may notice. A write that makes no progress for the connection's
//! send wait fails, which closes the connection.

#[cfg(test)]
mod tests;

use std::future::Future as _;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

/// `inner`, whose writes fail once they have made no progress for its send wait.
pub(super) struct Sending<S> {
    inner: S,
    send: Duration,
    /// When a write waiting since its start fails; none while writes make progress.
    due: Option<Pin<Box<Sleep>>>,
}

impl<S> Sending<S> {
    pub(super) fn within(inner: S, send: Duration) -> Self {
        Self {
            inner,
            send,
            due: None,
        }
    }

    /// `written`, what a write of the connection answered: a wait starts the send wait, or goes
    /// on with it, and anything else ends it.
    fn progress<T>(
        &mut self,
        context: &mut Context<'_>,
        written: Poll<std::io::Result<T>>,
    ) -> Poll<std::io::Result<T>> {
        if written.is_ready() {
            self.due = None;
            return written;
        }
        let send = self.send;
        let due = self
            .due
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(send)));
        if due.as_mut().poll(context).is_ready() {
            self.due = None;
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "the host took none of what was sent to it in time",
            )));
        }
        Poll::Pending
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Sending<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Sending<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write(context, buffer);
        self.progress(context, written)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let flushed = Pin::new(&mut self.inner).poll_flush(context);
        self.progress(context, flushed)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let shut = Pin::new(&mut self.inner).poll_shutdown(context);
        self.progress(context, shut)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let written = Pin::new(&mut self.inner).poll_write_vectored(context, buffers);
        self.progress(context, written)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}
