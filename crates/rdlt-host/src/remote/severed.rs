//! A connection's transport, which ends with the connection: once it is dropped, or its connector
//! lost, reads and writes fail at once, so HTTP/2's task for it ends then, rather than when its
//! pings time out, which on a network that stopped answering takes the heartbeat's patience.

use std::future::Future as _;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// `io`, cut once `cut` is cancelled.
pub(super) struct Severed<IO> {
    io: IO,
    cut: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl<IO> Severed<IO> {
    pub(super) fn new(io: IO, cut: CancellationToken) -> Self {
        Self {
            io,
            cut: Box::pin(cut.cancelled_owned()),
        }
    }

    /// Whether the connection has ended; waits to wake the task that asks, when not.
    fn ended(&mut self, context: &mut Context<'_>) -> bool {
        self.cut.as_mut().poll(context).is_ready()
    }
}

fn severed<T>() -> Poll<std::io::Result<T>> {
    Poll::Ready(Err(std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        "the connection ended",
    )))
}

impl<IO: AsyncRead + Unpin> AsyncRead for Severed<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.ended(context) {
            return severed();
        }
        Pin::new(&mut self.io).poll_read(context, buffer)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Severed<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.ended(context) {
            return severed();
        }
        Pin::new(&mut self.io).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.ended(context) {
            return severed();
        }
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.ended(context) {
            return severed();
        }
        Pin::new(&mut self.io).poll_shutdown(context)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        if self.ended(context) {
            return severed();
        }
        Pin::new(&mut self.io).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
}
