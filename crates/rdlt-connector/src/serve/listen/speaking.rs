//! A connection its host must begin HTTP/2 on in time.
//!
//! HTTP/2's server waits for the host's preface without a deadline, and pings no host that has
//! not sent all of it: a host that completed its TLS handshake and then went silent, its network
//! dropped or its process gone, would hold its connection, and its session, forever.

#[cfg(test)]
mod tests;

use std::future::Future as _;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

/// The length of HTTP/2's connection preface, which a host sends first (RFC 9113 §3.4).
const PREFACE: usize = 24;

/// `inner`, whose reads fail once `within` passes before the host's preface has arrived.
pub(super) struct Speaking<S> {
    inner: S,
    /// When the host must have sent its preface; none once it has.
    deadline: Option<Pin<Box<Sleep>>>,
    /// How much of the preface has arrived.
    read: usize,
}

impl<S> Speaking<S> {
    pub(super) fn within(inner: S, within: Duration) -> Self {
        Self {
            inner,
            deadline: Some(Box::pin(tokio::time::sleep(within))),
            read: 0,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Speaking<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        let before = buffer.filled().len();
        let read = Pin::new(&mut this.inner).poll_read(context, buffer);
        let Some(deadline) = &mut this.deadline else {
            return read;
        };
        match read {
            Poll::Ready(Ok(())) => {
                this.read += buffer.filled().len() - before;
                if this.read >= PREFACE {
                    this.deadline = None;
                }
            }
            Poll::Pending if deadline.as_mut().poll(context).is_ready() => {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "the host did not send HTTP/2's preface in time",
                )));
            }
            Poll::Pending | Poll::Ready(Err(_)) => {}
        }
        read
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Speaking<S> {
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
