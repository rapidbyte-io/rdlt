//! The host's end of a connection a kill is to end: once the kill has come and the connection is
//! seen to end, the kill has landed.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::CancellationToken;

use super::Kills;

/// `io`, which counts a kill as landed, once, when it ends after the kill it awaits has come.
///
/// A connector's socket ends once every process that held its other end is gone, so its end is
/// what shows a kill reached all of them.
pub(crate) struct Landing<IO> {
    io: IO,
    /// Cancelled by the kill this connection awaits.
    killed: CancellationToken,
    /// What counts the kill as landed, until it has.
    kills: Option<Kills>,
    /// Whether the kill cuts this connection itself, rather than what holds its other end.
    cut: bool,
}

impl<IO> Landing<IO> {
    pub(crate) fn new(io: IO, killed: CancellationToken, kills: Kills, cut: bool) -> Self {
        Self {
            io,
            killed,
            kills: Some(kills),
            cut,
        }
    }

    /// Counts the kill as landed when the connection `ended` and the kill has come: a
    /// connection ends once, so one that ended before its kill never lands it.
    fn seen(&mut self, ended: bool) {
        if !ended {
            return;
        }
        if let Some(kills) = self.kills.take()
            && self.killed.is_cancelled()
        {
            kills.land(self.cut);
        }
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for Landing<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let (filled, room) = (buffer.filled().len(), buffer.remaining());
        let read = Pin::new(&mut self.io).poll_read(context, buffer);
        // A read that fills nothing of a buffer with room is the stream's end.
        let ended = match &read {
            Poll::Ready(Ok(())) => room > 0 && buffer.filled().len() == filled,
            Poll::Ready(Err(_)) => true,
            Poll::Pending => false,
        };
        self.seen(ended);
        read
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Landing<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let written = Pin::new(&mut self.io).poll_write(context, buffer);
        self.seen(matches!(written, Poll::Ready(Err(_))));
        written
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let flushed = Pin::new(&mut self.io).poll_flush(context);
        self.seen(matches!(flushed, Poll::Ready(Err(_))));
        flushed
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(context)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let written = Pin::new(&mut self.io).poll_write_vectored(context, buffers);
        self.seen(matches!(written, Poll::Ready(Err(_))));
        written
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
}
