//! Raw connections to connectors, before any handshake, for clients that speak the protocol
//! themselves, as a certification suite does.

#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::local::process::Process;
use crate::network::Stream;

/// A raw connection to a connector, before its handshake: a stream whose other end serves the
/// protocol, and the connector's process when this process spawned it, which stops once the
/// wire is dropped.
pub struct Wire {
    stream: Box<dyn Stream>,
    process: Option<Process>,
}

impl std::fmt::Debug for Wire {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Wire")
            .field("spawned", &self.process.is_some())
            .finish_non_exhaustive()
    }
}

impl Wire {
    pub(crate) fn new(stream: Box<dyn Stream>, process: Option<Process>) -> Self {
        Self { stream, process }
    }
}

impl AsyncRead for Wire {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for Wire {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}
