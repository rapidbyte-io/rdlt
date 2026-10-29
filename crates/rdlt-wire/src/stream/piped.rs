//! A data call's stream over HTTP/2: one call of the control connection, whose request and
//! answer bodies carry the stream's bytes, each end reading the body the other sends and writing
//! its own.
//!
//! HTTP/2 holds what is sent past the write that sent it, so each write is copied into a chunk of
//! the body, at most a transport frame's bytes: nothing a frame lends the stream outlives the
//! write, and a call holds at most a chunk waiting beside the chunk HTTP/2 sends.

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf as _, Bytes, BytesMut};
use http_body::{Body, Frame};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;

/// The path of the call a data stream rides over HTTP/2.
pub const DATA: &str = "/rdlt.connector.v1.Connector/Data";

/// One end of a data stream over an HTTP/2 call: its reads take the body the other end sends,
/// its writes become the body it sends.
pub struct Piped<B> {
    incoming: Pin<Box<B>>,
    /// What arrived of the incoming body and is not yet read.
    unread: Bytes,
    ended: bool,
    outgoing: PollSender<Bytes>,
}

impl<B> std::fmt::Debug for Piped<B> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Piped")
            .field("unread", &self.unread.len())
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

/// The body one end of a data stream sends: what its writes copied, in order, ending once its
/// end shuts down or is dropped.
#[derive(Debug)]
pub struct Sent {
    chunks: mpsc::Receiver<Bytes>,
}

impl Body for Sent {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.chunks
            .poll_recv(context)
            .map(|chunk| chunk.map(|chunk| Ok(Frame::data(chunk))))
    }
}

/// What one end of a data stream over HTTP/2 writes, before it has the body it reads: a host's,
/// whose request's body goes before its answer comes.
#[derive(Debug)]
pub struct Pipe {
    outgoing: PollSender<Bytes>,
}

impl Pipe {
    /// The end, reading `incoming`.
    pub fn reading<B>(self, incoming: B) -> Piped<B> {
        Piped {
            incoming: Box::pin(incoming),
            unread: Bytes::new(),
            ended: false,
            outgoing: self.outgoing,
        }
    }
}

/// What one end of a data stream over HTTP/2 writes, and the body it sends.
pub fn pipe() -> (Pipe, Sent) {
    // One chunk waits for HTTP/2 at a time: its stream's window paces the rest.
    let (sender, chunks) = mpsc::channel(1);
    let pipe = Pipe {
        outgoing: PollSender::new(sender),
    };
    (pipe, Sent { chunks })
}

/// One end of a data stream over HTTP/2 reading `incoming`, and the body it sends.
pub fn piped<B>(incoming: B) -> (Piped<B>, Sent) {
    let (pipe, sent) = pipe();
    (pipe.reading(incoming), sent)
}

impl<B> AsyncRead for Piped<B>
where
    B: Body<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        while self.unread.is_empty() && !self.ended {
            match std::task::ready!(self.incoming.as_mut().poll_frame(context)) {
                None => self.ended = true,
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        self.unread = data;
                    }
                }
                Some(Err(error)) => {
                    self.ended = true;
                    let failed = format!("the stream's call failed: {error}");
                    return Poll::Ready(Err(std::io::Error::other(failed)));
                }
            }
        }
        let taken = self.unread.len().min(buffer.remaining());
        buffer.put_slice(&self.unread[..taken]);
        self.unread.advance(taken);
        Poll::Ready(Ok(()))
    }
}

/// Bytes: the most one chunk of a body holds, a transport frame's.
const CHUNK: usize = crate::limits::TRANSPORT_FRAME_BYTES as usize;

impl<B> Piped<B> {
    /// Sends the first [`CHUNK`] bytes at most of `buffers` as one chunk, once the body has room
    /// for it; how many it sent.
    fn poll_send(
        &mut self,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let gone = || std::io::Error::from(std::io::ErrorKind::BrokenPipe);
        std::task::ready!(self.outgoing.poll_reserve(context)).map_err(|_| gone())?;
        let len = buffers
            .iter()
            .map(|buffer| buffer.len())
            .sum::<usize>()
            .min(CHUNK);
        let mut chunk = BytesMut::with_capacity(len);
        for buffer in buffers {
            let room = len - chunk.len();
            if room == 0 {
                break;
            }
            chunk.extend_from_slice(&buffer[..buffer.len().min(room)]);
        }
        self.outgoing
            .send_item(chunk.freeze())
            .map_err(|_| gone())?;
        Poll::Ready(Ok(len))
    }
}

impl<B> AsyncWrite for Piped<B> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.poll_send(context, &[std::io::IoSlice::new(buffer)])
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        self.poll_send(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.outgoing.close();
        Poll::Ready(Ok(()))
    }
}
