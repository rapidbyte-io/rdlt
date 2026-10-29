//! A stream's frames as they arrive: each head checked against what its sender may send before
//! anything is reserved for its body, then the body read into an allocation of exactly its length.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use super::StreamError;
use super::head::{Bounds, HEAD_BYTES, Head};
use crate::aligned::{Aligned, Frozen};

/// A frame as it arrived: its head, and its body in an allocation of its own from a first byte
/// at a 64-byte address.
#[derive(Debug)]
pub struct Received {
    /// The frame's head.
    pub head: Head,
    /// Its body, and the allocation it lies in.
    pub body: Frozen,
}

/// The frames of one end of a call, read from `io`.
///
/// Each wait keeps what has arrived in the reader, so a read dropped while it waits loses
/// nothing: the next asks for the same frame and goes on where it was.
#[derive(Debug)]
pub struct FrameReader<R> {
    io: R,
    bounds: Bounds,
    /// The bytes of the next head that have arrived.
    bytes: [u8; HEAD_BYTES],
    filled: usize,
    /// The head parsed, while its body has not wholly arrived.
    parsed: Option<Head>,
    /// What has arrived of its body, once asked for.
    body: Option<Aligned>,
    /// Whether the stream ended between frames, or reading it failed: nothing more is read.
    ended: bool,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// The frames `io` carries from an end that sends within `bounds`.
    pub const fn new(io: R, bounds: Bounds) -> Self {
        Self {
            io,
            bounds,
            bytes: [0; HEAD_BYTES],
            filled: 0,
            parsed: None,
            body: None,
            ended: false,
        }
    }

    /// The stream it reads.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.io
    }

    /// Reads the frames after the next whole one within `bounds`, as an end sends them once its
    /// call is open.
    pub fn set_bounds(&mut self, bounds: Bounds) {
        self.bounds = bounds;
    }

    /// The next frame's head, once it has arrived and is within the bounds; the same head again
    /// while its body has not been read; `None` once the stream ends between frames, or after a
    /// failure.
    ///
    /// # Errors
    ///
    /// A [`StreamError`] where the head is refused, the stream ends within it, or reading fails.
    pub async fn head(&mut self) -> Result<Option<Head>, StreamError> {
        std::future::poll_fn(|context| self.poll_head(context)).await
    }

    /// The body of the head [`head`](Self::head) returned last, once all of it has arrived,
    /// read into an allocation of exactly its length; the head is read first where it was not.
    ///
    /// # Errors
    ///
    /// As [`head`](Self::head), and where the stream ends within the body; a stream that ends
    /// between frames fails here, as no frame is there to be read.
    pub async fn body(&mut self) -> Result<Received, StreamError> {
        std::future::poll_fn(|context| self.poll_body(context)).await
    }

    /// The next frame whole, or `None` once the stream ends between frames.
    ///
    /// # Errors
    ///
    /// As [`body`](Self::body).
    pub async fn next(&mut self) -> Result<Option<Received>, StreamError> {
        if self.head().await?.is_none() {
            return Ok(None);
        }
        self.body().await.map(Some)
    }

    fn poll_head(&mut self, context: &mut Context<'_>) -> Poll<Result<Option<Head>, StreamError>> {
        let polled = self.poll_parsed(context);
        self.ended |= matches!(polled, Poll::Ready(Err(_)));
        polled
    }

    fn poll_parsed(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<Option<Head>, StreamError>> {
        while self.parsed.is_none() && !self.ended {
            let mut buffer = ReadBuf::new(&mut self.bytes[self.filled..]);
            std::task::ready!(Pin::new(&mut self.io).poll_read(context, &mut buffer))
                .map_err(StreamError::Io)?;
            let read = buffer.filled().len();
            if read == 0 {
                self.ended = true;
                if self.filled > 0 {
                    return Poll::Ready(Err(truncated(self.filled, HEAD_BYTES)));
                }
                break;
            }
            self.filled += read;
            if self.filled == HEAD_BYTES {
                self.parsed = Some(Head::parse(self.bytes, &self.bounds)?);
            }
        }
        Poll::Ready(Ok(self.parsed.filter(|_| !self.ended)))
    }

    fn poll_body(&mut self, context: &mut Context<'_>) -> Poll<Result<Received, StreamError>> {
        let polled = self.poll_arrived(context);
        self.ended |= matches!(polled, Poll::Ready(Err(_)));
        polled
    }

    fn poll_arrived(&mut self, context: &mut Context<'_>) -> Poll<Result<Received, StreamError>> {
        let Some(head) = std::task::ready!(self.poll_parsed(context))? else {
            return Poll::Ready(Err(truncated(0, HEAD_BYTES)));
        };
        let len = usize::try_from(head.len).unwrap_or(usize::MAX);
        let body = self.body.get_or_insert_with(|| Aligned::with_room(len));
        while body.len() < len {
            let read = std::task::ready!(body.poll_read_from(Pin::new(&mut self.io), context))
                .map_err(StreamError::Io)?;
            if read == 0 {
                return Poll::Ready(Err(truncated(body.len(), len)));
            }
        }
        let body = self.body.take().unwrap_or_default();
        (self.parsed, self.filled) = (None, 0);
        Poll::Ready(Ok(Received {
            head,
            body: body.freeze(),
        }))
    }
}

/// The stream ended `arrived` bytes into something of `expected` bytes.
fn truncated(arrived: usize, expected: usize) -> StreamError {
    StreamError::Truncated {
        arrived: crate::limits::count(arrived),
        expected: crate::limits::count(expected),
    }
}
