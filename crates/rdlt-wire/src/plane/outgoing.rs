//! A call's body of messages as their chunks: each message's head, then its batch's body as the
//! bytes it already is.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use tonic::Status;
use tonic::codegen::http::HeaderMap;
use tonic::codegen::tokio_stream::Stream;

use super::PREFIX;
use super::chained::{Chained, Chunks};

/// Which end sends the body, and so how it ends.
#[derive(Clone, Copy, Debug)]
enum Sending {
    /// A request's: it ends with its last message.
    Request,
    /// An answer's: it ends with trailers saying how the call ended.
    Answer,
}

/// A body sending the messages of a stream, each as its [`Chunks`].
pub struct Outgoing<M> {
    messages: Pin<Box<dyn Stream<Item = Result<M, Status>> + Send>>,
    sending: Sending,
    /// Bytes: the most a message may take, its prefix aside.
    most: usize,
    /// A message's body, sent after its head.
    body: Option<Bytes>,
    ended: bool,
}

impl<M: Chained + Send + 'static> Outgoing<M> {
    /// A request's body of `messages`, each at most `most` bytes, which ends after the last.
    pub fn request(messages: impl Stream<Item = M> + Send + 'static, most: usize) -> Self {
        use tonic::codegen::tokio_stream::StreamExt as _;
        Self::sending(Box::pin(messages.map(Ok)), Sending::Request, most)
    }

    /// An answer's body of `messages`, each at most `most` bytes: trailers end it, carrying the
    /// status the stream failed with where it did, and success where it ended.
    pub fn answer(
        messages: Pin<Box<dyn Stream<Item = Result<M, Status>> + Send>>,
        most: usize,
    ) -> Self {
        Self::sending(messages, Sending::Answer, most)
    }

    fn sending(
        messages: Pin<Box<dyn Stream<Item = Result<M, Status>> + Send>>,
        sending: Sending,
        most: usize,
    ) -> Self {
        Self {
            messages,
            sending,
            most: most.min(u32::MAX as usize),
            body: None,
            ended: false,
        }
    }

    /// What ends the body once its messages have ended, as `failed` where they failed.
    fn end(&mut self, failed: Option<Status>) -> Option<Result<Frame<Bytes>, Status>> {
        self.ended = true;
        match self.sending {
            Sending::Request => failed.map(Err),
            Sending::Answer => {
                let status = failed.unwrap_or_else(|| Status::ok(""));
                let mut trailers = HeaderMap::new();
                Some(
                    status
                        .add_header(&mut trailers)
                        .map(|()| Frame::trailers(trailers)),
                )
            }
        }
    }

    /// The head of `chunks`, its body sent next; a message beyond `most` fails the call.
    fn head(&mut self, chunks: Chunks) -> Option<Result<Frame<Bytes>, Status>> {
        let length = chunks.len().saturating_sub(PREFIX);
        if length > self.most {
            let status = Status::out_of_range(format!(
                "a message of {length} bytes is too large to send, beyond the limit of {} bytes",
                self.most
            ));
            return self.end(Some(status));
        }
        self.body = chunks.body;
        Some(Ok(Frame::data(chunks.head)))
    }
}

impl<M: Chained + Send + 'static> Body for Outgoing<M> {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if let Some(body) = self.body.take() {
            return Poll::Ready(Some(Ok(Frame::data(body))));
        }
        if self.ended {
            return Poll::Ready(None);
        }
        Poll::Ready(
            match std::task::ready!(self.messages.as_mut().poll_next(context)) {
                Some(Ok(message)) => self.head(message.chunks()),
                Some(Err(status)) => self.end(Some(status)),
                None => self.end(None),
            },
        )
    }

    fn is_end_stream(&self) -> bool {
        self.ended && self.body.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

impl<M> std::fmt::Debug for Outgoing<M> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Outgoing")
            .field("sending", &self.sending)
            .field("most", &self.most)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}
