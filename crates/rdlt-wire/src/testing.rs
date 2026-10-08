//! Bodies fed by a test, and gRPC messages of a payload, for the tests of what reads a body.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use tokio::sync::mpsc;
use tonic::Status;

/// A body of the frames, or errors, sent to it, which ends once its sender is dropped.
pub(crate) struct Fed(mpsc::UnboundedReceiver<Result<Frame<Bytes>, Status>>);

impl Body for Fed {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        self.0.poll_recv(context)
    }
}

/// What feeds a body: frames, and an error that fails it.
pub(crate) struct Feed(mpsc::UnboundedSender<Result<Frame<Bytes>, Status>>);

impl Feed {
    /// Sends `frame` to the body, unless it was dropped.
    pub(crate) fn send(&self, frame: Frame<Bytes>) -> Result<(), Dropped> {
        self.0.send(Ok(frame)).map_err(|_| Dropped)
    }

    /// Fails the body with `status`, unless it was dropped.
    pub(crate) fn fail(&self, status: Status) -> Result<(), Dropped> {
        self.0.send(Err(status)).map_err(|_| Dropped)
    }
}

/// The body a feed sends to was dropped.
#[derive(Debug)]
pub(crate) struct Dropped;

/// A body and what feeds it.
pub(crate) fn fed() -> (Feed, tonic::body::Body) {
    let (feed, frames) = mpsc::unbounded_channel();
    (Feed(feed), tonic::body::Body::new(Fed(frames)))
}

/// A body of `chunks`, which then ends.
pub(crate) fn chunks(chunks: &[&[u8]]) -> tonic::body::Body {
    let (feed, body) = fed();
    for chunk in chunks {
        feed.send(Frame::data(Bytes::copy_from_slice(chunk)))
            .expect("the body is held");
    }
    body
}

/// A gRPC message of `payload`.
pub(crate) fn message(payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("a payload of a message");
    let mut message = vec![0];
    message.extend(length.to_be_bytes());
    message.extend(payload);
    message
}
