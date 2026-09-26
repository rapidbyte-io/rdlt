//! A stream that ends early once its connection is stopping: a host's heartbeat stream lasts as
//! long as the host, and would hold a graceful stop open.

#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::task::{Context, Poll};

use rdlt_wire::tonic::codegen::tokio_stream::Stream;
use tokio_util::sync::WaitForCancellationFutureOwned;

/// `stream`, ended once `stopped` completes.
pub(super) struct Until<S> {
    stream: Pin<Box<S>>,
    stopped: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl<S> Until<S> {
    pub(super) fn new(stream: S, stopped: WaitForCancellationFutureOwned) -> Self {
        Self {
            stream: Box::pin(stream),
            stopped: Box::pin(stopped),
        }
    }
}

impl<S: Stream> Stream for Until<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<S::Item>> {
        if self.stopped.as_mut().poll(context).is_ready() {
            return Poll::Ready(None);
        }
        self.stream.as_mut().poll_next(context)
    }
}
