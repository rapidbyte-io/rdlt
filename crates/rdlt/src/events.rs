//! A run's events as each subscriber reads them: in order, numbered, and with what it missed
//! told.

use std::pin::Pin;
use std::task::{Context, Poll};

use rdlt_engine::{Event, EventKind};
use serde::Serialize;
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

/// A subscriber fell more than its buffer behind and the events it missed were dropped; the next
/// event's `seq` says where it reads on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, thiserror::Error)]
#[error("a subscriber fell behind and missed {dropped} events")]
#[non_exhaustive]
pub struct Lagged {
    /// The events it missed.
    pub dropped: u64,
}

/// One subscriber's events of a run, ending once the run has ended and its events are read.
///
/// Each subscriber reads at its own place in the run's ring: one that falls behind misses the
/// oldest, is told how many as a [`Lagged`], and carries on, slowing neither the run nor another
/// subscriber.
///
/// ```no_run
/// # async fn example(pipeline: rdlt::Pipeline) {
/// let run = pipeline.start();
/// let mut events = run.events();
/// let telling = tokio::spawn(async move {
///     while let Some(told) = events.next().await {
///         match told {
///             Ok(event) => println!("{} {:?}", event.seq, event.kind),
///             Err(lagged) => eprintln!("missed {} events", lagged.dropped),
///         }
///     }
/// });
/// let outcome = run.await;
/// telling.await.ok();
/// # drop(outcome);
/// # }
/// ```
#[derive(Debug)]
pub struct Events {
    inner: BroadcastStream<Event>,
    ended: bool,
}

impl Events {
    pub(crate) fn new(receiver: broadcast::Receiver<Event>) -> Self {
        Self {
            inner: BroadcastStream::new(receiver),
            ended: false,
        }
    }

    /// The next event, or how many were missed before it; `None` once the run has ended and
    /// every event is read.
    pub async fn next(&mut self) -> Option<Result<Event, Lagged>> {
        std::future::poll_fn(|context| Pin::new(&mut *self).poll_next(context)).await
    }
}

impl Stream for Events {
    type Item = Result<Event, Lagged>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // A run's end is its last event, and a run kept after it keeps the channel open.
        if self.ended {
            return Poll::Ready(None);
        }
        Pin::new(&mut self.inner).poll_next(context).map(|item| {
            let read = item?.map_err(|error| match error {
                BroadcastStreamRecvError::Lagged(dropped) => Lagged { dropped },
            });
            self.ended = read
                .as_ref()
                .is_ok_and(|event| matches!(event.kind, EventKind::RunEnded { .. }));
            Some(read)
        })
    }
}
