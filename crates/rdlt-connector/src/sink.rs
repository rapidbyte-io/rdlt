//! The channel between one partition's read and the engine.

#[cfg(test)]
mod tests;

use std::any::Any;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::cursor::Cursor;
use crate::error::{ConnectorError, Result};
use crate::spec::BoxFuture;

/// One delivery of data from a source.
#[derive(Clone, Debug, PartialEq)]
pub enum Push {
    /// An Arrow batch in the stream's declared schema.
    Arrow(RecordBatch),
    /// A JSON array of objects, or newline-delimited JSON objects.
    Json(Bytes),
    /// A change batch; see [`validate_change_batch`](crate::validate_change_batch).
    Changes(RecordBatch),
}

/// The severity of a connector log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogLevel {
    /// Something failed.
    Error,
    /// Something is wrong but reading continues.
    Warn,
    /// Progress worth recording.
    Info,
    /// Detail for debugging.
    Debug,
}

/// Everything a partition's read sends to the engine.
#[derive(Clone, Debug, PartialEq)]
pub enum SourceEvent {
    /// Data.
    Push(Push),
    /// A resumable position; it seals everything pushed since the previous checkpoint.
    Checkpoint {
        /// Where to resume.
        cursor: Cursor,
        /// The barrier this checkpoint answers, if one was pending.
        answers: Option<u64>,
    },
    /// A log line.
    Log {
        /// Severity.
        level: LogLevel,
        /// The line.
        message: String,
    },
    /// A metric sample.
    Metric {
        /// The metric's name.
        name: String,
        /// The sample.
        value: f64,
    },
    /// The stream's partitions changed: the engine plans the stream again.
    Replan,
    /// How many records the read is behind its source's newest, as the source measures it.
    Behind {
        /// The records.
        records: u64,
    },
}

/// Holds bytes against a memory budget until it is dropped.
pub type Permit = Box<dyn Any + Send>;

/// Admits events into a partition channel by what they hold, so nothing a source has handed
/// over waits outside the budget of whoever reads the channel.
pub trait Admission: Send + Sync {
    /// Waits until what `event` holds may enter, and returns what holds it; `None` for an event
    /// that holds nothing to charge.
    ///
    /// # Errors
    ///
    /// Why the event may not enter, as an admission that waited as long as it waits: the send
    /// fails with it, and so does the read.
    fn admit<'a>(&'a self, event: &'a SourceEvent) -> BoxFuture<'a, Result<Option<Permit>>>;

    /// Charges `bytes` a read keeps beside its events, at once.
    ///
    /// # Errors
    ///
    /// A [`ConnectorError::exceeds`] naming the limit, where the read would keep more than a
    /// read may: the read fails with it.
    fn charge(&self, bytes: u64) -> Result<Permit>;
}

/// Creates the two ends of one partition's channel, buffering up to `capacity` events.
pub fn partition_channel(capacity: NonZeroUsize) -> (PartitionSink, PartitionFeed) {
    channel(capacity, None)
}

/// Creates a partition channel whose events each wait for `admission` of what they hold before
/// they enter it; the feed hands every event over with its [`Permit`].
pub fn admitted_partition_channel(
    capacity: NonZeroUsize,
    admission: Arc<dyn Admission>,
) -> (PartitionSink, PartitionFeed) {
    channel(capacity, Some(admission))
}

fn channel(
    capacity: NonZeroUsize,
    admission: Option<Arc<dyn Admission>>,
) -> (PartitionSink, PartitionFeed) {
    let (events, receiver) = mpsc::channel(capacity.get());
    let (barrier_sender, barrier) = watch::channel(0);
    let stop = CancellationToken::new();
    let sink = PartitionSink {
        events,
        barrier,
        stop: stop.clone(),
        answered: 0,
        admission,
        cut: false,
    };
    let feed = PartitionFeed {
        events: receiver,
        barrier: barrier_sender,
        stop,
    };
    (sink, feed)
}

/// The connector's end of a partition channel; the SDK wraps it in an
/// [`Emitter`](crate::Emitter).
pub struct PartitionSink {
    events: mpsc::Sender<(SourceEvent, Option<Permit>)>,
    barrier: watch::Receiver<u64>,
    stop: CancellationToken,
    answered: u64,
    admission: Option<Arc<dyn Admission>>,
    /// Whether what receives a batch sent here cuts it before anything holds it.
    cut: bool,
}

impl fmt::Debug for PartitionSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionSink")
            .field("answered", &self.answered)
            .field("admitted", &self.admission.is_some())
            .finish_non_exhaustive()
    }
}

impl PartitionSink {
    /// The sink of a read whose batches are cut a frame at a time before anything else holds
    /// them, as a served read's are: a batch sent here need not fit a frame.
    #[cfg(any(test, feature = "serve"))]
    pub(crate) fn cut(mut self) -> Self {
        self.cut = true;
        self
    }

    /// Whether a batch sent here is held as it is, and so must be within what a frame may hold.
    pub(crate) fn holds_whole(&self) -> bool {
        !self.cut
    }

    /// Resolves once the engine asks the read to stop or drops its end.
    pub async fn stopped(&self) {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => {}
            () = self.events.closed() => {}
        }
    }

    /// Sends `event`, waiting while the channel is full; a read that runs elsewhere and forwards
    /// its events sends them here.
    ///
    /// # Errors
    ///
    /// A [`Stopped`](crate::ConnectorErrorKind::Stopped) error once the engine has asked the read
    /// to stop or dropped its end, and an `Internal` error coded `barrier_unrequested` for a
    /// checkpoint answering a barrier the engine never asked for. Where the channel's events are
    /// admitted, the error its admission refused the event with.
    pub async fn send(&mut self, event: SourceEvent) -> Result<()> {
        if let SourceEvent::Checkpoint {
            answers: Some(barrier),
            ..
        } = &event
        {
            let requested = *self.barrier.borrow();
            if *barrier > requested {
                return Err(ConnectorError::internal(format!(
                    "a checkpoint answers barrier {barrier}, past the newest the engine asked for, \
                     {requested}"
                ))
                .with_code("barrier_unrequested"));
            }
            self.answered = self.answered.max(*barrier);
        }
        let permit = match &self.admission {
            Some(admission) => tokio::select! {
                biased;
                // A stop request wins over an admission that could still arrive.
                () = self.stop.cancelled() => return Err(ConnectorError::stopped()),
                permit = admission.admit(&event) => permit?,
            },
            None => None,
        };
        tokio::select! {
            biased;
            // A stop request wins over a send that could still complete.
            () = self.stop.cancelled() => Err(ConnectorError::stopped()),
            sent = self.events.send((event, permit)) => sent.map_err(|_| ConnectorError::stopped()),
        }
    }

    /// The engine's next request of the read: a checkpoint answering a barrier newer than
    /// `forwarded` and than any checkpoint sent, or to stop; for a read that runs elsewhere and is
    /// forwarded the engine's requests.
    pub async fn requested(&mut self, forwarded: u64) -> Requested {
        loop {
            if self.stop.is_cancelled() {
                return Requested::Stop;
            }
            let barrier = *self.barrier.borrow_and_update();
            if barrier > forwarded.max(self.answered) {
                return Requested::Checkpoint(barrier);
            }
            tokio::select! {
                biased;
                () = self.stop.cancelled() => return Requested::Stop,
                changed = self.barrier.changed() => {
                    if changed.is_err() {
                        return Requested::Stop;
                    }
                }
            }
        }
    }

    /// Charges `bytes` the read keeps beside its events, as a decoder's dictionaries, to whoever
    /// admits the channel's events.
    ///
    /// The bytes are held until the permit is dropped; a channel nothing admits returns `None`.
    ///
    /// # Errors
    ///
    /// A [`ConnectorError::exceeds`] naming the limit, where the read would keep more than
    /// whoever admits its events lets a read keep.
    pub fn reserve(&self, bytes: u64) -> Result<Option<Permit>> {
        self.admission
            .as_ref()
            .map(|admission| admission.charge(bytes))
            .transpose()
    }

    /// The newest barrier no checkpoint has answered yet.
    pub fn pending_barrier(&self) -> Option<u64> {
        let requested = *self.barrier.borrow();
        (requested > self.answered).then_some(requested)
    }
}

/// What the engine asks of a partition's read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Requested {
    /// A checkpoint answering this barrier.
    Checkpoint(u64),
    /// To stop reading.
    Stop,
}

/// The engine's end of a partition channel.
#[derive(Debug)]
pub struct PartitionFeed {
    events: mpsc::Receiver<(SourceEvent, Option<Permit>)>,
    barrier: watch::Sender<u64>,
    stop: CancellationToken,
}

impl PartitionFeed {
    /// The next event, or `None` once the read has finished and every event was received; a
    /// push's permit, if it has one, is released here.
    pub async fn recv(&mut self) -> Option<SourceEvent> {
        self.recv_admitted().await.map(|(event, _)| event)
    }

    /// The next event with the permit that admitted it, for an event an admitted channel charged.
    pub async fn recv_admitted(&mut self) -> Option<(SourceEvent, Option<Permit>)> {
        self.events.recv().await
    }

    /// Asks the partition to checkpoint at its next safe point; barriers only move forward.
    pub fn request_checkpoint(&self, barrier: u64) {
        self.barrier
            .send_modify(|current| *current = (*current).max(barrier));
    }

    /// Asks the read to stop; its next emit fails with a stopped error.
    pub fn stop(&self) {
        self.stop.cancel();
    }
}
