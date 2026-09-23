//! The channel between one partition's read and the engine.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;

use arrow_array::RecordBatch;
use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::cursor::Cursor;
use crate::error::{ConnectorError, Result};

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
}

/// Creates the two ends of one partition's channel, buffering up to `capacity` events.
pub fn partition_channel(capacity: NonZeroUsize) -> (PartitionSink, PartitionFeed) {
    let (events, receiver) = mpsc::channel(capacity.get());
    let (barrier_sender, barrier) = watch::channel(0);
    let stop = CancellationToken::new();
    let sink = PartitionSink {
        events,
        barrier,
        stop: stop.clone(),
        answered: 0,
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
#[derive(Debug)]
pub struct PartitionSink {
    events: mpsc::Sender<SourceEvent>,
    barrier: watch::Receiver<u64>,
    stop: CancellationToken,
    answered: u64,
}

impl PartitionSink {
    /// Sends `event`, waiting while the channel is full.
    ///
    /// Fails with a [`Stopped`](crate::ConnectorErrorKind::Stopped) error once the engine has asked
    /// the read to stop or dropped its end.
    pub(crate) async fn send(&mut self, event: SourceEvent) -> Result<()> {
        if let SourceEvent::Checkpoint {
            answers: Some(barrier),
            ..
        } = &event
        {
            self.answered = *barrier;
        }
        tokio::select! {
            biased;
            // A stop request wins over a send that could still complete.
            () = self.stop.cancelled() => Err(ConnectorError::stopped()),
            sent = self.events.send(event) => sent.map_err(|_| ConnectorError::stopped()),
        }
    }

    /// The newest barrier no checkpoint has answered yet.
    pub(crate) fn pending_barrier(&self) -> Option<u64> {
        let requested = *self.barrier.borrow();
        (requested > self.answered).then_some(requested)
    }
}

/// The engine's end of a partition channel.
#[derive(Debug)]
pub struct PartitionFeed {
    events: mpsc::Receiver<SourceEvent>,
    barrier: watch::Sender<u64>,
    stop: CancellationToken,
}

impl PartitionFeed {
    /// The next event, or `None` once the read has finished and every event was received.
    pub async fn recv(&mut self) -> Option<SourceEvent> {
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
