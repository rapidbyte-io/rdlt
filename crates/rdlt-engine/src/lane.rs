//! Lanes: long-lived destination writers that stage batches in order.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;

use arrow_array::RecordBatch;
use rdlt_connector::{DestinationWriter, PartitionId, SegmentId};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::budget::Reservation;
use crate::error::{Error, Side};

/// A batch for one table, tagged with its segment; the reservation drops once it is staged.
pub(crate) struct Write {
    pub(crate) table: usize,
    pub(crate) segment: SegmentId,
    pub(crate) batch: RecordBatch,
    pub(crate) reservation: Reservation,
}

enum Message {
    Write(Write),
    Flush(oneshot::Sender<()>),
}

/// The senders of every lane of an attempt.
#[derive(Clone)]
pub(crate) struct Lanes {
    senders: Vec<mpsc::Sender<Message>>,
}

/// One lane's end: its queue and its writers, one per table.
pub(crate) struct Lane {
    receiver: mpsc::Receiver<Message>,
    writers: Vec<Box<dyn DestinationWriter>>,
}

impl Lanes {
    /// Lanes over `writers`, one writer per table in each, each queueing up to `window` writes.
    pub(crate) fn new(
        writers: Vec<Vec<Box<dyn DestinationWriter>>>,
        window: NonZeroUsize,
    ) -> (Self, Vec<Lane>) {
        let (senders, lanes) = writers
            .into_iter()
            .map(|writers| {
                let (sender, receiver) = mpsc::channel(window.get());
                (sender, Lane { receiver, writers })
            })
            .unzip();
        (Self { senders }, lanes)
    }

    /// The lane for `partition`'s writes to `table`, so they stay in order on one writer.
    pub(crate) fn route(&self, table: usize, partition: &PartitionId) -> usize {
        let mut hash = fnv(0xcbf2_9ce4_8422_2325, &table.to_le_bytes());
        hash = fnv(hash, partition.as_str().as_bytes());
        let lanes = u64::try_from(self.senders.len()).unwrap_or(u64::MAX);
        usize::try_from(hash % lanes).unwrap_or(0)
    }

    /// Queues `write` on `lane`, waiting while the lane's queue is full.
    pub(crate) async fn write(&self, lane: usize, write: Write) -> Result<(), Error> {
        self.senders[lane]
            .send(Message::Write(write))
            .await
            .map_err(|_| stopped())
    }

    /// Waits until every lane has staged and flushed every write queued before this call.
    pub(crate) async fn flush(&self) -> Result<(), Error> {
        let mut replies = Vec::with_capacity(self.senders.len());
        for sender in &self.senders {
            let (reply, done) = oneshot::channel();
            sender
                .send(Message::Flush(reply))
                .await
                .map_err(|_| stopped())?;
            replies.push(done);
        }
        for done in replies {
            done.await.map_err(|_| stopped())?;
        }
        Ok(())
    }
}

/// A lane that ended; the error that ended it is reported by the lane itself.
fn stopped() -> Error {
    Error::cancelled("a lane stopped")
}

impl Lane {
    /// Stages queued writes until every sender is dropped or `cancel` fires.
    pub(crate) async fn run(mut self, cancel: CancellationToken) -> Result<(), Error> {
        loop {
            let message = tokio::select! {
                biased;
                // Cancellation wins: the attempt is ending and its writes will be discarded.
                () = cancel.cancelled() => return Err(Error::cancelled("the attempt was cancelled")),
                message = self.receiver.recv() => message,
            };
            match message {
                Some(Message::Write(write)) => {
                    let writer = &mut self.writers[write.table];
                    writer
                        .write(write.segment, write.batch)
                        .await
                        .map_err(|error| {
                            Error::connector(Side::Destination, "writing a batch", error)
                        })?;
                    drop(write.reservation);
                }
                Some(Message::Flush(reply)) => {
                    for writer in &mut self.writers {
                        writer.flush().await.map_err(|error| {
                            Error::connector(Side::Destination, "flushing staged writes", error)
                        })?;
                    }
                    // The coordinator may have stopped waiting; the flush happened either way.
                    reply.send(()).ok();
                }
                None => return Ok(()),
            }
        }
    }
}

/// One step of the 64-bit FNV-1a hash, so routing is the same on every run and platform.
fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}
