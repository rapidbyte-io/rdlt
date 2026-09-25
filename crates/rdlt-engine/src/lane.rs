//! Lanes: long-lived destination writers that stage batches in order.

#[cfg(test)]
mod tests;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::{DestinationWriter, PartitionId, Permit, SchemaVersion, SegmentId, TableRef};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Side};
use crate::table::Tables;

/// A batch for one table, tagged with its segment and the schema version it was lowered for; the
/// reservation drops once it is staged.
pub(crate) struct Write {
    pub(crate) table: usize,
    pub(crate) version: SchemaVersion,
    pub(crate) segment: SegmentId,
    pub(crate) batch: RecordBatch,
    pub(crate) reservation: Permit,
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

/// One lane's end: its queue, a writer for each table and schema version it has written, and
/// which of them it wrote since its last flush.
pub(crate) struct Lane {
    receiver: mpsc::Receiver<Message>,
    tables: Arc<Tables>,
    writers: BTreeMap<(usize, SchemaVersion), Box<dyn DestinationWriter>>,
    written: BTreeSet<(usize, SchemaVersion)>,
}

impl Lanes {
    /// `count` lanes writing into `tables`, each queueing up to `window` writes.
    ///
    /// A lane opens a table's writer when it first writes to the table, since normalized streams
    /// add child tables as their rows arrive.
    pub(crate) fn new(
        count: NonZeroUsize,
        tables: &Arc<Tables>,
        window: NonZeroUsize,
    ) -> (Self, Vec<Lane>) {
        let (senders, lanes) = (0..count.get())
            .map(|_| {
                let (sender, receiver) = mpsc::channel(window.get());
                let lane = Lane {
                    receiver,
                    tables: Arc::clone(tables),
                    writers: BTreeMap::new(),
                    written: BTreeSet::new(),
                };
                (sender, lane)
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
                    self.written.insert((write.table, write.version));
                    let writer = self.writer(write.table, write.version).await?;
                    writer
                        .write(write.segment, write.batch)
                        .await
                        .map_err(|error| {
                            Error::connector(Side::Destination, "writing a batch", error)
                        })?;
                    drop(write.reservation);
                }
                Some(Message::Flush(reply)) => {
                    // A writer written before the last flush holds nothing more to flush.
                    for key in std::mem::take(&mut self.written) {
                        let Some(writer) = self.writers.get_mut(&key) else {
                            continue;
                        };
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

impl Lane {
    /// The lane's writer for `table`'s batches lowered for `version`, opened on the first of them:
    /// a writer's table names the schema its writes follow, and a partition may still write
    /// batches of an older version after the table changes.
    async fn writer(
        &mut self,
        table: usize,
        version: SchemaVersion,
    ) -> Result<&mut Box<dyn DestinationWriter>, Error> {
        match self.writers.entry((table, version)) {
            Entry::Occupied(writer) => Ok(writer.into_mut()),
            Entry::Vacant(vacant) => {
                let view = self.tables.view(table);
                let table = TableRef {
                    version,
                    ..view.table.clone()
                };
                let writer = self
                    .tables
                    .session()
                    .writer(&table)
                    .await?
                    .map_err(|error| {
                        let context = format!("creating a writer for table {}", view.table.name);
                        Error::connector(Side::Destination, context, error)
                    })?;
                Ok(vacant.insert(writer))
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
