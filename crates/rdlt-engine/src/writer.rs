//! Writers: long-lived tasks that stage batches into the destination in order.

#[cfg(test)]
mod tests;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use rdlt_connector::{DestinationWriter, Permit, SchemaVersion, SegmentId, TableRef};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::budget::MemoryBudget;
use crate::env::Env;
use crate::error::{Error, Side};
use crate::report::{Flush, Tally, WriterCounters};
use crate::status::{Call, WriterCalls};
use crate::table::Tables;

/// A batch for one table, tagged with its segment and the schema version it was lowered for; the
/// reservation drops once its table writer has flushed it, since a table writer may buffer what it
/// stages.
///
/// The batch is declared before its reservation: dropped, it is freed before what was charged for
/// it is released.
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

/// The senders of every writer of an attempt.
#[derive(Clone)]
pub(crate) struct Writers {
    senders: Vec<mpsc::Sender<Message>>,
    /// The clock a write waiting for room in a writer's queue is timed on.
    env: Arc<dyn Env>,
    /// Where each writer's waits and times are counted.
    tally: Arc<Tally>,
    /// Each writer's call into the destination in flight.
    calls: Arc<WriterCalls>,
}

/// One writer's end: its queue, a table writer for each table and schema version it writes, which
/// of them it wrote since its last flush, and the reservations of those writes.
///
/// A writer holds at most `share` table writers open, and a table writer of a version
/// older than one it writes is retired: each is a call into a served destination. What it times is
/// added to the run's tally once, when the writer is dropped, so a cancelled writer adds what it
/// counted.
pub(crate) struct Writer {
    receiver: mpsc::Receiver<Message>,
    tables: Arc<Tables>,
    table_writers: BTreeMap<(usize, SchemaVersion), Open>,
    written: BTreeSet<(usize, SchemaVersion)>,
    held: Vec<Permit>,
    budget: MemoryBudget,
    /// The most table writers the writer holds open.
    share: NonZeroUsize,
    /// Counts the writer's writes, so the table writer written longest ago is found.
    writes: u64,
    /// The writer's place among the attempt's writers.
    index: usize,
    /// The clock the writer's writes and flushes are timed on.
    env: Arc<dyn Env>,
    /// What the writer's writes and flushes took so far.
    counted: WriterCounters,
    /// Where the writer adds what it counted.
    tally: Arc<Tally>,
    /// Where the writer notes its call into the destination in flight.
    calls: Arc<WriterCalls>,
}

/// An open table writer, and the count of the writer's writes when it was last written.
struct Open {
    table_writer: Box<dyn DestinationWriter>,
    used: u64,
}

impl Writers {
    /// `count` writers writing into `tables`, each queueing up to `window` writes and holding an
    /// equal share of `table_writers` open, one at least.
    ///
    /// A writer opens a table writer for a table when it first writes to it, since normalized
    /// streams add child tables as their rows arrive. Each writer's waits and times are counted
    /// into `tally`, on `env`'s clock.
    pub(crate) fn new(
        (count, table_writers): (NonZeroUsize, NonZeroUsize),
        tables: &Arc<Tables>,
        window: NonZeroUsize,
        budget: &MemoryBudget,
        (env, tally): (&Arc<dyn Env>, &Arc<Tally>),
    ) -> (Self, Vec<Writer>) {
        let share =
            NonZeroUsize::new(table_writers.get() / count.get()).unwrap_or(NonZeroUsize::MIN);
        let calls = Arc::new(WriterCalls::new(env.instant(), count.get()));
        let (senders, writers) = (0..count.get())
            .map(|index| {
                let (sender, receiver) = mpsc::channel(window.get());
                let writer = Writer {
                    receiver,
                    tables: Arc::clone(tables),
                    table_writers: BTreeMap::new(),
                    written: BTreeSet::new(),
                    held: Vec::new(),
                    budget: budget.clone(),
                    share,
                    writes: 0,
                    index,
                    env: Arc::clone(env),
                    counted: WriterCounters::default(),
                    tally: Arc::clone(tally),
                    calls: Arc::clone(&calls),
                };
                (sender, writer)
            })
            .unzip();
        let ends = Self {
            senders,
            env: Arc::clone(env),
            tally: Arc::clone(tally),
            calls,
        };
        (ends, writers)
    }

    /// Each writer's call into the destination in flight.
    pub(crate) fn calls(&self) -> &Arc<WriterCalls> {
        &self.calls
    }

    /// The writer for the writes to `table` of the partition at `place` in the attempt, so places
    /// up to the writers take a writer each; one writer a partition and table, since spreading a
    /// partition's writes over more writers gains nothing (ADR 0004), and no order rests on it.
    pub(crate) fn route(&self, table: usize, place: usize) -> usize {
        place.wrapping_add(table) % self.senders.len()
    }

    /// Queues `write` on `writer`, waiting while the writer's queue is full; the wait is counted as
    /// the writer's.
    pub(crate) async fn write(&self, writer: usize, write: Write) -> Result<(), Error> {
        let sender = &self.senders[writer];
        let message = match sender.try_send(Message::Write(write)) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(message)) => message,
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(stopped()),
        };
        let began = self.env.instant();
        let sent = sender.send(message).await;
        let blocked = self.env.instant().saturating_duration_since(began);
        self.tally.add(|counters| {
            let writer = counters.writer(writer);
            writer.blocked = writer.blocked.saturating_add(blocked);
        });
        sent.map_err(|_| stopped())
    }

    /// Waits until every writer has staged and flushed every write queued before this call.
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

/// A writer that ended; the error that ended it is reported by the writer itself.
fn stopped() -> Error {
    Error::cancelled("a writer stopped")
}

impl Writer {
    /// Stages queued writes until every sender is dropped or `cancel` fires.
    ///
    /// A writer flushes its table writers when the coordinator asks, and whenever a request waits
    /// for bytes while the writer holds writes it has not flushed, which frees them.
    pub(crate) async fn run(mut self, cancel: CancellationToken) -> Result<(), Error> {
        let mut pressure = self.budget.pressure();
        loop {
            let message = tokio::select! {
                biased;
                // Cancellation wins: the attempt is ending and its writes will be discarded.
                () = cancel.cancelled() => return Err(Error::cancelled("the attempt was cancelled")),
                // Pressure comes before more writes, so the bytes it waits for are freed first.
                () = pressure.raised(), if !self.held.is_empty() => {
                    until_cancelled(&cancel, self.flush_written(Flush::Pressure)).await?;
                    continue;
                }
                message = self.receiver.recv() => message,
            };
            match message {
                Some(Message::Write(write)) => {
                    // A write that never returns ends with the attempt, whatever ended it.
                    until_cancelled(&cancel, self.stage(write)).await?;
                }
                Some(Message::Flush(reply)) => {
                    until_cancelled(&cancel, self.flush_written(Flush::Commit)).await?;
                    // The coordinator may have stopped waiting; the flush happened either way.
                    reply.send(()).ok();
                }
                None => return Ok(()),
            }
        }
    }

    /// Stages `write` with the table writer of its table, which then holds what the write held; the
    /// table writers of older versions of the table are retired first.
    async fn stage(&mut self, write: Write) -> Result<(), Error> {
        let older: Vec<_> = self
            .table_writers
            .range((write.table, SchemaVersion(0))..(write.table, write.version))
            .map(|(key, _)| *key)
            .collect();
        for key in older {
            self.retire(key).await?;
        }
        let index = self.index;
        let (table_writer, env, calls) = self.table_writer(write.table, write.version).await?;
        let began = env.instant();
        calls.calling(index, Call::Write, began);
        let written = table_writer.write(write.segment, write.batch).await;
        calls.returned(index);
        let writing = env.instant().saturating_duration_since(began);
        self.counted.writing = self.counted.writing.saturating_add(writing);
        written.map_err(|error| Error::connector(Side::Destination, "writing a batch", error))?;
        self.written.insert((write.table, write.version));
        self.held.push(write.reservation);
        Ok(())
    }

    /// Flushes every table writer written since the last flush, as `asked` asked, then releases
    /// what their writes held.
    async fn flush_written(&mut self, asked: Flush) -> Result<(), Error> {
        // A table writer written before the last flush holds nothing more to flush.
        for key in std::mem::take(&mut self.written) {
            let Some(open) = self.table_writers.get_mut(&key) else {
                continue;
            };
            let calling = (self.calls.as_ref(), self.index);
            let (flushing, flushed) =
                flush(open.table_writer.as_mut(), self.env.as_ref(), calling).await;
            self.flushed(asked, flushing);
            flushed?;
        }
        self.held.clear();
        Ok(())
    }

    /// Counts a flush `asked` asked for, which took `flushing`, as the writer's.
    fn flushed(&mut self, asked: Flush, flushing: Duration) {
        self.counted.flushing = self.counted.flushing.saturating_add(flushing);
        self.counted.flushes.count(asked);
    }

    /// Closes the table writer at `key`, having flushed what it was written since the last flush,
    /// so what it staged stays staged; what the writes held is released at the writer's next flush.
    async fn retire(&mut self, key: (usize, SchemaVersion)) -> Result<(), Error> {
        let Some(mut open) = self.table_writers.remove(&key) else {
            return Ok(());
        };
        if self.written.remove(&key) {
            let calling = (self.calls.as_ref(), self.index);
            let (flushing, flushed) =
                flush(open.table_writer.as_mut(), self.env.as_ref(), calling).await;
            self.flushed(Flush::Retirement, flushing);
            flushed?;
        }
        Ok(())
    }
}

/// Flushes `table_writer`, the flush noted as writer `index`'s call in `calls`: how long it took
/// on `env`'s clock.
async fn flush(
    table_writer: &mut dyn DestinationWriter,
    env: &dyn Env,
    (calls, index): (&WriterCalls, usize),
) -> (Duration, Result<(), Error>) {
    let began = env.instant();
    calls.calling(index, Call::Flush, began);
    let flushed = table_writer.flush().await.map(drop);
    calls.returned(index);
    let flushing = env.instant().saturating_duration_since(began);
    (
        flushing,
        flushed
            .map_err(|error| Error::connector(Side::Destination, "flushing staged writes", error)),
    )
}

impl Drop for Writer {
    fn drop(&mut self) {
        let (index, counted) = (self.index, self.counted);
        self.tally
            .add(|counters| counters.writer(index).add(&counted));
    }
}

impl Writer {
    /// The writer's table writer for `table`'s batches lowered for `version`, opened on the first
    /// of them, the clock its writes are timed on and where its calls are noted: a table writer's
    /// table names the schema its writes follow, and a partition may still write batches of an
    /// older version after the table changes.
    async fn table_writer(
        &mut self,
        table: usize,
        version: SchemaVersion,
    ) -> Result<(&mut dyn DestinationWriter, &dyn Env, &WriterCalls), Error> {
        self.writes = self.writes.saturating_add(1);
        let key = (table, version);
        if !self.table_writers.contains_key(&key) && self.table_writers.len() >= self.share.get() {
            let oldest = self.table_writers.iter().min_by_key(|(_, open)| open.used);
            if let Some(oldest) = oldest.map(|(key, _)| *key) {
                self.retire(oldest).await?;
            }
        }
        match self.table_writers.entry(key) {
            Entry::Occupied(open) => {
                let open = open.into_mut();
                open.used = self.writes;
                Ok((
                    open.table_writer.as_mut(),
                    self.env.as_ref(),
                    self.calls.as_ref(),
                ))
            }
            Entry::Vacant(vacant) => {
                let view = self.tables.view(table);
                let table = TableRef {
                    version,
                    ..view.table.clone()
                };
                self.calls
                    .calling(self.index, Call::Open, self.env.instant());
                let table_writer = self.tables.session().writer(&table).await;
                self.calls.returned(self.index);
                let table_writer = table_writer?.map_err(|error| {
                    let context = format!("creating a table writer for table {}", view.table.name);
                    Error::connector(Side::Destination, context, error)
                })?;
                let open = vacant.insert(Open {
                    table_writer,
                    used: self.writes,
                });
                Ok((
                    open.table_writer.as_mut(),
                    self.env.as_ref(),
                    self.calls.as_ref(),
                ))
            }
        }
    }
}

/// Runs `work` until it ends or `cancel` fires: a call into a destination that never returns
/// keeps no attempt from ending, and what it held goes with it.
async fn until_cancelled(
    cancel: &CancellationToken,
    work: impl Future<Output = Result<(), Error>>,
) -> Result<(), Error> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(Error::cancelled("the attempt was cancelled")),
        done = work => done,
    }
}
