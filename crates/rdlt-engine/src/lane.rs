//! Lanes: long-lived destination writers that stage batches in order.

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
use crate::report::{Flush, LaneCounters, Tally};
use crate::status::{Call, LaneCalls};
use crate::table::Tables;

/// A batch for one table, tagged with its segment and the schema version it was lowered for; the
/// reservation drops once its writer has flushed it, since a writer may buffer what it stages.
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

/// The senders of every lane of an attempt.
#[derive(Clone)]
pub(crate) struct Lanes {
    senders: Vec<mpsc::Sender<Message>>,
    /// The clock a write waiting for room in a lane's queue is timed on.
    env: Arc<dyn Env>,
    /// Where each lane's waits and times are counted.
    tally: Arc<Tally>,
    /// Each lane's call into the destination in flight.
    calls: Arc<LaneCalls>,
}

/// One lane's end: its queue, a writer for each table and schema version it writes, which of
/// them it wrote since its last flush, and the reservations of those writes.
///
/// A lane holds at most `share` writers open, and a table's writer of a version older than one
/// it writes is retired: each is a call into a served destination. What it times is added to the
/// run's tally once, when the lane is dropped, so a cancelled lane adds what it counted.
pub(crate) struct Lane {
    receiver: mpsc::Receiver<Message>,
    tables: Arc<Tables>,
    writers: BTreeMap<(usize, SchemaVersion), Open>,
    written: BTreeSet<(usize, SchemaVersion)>,
    held: Vec<Permit>,
    budget: MemoryBudget,
    /// The most writers the lane holds open.
    share: NonZeroUsize,
    /// Counts the lane's writes, so the writer written longest ago is found.
    writes: u64,
    /// The lane's place among the attempt's lanes.
    index: usize,
    /// The clock the lane's writes and flushes are timed on.
    env: Arc<dyn Env>,
    /// What the lane's writes and flushes took so far.
    counted: LaneCounters,
    /// Where the lane adds what it counted.
    tally: Arc<Tally>,
    /// Where the lane notes its call into the destination in flight.
    calls: Arc<LaneCalls>,
}

/// An open writer, and the count of the lane's writes when it was last written.
struct Open {
    writer: Box<dyn DestinationWriter>,
    used: u64,
}

impl Lanes {
    /// `count` lanes writing into `tables`, each queueing up to `window` writes and holding an
    /// equal share of `writers` open, one at least.
    ///
    /// A lane opens a table's writer when it first writes to the table, since normalized streams
    /// add child tables as their rows arrive. Each lane's waits and times are counted into
    /// `tally`, on `env`'s clock.
    pub(crate) fn new(
        (count, writers): (NonZeroUsize, NonZeroUsize),
        tables: &Arc<Tables>,
        window: NonZeroUsize,
        budget: &MemoryBudget,
        (env, tally): (&Arc<dyn Env>, &Arc<Tally>),
    ) -> (Self, Vec<Lane>) {
        let share = NonZeroUsize::new(writers.get() / count.get()).unwrap_or(NonZeroUsize::MIN);
        let calls = Arc::new(LaneCalls::new(env.instant(), count.get()));
        let (senders, lanes) = (0..count.get())
            .map(|index| {
                let (sender, receiver) = mpsc::channel(window.get());
                let lane = Lane {
                    receiver,
                    tables: Arc::clone(tables),
                    writers: BTreeMap::new(),
                    written: BTreeSet::new(),
                    held: Vec::new(),
                    budget: budget.clone(),
                    share,
                    writes: 0,
                    index,
                    env: Arc::clone(env),
                    counted: LaneCounters::default(),
                    tally: Arc::clone(tally),
                    calls: Arc::clone(&calls),
                };
                (sender, lane)
            })
            .unzip();
        let ends = Self {
            senders,
            env: Arc::clone(env),
            tally: Arc::clone(tally),
            calls,
        };
        (ends, lanes)
    }

    /// Each lane's call into the destination in flight.
    pub(crate) fn calls(&self) -> &Arc<LaneCalls> {
        &self.calls
    }

    /// The lane for the writes to `table` of the partition at `place` in the attempt: a
    /// partition keeps its place while it reads, and one read again takes its ended read's, so
    /// its writes to a table stay in order on one lane; places up to the lanes take a lane each.
    pub(crate) fn route(&self, table: usize, place: usize) -> usize {
        place.wrapping_add(table) % self.senders.len()
    }

    /// Queues `write` on `lane`, waiting while the lane's queue is full; the wait is counted as
    /// the lane's.
    pub(crate) async fn write(&self, lane: usize, write: Write) -> Result<(), Error> {
        let sender = &self.senders[lane];
        let message = match sender.try_send(Message::Write(write)) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(message)) => message,
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(stopped()),
        };
        let began = self.env.instant();
        let sent = sender.send(message).await;
        let blocked = self.env.instant().saturating_duration_since(began);
        self.tally.add(|counters| {
            let lane = counters.lane(lane);
            lane.blocked = lane.blocked.saturating_add(blocked);
        });
        sent.map_err(|_| stopped())
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
    ///
    /// A lane flushes its writers when the coordinator asks, and whenever a request waits for
    /// bytes while the lane holds writes it has not flushed, which frees them.
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

    /// Stages `write` with its table's writer, which then holds what the write held; the table's
    /// writers of older versions are retired first.
    async fn stage(&mut self, write: Write) -> Result<(), Error> {
        let older: Vec<_> = self
            .writers
            .range((write.table, SchemaVersion(0))..(write.table, write.version))
            .map(|(key, _)| *key)
            .collect();
        for key in older {
            self.retire(key).await?;
        }
        let lane = self.index;
        let (writer, env, calls) = self.writer(write.table, write.version).await?;
        let began = env.instant();
        calls.calling(lane, Call::Write, began);
        let written = writer.write(write.segment, write.batch).await;
        calls.returned(lane);
        let writing = env.instant().saturating_duration_since(began);
        self.counted.writing = self.counted.writing.saturating_add(writing);
        written.map_err(|error| Error::connector(Side::Destination, "writing a batch", error))?;
        self.written.insert((write.table, write.version));
        self.held.push(write.reservation);
        Ok(())
    }

    /// Flushes every writer written since the last flush, as `asked` asked, then releases what
    /// their writes held.
    async fn flush_written(&mut self, asked: Flush) -> Result<(), Error> {
        // A writer written before the last flush holds nothing more to flush.
        for key in std::mem::take(&mut self.written) {
            let Some(open) = self.writers.get_mut(&key) else {
                continue;
            };
            let calling = (self.calls.as_ref(), self.index);
            let (flushing, flushed) = flush(open.writer.as_mut(), self.env.as_ref(), calling).await;
            self.flushed(asked, flushing);
            flushed?;
        }
        self.held.clear();
        Ok(())
    }

    /// Counts a flush `asked` asked for, which took `flushing`, as the lane's.
    fn flushed(&mut self, asked: Flush, flushing: Duration) {
        self.counted.flushing = self.counted.flushing.saturating_add(flushing);
        self.counted.flushes.count(asked);
    }

    /// Closes the writer at `key`, having flushed what it was written since the last flush, so
    /// what it staged stays staged; what the writes held is released at the lane's next flush.
    async fn retire(&mut self, key: (usize, SchemaVersion)) -> Result<(), Error> {
        let Some(mut open) = self.writers.remove(&key) else {
            return Ok(());
        };
        if self.written.remove(&key) {
            let calling = (self.calls.as_ref(), self.index);
            let (flushing, flushed) = flush(open.writer.as_mut(), self.env.as_ref(), calling).await;
            self.flushed(Flush::Retirement, flushing);
            flushed?;
        }
        Ok(())
    }
}

/// Flushes `writer`, the flush noted as `lane`'s call in `calls`: how long it took on `env`'s
/// clock.
async fn flush(
    writer: &mut dyn DestinationWriter,
    env: &dyn Env,
    (calls, lane): (&LaneCalls, usize),
) -> (Duration, Result<(), Error>) {
    let began = env.instant();
    calls.calling(lane, Call::Flush, began);
    let flushed = writer.flush().await.map(drop);
    calls.returned(lane);
    let flushing = env.instant().saturating_duration_since(began);
    (
        flushing,
        flushed
            .map_err(|error| Error::connector(Side::Destination, "flushing staged writes", error)),
    )
}

impl Drop for Lane {
    fn drop(&mut self) {
        let (lane, counted) = (self.index, self.counted);
        self.tally.add(|counters| counters.lane(lane).add(&counted));
    }
}

impl Lane {
    /// The lane's writer for `table`'s batches lowered for `version`, opened on the first of them,
    /// the clock its writes are timed on and where its calls are noted: a writer's table names
    /// the schema its writes follow, and a partition may still write batches of an older version
    /// after the table changes.
    async fn writer(
        &mut self,
        table: usize,
        version: SchemaVersion,
    ) -> Result<(&mut dyn DestinationWriter, &dyn Env, &LaneCalls), Error> {
        self.writes = self.writes.saturating_add(1);
        let key = (table, version);
        if !self.writers.contains_key(&key) && self.writers.len() >= self.share.get() {
            let oldest = self.writers.iter().min_by_key(|(_, open)| open.used);
            if let Some(oldest) = oldest.map(|(key, _)| *key) {
                self.retire(oldest).await?;
            }
        }
        match self.writers.entry(key) {
            Entry::Occupied(open) => {
                let open = open.into_mut();
                open.used = self.writes;
                Ok((open.writer.as_mut(), self.env.as_ref(), self.calls.as_ref()))
            }
            Entry::Vacant(vacant) => {
                let view = self.tables.view(table);
                let table = TableRef {
                    version,
                    ..view.table.clone()
                };
                self.calls
                    .calling(self.index, Call::Open, self.env.instant());
                let writer = self.tables.session().writer(&table).await;
                self.calls.returned(self.index);
                let writer = writer?.map_err(|error| {
                    let context = format!("creating a writer for table {}", view.table.name);
                    Error::connector(Side::Destination, context, error)
                })?;
                let open = vacant.insert(Open {
                    writer,
                    used: self.writes,
                });
                Ok((open.writer.as_mut(), self.env.as_ref(), self.calls.as_ref()))
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
