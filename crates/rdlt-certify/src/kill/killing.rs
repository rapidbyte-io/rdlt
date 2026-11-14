//! A destination through which a kill clause kills a connector at scheduled points of its loads:
//! at a write once a commit has started, before a commit, and after a commit that landed, losing
//! its answer as a kill between the commit and its answer does.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectorError, ConnectorErrorKind, Destination,
    DestinationSession, DestinationWriter, OpenContext, OpenedSession, Receipt, Result, SegmentId,
    Source, TableChange, TableRef, WriteStats,
};
use rdlt_host::{CONNECTOR_LOST, Kills};
use tokio::sync::watch;

use super::paced::{Pace, Paced};

/// When to kill, counting commits from one across every load as each starts.
///
/// A kill lands at the first write once the `settled`th commit has started, before the
/// `commit`th commit, and, when `answer` is set, after the first commit from the `answer`th on
/// that lands, losing its answer: a lost answer drawn on a commit an earlier point failed is lost
/// from the next that lands. Each point follows a commit, so a load killed there resumes from
/// what it recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Schedule {
    pub(crate) settled: u64,
    pub(crate) commit: u64,
    pub(crate) answer: Option<u64>,
}

impl Schedule {
    /// The points `seed` draws, with a lost answer when `answers`: each after a commit that
    /// published rows, as a load's first commit may publish none, and early enough that a load
    /// committing a few times reaches it.
    pub(crate) fn seeded(seed: u64, answers: bool) -> Self {
        Self {
            settled: 2 + seed % 2,
            commit: 3 + (seed >> 8) % 3,
            answer: answers.then_some(2 + (seed >> 16) % 3),
        }
    }

    /// The commits its latest point counts to: the load's source is paced until that many have
    /// started.
    fn last(&self) -> u64 {
        self.settled
            .max(self.commit)
            .max(self.answer.unwrap_or_default())
    }
}

/// `destination`, killing through `kills` at the points `schedule` names.
pub(crate) struct Killing {
    destination: Arc<dyn Destination>,
    points: Arc<Points>,
}

/// The points reached so far, the tables written, and the pace of the load's source.
struct Points {
    schedule: Schedule,
    kills: Kills,
    settled: AtomicBool,
    answered: AtomicBool,
    commits: AtomicU64,
    tables: Mutex<Vec<TableRef>>,
    pace: Arc<watch::Sender<Pace>>,
}

impl Killing {
    pub(crate) fn new(
        destination: Arc<dyn Destination>,
        kills: &Kills,
        schedule: Schedule,
    ) -> Self {
        let points = Points {
            schedule,
            kills: kills.clone(),
            settled: AtomicBool::new(false),
            answered: AtomicBool::new(false),
            commits: AtomicU64::new(0),
            tables: Mutex::new(Vec::new()),
            pace: Arc::new(watch::Sender::new(Pace::default())),
        };
        Self {
            destination,
            points: Arc::new(points),
        }
    }

    /// Each table written, by name, as last written.
    pub(crate) fn tables(&self) -> Vec<TableRef> {
        self.points.tables.lock().clone()
    }

    /// `source`, read no further ahead of the commits this destination starts than a window,
    /// until it has started as many as the schedule's last point counts.
    pub(super) fn paced(&self, source: Arc<dyn Source>) -> Arc<Paced> {
        Arc::new(Paced::new(source, Arc::clone(&self.points.pace)))
    }

    /// The pace of the source [`paced`](Self::paced) gives, as it changes.
    #[cfg(test)]
    pub(super) fn pace(&self) -> watch::Receiver<Pace> {
        self.points.pace.subscribe()
    }
}

impl Points {
    /// Kills before a write when it is the first once the `settled`th commit has started.
    fn write(&self) {
        let settled = self.commits.load(Ordering::SeqCst) >= self.schedule.settled;
        if settled && !self.settled.swap(true, Ordering::SeqCst) {
            self.kills.kill();
        }
    }

    /// Counts a commit as it starts, killing before it when it is the scheduled one, and
    /// releases the reads of the paced source, so they write as it runs; which commit it is.
    fn commit(&self) -> u64 {
        let commit = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        let last = self.schedule.last();
        self.pace.send_modify(|pace| pace.start(commit, last));
        if commit == self.schedule.commit {
            self.kills.kill();
        }
        commit
    }

    /// Whether to lose the answer of the `commit`th commit, which landed: the first that lands
    /// from the scheduled one on loses it.
    fn lose(&self, commit: u64) -> bool {
        let due = self.schedule.answer.is_some_and(|answer| commit >= answer);
        due && !self.answered.swap(true, Ordering::SeqCst)
    }

    fn written(&self, table: &TableRef) {
        let mut tables = self.tables.lock();
        tables.retain(|written| written.name != table.name);
        tables.push(table.clone());
    }
}

impl Destination for Killing {
    fn capabilities(&self) -> &Capabilities {
        self.destination.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.destination.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.destination.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(Session {
                    session: opened.session,
                    points: Arc::clone(&self.points),
                }),
                ..opened
            })
        })
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.destination.max_calls()
    }
}

struct Session {
    session: Box<dyn DestinationSession>,
    points: Arc<Points>,
}

impl DestinationSession for Session {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.session.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            self.points.written(table);
            let writer = self.session.writer(table).await?;
            Ok(Box::new(Writer {
                writer,
                points: Arc::clone(&self.points),
            }) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            let commit = self.points.commit();
            let receipt = self.session.commit(meta).await?;
            if self.points.lose(commit) {
                self.points.kills.kill();
                let message = "the connector was killed after the commit, before its answer";
                return Err(ConnectorError::new(ConnectorErrorKind::Transient, message)
                    .with_code(CONNECTOR_LOST));
            }
            Ok(receipt)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.session.close()
    }
}

struct Writer {
    writer: Box<dyn DestinationWriter>,
    points: Arc<Points>,
}

impl DestinationWriter for Writer {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        self.points.write();
        self.writer.write(segment, batch)
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        self.writer.flush()
    }
}
