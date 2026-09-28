//! A destination through which a kill clause kills a connector at scheduled points of its loads:
//! at a write after a commit, before a commit, and after a commit, losing its answer as a kill
//! between the commit and its answer does.

#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectorError, ConnectorErrorKind, Destination,
    DestinationSession, DestinationWriter, OpenContext, OpenedSession, Receipt, Result, SegmentId,
    TableChange, TableRef, WriteStats,
};
use rdlt_host::{CONNECTOR_LOST, Kills};

/// When to kill, counting commits from one across every load.
///
/// A kill lands at the first write after the `settled`th commit, before the `commit`th commit,
/// and after the `answer`th commit, losing its answer, when set. Each point follows a commit, so
/// a load killed there resumes from what it recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Schedule {
    pub(crate) settled: u64,
    pub(crate) commit: u64,
    pub(crate) answer: Option<u64>,
}

impl Schedule {
    /// The points `seed` draws, with a lost answer when `answers`: early enough that a load
    /// committing a few times reaches them.
    pub(crate) fn seeded(seed: u64, answers: bool) -> Self {
        Self {
            settled: 1 + seed % 2,
            commit: 2 + (seed >> 8) % 3,
            answer: answers.then_some(1 + (seed >> 16) % 3),
        }
    }
}

/// `destination`, killing through `kills` at the points `schedule` names.
pub(crate) struct Killing {
    destination: Arc<dyn Destination>,
    points: Arc<Points>,
}

/// The points reached so far, and the tables written.
struct Points {
    schedule: Schedule,
    kills: Kills,
    settled: AtomicBool,
    commits: AtomicU64,
    tables: Mutex<Vec<TableRef>>,
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
            commits: AtomicU64::new(0),
            tables: Mutex::new(Vec::new()),
        };
        Self {
            destination,
            points: Arc::new(points),
        }
    }

    /// Each table written, by name, as last written.
    pub(crate) fn tables(&self) -> Vec<TableRef> {
        self.points
            .tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Points {
    /// Kills before a write when it is the first after the scheduled commit.
    fn write(&self) {
        let settled = self.commits.load(Ordering::SeqCst) >= self.schedule.settled;
        if settled && !self.settled.swap(true, Ordering::SeqCst) {
            self.kills.kill();
        }
    }

    /// Counts a commit, killing before it when it is the scheduled one; whether to lose its
    /// answer.
    fn commit(&self) -> bool {
        let commit = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        if commit == self.schedule.commit {
            self.kills.kill();
        }
        self.schedule.answer == Some(commit)
    }

    fn written(&self, table: &TableRef) {
        let mut tables = self.tables.lock().unwrap_or_else(PoisonError::into_inner);
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
            let lose = self.points.commit();
            let receipt = self.session.commit(meta).await?;
            if lose {
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
