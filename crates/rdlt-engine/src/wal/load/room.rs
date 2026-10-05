//! Room in a load's log: a batch that finds the log full waits for a commit to free room, and
//! is refused only where no commit can.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

use super::{LoadLog, count};
use crate::error::Error;
use crate::limits::LOG_BYTES_EXCEEDED;

/// Whether a commit can free room in a full log: a checkpoint no commit has taken, or a commit
/// under way, and the bell a batch rings when it waits for one.
#[derive(Default)]
pub(super) struct Pressure {
    /// Checkpoints partitions sealed that no commit has taken yet.
    sealed: AtomicU64,
    /// Commits under way.
    committing: AtomicU64,
    /// Batches waiting for room.
    waiting: AtomicU64,
    /// Rung by a batch that waits for room: a commit is due.
    full: Notify,
}

/// A commit under way, which a batch waiting for room waits for; its end wakes such a batch.
pub(crate) struct Committing {
    pressure: Arc<Pressure>,
    log: LoadLog,
}

/// A batch waiting for room, counted until the wait ends however it ends.
struct Waiting<'a>(&'a AtomicU64);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for Committing {
    fn drop(&mut self) {
        self.pressure.committing.fetch_sub(1, Ordering::SeqCst);
        self.log.writer.shared().room.notify_waiters();
    }
}

impl LoadLog {
    /// Checkpoints sealed that no commit has taken yet.
    #[cfg(test)]
    pub(crate) fn sealed(&self) -> u64 {
        self.pressure.sealed.load(Ordering::SeqCst)
    }

    /// Notes a partition sealed a checkpoint, before the coordinator hears of it: a commit can
    /// take it, and free what the log holds of it.
    pub(crate) fn checkpointed(&self) {
        self.pressure.sealed.fetch_add(1, Ordering::SeqCst);
    }

    /// Notes a commit took `seals` checkpoints.
    pub(crate) fn took(&self, seals: usize) {
        let seals = count(seals);
        let sealed = &self.pressure.sealed;
        // The update only lowers the count, so it never fails.
        sealed
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                Some(held.saturating_sub(seals))
            })
            .ok();
    }

    /// A commit begins: a batch that finds the log full waits for its end.
    pub(crate) fn committing(&self) -> Committing {
        self.pressure.committing.fetch_add(1, Ordering::SeqCst);
        Committing {
            pressure: Arc::clone(&self.pressure),
            log: self.clone(),
        }
    }

    /// Completes once a batch found the log full and waits for a commit to free room.
    pub(crate) async fn full(&self) {
        self.pressure.full.notified().await;
    }

    /// Whether a batch waits for room in the log, which only a commit frees: no barrier waits for
    /// its partition, which cannot answer before the batch is logged.
    pub(crate) fn waits(&self) -> bool {
        self.pressure.waiting.load(Ordering::SeqCst) > 0
    }

    /// Counts `bytes` of a batch frame on disk, once the log may hold them.
    ///
    /// While a checkpoint waits for a commit, or a commit is under way, a batch that finds the
    /// log full, or the chunk staged past a third of what it may hold, rings for a commit and
    /// waits for the room it frees.
    ///
    /// # Errors
    ///
    /// `log_bytes_exceeded` where the log would hold more than it may and no commit can free
    /// room: no checkpoint was sealed since the last commit took them, and none is under way.
    pub(super) async fn admit(&self, bytes: u64) -> Result<(), Error> {
        let shared = self.writer.shared();
        let limit = self.disk.limit;
        loop {
            // Listening before the look, so room freed after it wakes the wait.
            let room = shared.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            shared.failure()?;
            let held = shared.held.load(Ordering::Relaxed);
            let fits = held.saturating_add(bytes) <= limit;
            // A chunk is kept until two after it are published: while a commit can free room,
            // a batch past a third of the log waits, so two chunks published leave room.
            let unpublished = shared.unpublished.load(Ordering::Relaxed);
            let third = unpublished == 0 || unpublished.saturating_add(bytes) <= limit / 3;
            let pressure = &self.pressure;
            let freeing = pressure.sealed.load(Ordering::SeqCst) > 0
                || pressure.committing.load(Ordering::SeqCst) > 0;
            if fits && (third || !freeing) {
                shared.held.fetch_add(bytes, Ordering::Relaxed);
                shared.unpublished.fetch_add(bytes, Ordering::Relaxed);
                return Ok(());
            }
            if !freeing {
                return Err(Error::wal(format!(
                    "the load's write-ahead log would hold more than {limit} bytes, and no \
                     checkpoint since its last commit lets a commit free any of it"
                ))
                .with_code(LOG_BYTES_EXCEEDED));
            }
            pressure.waiting.fetch_add(1, Ordering::SeqCst);
            let _waiting = Waiting(&pressure.waiting);
            pressure.full.notify_one();
            room.await;
        }
    }
}
