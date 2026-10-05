//! Room in a load's log: a batch that finds the log full has it publish a chunk that frees what
//! is committed, waits for a commit where one can free room, and is refused only where the
//! frames of segments not yet sealed leave none.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Notify, oneshot};

use super::{Command, LoadLog, count};
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

    /// Counts `bytes` of a batch frame on disk, once the log may hold them beside room for the
    /// copies a carry makes.
    ///
    /// A batch finding no room first has the writer publish its chunk between commits where that
    /// frees any; then, while a checkpoint waits for a commit or a commit is under way, it rings
    /// for a commit and waits for the room it frees.
    ///
    /// # Errors
    ///
    /// `log_bytes_exceeded` where the log has no room for the batch, publishing its chunk frees
    /// none, and no commit can: no checkpoint was sealed since the last commit took them, and
    /// none is under way.
    pub(super) async fn admit(&self, bytes: u64) -> Result<(), Error> {
        let shared = self.writer.shared();
        loop {
            // Listening before the look, so room freed after it wakes the wait.
            let room = shared.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            shared.failure()?;
            if self.reserve(bytes, true) {
                return Ok(());
            }
            // Looked at before the relief: a commit that ended since queued its receipt before
            // the relief, which the relief then counts.
            let freeing = self.freeing();
            self.relieve().await?;
            // The writer answered once it wrote every frame sent before and freed what it could.
            if self.reserve(bytes, true) {
                return Ok(());
            }
            if !(freeing || self.freeing()) {
                // No commit can free room: the batch takes what room is left, as it may be what
                // brings its partition's next checkpoint.
                if self.reserve(bytes, false) {
                    return Ok(());
                }
                let limit = self.disk.limit;
                return Err(Error::wal(format!(
                    "the load's write-ahead log would hold more than {limit} bytes, what its \
                     partitions have not sealed fills it, and no checkpoint since its last \
                     commit lets a commit free any of it"
                ))
                .with_code(LOG_BYTES_EXCEEDED));
            }
            let pressure = &self.pressure;
            pressure.waiting.fetch_add(1, Ordering::SeqCst);
            let _waiting = Waiting(&pressure.waiting);
            pressure.full.notify_one();
            room.await;
        }
    }

    /// Whether a commit can free room: a checkpoint was sealed that no commit has taken, or a
    /// commit is under way.
    fn freeing(&self) -> bool {
        self.pressure.sealed.load(Ordering::SeqCst) > 0
            || self.pressure.committing.load(Ordering::SeqCst) > 0
    }

    /// Has the writer publish chunks between commits while each lets the log hold less.
    async fn relieve(&self) -> Result<(), Error> {
        let (done, answer) = oneshot::channel();
        self.writer.send(Command::Relieve { done }).await?;
        answer
            .await
            .map_err(|_| Error::wal("the write-ahead log's writer stopped"))?
    }

    /// Counts `bytes` of a batch frame on disk where the log holds them, `beside` room for the
    /// most a carry of a chunk copies and the frame itself once more where asked: whether it did.
    ///
    /// Frames counted and not yet written may come to be copied too, so they are kept room for
    /// as well.
    fn reserve(&self, bytes: u64, beside: bool) -> bool {
        let shared = self.writer.shared();
        let limit = self.disk.limit;
        let unwritten = shared.unwritten.fetch_add(bytes, Ordering::SeqCst);
        let beside = if beside {
            shared
                .copied
                .load(Ordering::SeqCst)
                .saturating_add(unwritten)
                .saturating_add(bytes)
        } else {
            0
        };
        let counted = shared
            .held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                let held = held.checked_add(bytes)?;
                (held.saturating_add(beside) <= limit).then_some(held)
            })
            .is_ok();
        if !counted {
            // The update only lowers the count, so it never fails.
            shared
                .unwritten
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |unwritten| {
                    Some(unwritten.saturating_sub(bytes))
                })
                .ok();
        }
        counted
    }
}
