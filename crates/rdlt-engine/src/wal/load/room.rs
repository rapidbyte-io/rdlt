//! Room in a load's log: a batch that finds the log full has it publish a chunk that frees what
//! is committed, waits for a commit where one can free room, and is refused only where the
//! frames of segments not yet sealed leave none.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Notify, oneshot};

use super::super::writer::Kept;
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
    /// Batches waiting for room, in the order they began to wait.
    waiting: parking_lot::Mutex<Queue>,
    /// Rung by a batch that waits for room: a commit is due.
    full: Notify,
}

/// Batches waiting for room: the bytes each counts, by its place in line.
#[derive(Default)]
struct Queue {
    next: u64,
    places: BTreeMap<u64, u64>,
}

/// A commit under way, which a batch waiting for room waits for; its end wakes such a batch.
pub(crate) struct Committing {
    pressure: Arc<Pressure>,
    log: LoadLog,
}

/// A batch's place in line for room, given up however its wait ends; the batches behind it look
/// again.
struct Waiting<'a> {
    log: &'a LoadLog,
    place: u64,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.log.pressure.waiting.lock().places.remove(&self.place);
        self.log.writer.shared().room.notify_waiters();
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
        !self.pressure.waiting.lock().places.is_empty()
    }

    /// Counts `bytes` of a batch frame, and its table's schema frame, on disk once the log holds
    /// them beside the room it keeps, what ends its chunk, what a carry copies and the frames of
    /// a commit, and beside what the batches waiting before it count: what it counted for ending
    /// its chunk, where it would take that past what a carry may copy.
    ///
    /// A batch finding no room first has the writer publish chunks between commits while that
    /// frees any; then, while a checkpoint waits for a commit or a commit is under way, it takes
    /// a place in line, rings for a commit and waits for the room it frees, which goes to the
    /// batches in line first. Where no commit can free room, it takes the carry's room and the
    /// line's, as it may be what brings its partition's next checkpoint.
    ///
    /// # Errors
    ///
    /// `log_bytes_exceeded` where the log has no room for the batch beside what ends its chunk
    /// and a commit, publishing chunks frees none, and no commit can: no checkpoint was sealed
    /// since the last commit took them, and none is under way.
    pub(super) async fn admit(&self, bytes: u64) -> Result<u64, Error> {
        let shared = self.writer.shared();
        let mut waiting: Option<Waiting<'_>> = None;
        loop {
            // Listening before the look, so room freed after it wakes the wait.
            let room = shared.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            shared.failure()?;
            let closing = shared.closing.load(Ordering::SeqCst);
            let counted = bytes.saturating_add(closing);
            let ahead = self.ahead(waiting.as_ref());
            if shared.reserve(counted, shared.kept(Kept::All).saturating_add(ahead)) {
                return Ok(closing);
            }
            // Looked at before the relief: a commit that ended since queued its receipt before
            // the relief, which the relief then counts.
            let freeing = self.freeing();
            self.relieve().await?;
            // The writer answered once it wrote every frame sent before and freed what it could.
            let ahead = self.ahead(waiting.as_ref());
            if shared.reserve(counted, shared.kept(Kept::All).saturating_add(ahead)) {
                return Ok(closing);
            }
            if !(freeing || self.freeing()) {
                if shared.reserve(counted, shared.kept(Kept::Commit)) {
                    return Ok(closing);
                }
                return Err(full(self.disk.limit));
            }
            if waiting.is_none() {
                waiting = Some(self.wait(counted));
            }
            self.pressure.full.notify_one();
            room.await;
        }
    }

    /// Takes a place in line for room for `bytes`.
    fn wait(&self, bytes: u64) -> Waiting<'_> {
        let mut queue = self.pressure.waiting.lock();
        let place = queue.next;
        queue.next += 1;
        queue.places.insert(place, bytes);
        Waiting { log: self, place }
    }

    /// Bytes: what the batches in line before `waiting` count, all of them where it is not in
    /// line.
    fn ahead(&self, waiting: Option<&Waiting<'_>>) -> u64 {
        let queue = self.pressure.waiting.lock();
        let before = waiting.map_or(u64::MAX, |waiting| waiting.place);
        queue
            .places
            .range(..before)
            .map(|(_, bytes)| *bytes)
            .fold(0, u64::saturating_add)
    }

    /// Counts `bytes` of one of a commit's seal, phase and commit frames on disk, of the `whole`
    /// commit's so far, where the log holds them beside the room it keeps, first having it
    /// publish chunks between commits while that frees any: the commit frees what it seals, so
    /// it may take the carry's room, and a commit larger than all that room is counted all the
    /// same, taking the log past what it may hold by its own frames until a later relief frees
    /// what it settles.
    pub(super) async fn admit_commit(&self, bytes: u64, whole: u64) -> Result<(), Error> {
        let shared = self.writer.shared();
        shared.failure()?;
        shared.committed.fetch_max(whole, Ordering::SeqCst);
        if shared.reserve(bytes, shared.kept(Kept::Carry)) {
            return Ok(());
        }
        self.relieve().await?;
        if !shared.reserve(bytes, shared.kept(Kept::Closing)) {
            shared.held.fetch_add(bytes, Ordering::SeqCst);
        }
        Ok(())
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
}

/// The refusal of a frame a log of `limit` bytes has no room for.
fn full(limit: u64) -> Error {
    Error::wal(format!(
        "the load's write-ahead log would hold more than {limit} bytes, what its partitions have \
         not sealed fills it, and no checkpoint since its last commit lets a commit free any of it"
    ))
    .with_code(LOG_BYTES_EXCEEDED)
}
