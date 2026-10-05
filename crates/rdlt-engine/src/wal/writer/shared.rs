//! What a load's writer and its senders share: what the log holds on disk, the room every
//! frame keeps beside it, and the log's first failure.

use std::sync::atomic::{AtomicU64, Ordering};

use rdlt_connector::CommitSeq;

use crate::error::Error;

/// What a load's writer and its senders share: what the log holds on disk, and its first
/// failure.
pub(crate) struct Shared {
    /// Bytes: what the log holds on disk, its chunks published and the chunk staged, and what was
    /// counted for frames not yet written.
    pub(crate) held: AtomicU64,
    /// Bytes: what the log may hold on disk.
    pub(crate) limit: AtomicU64,
    /// Bytes: what the writer needs to end the chunk staged, and to stage and end one more: a
    /// header, an end naming every chunk and commit the log holds, and a closing frame each.
    pub(crate) closing: AtomicU64,
    /// Bytes: the room a carry keeps: the most a carry of a chunk holding settled frames beside
    /// open ones copies, and at least what a chunk holds before it is published.
    pub(crate) carry: AtomicU64,
    /// Bytes: the most a commit of the load wrote, its seal, phase and commit frames, and at
    /// least an eighth of `limit`, which a batch keeps room for, so the commit its checkpoint
    /// brings can be written.
    pub(crate) committed: AtomicU64,
    /// The first failure, which every later batch and command is answered with.
    pub(crate) failed: parking_lot::Mutex<Option<Error>>,
    /// The oldest commit a replay of the log may repeat: one waiting for its receipt, or whose
    /// receipt no published chunk records yet; none where there is none.
    pub(crate) oldest: parking_lot::Mutex<Option<CommitSeq>>,
    /// Rung once the log holds less, or has failed: a batch waiting for room looks again.
    pub(crate) room: tokio::sync::Notify,
}

impl Shared {
    /// What a log of `limit` bytes shares before anything is written.
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            held: AtomicU64::new(0),
            limit: AtomicU64::new(limit),
            closing: AtomicU64::new(0),
            carry: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            failed: parking_lot::Mutex::default(),
            oldest: parking_lot::Mutex::default(),
            room: tokio::sync::Notify::new(),
        }
    }

    /// The failure every command answers with once one failed.
    pub(crate) fn failure(&self) -> Result<(), Error> {
        match &*self.failed.lock() {
            Some(failed) => Err(Error::wal_failed_before(failed)),
            None => Ok(()),
        }
    }

    /// Counts `bytes` against what the log may hold where it holds them with `beside` bytes to
    /// spare: whether it did.
    pub(crate) fn reserve(&self, bytes: u64, beside: u64) -> bool {
        let limit = self.limit.load(Ordering::SeqCst);
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                let held = held.checked_add(bytes)?;
                (held.saturating_add(beside) <= limit).then_some(held)
            })
            .is_ok()
    }

    /// Gives back `bytes` counted for frames never written.
    pub(crate) fn release(&self, bytes: u64) {
        // The update only lowers the count, so it never fails.
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                Some(held.saturating_sub(bytes))
            })
            .ok();
    }

    /// Bytes: the room every reservation of the log's frames keeps: what ends the chunk staged,
    /// and beside it what each of `kept` names.
    pub(crate) fn kept(&self, kept: Kept) -> u64 {
        let closing = self.closing.load(Ordering::SeqCst);
        let carry = self.carry.load(Ordering::SeqCst);
        let committed = self.committed.load(Ordering::SeqCst);
        match kept {
            Kept::Closing => closing,
            Kept::Commit => closing.saturating_add(committed),
            Kept::Carry => closing.saturating_add(carry),
            Kept::All => closing.saturating_add(carry).saturating_add(committed),
        }
    }
}

/// What a reservation keeps room for beside what ends the chunk staged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kept {
    /// Nothing more.
    Closing,
    /// The frames of a commit as large as the largest yet.
    Commit,
    /// A carry of one chunk.
    Carry,
    /// Both.
    All,
}
