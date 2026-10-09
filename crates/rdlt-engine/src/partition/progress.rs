//! What a partition tells the commit coordinator as it reads.

use std::fmt;

use rdlt_connector::{PartitionState, Permit, SegmentId};

use tokio_util::sync::CancellationToken;

use crate::budget::{Denied, MemoryBudget};
use crate::cost::Admitted;
use crate::error::Error;
use crate::report::ShredCounts;

/// What a partition tells the commit coordinator.
#[derive(Debug, PartialEq)]
pub(crate) enum Progress {
    /// The partition started reading.
    Started {
        /// The partition's index in the attempt.
        partition: usize,
    },
    /// Rows were queued for staging.
    Written {
        /// The partition's index in the attempt.
        partition: usize,
        /// Rows queued.
        rows: u64,
        /// Their bytes in memory.
        bytes: u64,
    },
    /// A segment was sealed.
    Sealed(Seal),
    /// The partition has a seal of no rows waiting, queued in this epoch of its seals.
    Moved {
        /// The partition's index in the attempt.
        partition: usize,
        /// How many seals with rows the partition had sent.
        epoch: u64,
    },
    /// The partition has a signal waiting: how far its read is behind, or that its stream's
    /// partitions changed.
    Signalled {
        /// The partition's index in the attempt.
        partition: usize,
    },
    /// The partition's source had dropped where its read would resume, and it read again from
    /// its earliest.
    RetentionReset {
        /// The partition's index in the attempt.
        partition: usize,
    },
    /// Rows were staged to a segment no commit will take: no checkpoint sealed them.
    Abandoned {
        /// The partition's index in the attempt.
        partition: usize,
        /// The segment it abandoned.
        segment: SegmentId,
    },
    /// The partition stopped reading; a partition that was not stopped sealed its end first.
    Ended {
        /// The partition's index in the attempt.
        partition: usize,
        /// Whether the read ended because the engine asked it to stop.
        stopped: bool,
    },
}

/// A sealed segment: every row written to it, and where to resume after it.
#[derive(Debug, PartialEq)]
pub(crate) struct Seal {
    /// The partition's index in the attempt.
    pub(crate) partition: usize,
    /// The segment.
    pub(crate) segment: SegmentId,
    /// Rows written to the segment.
    pub(crate) rows: u64,
    /// Their bytes in memory.
    pub(crate) bytes: u64,
    /// Where the partition resumes once the segment is committed.
    pub(crate) state: PartitionState,
    /// The barrier this seal answers.
    pub(crate) answers: Option<u64>,
    /// Rows the schema policy dropped from the segment.
    pub(crate) discarded_rows: u64,
    /// Values the schema policy nulled in the segment.
    pub(crate) discarded_values: u64,
    /// Deletes the stream ignores, dropped from the segment.
    pub(crate) deletes_ignored: u64,
    /// Truncates the stream ignores, dropped from the segment.
    pub(crate) truncates_ignored: u64,
    /// What shredding the segment's JSON took.
    pub(crate) shred: ShredCounts,
    /// What holds the cursor's bytes in the budget until the seal's commit lands.
    pub(crate) held: CursorHold,
}

impl Seal {
    /// Whether the seal moves its partition's position and nothing else: no row, discard or
    /// ignored change is its segment's.
    pub(crate) fn moves_only(&self) -> bool {
        let counts = [
            self.rows,
            self.bytes,
            self.discarded_rows,
            self.discarded_values,
            self.deletes_ignored,
            self.truncates_ignored,
        ];
        counts == [0; 6]
    }

    /// The bytes of the cursor the seal resumes from.
    pub(crate) fn cursor_bytes(&self) -> u64 {
        match &self.state {
            PartitionState::Cursor(cursor) => {
                u64::try_from(cursor.bytes().len()).unwrap_or(u64::MAX)
            }
            PartitionState::Done => 0,
        }
    }
}

/// What holds a seal's cursor in the budget while the seal waits: the permit that admitted its
/// checkpoint, or nothing for a seal the engine made itself.
///
/// It is no part of what a seal says, so every hold equals every other.
#[derive(Default)]
pub(crate) struct CursorHold(Option<Box<Admitted>>);

impl CursorHold {
    /// A hold of what `permit` holds, where the engine's admission issued it.
    pub(crate) fn new(permit: Option<Permit>) -> Self {
        Self(permit.and_then(Admitted::of))
    }

    /// A hold of the bytes of `state`'s cursor, reserved from `budget`'s cursors' share.
    ///
    /// It is for a seal the engine makes itself that records a cursor: no checkpoint admitted
    /// it, and it waits for its commit as one would. The wait ends when `cancel` fires.
    ///
    /// # Errors
    ///
    /// A cancelled error once `cancel` fires, and the budget's refusal where the cursor is
    /// beyond its share or the wait reaches the deadline.
    pub(crate) async fn reserve(
        budget: &MemoryBudget,
        cancel: &CancellationToken,
        state: &PartitionState,
    ) -> Result<Self, Error> {
        let PartitionState::Cursor(cursor) = state else {
            return Ok(Self::default());
        };
        let bytes = u64::try_from(cursor.bytes().len()).unwrap_or(u64::MAX);
        let reserved = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(Error::cancelled("the attempt was cancelled")),
            reserved = budget.acquire_cursor(bytes) => reserved,
        };
        let reserved = reserved.map_err(|denied| match denied {
            Denied::Exhausted(exhausted) => Error::memory(exhausted),
            Denied::TooLarge(large) => Error::internal(large.to_string()),
        })?;
        Ok(Self(Some(Box::new(Admitted::new(bytes, reserved)))))
    }

    /// The bytes held.
    pub(crate) fn bytes(&self) -> u64 {
        self.0.as_ref().map_or(0, |admitted| admitted.bytes)
    }
}

impl PartialEq for CursorHold {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl fmt::Debug for CursorHold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CursorHold").field(&self.bytes()).finish()
    }
}
