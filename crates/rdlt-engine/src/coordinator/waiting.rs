//! The seals waiting for the next commit, and the cursor bytes they hold.

use std::collections::BTreeMap;

use rdlt_connector::{Cursor, PartitionState, SegmentId};

use crate::partition::{CursorHold, Seal};
use crate::report::ShredCounts;

/// Seals as they arrive, until a commit takes them: what each sealed segment counts, and where
/// each partition stands once they are committed.
///
/// A seal at a cursor takes its partition's place and releases the cursor before it, so however
/// many checkpoints a source sends, one cursor a partition waits; a seal that counts nothing
/// keeps nothing else. A seal that ends a partition done keeps the cursor before it, which its
/// source is still told.
#[derive(Debug, Default)]
pub(super) struct WaitingSeals {
    counted: Vec<Counted>,
    standing: BTreeMap<usize, Standing>,
    /// Bytes: the cursors the partitions stand at and are still to be told.
    cursor_bytes: u64,
}

/// What a sealed segment adds to its stream's counts.
#[derive(Debug, PartialEq)]
pub(super) struct Counted {
    pub(super) partition: usize,
    pub(super) segment: SegmentId,
    pub(super) rows: u64,
    pub(super) bytes: u64,
    pub(super) discarded_rows: u64,
    pub(super) discarded_values: u64,
    pub(super) deletes_ignored: u64,
    pub(super) truncates_ignored: u64,
    pub(super) shred: ShredCounts,
}

/// Where a partition stands once the seals waiting are committed.
#[derive(Debug)]
pub(super) struct Standing {
    pub(super) position: PartitionState,
    /// The cursor before a position of done, which the partition's source is told.
    pub(super) before: Option<Cursor>,
    /// What holds the cursors in the budget, declared after them so they are freed first.
    pub(super) held: Vec<CursorHold>,
}

impl Standing {
    /// The cursor the partition's source is told once the seals are committed.
    pub(super) fn told(&self) -> Option<&Cursor> {
        match &self.position {
            PartitionState::Cursor(cursor) => Some(cursor),
            PartitionState::Done => self.before.as_ref(),
        }
    }
}

impl WaitingSeals {
    /// Adds `seal`: its counts, unless it counts nothing, and its position in place of its
    /// partition's.
    pub(super) fn push(&mut self, seal: Seal) {
        let counts = !seal.moves_only() || seal.shred != ShredCounts::default();
        let Seal {
            partition,
            segment,
            rows,
            bytes,
            state,
            answers: _,
            discarded_rows,
            discarded_values,
            deletes_ignored,
            truncates_ignored,
            shred,
            held,
        } = seal;
        if counts {
            self.counted.push(Counted {
                partition,
                segment,
                rows,
                bytes,
                discarded_rows,
                discarded_values,
                deletes_ignored,
                truncates_ignored,
                shred,
            });
        }
        let earlier = self.standing.remove(&partition);
        if let Some(told) = earlier.as_ref().and_then(Standing::told) {
            self.cursor_bytes = self.cursor_bytes.saturating_sub(bytes_of(told));
        }
        let standing = match (state, earlier) {
            (PartitionState::Done, Some(mut earlier)) => {
                let before = match earlier.position {
                    PartitionState::Cursor(cursor) => Some(cursor),
                    PartitionState::Done => earlier.before.take(),
                };
                earlier.held.push(held);
                Standing {
                    position: PartitionState::Done,
                    before,
                    held: earlier.held,
                }
            }
            (position, _) => Standing {
                position,
                before: None,
                held: vec![held],
            },
        };
        if let Some(told) = standing.told() {
            self.cursor_bytes = self.cursor_bytes.saturating_add(bytes_of(told));
        }
        self.standing.insert(partition, standing);
    }

    /// Bytes: the cursors of the seals waiting.
    pub(super) fn cursor_bytes(&self) -> u64 {
        self.cursor_bytes
    }

    /// What the seals waiting counted, in order.
    #[cfg(test)]
    pub(super) fn counted(&self) -> impl Iterator<Item = &Counted> {
        self.counted.iter()
    }

    /// Where each partition stands once the seals waiting are committed.
    #[cfg(test)]
    pub(super) fn standing(&self) -> &BTreeMap<usize, Standing> {
        &self.standing
    }

    /// Takes every seal waiting: what each counted, in order, and where each partition stands.
    pub(super) fn take(&mut self) -> (Vec<Counted>, BTreeMap<usize, Standing>) {
        self.cursor_bytes = 0;
        (
            std::mem::take(&mut self.counted),
            std::mem::take(&mut self.standing),
        )
    }
}

/// Bytes: what `cursor` holds.
fn bytes_of(cursor: &Cursor) -> u64 {
    u64::try_from(cursor.bytes().len()).unwrap_or(u64::MAX)
}
