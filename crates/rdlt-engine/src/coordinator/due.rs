//! What makes a commit due: the rows and bytes it could take.
//!
//! A commit takes sealed segments. A partition that seals when a barrier asks has its rows
//! counted from when it writes them, as only a barrier, raised by rows that are due, seals them;
//! but rows it held when it did not answer a barrier in time count no more, so one that never
//! answers makes no later event wait a barrier out until it has written as much again. A
//! partition that seals on its own has its rows counted once sealed, and only then.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

/// The rows and bytes a commit could take.
#[derive(Debug, Default)]
pub(super) struct Due {
    /// Rows and bytes of the seals no commit has taken.
    sealed: Counts,
    /// Each partition that seals when a barrier asks: what it wrote since it last sealed that
    /// makes a commit due.
    unsealed: BTreeMap<usize, Counts>,
    /// The sum of `unsealed`.
    fresh: Counts,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counts {
    rows: u64,
    bytes: u64,
}

impl Counts {
    fn add(&mut self, rows: u64, bytes: u64) {
        self.rows = self.rows.saturating_add(rows);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    /// Takes up to `rows` and `bytes`; returns what it took.
    fn take(&mut self, rows: u64, bytes: u64) -> Self {
        let taken = Self {
            rows: rows.min(self.rows),
            bytes: bytes.min(self.bytes),
        };
        self.rows -= taken.rows;
        self.bytes -= taken.bytes;
        taken
    }
}

impl Due {
    /// `partition`, which seals when a barrier asks, wrote `rows` of `bytes`.
    pub(super) fn written(&mut self, partition: usize, rows: u64, bytes: u64) {
        self.unsealed.entry(partition).or_default().add(rows, bytes);
        self.fresh.add(rows, bytes);
    }

    /// `partition` sealed `rows` of `bytes`, all it wrote to the segment.
    pub(super) fn sealed(&mut self, partition: usize, rows: u64, bytes: u64) {
        if let Some(written) = self.unsealed.get_mut(&partition) {
            let taken = written.take(rows, bytes);
            self.fresh.take(taken.rows, taken.bytes);
            if *written == Counts::default() {
                self.unsealed.remove(&partition);
            }
        }
        self.sealed.add(rows, bytes);
    }

    /// `partition` abandoned what it wrote since it last sealed.
    pub(super) fn abandoned(&mut self, partition: usize) {
        if let Some(written) = self.unsealed.remove(&partition) {
            self.fresh.take(written.rows, written.bytes);
        }
    }

    /// `partition` was asked to seal and did not within the barrier's wait: what it wrote so far
    /// makes no commit due, and what it writes next does, so it is asked again only once it has
    /// written more.
    pub(super) fn unanswered(&mut self, partition: usize) {
        self.abandoned(partition);
    }

    /// A commit is about to take every seal.
    pub(super) fn committing(&mut self) {
        self.sealed = Counts::default();
    }

    /// The rows a commit could take.
    pub(super) fn rows(&self) -> u64 {
        self.sealed.rows.saturating_add(self.fresh.rows)
    }

    /// The bytes a commit could take.
    pub(super) fn bytes(&self) -> u64 {
        self.sealed.bytes.saturating_add(self.fresh.bytes)
    }
}
