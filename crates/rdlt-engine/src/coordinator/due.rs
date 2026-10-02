//! What makes a commit due: the rows and bytes it could take.
//!
//! A commit takes sealed segments. Rows a partition has written and not sealed are counted too,
//! as a barrier may seal them. A partition that seals when a barrier asks keeps its rows counted
//! until it seals them: only a barrier seals them, and only rows that are due raise one; but rows
//! it held when it did not answer a barrier in time count no more, so one that never answers
//! makes no later event wait a barrier out until it has written as much again. Rows of a
//! partition that seals on its own are passed by a commit that does not take them, and count
//! again once sealed: a partition that checkpoints only at its end would otherwise keep every
//! later event due, each a barrier and a commit of whatever else sealed.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

/// The rows and bytes a commit could take.
#[derive(Debug, Default)]
pub(super) struct Due {
    /// Rows and bytes of the seals no commit has taken.
    sealed: Counts,
    /// Each partition's rows and bytes written since it last sealed.
    unsealed: BTreeMap<usize, Unsealed>,
    /// The rows and bytes of `unsealed` no commit has passed by.
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

    fn take(&mut self, counts: Self) {
        self.rows = self.rows.saturating_sub(counts.rows);
        self.bytes = self.bytes.saturating_sub(counts.bytes);
    }
}

#[derive(Debug, Default)]
struct Unsealed {
    counts: Counts,
    /// What of `counts` makes a commit due.
    counted: Counts,
    /// Whether a barrier seals these rows: their partition seals when one asks.
    asked: bool,
    /// Whether a commit passed these rows by, their partition sealing on its own: no row it
    /// writes counts until it seals.
    passed: bool,
}

impl Due {
    /// `partition`, which seals when a barrier asks where `asked`, wrote `rows` of `bytes`.
    pub(super) fn written(&mut self, partition: usize, asked: bool, rows: u64, bytes: u64) {
        let unsealed = self.unsealed.entry(partition).or_default();
        unsealed.asked = asked;
        unsealed.counts.add(rows, bytes);
        if !unsealed.passed {
            unsealed.counted.add(rows, bytes);
            self.fresh.add(rows, bytes);
        }
    }

    /// `partition` sealed what it wrote since it last sealed.
    pub(super) fn sealed(&mut self, partition: usize) {
        if let Some(counts) = self.forget(partition) {
            self.sealed.add(counts.rows, counts.bytes);
        }
    }

    /// `partition` abandoned what it wrote since it last sealed.
    pub(super) fn abandoned(&mut self, partition: usize) {
        self.forget(partition);
    }

    /// `partition` was asked to seal and did not within the barrier's wait: what it wrote so far
    /// makes no commit due, and what it writes next does, so it is asked again only once it has
    /// written more.
    pub(super) fn unanswered(&mut self, partition: usize) {
        if let Some(unsealed) = self.unsealed.get_mut(&partition) {
            self.fresh.take(std::mem::take(&mut unsealed.counted));
        }
    }

    /// A commit is about to take every seal, and pass by every row not sealed that no barrier
    /// seals.
    pub(super) fn committing(&mut self) {
        self.sealed = Counts::default();
        self.fresh = Counts::default();
        for unsealed in self.unsealed.values_mut() {
            if unsealed.asked {
                self.fresh
                    .add(unsealed.counted.rows, unsealed.counted.bytes);
            } else {
                unsealed.passed = true;
                unsealed.counted = Counts::default();
            }
        }
    }

    /// The rows a commit could take.
    pub(super) fn rows(&self) -> u64 {
        self.sealed.rows.saturating_add(self.fresh.rows)
    }

    /// The bytes a commit could take.
    pub(super) fn bytes(&self) -> u64 {
        self.sealed.bytes.saturating_add(self.fresh.bytes)
    }

    /// Forgets what `partition` wrote since it last sealed: its counts, where it wrote any.
    fn forget(&mut self, partition: usize) -> Option<Counts> {
        let unsealed = self.unsealed.remove(&partition)?;
        self.fresh.take(unsealed.counted);
        Some(unsealed.counts)
    }
}
