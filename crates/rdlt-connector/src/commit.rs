//! What a commit publishes and what a destination acknowledges.

#[cfg(test)]
mod tests;

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::id::{CommitSeq, Epoch, GenerationId, LoadId, SegmentId, TablePath};
use crate::state::StateChange;

/// An inclusive run of consecutive segment ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRange {
    /// The first id in the run.
    pub first: SegmentId,
    /// The last id in the run.
    pub last: SegmentId,
}

/// A set of segment ids, stored as sorted, disjoint, non-adjacent ranges.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<SegmentRange>", into = "Vec<SegmentRange>")]
pub struct SegmentSet {
    ranges: Vec<SegmentRange>,
}

/// Serialized segment ranges that are not sorted, disjoint and non-adjacent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("segment ranges must be ascending, disjoint and non-adjacent")]
pub struct UnorderedRanges;

impl SegmentSet {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `id`, merging it with neighbouring ranges.
    pub fn insert(&mut self, id: SegmentId) {
        let value = id.0;
        let index = self
            .ranges
            .partition_point(|range| range.last.0.saturating_add(1) < value);
        match self.ranges.get_mut(index) {
            Some(range) if range.first.0 <= value.saturating_add(1) => {
                range.first = SegmentId(range.first.0.min(value));
                range.last = SegmentId(range.last.0.max(value));
                let last = range.last.0;
                if let Some(next) = self.ranges.get(index + 1)
                    && next.first.0 <= last.saturating_add(1)
                {
                    let next_last = next.last;
                    self.ranges[index].last = next_last;
                    self.ranges.remove(index + 1);
                }
            }
            _ => self.ranges.insert(
                index,
                SegmentRange {
                    first: id,
                    last: id,
                },
            ),
        }
    }

    /// Whether `id` is in the set.
    pub fn contains(&self, id: SegmentId) -> bool {
        let index = self.ranges.partition_point(|range| range.last < id);
        self.ranges
            .get(index)
            .is_some_and(|range| range.first <= id)
    }

    /// The number of ids in the set.
    pub fn len(&self) -> u64 {
        self.ranges
            .iter()
            .map(|range| range.last.0 - range.first.0 + 1)
            .sum()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// The ids, ascending.
    pub fn iter(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.ranges
            .iter()
            .flat_map(|range| (range.first.0..=range.last.0).map(SegmentId))
    }

    /// The ranges, ascending.
    pub fn ranges(&self) -> &[SegmentRange] {
        &self.ranges
    }
}

impl FromIterator<SegmentId> for SegmentSet {
    fn from_iter<I: IntoIterator<Item = SegmentId>>(ids: I) -> Self {
        let mut set = Self::new();
        for id in ids {
            set.insert(id);
        }
        set
    }
}

impl TryFrom<Vec<SegmentRange>> for SegmentSet {
    type Error = UnorderedRanges;

    fn try_from(ranges: Vec<SegmentRange>) -> Result<Self, Self::Error> {
        let ordered = ranges.iter().all(|range| range.first <= range.last)
            && ranges
                .windows(2)
                .all(|pair| pair[0].last.0.saturating_add(1) < pair[1].first.0);
        if ordered {
            Ok(Self { ranges })
        } else {
            Err(UnorderedRanges)
        }
    }
}

impl From<SegmentSet> for Vec<SegmentRange> {
    fn from(set: SegmentSet) -> Self {
        set.ranges
    }
}

/// Everything one commit publishes: sealed segments and the state they imply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitMeta {
    /// The load committing.
    pub load_id: LoadId,
    /// The commit's position in the load; `(load_id, commit_seq)` is the idempotence key.
    pub commit_seq: CommitSeq,
    /// The epoch the session opened with; a destination refuses commits from older epochs.
    pub epoch: Epoch,
    /// The sealed segments to publish.
    pub segments: SegmentSet,
    /// State records to write in the same atomic step.
    pub state_delta: Vec<StateChange>,
    /// Replace generations to swap in with this commit.
    pub finish_generations: Vec<(TablePath, GenerationId)>,
}

/// A destination's acknowledgment of a commit.
///
/// Re-committing the same `(load_id, commit_seq)` returns the stored receipt without publishing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    /// The load that committed.
    pub load_id: LoadId,
    /// The commit's position in the load.
    pub commit_seq: CommitSeq,
    /// When the destination committed.
    pub committed_at: SystemTime,
    /// Rows published.
    pub rows: u64,
    /// Bytes published, as the destination measures them.
    pub bytes: u64,
}
