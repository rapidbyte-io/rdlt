//! What a commit publishes and what a destination acknowledges.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::destination::MergeKey;
use crate::id::{CommitSeq, Epoch, GenerationId, LoadId, SegmentId, TablePath};
use crate::state::StateChange;

/// An inclusive run of consecutive segment ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// The number of ids in the set; a set of every id, one more than a `u64` holds, has
    /// `u64::MAX`.
    pub fn len(&self) -> u64 {
        self.ranges
            .iter()
            .map(|range| (range.last.0 - range.first.0).saturating_add(1))
            .fold(0, u64::saturating_add)
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// The ids, ascending: as many as the set holds, so only a set built from ids, not ranges a
    /// peer sent, is walked so.
    pub fn iter(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.ranges
            .iter()
            .flat_map(|range| (range.first.0..=range.last.0).map(SegmentId))
    }

    /// The ranges, ascending.
    pub fn ranges(&self) -> &[SegmentRange] {
        &self.ranges
    }

    /// Whether the set and `other` hold an id in common.
    pub fn overlaps(&self, other: &Self) -> bool {
        let (mut mine, mut theirs) = (
            self.ranges.iter().peekable(),
            other.ranges.iter().peekable(),
        );
        while let (Some(left), Some(right)) = (mine.peek(), theirs.peek()) {
            if left.last < right.first {
                mine.next();
            } else if right.last < left.first {
                theirs.next();
            } else {
                return true;
            }
        }
        false
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
#[serde(deny_unknown_fields)]
pub struct CommitMeta {
    /// The load committing.
    pub load_id: LoadId,
    /// The commit's position in the load; `(load_id, commit_seq)` is the idempotence key.
    pub commit_seq: CommitSeq,
    /// The epoch the session opened with; a destination refuses commits from older epochs.
    pub epoch: Epoch,
    /// The sealed segments to publish.
    pub segments: SegmentSet,
    /// Segments of the load the engine abandoned since its last commit, never sealed.
    ///
    /// The commit removes what the session staged in them, which nothing publishes. None of them
    /// is among `segments`.
    pub abandoned: SegmentSet,
    /// State records to write in the same atomic step.
    pub state_delta: Vec<StateChange>,
    /// Replace generations to swap in with this commit.
    pub finish_generations: Vec<(TablePath, GenerationId)>,
    /// The child tables of merge tables: each follows the root rows this commit publishes,
    /// whether or not the commit stages rows of its own (see [`RootKey`]).
    ///
    /// [`RootKey`]: crate::RootKey
    pub child_tables: Vec<ChildTable>,
    /// Tables to drop with this commit, as a reset of their streams asks: each with its
    /// generations and tombstones, releasing its owner record.
    ///
    /// Only a destination that declares
    /// [`Capabilities::drop_tables`](crate::Capabilities::drop_tables) receives them.
    pub drop_tables: Vec<DroppedTable>,
    /// The oldest commit the engine may still repeat, where it says: the destination may forget
    /// the receipt of every commit before it, and keeps those of the rest.
    #[serde(deserialize_with = "Option::deserialize")]
    pub horizon: Option<Horizon>,
}

/// The oldest commit an engine may still repeat: a receipt of a commit before it is never asked
/// for again, so a destination may forget it.
///
/// Commits are ordered by their load's id, then by their sequence in it, as horizons are.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Horizon {
    /// The load of the oldest commit the engine may repeat.
    pub load_id: LoadId,
    /// That commit's position in its load.
    pub commit_seq: CommitSeq,
}

impl Horizon {
    /// Whether the engine may still repeat commit `commit_seq` of load `load_id`: it is not
    /// before the horizon, so its receipt is kept.
    pub fn keeps(&self, load_id: LoadId, commit_seq: CommitSeq) -> bool {
        (load_id, commit_seq) >= (self.load_id, self.commit_seq)
    }
}

/// A table a commit drops.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DroppedTable {
    /// The table's path, as the pipeline's state names it.
    pub path: TablePath,
    /// The table's identifier in the destination, as the pipeline's state records it.
    pub name: Arc<str>,
}

/// A child table of a merge table, as a commit lists it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildTable {
    /// The child table's identifier.
    pub table: Arc<str>,
    /// How it merges: by its root, which [`MergeKey::root`] names.
    pub merge: MergeKey,
}

/// A destination's acknowledgment of a commit.
///
/// Re-committing the same `(load_id, commit_seq)` returns the stored receipt without publishing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

impl Receipt {
    /// Whether this is the receipt of the commit `meta` describes: of its load and sequence.
    pub fn answers(&self, meta: &CommitMeta) -> bool {
        (self.load_id, self.commit_seq) == (meta.load_id, meta.commit_seq)
    }
}
