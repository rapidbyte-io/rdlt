//! Deciding what a logged commit's replay stages and records, from where the destination stands.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::{CommitMeta, CommitSeq, Epoch, LoadId, SegmentSet, StateChange, StateEntry};

use crate::wal::Positions;
use crate::wal::scan::Logged;

/// What replaying a logged commit does.
#[derive(Debug, PartialEq)]
pub(super) struct Decision {
    /// The segments to stage again.
    pub(super) staged: SegmentSet,
    /// The positions of the partitions the commit still moves.
    pub(super) moved: Vec<StateChange>,
    /// Whether the destination stands exactly where the commit's load left it, so the commit's
    /// whole state applies.
    pub(super) whole: bool,
}

impl Decision {
    /// The commit that replays `meta`, as it decided, under the replaying session's `epoch`.
    ///
    /// Where another load committed since, only the partitions the commit still moves change,
    /// and no generation swaps in: the rest of its state is older than the destination's.
    pub(super) fn replayed(self, meta: &CommitMeta, epoch: Epoch) -> CommitMeta {
        let (state_delta, finish_generations) = if self.whole {
            (meta.state_delta.clone(), meta.finish_generations.clone())
        } else {
            (self.moved, Vec::new())
        };
        CommitMeta {
            epoch,
            segments: self.staged,
            state_delta,
            finish_generations,
            ..meta.clone()
        }
    }
}

/// What replaying `logged` does where the destination holds `positions` and last received
/// `last`, a load and commit number; `opened` is what the load's log says it last received when
/// it opened.
///
/// Where the destination stands as the commit's load left it, the whole commit applies. Where a
/// newer load committed since, only seals of streams that cannot read again apply, each where
/// its partition stands where the seal says it started: elsewhere, the newer load committed the
/// partition, or the commit itself landed and only its receipt was lost. A stream that reads again
/// is left to the next load, which reads whatever is not committed: its positions cannot tell
/// whether its segments landed, as a completed full read leaves every partition without one.
pub(super) fn decide(
    positions: &Positions,
    last: Option<(LoadId, u64)>,
    opened: Option<(LoadId, CommitSeq)>,
    logged: &Logged,
) -> Decision {
    let meta = &logged.meta;
    let previous = if meta.commit_seq == CommitSeq::FIRST {
        opened.map(|(load, seq)| (load, seq.get()))
    } else {
        Some((meta.load_id, meta.commit_seq.get() - 1))
    };
    let untouched = last == previous;
    let mut positions = positions.clone();
    let mut staged = SegmentSet::new();
    let mut moved = BTreeMap::new();
    let mut matched = true;
    for seal in &logged.seals {
        let stale = !untouched && seal.replayable;
        if stale || positions.get(&seal.stream, &seal.partition) != seal.from.as_ref() {
            matched = false;
            continue;
        }
        let partition = (seal.stream.clone(), seal.partition.clone());
        positions.set(partition.0.clone(), partition.1.clone(), seal.state.clone());
        moved.insert(partition, seal.state.clone());
        if meta.segments.contains(seal.segment) {
            staged.insert(seal.segment);
        }
    }
    let moved = moved
        .into_iter()
        .map(|((stream, partition), state)| {
            let entry = StateEntry::Partition {
                stream,
                partition,
                state,
            };
            StateChange::Put(entry.to_record())
        })
        .collect();
    Decision {
        staged,
        moved,
        whole: matched && untouched,
    }
}
