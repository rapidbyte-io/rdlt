//! Deciding what a logged commit's replay stages and records, from where the destination stands.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, LoadId, SegmentSet, StateChange, StateEntry, StateKey, StreamName,
};

use crate::wal::Positions;
use crate::wal::frame::BegunPhase;
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
    ///
    /// A replay drops no table. Child tables follow the root rows a commit publishes, so a commit
    /// that stages nothing names none: one that landed long ago changes nothing again.
    pub(super) fn replayed(self, meta: &CommitMeta, epoch: Epoch) -> CommitMeta {
        let (state_delta, finish_generations) = if self.whole {
            (meta.state_delta.clone(), meta.finish_generations.clone())
        } else {
            (self.moved, Vec::new())
        };
        let child_tables = if self.staged.is_empty() {
            Vec::new()
        } else {
            meta.child_tables.clone()
        };
        CommitMeta {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
            epoch,
            segments: self.staged,
            state_delta,
            finish_generations,
            child_tables,
            drop_tables: Vec::new(),
            horizon: None,
        }
    }
}

/// What replaying `logged` does where the destination holds `positions` and `resets`, and last
/// received `last`, a load and commit number; `opened` is what the load's log says it last
/// received when it opened.
///
/// Where the destination stands as the commit's load left it, the whole commit applies. Where a
/// newer load committed since, only seals of streams that cannot read again apply, each where
/// its partition stands where the seal says it started: elsewhere, the newer load committed the
/// partition, or the commit itself landed and only its receipt was lost. A stream that reads again
/// is left to the next load, which reads whatever is not committed: its positions cannot tell
/// whether its segments landed, as a completed full read leaves every partition without one.
///
/// A seal of a stream reset after the commit's session opened, which the reset's epoch marks,
/// never applies: the reset cleared what the load read of it.
///
/// A phase the commit began applies first, where the destination still stands before it with
/// every entry the phase deletes: a destination that moved on, or was reset, keeps its own. A
/// seal then applies only in the phase the destination stands at.
pub(super) fn decide(
    positions: &Positions,
    resets: &BTreeMap<StreamName, Epoch>,
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
    let reset = |stream: &StreamName| {
        resets
            .get(stream)
            .is_some_and(|marker| meta.epoch < *marker)
    };
    let mut positions = positions.clone();
    let mut transitions = Vec::new();
    let mut matched = true;
    for begun in &logged.begun {
        if reset(&begun.stream) || !begins(&positions, begun) {
            matched = false;
            continue;
        }
        positions.apply(&begun.changes);
        transitions.extend(begun.changes.iter().cloned());
    }
    let mut staged = SegmentSet::new();
    let mut moved = BTreeMap::new();
    for seal in &logged.seals {
        let stale = !untouched && seal.replayable;
        let elsewhere = seal.phase != positions.phase(&seal.stream)
            || positions.get(&seal.stream, &seal.partition) != seal.from.as_ref();
        if stale || reset(&seal.stream) || elsewhere {
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
    let moved = transitions
        .into_iter()
        .chain(moved.into_iter().map(|((stream, partition), state)| {
            let entry = StateEntry::Partition {
                stream,
                partition,
                state,
                load: meta.load_id,
            };
            StateChange::Put(entry.to_record())
        }))
        .collect();
    Decision {
        staged,
        moved,
        whole: matched && untouched,
    }
}

/// Whether `begun`, a phase a logged commit began, applies where the destination holds
/// `positions`: the destination stands before the phase, with every entry of the phase before it
/// the commit deletes.
///
/// A phase begins only once every end of the phase before it is committed, and a newer load that
/// finds them begins the phase in its first commit, so a destination before the phase holds those
/// entries unless a reset or another load's reading of the phase moved it on. Such a destination
/// keeps its own: the phase and its seals are skipped, and a source that cannot read again refuses
/// the read from before what it acknowledged that follows, failing the run rather than losing
/// changes.
fn begins(positions: &Positions, begun: &BegunPhase) -> bool {
    let stale = begun.changes.iter().all(|change| match change {
        StateChange::Delete(key) => match StateKey::parse(key) {
            Ok(StateKey::Partition(stream, partition)) => {
                positions.get(&stream, &partition).is_some()
            }
            _ => true,
        },
        StateChange::Put(_) => true,
    });
    positions.phase(&begun.stream) < begun.phase && stale
}
