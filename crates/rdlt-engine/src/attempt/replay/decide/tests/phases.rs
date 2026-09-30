//! Replaying a logged commit that began a stream's phase: the phase applies where the destination
//! stands before it, and each seal only in the phase the destination stands at.

use std::collections::BTreeMap;

use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, PartitionState, PipelineState, SegmentId, StateChange,
    StateEntry, StateKey,
};

use super::{at, load, partition, position, segments, stream, unreset};
use crate::attempt::replay::decide::{Decision, decide};
use crate::wal::Positions;
use crate::wal::frame::{BegunPhase, Seal};
use crate::wal::scan::Logged;

/// The changes phase 1 of the stream begins with: the snapshot's partition `s0` deleted, and
/// `changes` started at 10.
fn transition() -> BegunPhase {
    let phase = StateEntry::Phase {
        stream: stream(),
        phase: 1,
    };
    BegunPhase {
        stream: stream(),
        phase: 1,
        changes: vec![
            StateChange::Delete(StateKey::Partition(stream(), partition("s0")).encode()),
            position("changes", 10),
            StateChange::Put(phase.to_record()),
        ],
    }
}

/// Load 2's second commit, at epoch 4: it begins phase 1 and moves `changes` from 10 to 15 in
/// segment 7.
fn crossing() -> Logged {
    let begun = transition();
    let mut state_delta = begun.changes.clone();
    state_delta.push(position("changes", 15));
    Logged {
        meta: CommitMeta {
            load_id: load(2),
            commit_seq: CommitSeq::FIRST.next(),
            epoch: Epoch(4),
            segments: segments(&[7]),
            state_delta,
            finish_generations: Vec::new(),
            child_tables: Vec::new(),
            drop_tables: Vec::new(),
        },
        seals: vec![Seal {
            segment: SegmentId(7),
            stream: stream(),
            partition: partition("changes"),
            replayable: false,
            phase: 1,
            from: Some(at(10)),
            state: at(15),
        }],
        begun: vec![begun],
    }
}

/// A destination whose stream stands as `changes` leave it.
fn standing(changes: &[StateChange]) -> Positions {
    let mut positions = Positions::of(&PipelineState::default());
    positions.apply(changes);
    positions
}

/// The snapshot read: phase 0, its partition `s0` done.
fn snapshot() -> Positions {
    let done = StateEntry::Partition {
        stream: stream(),
        partition: partition("s0"),
        state: PartitionState::Done,
    };
    standing(&[StateChange::Put(done.to_record())])
}

/// The stream at `phase`, its `changes` partition at 10.
fn changing(phase: u16) -> Positions {
    let phase = StateEntry::Phase {
        stream: stream(),
        phase,
    };
    standing(&[StateChange::Put(phase.to_record()), position("changes", 10)])
}

fn nothing() -> Decision {
    Decision {
        staged: segments(&[]),
        moved: Vec::new(),
        whole: false,
    }
}

#[test]
fn a_commit_that_began_a_phase_replays_it_and_its_first_seals_where_the_destination_is_before_it() {
    // Another load committed since: the phase and the seal apply, the rest of the commit not.
    let decision = decide(
        &snapshot(),
        &unreset(),
        Some((load(3), 1)),
        None,
        &crossing(),
    );
    let mut moved = transition().changes;
    moved.push(position("changes", 15));
    assert_eq!(
        decision,
        Decision {
            staged: segments(&[7]),
            moved,
            whole: false,
        }
    );
    // Where nothing moved since, the whole commit replays.
    let untouched = decide(
        &snapshot(),
        &unreset(),
        Some((load(2), 1)),
        None,
        &crossing(),
    );
    assert!(untouched.whole);
    assert_eq!(untouched.staged, segments(&[7]));
}

#[test]
fn a_phase_the_destination_already_began_replays_only_its_seals() {
    let decision = decide(
        &changing(1),
        &unreset(),
        Some((load(3), 1)),
        None,
        &crossing(),
    );
    assert_eq!(
        decision,
        Decision {
            staged: segments(&[7]),
            moved: vec![position("changes", 15)],
            whole: false,
        }
    );
}

#[test]
fn a_phase_begun_already_is_not_begun_again_where_its_partitions_reuse_the_old_ids() {
    // Phase 1 has a partition named as the snapshot's was: its entry is not the stale one.
    let mut begun = changing(1);
    let reused = StateEntry::Partition {
        stream: stream(),
        partition: partition("s0"),
        state: at(3),
    };
    begun.apply(&[StateChange::Put(reused.to_record())]);
    let decision = decide(&begun, &unreset(), Some((load(3), 1)), None, &crossing());
    assert_eq!(decision.moved, [position("changes", 15)]);
}

#[test]
fn seals_of_a_phase_the_destination_moved_past_never_apply() {
    let decision = decide(
        &changing(2),
        &unreset(),
        Some((load(3), 1)),
        None,
        &crossing(),
    );
    assert_eq!(decision, nothing());
}

#[test]
fn a_phase_whose_entries_a_reset_cleared_replays_nothing() {
    let cleared = standing(&[]);
    // The reset's marker alone keeps the load's commit out.
    let reset = BTreeMap::from([(stream(), Epoch(5))]);
    assert_eq!(
        decide(&cleared, &reset, Some((load(9), 1)), None, &crossing()),
        nothing()
    );
    // Read again from its snapshot since, the stream holds the entries the phase deletes, but the
    // reset's marker still keeps the load's commit out.
    assert_eq!(
        decide(&snapshot(), &reset, Some((load(9), 1)), None, &crossing()),
        nothing()
    );
    // And without it, the entries the phase deletes are gone: the destination is not where the
    // load left it, so neither the phase nor its seals apply.
    assert_eq!(
        decide(&cleared, &unreset(), Some((load(9), 1)), None, &crossing()),
        nothing()
    );
}
