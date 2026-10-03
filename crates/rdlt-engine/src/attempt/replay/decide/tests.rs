mod phases;

use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use rdlt_connector::{
    CommitMeta, CommitSeq, Cursor, Epoch, LoadId, PartitionId, PartitionState, PipelineState,
    SegmentId, StateChange, StateEntry, StreamName,
};

use super::{Decision, decide};
use crate::wal::Positions;
use crate::wal::frame::Seal;
use crate::wal::scan::Logged;

fn stream() -> StreamName {
    StreamName::new("orders").expect("a valid stream")
}

fn partition(id: &str) -> PartitionId {
    PartitionId::parse(id).expect("a valid partition")
}

fn at(next: u64) -> PartitionState {
    PartitionState::Cursor(Cursor::encode(1, &next).expect("a cursor"))
}

fn load(random: u128) -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, random)
}

/// A seal of a stream that cannot read again.
fn seal(segment: u64, id: &str, from: Option<u64>, to: u64) -> Seal {
    Seal {
        segment: SegmentId(segment),
        stream: stream(),
        partition: partition(id),
        replayable: false,
        phase: 0,
        from: from.map(at),
        state: at(to),
        batches: 0,
        rows: 0,
    }
}

/// Load 2's second commit, of segments 3, 4 and 6: partition p0 from 10 to 20 then 30, p1 from
/// 5 to 6 with an empty segment 5, and p2, new, to 1.
fn logged() -> Logged {
    Logged {
        meta: CommitMeta {
            load_id: load(2),
            commit_seq: CommitSeq::FIRST.next(),
            epoch: Epoch(4),
            segments: [3, 4, 6].into_iter().map(SegmentId).collect(),
            state_delta: vec![position("p0", 30), position("p1", 6), position("p2", 1)],
            finish_generations: Vec::new(),
            child_tables: Vec::new(),
            drop_tables: Vec::new(),
        },
        seals: vec![
            seal(3, "p0", Some(10), 20),
            seal(4, "p0", Some(20), 30),
            seal(5, "p1", Some(5), 6),
            seal(6, "p2", None, 1),
        ],
        begun: Vec::new(),
    }
}

fn position(id: &str, next: u64) -> StateChange {
    StateChange::Put(
        StateEntry::Partition {
            stream: stream(),
            partition: partition(id),
            state: at(next),
            // As load 2's commit records it.
            load: load(2),
        }
        .to_record(),
    )
}

/// The destination's positions: `p0` and `p1` at theirs, `p2` nowhere.
fn standing(p0: u64, p1: u64) -> Positions {
    let mut positions = Positions::of(&PipelineState::default());
    positions.set(stream(), partition("p0"), at(p0));
    positions.set(stream(), partition("p1"), at(p1));
    positions
}

/// No stream reset.
fn unreset() -> BTreeMap<StreamName, Epoch> {
    BTreeMap::new()
}

fn segments(ids: &[u64]) -> rdlt_connector::SegmentSet {
    ids.iter().copied().map(SegmentId).collect()
}

#[test]
fn a_commit_the_destination_never_saw_replays_whole_where_nothing_moved_since() {
    let decision = decide(
        &standing(10, 5),
        &unreset(),
        Some((load(2), 1)),
        None,
        &logged(),
    );
    assert_eq!(
        decision,
        Decision {
            staged: segments(&[3, 4, 6]),
            moved: vec![position("p0", 30), position("p1", 6), position("p2", 1)],
            whole: true,
        }
    );
}

#[test]
fn a_load_s_first_commit_follows_what_it_opened_on() {
    let mut first = logged();
    first.meta.commit_seq = CommitSeq::FIRST;
    let opened = Some((load(1), CommitSeq::FIRST.next()));
    let decided = |last| decide(&standing(10, 5), &unreset(), last, opened, &first).whole;
    assert!(decided(Some((load(1), 2))));
    assert!(!decided(Some((load(1), 1))));
    assert!(!decided(None));
    // A pipeline's first load, on a destination that had received nothing.
    assert!(decide(&standing(10, 5), &unreset(), None, None, &first).whole);
}

#[test]
fn a_commit_that_landed_without_its_receipt_stages_nothing() {
    let mut landed = standing(30, 6);
    landed.set(stream(), partition("p2"), at(1));
    let decision = decide(&landed, &unreset(), Some((load(2), 2)), None, &logged());
    assert_eq!(
        decision,
        Decision {
            staged: segments(&[]),
            moved: Vec::new(),
            whole: false,
        }
    );
}

#[test]
fn partitions_a_newer_load_moved_are_left_to_it() {
    // A newer load committed p0 past 10 meanwhile; p1 and p2 are where this load left them.
    let decision = decide(
        &standing(25, 5),
        &unreset(),
        Some((load(3), 1)),
        None,
        &logged(),
    );
    assert_eq!(
        decision,
        Decision {
            staged: segments(&[6]),
            moved: vec![position("p1", 6), position("p2", 1)],
            whole: false,
        }
    );
}

#[test]
fn a_stream_that_reads_again_is_left_to_the_next_load_where_a_newer_one_committed() {
    // A newer load completed a full read meanwhile, leaving every partition without a position:
    // positions no longer tell whether the segments landed, and the source serves them again.
    let mut replayable = logged();
    for seal in &mut replayable.seals {
        seal.replayable = true;
    }
    let cleared = Positions::of(&PipelineState::default());
    let decision = decide(&cleared, &unreset(), Some((load(3), 1)), None, &replayable);
    assert_eq!(
        decision,
        Decision {
            staged: segments(&[]),
            moved: Vec::new(),
            whole: false,
        }
    );
    // Where nothing moved since, the whole commit replays, whether its streams read again or not.
    let untouched = decide(
        &standing(10, 5),
        &unreset(),
        Some((load(2), 1)),
        None,
        &replayable,
    );
    assert!(untouched.whole);
    assert_eq!(untouched.staged, segments(&[3, 4, 6]));
    // Where the load's own partition moved but no newer load committed, what still matches stages.
    let moved = decide(
        &standing(25, 5),
        &unreset(),
        Some((load(2), 1)),
        None,
        &replayable,
    );
    assert!(!moved.whole);
    assert_eq!(moved.staged, segments(&[6]));
}

#[test]
fn a_whole_replay_commits_the_logged_commit_under_the_replaying_epoch() {
    let mut logged = logged();
    logged.meta.finish_generations = vec![(
        rdlt_connector::TablePath::new(["orders"]).expect("a valid path"),
        rdlt_connector::GenerationId(4),
    )];
    let decision = decide(
        &standing(10, 5),
        &unreset(),
        Some((load(2), 1)),
        None,
        &logged,
    );
    let replayed = decision.replayed(&logged.meta, Epoch(9));
    assert_eq!(
        replayed,
        CommitMeta {
            epoch: Epoch(9),
            ..logged.meta.clone()
        }
    );
}

#[test]
fn a_partial_replay_commits_only_what_it_staged_and_the_positions_it_moves() {
    let mut logged = logged();
    logged.meta.finish_generations = vec![(
        rdlt_connector::TablePath::new(["orders"]).expect("a valid path"),
        rdlt_connector::GenerationId(4),
    )];
    let decision = decide(
        &standing(25, 5),
        &unreset(),
        Some((load(3), 1)),
        None,
        &logged,
    );
    let replayed = decision.replayed(&logged.meta, Epoch(9));
    assert_eq!(
        replayed,
        CommitMeta {
            epoch: Epoch(9),
            segments: segments(&[6]),
            state_delta: vec![position("p1", 6), position("p2", 1)],
            finish_generations: Vec::new(),
            ..logged.meta.clone()
        }
    );
}

/// A child table of a merge table, as a commit names one.
fn child() -> rdlt_connector::ChildTable {
    rdlt_connector::ChildTable {
        table: "orders__items".into(),
        merge: rdlt_connector::MergeKey {
            columns: vec!["_rdlt_root_id".into()],
            seq: "_rdlt_seq".into(),
            root: None,
            changes: None,
            history: None,
        },
    }
}

#[test]
fn a_replay_staging_nothing_names_no_child_table_and_no_replay_drops_one() {
    let mut logged = logged();
    logged.meta.child_tables = vec![child()];
    logged.meta.drop_tables = vec![rdlt_connector::DroppedTable {
        path: rdlt_connector::TablePath::new(["gone"]).expect("a valid path"),
        name: "gone".into(),
    }];
    // The commit landed long ago and its partitions moved on: it stages nothing.
    let mut moved_on = standing(40, 9);
    moved_on.set(stream(), partition("p2"), at(5));
    let landed = decide(&moved_on, &unreset(), Some((load(3), 1)), None, &logged);
    assert!(landed.staged.is_empty());
    let replayed = landed.replayed(&logged.meta, Epoch(9));
    assert_eq!(replayed.child_tables, []);
    assert_eq!(replayed.drop_tables, []);
    // One that stages its rows again names its children, whose rows follow its roots'.
    let staging = decide(
        &standing(10, 5),
        &unreset(),
        Some((load(2), 1)),
        None,
        &logged,
    );
    let replayed = staging.replayed(&logged.meta, Epoch(9));
    assert_eq!(replayed.child_tables, [child()]);
    assert_eq!(replayed.drop_tables, []);
}

#[test]
fn seals_of_a_stream_reset_after_their_session_opened_never_apply() {
    // A reset at epoch 5 cleared the stream's positions: p2's first seal, from nowhere, would
    // otherwise match and stage its rows into the cleared stream.
    let reset = BTreeMap::from([(stream(), Epoch(5))]);
    let cleared = Positions::of(&PipelineState::default());
    let decision = decide(&cleared, &reset, Some((load(9), 1)), None, &logged());
    assert_eq!(
        decision,
        Decision {
            staged: segments(&[]),
            moved: Vec::new(),
            whole: false,
        }
    );
    // A load that opened after the reset replays as any other; the reset's own epoch is not
    // older than itself.
    for epoch in [5, 6] {
        let mut later = logged();
        later.meta.epoch = Epoch(epoch);
        let decision = decide(&cleared, &reset, Some((load(9), 1)), None, &later);
        assert_eq!(decision.staged, segments(&[6]), "epoch {epoch}");
    }
}
