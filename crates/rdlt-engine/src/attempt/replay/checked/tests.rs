use std::time::UNIX_EPOCH;

use rdlt_connector::{
    CommitMeta, CommitSeq, Cursor, Epoch, LoadId, PartitionId, PartitionState, Receipt, SegmentId,
    StateChange, StateEntry, StreamName,
};

use super::checked;
use crate::wal::frame::{BegunPhase, Seal};
use crate::wal::scan::Logged;

fn pipeline() -> rdlt_connector::PipelineId {
    rdlt_connector::PipelineId::parse("orders").expect("a valid pipeline")
}

fn stream() -> StreamName {
    StreamName::new("orders").expect("a valid stream")
}

fn load() -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, 2)
}

fn at(next: u64) -> PartitionState {
    PartitionState::Cursor(Cursor::encode(1, &next).expect("a cursor"))
}

fn position(partition: &str, next: u64, load: LoadId) -> StateChange {
    StateChange::Put(
        StateEntry::Partition {
            stream: stream(),
            partition: PartitionId::parse(partition).expect("a valid partition"),
            state: at(next),
            load,
        }
        .to_record(),
    )
}

fn receipt(load: LoadId, seq: CommitSeq) -> StateChange {
    let receipt = Receipt {
        load_id: load,
        commit_seq: seq,
        committed_at: UNIX_EPOCH,
        rows: 1,
        bytes: 1,
    };
    StateChange::Put(StateEntry::Receipt(receipt).to_record())
}

/// Load 2's commit 1 at epoch 4: p0 sealed at 20, p1 begun at 7 by a phase, its own receipt.
fn logged(delta: Vec<StateChange>) -> Logged {
    Logged {
        meta: CommitMeta {
            load_id: load(),
            commit_seq: CommitSeq::FIRST,
            epoch: Epoch(4),
            segments: [SegmentId(1)].into_iter().collect(),
            state_delta: delta,
            finish_generations: Vec::new(),
            child_tables: Vec::new(),
            drop_tables: Vec::new(),
            horizon: None,
        },
        seals: vec![Seal {
            segment: SegmentId(1),
            stream: stream(),
            partition: PartitionId::parse("p0").expect("a valid partition"),
            replayable: false,
            phase: 0,
            from: None,
            state: at(20),
            batches: 1,
            rows: 1,
        }],
        begun: vec![BegunPhase {
            stream: stream(),
            phase: 1,
            changes: vec![position("p1", 7, load())],
        }],
    }
}

fn honest() -> Vec<StateChange> {
    vec![
        position("p1", 7, load()),
        position("p0", 20, load()),
        StateChange::Delete("partition:old".to_owned()),
        receipt(load(), CommitSeq::FIRST),
    ]
}

#[test]
fn a_commit_a_load_logged_before_the_replaying_session_is_replayed() {
    checked(&logged(honest()), Epoch(5), &pipeline()).expect("as its load logged it");
}

#[test]
fn a_commit_logged_by_a_session_not_older_than_the_replaying_one_is_refused() {
    for epoch in [Epoch(4), Epoch(3)] {
        let refused = checked(&logged(honest()), epoch, &pipeline()).expect_err("not older");
        assert_eq!(refused.code(), Some("wal_unreadable"));
    }
}

#[test]
fn a_commit_recording_what_its_frames_do_not_back_is_refused() {
    let reset = StateEntry::Reset {
        stream: stream(),
        epoch: Epoch(9),
    };
    let reset_key = rdlt_connector::StateKey::Reset(stream()).encode();
    let other = LoadId::from_parts(UNIX_EPOCH, 3);
    for (case, change) in [
        ("a reset put", StateChange::Put(reset.to_record())),
        ("a reset deleted", StateChange::Delete(reset_key)),
        ("another load's receipt", receipt(other, CommitSeq::FIRST)),
        (
            "another commit's receipt",
            receipt(load(), CommitSeq::FIRST.next()),
        ),
        ("a position no seal sets", position("p0", 21, load())),
        ("a partition no seal names", position("p9", 20, load())),
        ("a position as another load's", position("p0", 20, other)),
    ] {
        let mut delta = honest();
        delta.push(change);
        let refused = checked(&logged(delta), Epoch(5), &pipeline()).expect_err(case);
        assert_eq!(refused.code(), Some("wal_unreadable"), "{case}");
    }
}
