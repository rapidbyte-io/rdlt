//! The state a commit leaves: never more decoded than an open may answer, so a pipeline can open
//! again whatever it committed.

use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use rdlt_connector::{
    CommitMeta, CommitSeq, Cursor, Epoch, LoadId, PartitionId, PartitionState, PipelineState,
    SegmentSet, StateChange, StateRecord,
};

use super::{Setup, ended, name, partition, position, stream};
use crate::partition::Progress;
use crate::plan::WriteMode;
use crate::stored::{StateLimits, Stored};
use crate::wal::Positions;

/// A cursor of `bytes` padding.
fn padded(bytes: usize) -> Cursor {
    Cursor::encode(1, &"x".repeat(bytes)).unwrap()
}

/// Runs one partition sealing a row at a cursor of `bytes` padding, under a budget whose
/// answers' share holds 64 KiB: how the attempt ended, and the commits that landed.
async fn sealing(bytes: usize) -> (Result<(), crate::error::Error>, usize) {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.budget = 1 << 20;
    let (task, harness) = setup.start().await;
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Written {
        partition: 0,
        rows: 1,
        bytes: 8,
    });
    harness.seal(0, 1, 1, PartitionState::Cursor(padded(bytes)), None);
    harness.end(0, false);
    let outcome = ended(task).await;
    let commits = harness.commits.lock().len();
    (outcome, commits)
}

#[tokio::test(start_paused = true)]
async fn a_commit_whose_state_an_open_could_not_answer_is_refused_before_it_lands() {
    let (ended, commits) = sealing(32 << 10).await;
    ended.expect("state well within the bound commits");
    assert_eq!(commits, 1);
    let (ended, commits) = sealing(64 << 10).await;
    let refused = ended.expect_err("state beyond the bound is refused");
    assert_eq!(refused.code(), Some("state_bytes_exceeded"), "{refused}");
    assert_eq!(commits, 0, "nothing of it was committed");
}

#[tokio::test(start_paused = true)]
async fn state_is_bounded_with_all_the_commits_before_it() {
    // Each of two partitions' cursors is within the bound alone, and the two are not.
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 2)],
        vec![partition("p0", false), partition("p1", false)],
    );
    setup.budget = 1 << 20;
    setup.policy = crate::config::CommitPolicy::new(None, Some(1), None).unwrap();
    let (task, harness) = setup.start().await;
    for partition in 0..2 {
        harness.send(Progress::Started { partition });
        harness.send(Progress::Written {
            partition,
            rows: 1,
            bytes: 8,
        });
        harness.seal(
            partition,
            1,
            1,
            PartitionState::Cursor(padded(32 << 10)),
            None,
        );
        harness.end(partition, false);
    }
    let refused = ended(task).await.expect_err("the second commit is refused");
    assert_eq!(refused.code(), Some("state_bytes_exceeded"), "{refused}");
    assert_eq!(harness.commits.lock().len(), 1, "the first landed");
}

/// The record of `id`'s done marker.
fn done(id: &str) -> StateRecord {
    let StateChange::Put(record) = position(id, PartitionState::Done) else {
        panic!("a position is put");
    };
    record
}

#[tokio::test(start_paused = true)]
async fn done_markers_make_room_for_state_and_never_for_new_child_tables() {
    let (marker, born) = (done("d0"), done("d1"));
    // State holds the marker of a partition no plan names; the commit's record passes the limit
    // beside it and fits without it.
    let unlimited = StateLimits {
        stored: u64::MAX,
        request: u64::MAX,
    };
    let both = Stored::of(&[marker.clone(), born.clone()], unlimited).total();
    let limits = StateLimits {
        stored: both - 1,
        request: u64::MAX,
    };
    let (mut coordinator, _harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .coordinator()
    .await;
    let state = PipelineState::from_records(std::slice::from_ref(&marker)).unwrap();
    coordinator.parts.positions = Positions::of(&state);
    coordinator.parts.stored = Stored::of(&[marker], limits);
    let commit = || CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch(3),
        segments: SegmentSet::new(),
        abandoned: SegmentSet::new(),
        state_delta: vec![StateChange::Put(born.clone())],
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    };
    // A new child table's record is refused as one; no marker goes for it.
    let mut meta = commit();
    let children = [(name(), born.key.clone())];
    assert!(coordinator.relieve(&mut meta, &children).is_empty());
    assert_eq!(meta.state_delta, commit().state_delta);
    // Any other record takes the marker's room.
    let mut meta = commit();
    let forgotten = coordinator.relieve(&mut meta, &[]);
    let d0 = PartitionId::parse("d0").unwrap();
    assert_eq!(forgotten, BTreeMap::from([(name(), vec![d0])]));
    assert!(coordinator.parts.stored.admit(&meta, &[]).is_ok());
}
