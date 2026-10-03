//! The state a commit leaves: never more decoded than an open may answer, so a pipeline can open
//! again whatever it committed.

use rdlt_connector::{Cursor, PartitionState};

use super::{Setup, partition, stream};
use crate::partition::Progress;
use crate::plan::WriteMode;

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
    let ended = task.await.unwrap();
    let commits = harness.commits.lock().len();
    (ended, commits)
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
    let refused = task
        .await
        .unwrap()
        .expect_err("the second commit is refused");
    assert_eq!(refused.code(), Some("state_bytes_exceeded"), "{refused}");
    assert_eq!(harness.commits.lock().len(), 1, "the first landed");
}
