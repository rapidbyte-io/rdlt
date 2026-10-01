//! Seals as they wait for their commit: one cursor a partition for checkpoints of no rows, each
//! charged until its commit lands, and a commit due once they hold their share of the budget.

use std::sync::Arc;

use rdlt_connector::cost::Rendering;
use rdlt_connector::{Admission, PartitionState, SegmentId, SourceEvent};

use super::{Setup, cursor, partition, position, stream, without_receipt};
use crate::budget::MemoryBudget;
use crate::coordinator::waiting::WaitingSeals;
use crate::cost::Charging;
use crate::partition::{CursorHold, Progress, Seal};
use crate::plan::WriteMode;

fn seal(partition: usize, segment: u64, rows: u64, next: u64, answers: Option<u64>) -> Seal {
    Seal {
        partition,
        segment: SegmentId(segment),
        rows,
        bytes: rows * 8,
        state: PartitionState::Cursor(cursor(next)),
        answers,
        discarded_rows: 0,
        discarded_values: 0,
        deletes_ignored: 0,
        truncates_ignored: 0,
        held: CursorHold::default(),
    }
}

fn waiting(seals: &WaitingSeals) -> Vec<(usize, u64)> {
    seals
        .iter()
        .map(|seal| (seal.partition, seal.segment.0))
        .collect()
}

#[test]
fn a_seal_of_no_rows_replaces_its_partitions_last_where_that_has_none() {
    let mut seals = WaitingSeals::default();
    let bytes = seal(0, 1, 0, 1, None).cursor_bytes();
    assert!(bytes > 0);
    seals.push(seal(0, 1, 0, 1, Some(2)));
    seals.push(seal(1, 2, 0, 1, None));
    seals.push(seal(0, 3, 0, 2, None));
    assert_eq!(waiting(&seals), [(0, 3), (1, 2)]);
    assert_eq!(seals.cursor_bytes(), 2 * bytes);
    // The replacement answers the barrier the replaced one did.
    assert_eq!(seals.iter().next().unwrap().answers, Some(2));
    // A seal with rows is kept, and so is the seal of no rows after it, which the next replaces.
    seals.push(seal(0, 4, 5, 3, None));
    seals.push(seal(0, 5, 0, 4, None));
    seals.push(seal(0, 6, 0, 5, Some(1)));
    assert_eq!(waiting(&seals), [(0, 3), (1, 2), (0, 4), (0, 6)]);
    assert_eq!(seals.cursor_bytes(), 4 * bytes);
    // A seal that discarded rows moves more than its position.
    let mut discarding = seal(0, 7, 0, 6, None);
    discarding.discarded_rows = 1;
    assert!(!discarding.moves_only());
    seals.push(discarding);
    assert_eq!(seals.iter().count(), 5);
    let taken = seals.take();
    assert_eq!(taken.len(), 5);
    assert_eq!(seals.cursor_bytes(), 0);
    // Nothing of a taken seal is replaced.
    seals.push(seal(0, 8, 0, 7, None));
    assert_eq!(waiting(&seals), [(0, 8)]);
    assert_eq!(seals.cursor_bytes(), bytes);
}

#[test]
fn a_finished_partitions_seal_holds_no_cursor() {
    let done = Seal {
        state: PartitionState::Done,
        ..seal(0, 1, 0, 1, None)
    };
    assert_eq!(done.cursor_bytes(), 0);
    assert!(done.moves_only());
    for counted in 0..5 {
        let mut seal = seal(0, 1, 0, 1, None);
        match counted {
            0 => seal.rows = 1,
            1 => seal.bytes = 1,
            2 => seal.discarded_values = 1,
            3 => seal.deletes_ignored = 1,
            _ => seal.truncates_ignored = 1,
        }
        assert!(!seal.moves_only(), "{counted}");
    }
}

#[tokio::test(start_paused = true)]
async fn checkpoints_of_no_rows_commit_only_their_partitions_newest_position() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.send(Progress::Started { partition: 0 });
    for next in 1..=50 {
        let told = harness.latest.moved(seal(0, next, 0, next, None));
        assert_eq!(told, (next == 1).then_some(0));
    }
    harness.send(Progress::Moved {
        partition: 0,
        epoch: 0,
    });
    harness.end(0, false);
    task.await.unwrap().unwrap();
    let commits = harness.commits.lock();
    assert_eq!(commits.len(), 1);
    assert_eq!(
        without_receipt(&commits[0].state_delta),
        [position("p0", PartitionState::Cursor(cursor(50)))]
    );
}

#[tokio::test(start_paused = true)]
async fn a_commit_is_due_once_waiting_cursors_reach_their_limit() {
    let bytes = seal(0, 1, 1, 1, None).cursor_bytes();
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.cursor_limit = 3 * bytes;
    let (mut coordinator, _harness) = setup.coordinator().await;
    for segment in 1..=2 {
        coordinator.observe(Progress::Sealed(seal(0, segment, 1, segment, None)));
        assert!(!coordinator.due(), "{segment} cursors of a limit of three");
    }
    coordinator.observe(Progress::Sealed(seal(0, 3, 1, 3, None)));
    assert!(coordinator.due());
    // A limit of nothing, as a budget of a few bytes has, is not due without a cursor.
    coordinator.sealed.take();
    coordinator.parts.cursor_limit = 0;
    assert!(!coordinator.due());
}

#[tokio::test(start_paused = true)]
async fn a_seals_cursor_stays_charged_until_its_commit_has_it() {
    let budget = MemoryBudget::new(1 << 20);
    let admission = Charging::new(budget.clone(), Arc::new(Rendering::text()));
    let checkpoint = SourceEvent::Checkpoint {
        cursor: cursor(7),
        answers: None,
    };
    let permit = admission.admit(&checkpoint).await;
    let charged = budget.reserved();
    assert_eq!(charged, seal(0, 1, 1, 7, None).cursor_bytes());
    let (mut coordinator, _harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .coordinator()
    .await;
    let held = CursorHold::new(permit);
    assert_eq!(held.bytes(), charged);
    coordinator.observe(Progress::Sealed(Seal {
        held,
        ..seal(0, 1, 1, 7, None)
    }));
    let collected = coordinator.collect(&[]);
    assert_eq!(budget.reserved(), charged, "held while its commit is made");
    assert_eq!(collected.held.len(), 1);
    drop(collected);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn what_a_partition_has_waiting_is_taken_as_its_newest() {
    let (mut coordinator, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", true)],
    )
    .coordinator()
    .await;
    coordinator.observe(Progress::Started { partition: 0 });
    assert!(harness.latest.behind(0, 40));
    assert!(!harness.latest.behind(0, 12));
    coordinator.observe(Progress::Signalled { partition: 0 });
    assert_eq!(harness.log.lock().behind.get(&super::name()), Some(&12));
    assert_eq!(harness.latest.moved(seal(0, 1, 0, 3, Some(6))), Some(0));
    coordinator.observe(Progress::Moved {
        partition: 0,
        epoch: 0,
    });
    assert_eq!(coordinator.sealed.iter().count(), 1);
    assert!(
        !coordinator.parts.partitions[0].owes(6),
        "its seal answered"
    );
    // A message that finds nothing waiting changes nothing.
    coordinator.observe(Progress::Moved {
        partition: 0,
        epoch: 0,
    });
    coordinator.observe(Progress::Signalled { partition: 0 });
    assert_eq!(coordinator.sealed.iter().count(), 1);
    assert_eq!(harness.log.lock().behind.get(&super::name()), Some(&12));
}

#[tokio::test(start_paused = true)]
async fn a_commit_never_takes_a_position_past_rows_still_on_their_way() {
    let (mut coordinator, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .coordinator()
    .await;
    coordinator.observe(Progress::Started { partition: 0 });
    // The partition seals no rows, then rows, then no rows again, before the coordinator has
    // taken anything: its queue holds the first message, and the seal with rows after it.
    let first = harness.latest.moved(seal(0, 1, 0, 1, None)).unwrap();
    assert_eq!(harness.latest.superseded(0), None);
    let second = harness.latest.moved(seal(0, 3, 0, 3, None)).unwrap();
    assert_ne!(first, second);
    coordinator.observe(Progress::Moved {
        partition: 0,
        epoch: first,
    });
    // A commit made now holds no position: the newest waits behind the rows before it.
    assert_eq!(coordinator.sealed.iter().count(), 0);
    coordinator.observe(Progress::Sealed(seal(0, 2, 5, 2, None)));
    coordinator.observe(Progress::Moved {
        partition: 0,
        epoch: second,
    });
    let segments: Vec<u64> = coordinator
        .sealed
        .iter()
        .map(|seal| seal.segment.0)
        .collect();
    assert_eq!(segments, [2, 3]);
    let collected = coordinator.collect(&[]);
    assert_eq!(
        collected.positions.get(&0),
        Some(&PartitionState::Cursor(cursor(3)))
    );
}
