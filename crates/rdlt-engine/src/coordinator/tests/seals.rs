//! Seals as they wait for their commit: one cursor a partition, charged until its commit lands,
//! and a commit due once the cursors hold their share of the budget.

use rdlt_connector::{Admission, Cursor, Headroom, PartitionState, SegmentId, SourceEvent};

use super::{Setup, cursor, ended, partition, position, stream};
use crate::coordinator::waiting::WaitingSeals;
use crate::cost::{Charging, Keeping};
use crate::partition::{CursorHold, Progress, Seal};
use crate::plan::WriteMode;
use crate::report::Trigger;

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
        shred: crate::report::ShredCounts::default(),
        held: CursorHold::default(),
    }
}

/// Bytes: what a seal's cursor at `next` holds.
fn cursor_bytes(next: u64) -> u64 {
    u64::try_from(cursor(next).bytes().len()).unwrap()
}

/// The segments the waiting seals counted, by partition, in order.
fn counted(seals: &WaitingSeals) -> Vec<(usize, u64)> {
    seals
        .counted()
        .map(|seal| (seal.partition, seal.segment.0))
        .collect()
}

/// Where each partition stands once the waiting seals are committed, and the cursor it is told.
fn standing(seals: &WaitingSeals) -> Vec<(usize, PartitionState, Option<Cursor>)> {
    seals
        .standing()
        .iter()
        .map(|(partition, standing)| {
            let told = standing.told().cloned();
            (*partition, standing.position.clone(), told)
        })
        .collect()
}

#[test]
fn a_seal_at_a_cursor_takes_its_partitions_place_and_one_counting_nothing_keeps_nothing_else() {
    let at = |next| PartitionState::Cursor(cursor(next));
    let mut seals = WaitingSeals::default();
    let bytes = cursor_bytes(1);
    assert!(bytes > 0);
    seals.push(seal(0, 1, 0, 1, Some(2)));
    seals.push(seal(1, 2, 0, 1, None));
    seals.push(seal(0, 3, 0, 2, None));
    assert_eq!(counted(&seals), []);
    assert_eq!(
        standing(&seals),
        [(0, at(2), Some(cursor(2))), (1, at(1), Some(cursor(1)))]
    );
    assert_eq!(seals.cursor_bytes(), 2 * bytes);
    // Seals with rows are each counted, and a partition stands at its newest cursor whatever
    // came between.
    seals.push(seal(0, 4, 5, 3, None));
    seals.push(seal(1, 5, 2, 2, None));
    seals.push(seal(0, 6, 0, 5, Some(1)));
    assert_eq!(counted(&seals), [(0, 4), (1, 5)]);
    assert_eq!(
        standing(&seals),
        [(0, at(5), Some(cursor(5))), (1, at(2), Some(cursor(2)))]
    );
    assert_eq!(seals.cursor_bytes(), 2 * bytes);
    // A seal that discarded rows counts more than its position.
    let mut discarding = seal(0, 7, 0, 6, None);
    discarding.discarded_rows = 1;
    assert!(!discarding.moves_only());
    seals.push(discarding);
    assert_eq!(counted(&seals), [(0, 4), (1, 5), (0, 7)]);
    // A seal ending its partition done keeps the cursor before it, which its source is told.
    seals.push(Seal {
        state: PartitionState::Done,
        ..seal(1, 8, 1, 0, None)
    });
    assert_eq!(
        standing(&seals),
        [
            (0, at(6), Some(cursor(6))),
            (1, PartitionState::Done, Some(cursor(2)))
        ]
    );
    assert_eq!(seals.cursor_bytes(), 2 * bytes);
    let (taken, stood) = seals.take();
    assert_eq!((taken.len(), stood.len()), (4, 2));
    assert_eq!(seals.cursor_bytes(), 0);
    // Nothing of a taken seal is replaced.
    seals.push(seal(0, 9, 0, 7, None));
    assert_eq!(standing(&seals), [(0, at(7), Some(cursor(7)))]);
    assert_eq!(seals.cursor_bytes(), bytes);
    // A cursor counts its bytes, however many.
    let far = 1 << 40;
    assert!(cursor_bytes(far) > bytes);
    seals.push(seal(1, 10, 0, far, None));
    assert_eq!(seals.cursor_bytes(), bytes + cursor_bytes(far));
}

#[test]
fn a_finished_partitions_seal_holds_no_cursor() {
    let done = Seal {
        state: PartitionState::Done,
        ..seal(0, 1, 0, 1, None)
    };
    assert!(done.moves_only());
    let mut seals = WaitingSeals::default();
    seals.push(done);
    assert_eq!(seals.cursor_bytes(), 0);
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
    ended(task).await.unwrap();
    let commits = harness.commits.lock();
    assert_eq!(commits.len(), 1);
    assert_eq!(
        commits[0].state_delta,
        [position("p0", PartitionState::Cursor(cursor(50)))]
    );
}

#[tokio::test(start_paused = true)]
async fn a_commit_is_due_once_waiting_cursors_reach_their_limit() {
    let bytes = cursor_bytes(1);
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![
            partition("p0", false),
            partition("p1", false),
            partition("p2", false),
        ],
    );
    // Half the cursors' share, a 64th of the budget, is three cursors.
    setup.budget = 3 * 128 * bytes;
    let (mut coordinator, _harness) = setup.coordinator().await;
    for partition in 0..2 {
        let segment = u64::try_from(partition).unwrap() + 1;
        coordinator.observe(Progress::Sealed(seal(partition, segment, 1, segment, None)));
        assert!(
            coordinator.commit_due().is_none(),
            "{segment} cursors of a limit of three"
        );
    }
    coordinator.observe(Progress::Sealed(seal(2, 3, 1, 3, None)));
    assert_eq!(coordinator.commit_due(), Some(Trigger::Cursors));
    coordinator.sealed.take();
    assert!(coordinator.commit_due().is_none());
    // A share of nothing, as a budget of a few bytes has, is not due without a cursor.
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.budget = 8;
    let (coordinator, _harness) = setup.coordinator().await;
    assert!(coordinator.commit_due().is_none());
}

#[tokio::test(start_paused = true)]
async fn seals_of_one_partition_at_growing_cursors_hold_one_cursor_and_are_not_due() {
    let budget = crate::budget::budget(1 << 20);
    let admission = Charging::new(budget.clone().read_by(1), Keeping::Part);
    let bytes = cursor_bytes(1);
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    // Half the cursors' share, a 64th of the budget, is three cursors.
    setup.budget = 3 * 128 * bytes;
    let (mut coordinator, _harness) = setup.coordinator().await;
    for next in 1..=8 {
        let checkpoint = SourceEvent::Checkpoint {
            cursor: cursor(next),
            answers: None,
        };
        let held = CursorHold::new(admission.admit(&checkpoint).await.unwrap());
        coordinator.observe(Progress::Sealed(Seal {
            held,
            ..seal(0, next, 1, next, None)
        }));
        assert_eq!(coordinator.sealed.cursor_bytes(), bytes, "{next} seals");
        assert_eq!(budget.reserved(), bytes, "{next} seals hold one cursor");
        assert!(coordinator.commit_due().is_none(), "{next} seals");
    }
    let collected = coordinator.collect();
    assert_eq!(collected.segments.len(), 8);
    assert_eq!(
        collected.positions.get(&0),
        Some(&PartitionState::Cursor(cursor(8)))
    );
    drop(collected);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_seals_cursor_stays_charged_until_its_commit_has_it() {
    let budget = crate::budget::budget(1 << 20);
    let admission = Charging::new(budget.clone().read_by(1), Keeping::Part);
    let checkpoint = SourceEvent::Checkpoint {
        cursor: cursor(7),
        answers: None,
    };
    let permit = admission.admit(&checkpoint).await.unwrap();
    let charged = budget.reserved();
    assert_eq!(charged, cursor_bytes(7));
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
    let collected = coordinator.collect();
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
    assert!(harness.latest.behind(0, 40, Headroom::default()));
    assert!(!harness.latest.behind(0, 12, Headroom::default()));
    coordinator.observe(Progress::Signalled { partition: 0 });
    assert_eq!(
        harness.log.lock().behind.get(&super::name()),
        Some(&(12, Headroom::default()))
    );
    assert_eq!(harness.latest.moved(seal(0, 1, 0, 3, Some(6))), Some(0));
    coordinator.observe(Progress::Moved {
        partition: 0,
        epoch: 0,
    });
    assert_eq!(coordinator.sealed.standing().len(), 1);
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
    assert_eq!(coordinator.sealed.standing().len(), 1);
    assert_eq!(
        harness.log.lock().behind.get(&super::name()),
        Some(&(12, Headroom::default()))
    );
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
    assert!(coordinator.sealed.standing().is_empty());
    coordinator.observe(Progress::Sealed(seal(0, 2, 5, 2, None)));
    coordinator.observe(Progress::Moved {
        partition: 0,
        epoch: second,
    });
    assert_eq!(counted(&coordinator.sealed), [(0, 2)]);
    let collected = coordinator.collect();
    assert_eq!(
        collected.positions.get(&0),
        Some(&PartitionState::Cursor(cursor(3)))
    );
}
