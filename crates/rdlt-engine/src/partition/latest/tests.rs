use rdlt_connector::{Cursor, PartitionState, SegmentId};

use super::Latest;
use crate::partition::{CursorHold, Seal};

/// A seal of no rows of `partition`, resuming from `next` and answering `answers`.
fn moved(partition: usize, next: u64, answers: Option<u64>) -> Seal {
    Seal {
        partition,
        segment: SegmentId(next),
        rows: 0,
        bytes: 0,
        state: PartitionState::Cursor(Cursor::encode(1, &next).unwrap()),
        answers,
        discarded_rows: 0,
        discarded_values: 0,
        deletes_ignored: 0,
        truncates_ignored: 0,
        shred: crate::report::ShredCounts::default(),
        held: CursorHold::default(),
    }
}

#[test]
fn a_partitions_newest_seal_of_no_rows_replaces_the_one_waiting() {
    let latest = Latest::default();
    assert_eq!(
        latest.moved(moved(0, 1, Some(4))),
        Some(0),
        "the first is told"
    );
    assert_eq!(
        latest.moved(moved(0, 2, None)),
        None,
        "one message says both"
    );
    assert_eq!(latest.moved(moved(0, 3, Some(2))), None);
    // Another partition's waits beside it, told of its own.
    assert_eq!(latest.moved(moved(1, 9, None)), Some(0));
    // The newest position, answering the newest barrier any of them answered.
    assert_eq!(latest.seal(0, 0), Some(moved(0, 3, Some(4))));
    assert_eq!(latest.seal(1, 0), Some(moved(1, 9, None)));
    assert_eq!(latest.seal(0, 0), None);
}

#[test]
fn the_coordinator_is_told_again_once_it_took_what_waited() {
    let latest = Latest::default();
    assert_eq!(latest.moved(moved(0, 1, None)), Some(0));
    assert!(latest.seal(0, 0).is_some());
    assert_eq!(latest.moved(moved(0, 2, None)), Some(0));
    assert!(latest.behind(0, 7), "a signal has a message of its own");
}

#[test]
fn a_seal_of_no_rows_never_passes_a_seal_with_rows_sent_before_it() {
    let latest = Latest::default();
    assert_eq!(latest.superseded(0), None);
    assert_eq!(latest.moved(moved(0, 1, Some(3))), Some(1));
    // A seal with rows drops it: the barrier it answered goes to the seal that follows it.
    assert_eq!(latest.superseded(0), Some(3));
    assert_eq!(latest.superseded(0), None);
    // The message queued before the seals with rows takes nothing, now or once more waits.
    assert_eq!(latest.seal(0, 1), None);
    // A seal of no rows after them has a message of its own, queued after them.
    assert_eq!(latest.moved(moved(0, 2, None)), Some(3));
    assert_eq!(latest.moved(moved(0, 4, None)), None);
    assert_eq!(
        latest.seal(0, 1),
        None,
        "the earlier message still takes nothing"
    );
    assert_eq!(latest.seal(0, 3), Some(moved(0, 4, None)));
    assert_eq!(latest.seal(0, 3), None);
}

#[test]
fn signals_are_states_however_many_a_source_sends() {
    let latest = Latest::default();
    let told = (0..1_000_u64)
        .filter(|records| latest.behind(0, *records) | latest.replan(0))
        .count();
    assert_eq!(told, 1, "one message a partition, however many signals");
    assert_eq!(latest.signals(0), (Some(999), true));
    assert_eq!(latest.signals(0), (None, false));
    // Each alone is told too, and a waiting seal is no signal.
    assert!(latest.replan(0));
    assert!(!latest.replan(0));
    assert_eq!(latest.signals(0), (None, true));
    assert!(latest.behind(1, 5));
    assert_eq!(latest.moved(moved(1, 1, None)), Some(0));
    assert_eq!(latest.signals(1), (Some(5), false));
    assert!(latest.seal(1, 0).is_some());
}
