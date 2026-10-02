use rdlt_connector::{Cursor, PartitionState, SegmentId};

use super::coalesce::Coalescer;
use super::{CursorHold, Ingested, OpenSegment, end_state};
use crate::config::BatchPolicy;

/// A read that ended with `received` rows after its last cursor, `written` of them written.
fn received(received: u64, written: u64, cursor: Option<u64>) -> Ingested {
    Ingested {
        open: OpenSegment {
            id: SegmentId(9),
            rows: written,
            received,
            ..OpenSegment::default()
        },
        last_cursor: cursor.map(|next| Cursor::encode(1, &next).unwrap()),
        stopped: false,
        coalescer: Coalescer::new(BatchPolicy::default()),
    }
}

fn ingested(rows: u64, cursor: Option<u64>) -> Ingested {
    received(rows, rows, cursor)
}

#[test]
fn a_partition_ends_at_its_last_cursor_unless_rows_follow_it() {
    let cursor = Cursor::encode(1, &5u64).unwrap();
    assert_eq!(
        end_state(&ingested(0, Some(5)), false),
        Some(PartitionState::Cursor(cursor))
    );
    assert_eq!(
        end_state(&ingested(3, Some(5)), false),
        Some(PartitionState::Done)
    );
    assert_eq!(
        end_state(&ingested(3, None), false),
        Some(PartitionState::Done)
    );
}

#[test]
fn rows_after_the_last_cursor_its_policy_discarded_all_of_end_the_partition() {
    assert_eq!(
        end_state(&received(2, 0, Some(5)), false),
        Some(PartitionState::Done)
    );
    assert_eq!(
        end_state(&received(2, 0, None), false),
        Some(PartitionState::Done)
    );
}

#[test]
fn an_unbounded_partition_ends_only_at_its_last_cursor_and_rows_after_it_are_read_again() {
    let cursor = Cursor::encode(1, &5u64).unwrap();
    assert_eq!(
        end_state(&ingested(0, Some(5)), true),
        Some(PartitionState::Cursor(cursor))
    );
    assert_eq!(end_state(&ingested(3, Some(5)), true), None);
    assert_eq!(end_state(&ingested(3, None), true), None);
    assert_eq!(end_state(&received(2, 0, Some(5)), true), None);
}

#[test]
fn a_partition_that_read_nothing_and_never_checkpointed_records_no_position() {
    assert_eq!(end_state(&ingested(0, None), false), None);
}

#[test]
fn a_sealed_segment_carries_its_rows_state_and_discards() {
    let open = OpenSegment {
        id: SegmentId(4),
        rows: 7,
        bytes: 70,
        received: 9,
        discarded_rows: 2,
        discarded_values: 1,
        deletes_ignored: 0,
        truncates_ignored: 0,
    };
    let seal = open.seal(2, PartitionState::Done, Some(3), CursorHold::default());
    assert_eq!(
        (
            seal.partition,
            seal.segment,
            seal.rows,
            seal.bytes,
            seal.answers
        ),
        (2, SegmentId(4), 7, 70, Some(3))
    );
    assert_eq!(seal.state, PartitionState::Done);
    assert_eq!((seal.discarded_rows, seal.discarded_values), (2, 1));
}

#[tokio::test]
async fn a_cursors_hold_shows_the_bytes_it_holds() {
    let budget = crate::budget::MemoryBudget::new(1 << 20);
    let cursor = Cursor::new(1, b"abc").unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let held = CursorHold::reserve(&budget, &cancel, &PartitionState::Cursor(cursor))
        .await
        .unwrap();
    assert_eq!(format!("{held:?}"), "CursorHold(3)");
    assert_eq!(format!("{:?}", CursorHold::default()), "CursorHold(0)");
}
