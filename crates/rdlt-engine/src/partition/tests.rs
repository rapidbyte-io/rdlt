use rdlt_connector::{Cursor, PartitionState, SegmentId};

use super::{Ingested, OpenSegment, end_state};

fn ingested(rows: u64, cursor: Option<u64>) -> Ingested {
    Ingested {
        open: OpenSegment {
            id: SegmentId(9),
            rows,
            ..OpenSegment::default()
        },
        last_cursor: cursor.map(|next| Cursor::encode(1, &next).unwrap()),
        stopped: false,
    }
}

#[test]
fn a_partition_ends_at_its_last_cursor_unless_rows_follow_it() {
    let cursor = Cursor::encode(1, &5u64).unwrap();
    assert_eq!(
        end_state(&ingested(0, Some(5))),
        Some(PartitionState::Cursor(cursor))
    );
    assert_eq!(end_state(&ingested(3, Some(5))), Some(PartitionState::Done));
    assert_eq!(end_state(&ingested(3, None)), Some(PartitionState::Done));
}

#[test]
fn a_partition_that_read_nothing_and_never_checkpointed_records_no_position() {
    assert_eq!(end_state(&ingested(0, None)), None);
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
    };
    let seal = open.seal(2, PartitionState::Done, Some(3));
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
