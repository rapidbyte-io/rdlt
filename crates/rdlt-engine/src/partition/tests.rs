use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::{Cursor, PartitionState, SegmentId, StreamName};

use super::{Ingested, OpenSegment, conform, end_state};
use crate::error::ErrorKind;

fn table() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn stream() -> StreamName {
    StreamName::new("orders").unwrap()
}

#[test]
fn a_matching_batch_is_rebuilt_under_the_table_schema() {
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as _),
        (
            "name",
            Arc::new(StringArray::from(vec![Some("a"), None])) as _,
        ),
    ])
    .unwrap();
    let conformed = conform(&stream(), &table(), &batch).unwrap();
    assert_eq!(conformed.schema(), table());
    assert_eq!(conformed.num_rows(), 2);
}

#[test]
fn mismatched_batches_are_schema_errors() {
    let ids = || Arc::new(Int64Array::from(vec![1])) as _;
    let names = || Arc::new(StringArray::from(vec!["a"])) as _;
    let cases = [
        RecordBatch::try_from_iter([("id", ids())]).unwrap(),
        RecordBatch::try_from_iter([("id", ids()), ("other", names())]).unwrap(),
        RecordBatch::try_from_iter([("id", names()), ("name", names())]).unwrap(),
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![None])) as _),
            ("name", names()),
        ])
        .unwrap(),
    ];
    for batch in cases {
        let error = conform(&stream(), &table(), &batch).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Schema);
        assert_eq!(error.code(), Some("batch_schema_mismatch"));
        assert_eq!(error.stream(), Some(&stream()));
    }
}

fn ingested(rows: u64, cursor: Option<u64>) -> Ingested {
    Ingested {
        open: OpenSegment {
            id: SegmentId(9),
            rows,
            bytes: 0,
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
fn a_sealed_segment_carries_its_rows_and_state() {
    let open = OpenSegment {
        id: SegmentId(4),
        rows: 7,
        bytes: 70,
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
}
