//! Rows a files table holds fit a wider type exactly, or the widen is refused.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampNanosecondType, TimestampSecondType};
use arrow_array::{
    ArrayRef, Int32Array, Int64Array, RecordBatch, TimestampNanosecondArray, TimestampSecondArray,
};
use rdlt_connector::{
    CommitSeq, ConnectorError, ConnectorErrorKind, Field, LogicalType, OpenedSession, SegmentId,
    TableChange, TableSchema, TimeUnit,
};
use rdlt_connector_reference::files;

use crate::fixtures::{connect, merge_table, meta, open, tempdir};

async fn staged(session: &mut OpenedSession, segment: u64, id: i64, held: ArrayRef) {
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![id])) as ArrayRef),
        ("held", held),
        ("seq", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    ])
    .expect("a valid batch");
    let table = merge_table("events");
    let mut writer = session.session.writer(&table).await.expect("a writer");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

/// Commits one row holding `first` as `from`, widens the column to `to`, and commits another
/// row, `fitted` where the widen was taken and `kept` where it was refused; the widen's outcome
/// and the root the table is under.
async fn widened(
    format: &str,
    (from, to): (LogicalType, LogicalType),
    first: ArrayRef,
    (fitted, kept): (ArrayRef, ArrayRef),
) -> (Result<(), ConnectorError>, tempfile::TempDir) {
    let root = tempdir().expect("a root");
    let destination = connect(root.path(), format).await;
    let mut session = open(destination.as_ref(), 1).await;
    let create = TableChange::Create {
        table: merge_table("events"),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("held", from.clone(), true),
            Field::new("seq", LogicalType::Int64, false),
        ])
        .expect("the schema is valid"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    staged(&mut session, 1, 1, first).await;
    session
        .session
        .commit(&meta(&session, 1, CommitSeq::FIRST, &[1]))
        .await
        .expect("the first row commits");
    let widen = TableChange::Widen {
        table: merge_table("events"),
        column: "held".into(),
        from,
        to,
    };
    let widening = session.session.apply_schema(&widen).await;
    let second = if widening.is_ok() { fitted } else { kept };
    staged(&mut session, 2, 2, second).await;
    session
        .session
        .commit(&meta(&session, 1, CommitSeq::FIRST.next(), &[2]))
        .await
        .expect("the table merges at the type it has");
    (widening, root)
}

#[tokio::test]
async fn a_widen_a_published_value_does_not_fit_is_refused_and_the_table_merges_as_it_was() {
    let seconds = LogicalType::Timestamp(TimeUnit::Second, None);
    let nanos = LogicalType::Timestamp(TimeUnit::Nanosecond, None);
    // The last day of the year 9999, which nanoseconds do not reach.
    let first: ArrayRef = Arc::new(TimestampSecondArray::from(vec![253_402_214_400]));
    let fitted: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![5]));
    let kept: ArrayRef = Arc::new(TimestampSecondArray::from(vec![5]));
    let (widening, root) = widened("arrow", (seconds, nanos), first, (fitted, kept)).await;
    let error = widening.expect_err("the published value does not fit");
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("schema_conflict"));
    let mut values: Vec<Option<i64>> = files::published(root.path(), "events")
        .expect("the table reads")
        .iter()
        .flat_map(|batch| {
            let held = batch.column_by_name("held").expect("the column");
            let held = held.as_primitive::<TimestampSecondType>();
            held.iter().collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, [Some(5), Some(253_402_214_400)]);
}

#[tokio::test]
async fn a_published_value_a_wider_type_holds_is_kept_exactly() {
    let seconds = LogicalType::Timestamp(TimeUnit::Second, None);
    let nanos = LogicalType::Timestamp(TimeUnit::Nanosecond, None);
    let first: ArrayRef = Arc::new(TimestampSecondArray::from(vec![9_223_372_036]));
    let fitted: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![5]));
    let kept: ArrayRef = Arc::new(TimestampSecondArray::from(vec![5]));
    let (widening, root) = widened("arrow", (seconds, nanos), first, (fitted, kept)).await;
    widening.expect("the published value fits");
    let mut values: Vec<Option<i64>> = files::published(root.path(), "events")
        .expect("the table reads")
        .iter()
        .flat_map(|batch| {
            let held = batch.column_by_name("held").expect("the column");
            let held = held.as_primitive::<TimestampNanosecondType>();
            held.iter().collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, [Some(5), Some(9_223_372_036_000_000_000)]);
    // Lines hold a number as text, which a wider integer reads as it was.
    for format in ["jsonl", "arrow"] {
        let first: ArrayRef = Arc::new(Int32Array::from(vec![i32::MAX]));
        let fitted: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));
        let kept: ArrayRef = Arc::new(Int32Array::from(vec![5]));
        let types = (LogicalType::Int32, LogicalType::Int64);
        let (widening, root) = widened(format, types, first, (fitted, kept)).await;
        widening.expect("an integer fits a wider one");
        let mut values: Vec<i64> = files::published(root.path(), "events")
            .expect("the table reads")
            .iter()
            .flat_map(|batch| {
                let held = batch.column_by_name("held").expect("the column");
                held.as_primitive::<Int64Type>().values().to_vec()
            })
            .collect();
        values.sort_unstable();
        assert_eq!(values, [i64::from(i32::MAX), i64::MAX], "{format}");
    }
}
