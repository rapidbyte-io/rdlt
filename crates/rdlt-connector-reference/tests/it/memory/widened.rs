//! Rows a memory table holds are converted to a wider type exactly, or the widen is refused.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampNanosecondType, TimestampSecondType};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, RecordBatch, TimestampNanosecondArray,
    TimestampSecondArray,
};
use rdlt_connector::{
    ConnectorErrorKind, Field, LogicalType, MergeKey, OpenedSession, SegmentId, TableChange,
    TableRef, TableSchema, TimeUnit,
};
use rdlt_connector_reference::published;

use super::owned::{meta, open, store};

fn seq(value: u8) -> Vec<u8> {
    let mut seq = vec![0; 16];
    seq[15] = value;
    seq
}

fn events() -> TableRef {
    TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
        ..super::owned::table("events", "events", None)
    }
}

async fn staged(session: &mut OpenedSession, segment: u64, id: i64, until: ArrayRef) {
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![id])) as ArrayRef),
        ("until", until),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values([seq(1)])) as _,
        ),
    ])
    .expect("a valid batch");
    let mut writer = session.session.writer(&events()).await.expect("a writer");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

/// Commits one row whose `until` is `stored` seconds, widens `until` to nanoseconds, and commits
/// another row at the unit the column then has; returns the widen's outcome.
async fn widened(store_name: &str, stored: i64) -> Result<(), rdlt_connector::ConnectorError> {
    let destination = store(store_name).await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    let seconds = LogicalType::Timestamp(TimeUnit::Second, None);
    let nanos = LogicalType::Timestamp(TimeUnit::Nanosecond, None);
    let create = TableChange::Create {
        table: events(),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("until", seconds.clone(), true),
            Field::new("seq", LogicalType::Binary, false),
        ])
        .expect("the schema is valid"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let first: ArrayRef = Arc::new(TimestampSecondArray::from(vec![stored]));
    staged(&mut session, 1, 1, first).await;
    session
        .session
        .commit(&meta(&session, 1, 1, &[1]))
        .await
        .expect("the first row commits");
    let widen = TableChange::Widen {
        table: events(),
        column: "until".into(),
        from: seconds,
        to: nanos,
    };
    let widening = session.session.apply_schema(&widen).await;
    let second: ArrayRef = match &widening {
        Ok(()) => Arc::new(TimestampNanosecondArray::from(vec![5])),
        Err(_) => Arc::new(TimestampSecondArray::from(vec![5])),
    };
    staged(&mut session, 2, 2, second).await;
    session
        .session
        .commit(&meta(&session, 1, 2, &[2]))
        .await
        .expect("the table merges at the type it has");
    widening
}

#[tokio::test]
async fn a_widen_a_stored_value_does_not_fit_is_refused_and_the_table_merges_as_it_was() {
    // The last day of the year 9999, which nanoseconds do not reach.
    let error = widened("widened-beyond", 253_402_214_400)
        .await
        .expect_err("the stored value does not fit");
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("schema_conflict"));
    let mut values: Vec<Option<i64>> = published("widened-beyond", "events")
        .iter()
        .flat_map(|batch| {
            let until = batch.column_by_name("until").expect("the column");
            until
                .as_primitive::<TimestampSecondType>()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, [Some(5), Some(253_402_214_400)]);
}

#[tokio::test]
async fn a_stored_value_a_finer_unit_holds_is_kept_exactly() {
    widened("widened-within", 9_223_372_036)
        .await
        .expect("the stored value fits");
    let mut values: Vec<Option<i64>> = published("widened-within", "events")
        .iter()
        .flat_map(|batch| {
            let until = batch.column_by_name("until").expect("the column");
            assert_eq!(until.null_count(), 0);
            until
                .as_primitive::<TimestampNanosecondType>()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, [Some(5), Some(9_223_372_036_000_000_000)]);
}
