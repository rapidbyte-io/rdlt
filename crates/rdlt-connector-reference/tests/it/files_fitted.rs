//! A change of a column's type is checked against everything a files table holds: its
//! tombstones under their own schema, and what its session staged.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    ArrayRef, BinaryArray, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampSecondArray,
};
use rdlt_connector::{
    ChangeColumns, ChangeOp, CommitSeq, ConnectorErrorKind, Deletion, Field, LogicalType,
    OpenedSession, SegmentId, TableChange, TableRef, TableSchema, TimeUnit,
};
use rdlt_connector_reference::files;

use crate::fixtures::{connect, latest_manifest, merge_table, meta, open, tempdir};

/// A change stream's table whose deletes remove their rows.
fn changes() -> TableRef {
    let mut table = merge_table("events");
    table.merge.as_mut().expect("a key").changes = Some(ChangeColumns {
        op: "op".into(),
        unchanged: None,
        deletion: Deletion::Hard,
    });
    table
}

/// The sequences `values` as a column of `kind`, in an order `values` has too.
fn sequences(kind: &LogicalType, values: &[i8]) -> ArrayRef {
    match kind {
        LogicalType::Int8 => Arc::new(Int8Array::from(values.to_vec())),
        LogicalType::Int16 => Arc::new(Int16Array::from_iter_values(
            values.iter().map(|value| i16::from(*value)),
        )),
        LogicalType::Int32 => Arc::new(Int32Array::from_iter_values(
            values.iter().map(|value| i32::from(*value)),
        )),
        LogicalType::Int64 => Arc::new(Int64Array::from_iter_values(
            values.iter().map(|value| i64::from(*value)),
        )),
        LogicalType::Utf8 => Arc::new(StringArray::from_iter_values(
            values.iter().map(|value| format!("{value:04}")),
        )),
        _ => Arc::new(BinaryArray::from_iter_values(
            values.iter().map(|value| i128::from(*value).to_be_bytes()),
        )),
    }
}

/// Stages changes of `ids` at `seqs`, each `op`, with ids and values of the width the table has.
async fn changed(
    session: &mut OpenedSession,
    segment: u64,
    (ids, wide): (&[i32], bool),
    seqs: ArrayRef,
    op: ChangeOp,
) {
    let narrow = || Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef;
    let widened = || -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(
            ids.iter().map(|id| i64::from(*id)),
        ))
    };
    let column = || if wide { widened() } else { narrow() };
    let batch = RecordBatch::try_from_iter([
        ("id", column()),
        ("seq", seqs),
        ("v", column()),
        (
            "op",
            Arc::new(Int8Array::from(vec![op.code(); ids.len()])) as ArrayRef,
        ),
    ])
    .expect("a valid batch");
    let mut writer = session.session.writer(&changes()).await.expect("a writer");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

/// The ids the table publishes, ascending.
fn published(root: &std::path::Path) -> Vec<i64> {
    let mut ids: Vec<i64> = files::published(root, "events")
        .expect("the table reads")
        .iter()
        .flat_map(|batch| {
            let ids = batch.column_by_name("id").expect("the column");
            let ids = arrow_cast::cast(ids, &arrow_schema::DataType::Int64).expect("integers");
            ids.as_primitive::<Int64Type>().values().to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// The creation of [`changes`] with narrow ids and values and a sequence of `kind`.
fn created(kind: &LogicalType) -> TableChange {
    TableChange::Create {
        table: changes(),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int32, true),
            Field::new("seq", kind.clone(), false),
            Field::new("v", LogicalType::Int32, true),
        ])
        .expect("the schema is valid"),
    }
}

/// A table that holds a tombstone takes a widen of a value column and of its key, and the
/// tombstone still buries its key afterwards, for a sequence of `kind` in `format`.
async fn widened_over_a_tombstone(format: &str, kind: LogicalType) {
    let what = format!("{kind:?} in {format}");
    let root = tempdir().expect("a root");
    let destination = connect(root.path(), format).await;
    let mut session = open(destination.as_ref(), 1).await;
    session
        .session
        .apply_schema(&created(&kind))
        .await
        .expect(&what);
    let mut seq = CommitSeq::FIRST;
    let mut commit = async |session: &mut OpenedSession, segment: u64| {
        let commit = meta(session, 1, seq, &[segment]);
        session.session.commit(&commit).await.expect("a commit");
        seq = seq.next();
    };
    changed(
        &mut session,
        1,
        (&[1, 2], false),
        sequences(&kind, &[1, 2]),
        ChangeOp::Insert,
    )
    .await;
    commit(&mut session, 1).await;
    changed(
        &mut session,
        2,
        (&[1], false),
        sequences(&kind, &[5]),
        ChangeOp::Delete,
    )
    .await;
    commit(&mut session, 2).await;
    let (_, manifest) = latest_manifest(root.path());
    let stones = manifest["tables"]["events"]["tombstones"].as_array();
    assert_eq!(stones.map(Vec::len), Some(1), "{what}: a tombstone is kept");
    for column in ["v", "id"] {
        let widen = TableChange::Widen {
            table: changes(),
            column: column.into(),
            from: LogicalType::Int32,
            to: LogicalType::Int64,
        };
        let widening = session.session.apply_schema(&widen).await;
        widening.unwrap_or_else(|error| panic!("{what}: widening {column}: {error}"));
    }
    // The insert sent again, from before its delete, and one of another key.
    changed(
        &mut session,
        3,
        (&[1, 3], true),
        sequences(&kind, &[1, 3]),
        ChangeOp::Insert,
    )
    .await;
    commit(&mut session, 3).await;
    assert_eq!(published(root.path()), [2, 3], "{what}");
}

#[tokio::test]
async fn a_table_that_holds_tombstones_takes_a_widen_whatever_its_sequence_is() {
    use LogicalType as T;
    for format in ["jsonl", "arrow"] {
        for kind in [T::Int8, T::Int16, T::Int32, T::Int64, T::Utf8, T::Binary] {
            widened_over_a_tombstone(format, kind).await;
        }
    }
}

#[tokio::test]
async fn a_widen_a_staged_value_does_not_fit_is_refused_and_the_staged_rows_commit() {
    let seconds = LogicalType::Timestamp(TimeUnit::Second, None);
    let nanos = LogicalType::Timestamp(TimeUnit::Nanosecond, None);
    let root = tempdir().expect("a root");
    let destination = connect(root.path(), "arrow").await;
    let mut session = open(destination.as_ref(), 1).await;
    let table = merge_table("events");
    let create = TableChange::Create {
        table: table.clone(),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("held", seconds.clone(), true),
            Field::new("seq", LogicalType::Int64, false),
        ])
        .expect("the schema is valid"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("created");
    // Staged and not committed: the last day of the year 9999, which nanoseconds do not reach.
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        (
            "held",
            Arc::new(TimestampSecondArray::from(vec![253_402_214_400])) as ArrayRef,
        ),
        ("seq", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    ])
    .expect("a valid batch");
    let mut writer = session.session.writer(&table).await.expect("a writer");
    writer.write(SegmentId(1), batch).await.expect("buffers");
    writer.flush().await.expect("the flush stages");
    let widen = TableChange::Widen {
        table: table.clone(),
        column: "held".into(),
        from: seconds,
        to: nanos,
    };
    let refused = session.session.apply_schema(&widen).await;
    let refused = refused.expect_err("the staged value does not fit");
    assert_eq!(refused.kind(), ConnectorErrorKind::Data);
    assert_eq!(refused.code(), Some("schema_conflict"));
    let commit = meta(&session, 1, CommitSeq::FIRST, &[1]);
    session
        .session
        .commit(&commit)
        .await
        .expect("the row commits");
    assert_eq!(published(root.path()), [1]);
}
