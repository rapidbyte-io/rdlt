//! A change stream's table across runs whose settings change: who sequenced its rows, and the
//! columns its deletes need.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch, StringArray,
};
use rdlt_connector::{ChangeOp, OP_COLUMN, Push, ReadMode, SEQ_COLUMN, UNCHANGED_COLUMN};
use rdlt_connector_reference::changes::expected;
use rdlt_connector_reference::published;
use rdlt_engine::{DeleteMode, ErrorKind, RunStatus, WriteMode};

use crate::change_limits::Pushing;
use crate::changes::{changes, orders, rows};
use crate::support::{commit_every, engine, generator, memory, pipeline, stream};

#[tokio::test]
async fn a_table_the_engine_merged_takes_no_change_stream() {
    let store = "tables_engine_merged";
    let merged = pipeline("tables", [stream("orders").write(WriteMode::Merge)]);
    let outcome = engine(commit_every(16))
        .run(
            merged,
            generator(&[("orders", 40, 1, 8)]).await,
            memory(store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let before: Vec<RecordBatch> = published(store, "orders");
    // The engine's sequences compare above any source position, so the change stream's rows
    // would never replace them.
    let cdc = pipeline(
        "tables",
        [stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge)],
    );
    let outcome = engine(commit_every(16))
        .run(cdc, changes(13, &orders(&[])).await, memory(store).await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("the run failed");
    assert_eq!(error.kind(), ErrorKind::Config, "{error}");
    assert_eq!(error.code(), Some("table_sequences_mismatch"), "{error}");
    assert_eq!(published(store, "orders"), before);
}

#[tokio::test]
async fn deletes_turned_soft_add_the_column_that_records_them() {
    let store = "tables_soft_later";
    let run = |deletes: DeleteMode, changed: u64| async move {
        let mut stream_spec = orders(&[]);
        stream_spec.changes = changed;
        let plan = pipeline(
            "tables",
            [stream("orders")
                .read(ReadMode::Cdc)
                .write(WriteMode::Merge)
                .deletes(deletes)],
        );
        engine(commit_every(16))
            .run(plan, changes(14, &stream_spec).await, memory(store).await)
            .await
    };
    let hard = run(DeleteMode::Hard, 100).await;
    assert_eq!(hard.report.status, RunStatus::Succeeded, "{:?}", hard.error);
    let soft = run(DeleteMode::Soft, 300).await;
    assert_eq!(soft.report.status, RunStatus::Succeeded, "{:?}", soft.error);
    let deleted: usize = published(store, "orders")
        .iter()
        .map(|batch| {
            let at = batch
                .column_by_name("_rdlt_deleted_at")
                .expect("the table records soft deletes");
            batch.num_rows() - arrow_array::Array::null_count(at.as_ref())
        })
        .sum();
    assert!(deleted > 0, "the later deletes are recorded");
}

/// A change batch updating `id` to `value` at `position`.
fn update(id: i64, value: &str, position: u64) -> RecordBatch {
    let mut seq = [0_u8; 16];
    seq[8..].copy_from_slice(&position.to_be_bytes());
    let seqs = FixedSizeBinaryArray::try_from_iter([seq].into_iter()).expect("16-byte sequences");
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![id])) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(vec![value])) as ArrayRef,
        ),
        ("n", Arc::new(Int64Array::from(vec![-1])) as ArrayRef),
        (
            OP_COLUMN,
            Arc::new(Int8Array::from(vec![ChangeOp::Update.code()])) as ArrayRef,
        ),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
        (
            UNCHANGED_COLUMN,
            Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as ArrayRef,
        ),
    ])
    .expect("a valid change batch")
}

#[tokio::test]
async fn a_change_older_than_the_stored_row_is_ignored_in_a_later_run() {
    let stream_spec = orders(&[]);
    let store = "tables_replayed";
    let plan = || {
        pipeline(
            "tables",
            [stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge)],
        )
    };
    let first = engine(commit_every(16))
        .run(plan(), changes(15, &stream_spec).await, memory(store).await)
        .await;
    assert_eq!(
        first.report.status,
        RunStatus::Succeeded,
        "{:?}",
        first.error
    );
    // A key a later change set, replayed at a position before that change.
    let table = expected(15, &stream_spec);
    let (&id, row) = table
        .iter()
        .find(|(_, row)| row.n > 1)
        .expect("a key a change set");
    let stale = u64::try_from(row.n - 1).expect("a change's position");
    let replaying = Arc::new(Pushing {
        inner: changes(15, &stream_spec).await,
        push: Some(Push::Changes(update(id, "stale", stale))),
        phased: false,
    });
    let second = engine(commit_every(16))
        .run(plan(), replaying, memory(store).await)
        .await;
    assert_eq!(
        second.report.status,
        RunStatus::Succeeded,
        "{:?}",
        second.error
    );
    assert_eq!(rows(store, "orders"), table);
}
