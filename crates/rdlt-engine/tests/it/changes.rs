//! Change streams: a snapshot, then its changes, merged by key or appended as a log.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use rdlt_connector::{ConnectContext, ReadMode, Source, source_factory};
use rdlt_connector_reference::changes::{ChangedStream, Row, expected, snapshot};
use rdlt_connector_reference::{ChangesSource, published};
use rdlt_engine::{DeleteMode, OnTruncate, RunStatus, WriteMode};
use serde_json::json;

use crate::support::targets::Target;
use crate::support::{commit_every, each, engine, example, local, memory, pipeline, stream};

pub(crate) fn orders(truncates: &[u64]) -> ChangedStream {
    ChangedStream {
        name: "orders".into(),
        keys: 40,
        snapshot_partitions: 3,
        changes: 200,
        batch_rows: 7,
        truncates: truncates.to_vec(),
        captured: 0,
    }
}

/// The source's configuration of `streams` under `seed`.
fn config(seed: u64, streams: &[ChangedStream]) -> serde_json::Value {
    let streams: Vec<serde_json::Value> = streams
        .iter()
        .map(|stream| {
            json!({
                "name": stream.name, "keys": stream.keys,
                "snapshot_partitions": stream.snapshot_partitions, "changes": stream.changes,
                "batch_rows": stream.batch_rows, "truncates": stream.truncates,
                "captured": stream.captured,
            })
        })
        .collect();
    json!({ "seed": seed, "streams": streams })
}

pub(crate) async fn changes(seed: u64, stream: &ChangedStream) -> Arc<dyn Source> {
    changes_of(seed, std::slice::from_ref(stream)).await
}

/// The change source of `streams` under `seed`.
pub(crate) async fn changes_of(seed: u64, streams: &[ChangedStream]) -> Arc<dyn Source> {
    Arc::from(
        source_factory::<ChangesSource>()
            .connect(config(seed, streams), ConnectContext::new())
            .await
            .expect("the source connects"),
    )
}

/// The change source, as [`changes`], spawned in a process of its own.
pub(crate) async fn spawned_changes(seed: u64, stream: &ChangedStream) -> Arc<dyn Source> {
    use rdlt_host::Provider as _;
    let id = rdlt_connector::ConnectorId::parse("io.rapidbyte.changes").expect("a valid id");
    let reference = rdlt_host::ConnectorRef::new(id).path(example("serve_changes"));
    let placed = local()
        .source(&reference, &config(seed, std::slice::from_ref(stream)))
        .await
        .expect("the change source starts");
    Arc::from(placed.connector)
}

/// Each published row of `table` in `store` by key: its value and counter.
pub(crate) fn rows(store: &str, table: &str) -> BTreeMap<i64, Row> {
    rows_of(&published(store, table))
}

/// Each row of `batches` by key: its value and counter; a key published twice fails.
fn rows_of(batches: &[RecordBatch]) -> BTreeMap<i64, Row> {
    let mut rows = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .expect("the table has the column")
            .as_primitive::<Int64Type>();
        let values = batch
            .column_by_name("value")
            .expect("the table has the column")
            .as_string::<i32>();
        let n = batch
            .column_by_name("n")
            .expect("the table has the column")
            .as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let value = (!values.is_null(row)).then(|| values.value(row).to_owned());
            let previous = rows.insert(
                ids.value(row),
                Row {
                    value,
                    n: n.value(row),
                },
            );
            assert!(
                previous.is_none(),
                "key {} is published twice",
                ids.value(row)
            );
        }
    }
    rows
}

#[tokio::test]
async fn a_snapshot_and_its_changes_merge_into_the_table_the_source_holds() {
    for (seed, truncates) in [(1, vec![]), (2, vec![120]), (3, vec![5, 150])] {
        let stream_spec = orders(&truncates);
        let store = format!("changes_merged_{seed}");
        let plan = pipeline(
            "changes",
            [stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge)],
        );
        let outcome = engine(commit_every(16))
            .run(
                plan,
                changes(seed, &stream_spec).await,
                memory(&store).await,
            )
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
        assert_eq!(
            rows(&store, "orders"),
            expected(seed, &stream_spec),
            "seed {seed}"
        );
    }
}

#[tokio::test]
async fn a_second_run_reads_only_the_changes_the_first_left() {
    // Logged, a change read twice lands twice; captured, the snapshot holds the first changes.
    let mut stream_spec = orders(&[]);
    stream_spec.captured = 15;
    let store = "changes_again";
    for _ in 0..2 {
        let plan = pipeline("changes", [stream("orders").read(ReadMode::Cdc)]);
        let outcome = engine(commit_every(16))
            .run(plan, changes(4, &stream_spec).await, memory(store).await)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
    }
    assert_eq!(logged(store, "orders"), log(4, &stream_spec));
}

/// The positions a change log in `store` holds, sorted.
pub(crate) fn logged(store: &str, table: &str) -> Vec<u64> {
    let mut positions = Vec::new();
    for batch in published(store, table) {
        let seqs = batch
            .column_by_name("_rdlt_seq")
            .expect("a sequence column");
        let seqs =
            arrow_cast::cast(seqs, &arrow_schema::DataType::Binary).expect("sequences are binary");
        for seq in seqs.as_binary::<i32>().iter().flatten() {
            let position = seq[8..].try_into().expect("a sequence has 16 bytes");
            positions.push(u64::from_be_bytes(position));
        }
    }
    positions.sort_unstable();
    positions
}

/// The positions `stream`'s log holds once read: a row at the captured position for each row of
/// the snapshot, then each change after it once.
pub(crate) fn log(seed: u64, stream: &ChangedStream) -> Vec<u64> {
    let snapshotted = snapshot(seed, stream).len();
    let mut positions = vec![stream.captured; snapshotted];
    positions.extend(stream.captured + 1..=stream.changes);
    positions
}

#[tokio::test]
async fn ignored_deletes_and_truncates_are_counted_and_leave_their_rows() {
    let stream_spec = orders(&[100]);
    let modes = [
        (DeleteMode::Ignore, OnTruncate::Ignore),
        (DeleteMode::Ignore, OnTruncate::Apply),
        (DeleteMode::Hard, OnTruncate::Ignore),
    ];
    for (deletes, truncates) in modes {
        let store = format!("changes_ignored_{deletes:?}_{truncates:?}");
        let plan = pipeline(
            "changes",
            [stream("orders")
                .read(ReadMode::Cdc)
                .write(WriteMode::Merge)
                .deletes(deletes)
                .on_truncate(truncates)],
        );
        let outcome = engine(commit_every(16))
            .run(plan, changes(5, &stream_spec).await, memory(&store).await)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
        let report = &outcome.report.streams["orders"];
        let context = format!("{deletes:?} {truncates:?}: {report:?}");
        assert_eq!(
            report.deletes_ignored > 0,
            deletes == DeleteMode::Ignore,
            "{context}"
        );
        let truncated = u64::from(truncates == OnTruncate::Ignore);
        assert_eq!(report.truncates_ignored, truncated, "{context}");
        if (deletes, truncates) == (DeleteMode::Ignore, OnTruncate::Ignore) {
            let published = rows(&store, "orders");
            assert!(
                published.len() >= 40,
                "a row was removed: {}",
                published.len()
            );
        }
    }
}

#[tokio::test]
async fn soft_deletes_keep_their_rows_and_record_when() {
    each(Target::IN_PROCESS, soft_deletes).await;
}

/// Soft deletes in `target`'s table: every row stays, those the source deleted with when.
async fn soft_deletes(target: Target) {
    let stream_spec = orders(&[]);
    let store = "changes_soft";
    let plan = pipeline(
        "changes",
        [stream("orders")
            .read(ReadMode::Cdc)
            .write(WriteMode::Merge)
            .deletes(DeleteMode::Soft)],
    );
    let outcome = engine(commit_every(16))
        .run(
            plan,
            changes(6, &stream_spec).await,
            target.destination(store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{target:?}: {:?}",
        outcome.error
    );
    let batches: Vec<RecordBatch> = target.published(store, "orders");
    let live = expected(6, &stream_spec);
    let mut deleted = 0;
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .expect("the table has the column")
            .as_primitive::<Int64Type>();
        let at = batch
            .column_by_name("_rdlt_deleted_at")
            .expect("a deleted-at column");
        let loaded = batch
            .column_by_name("_rdlt_loaded_at")
            .expect("a loaded-at column");
        for row in 0..batch.num_rows() {
            let id = ids.value(row);
            assert_eq!(
                at.is_null(row),
                live.contains_key(&id),
                "{target:?}: key {id}"
            );
            if !at.is_null(row) {
                // One run loads every row, so a row is deleted when its load started.
                assert!(
                    *at.slice(row, 1) == *loaded.slice(row, 1),
                    "{target:?}: key {id}"
                );
                deleted += 1;
            }
        }
    }
    assert!(deleted > 0, "{target:?}");
}

#[tokio::test]
async fn a_change_log_appends_every_change_with_its_op() {
    let stream_spec = orders(&[50]);
    let store = "changes_log";
    let plan = pipeline("changes", [stream("orders").read(ReadMode::Cdc)]);
    let outcome = engine(commit_every(16))
        .run(plan, changes(7, &stream_spec).await, memory(store).await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let mut ops = BTreeMap::new();
    let mut rows = 0;
    for batch in published(store, "orders") {
        let op = batch.column_by_name("_rdlt_op").expect("an op column");
        assert!(batch.column_by_name("_rdlt_seq").is_some());
        let op = arrow_cast::cast(op, &arrow_schema::DataType::Int8).unwrap();
        for code in op.as_primitive::<Int8Type>().values() {
            *ops.entry(*code).or_insert(0) += 1;
        }
        rows += batch.num_rows();
    }
    assert_eq!(rows, 40 + 200);
    assert_eq!(ops.get(&3), Some(&1), "{ops:?}");
}

#[tokio::test]
async fn every_destination_that_merges_changes_holds_the_table_the_source_holds() {
    each(Target::IN_PROCESS, |target| async move {
        merges_changes(target, changes(8, &orders(&[90])).await).await;
    })
    .await;
}

/// A change stream, read from `source` (seed 8, truncated at 90), merges into `target`'s table
/// as the source holds it.
pub(crate) async fn merges_changes(target: Target, source: Arc<dyn Source>) {
    let store = "merged_changes";
    let plan = pipeline(
        "changes",
        [stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge)],
    );
    let outcome = engine(commit_every(16))
        .run(plan, source, target.destination(store).await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{target:?}: {:?}",
        outcome.error
    );
    let published = rows_of(&target.published(store, "orders"));
    assert_eq!(published, expected(8, &orders(&[90])), "{target:?}");
}
