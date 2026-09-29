//! A change stream's changes, sent again by a later session, never bring back the rows a hard
//! delete or truncate removed, whichever destination merges them.

use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch};
use rdlt_connector::{
    ChangeColumns, ChangeOp, CommitMeta, CommitSeq, ConnectContext, Deletion, Destination,
    DestinationConnector, Field, LoadId, LogicalType, MergeKey, OpenContext, PipelineId, ReadBack,
    SchemaVersion, SegmentId, TableChange, TablePath, TableRef, TableSchema,
    readable_destination_factory,
};
use rdlt_connector_reference::{FilesDestination, MemoryDestination};
use serde_json::json;

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["changes"]).expect("valid table path"),
        name: "changes".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: Some(ChangeColumns {
                op: "op".into(),
                unchanged: None,
                deletion: Deletion::Hard,
            }),
        }),
    }
}

/// Changes of `(id, seq byte, op)`, a truncate's id none, as a change stream writes them.
fn changes(rows: &[(Option<i64>, u8, ChangeOp)]) -> RecordBatch {
    let seq = |byte: u8| {
        let mut bytes = vec![0; 16];
        bytes[15] = byte;
        bytes
    };
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(rows.iter().map(|row| row.0).collect::<Int64Array>()) as ArrayRef,
        ),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| seq(row.1)),
            )) as _,
        ),
        (
            "op",
            Arc::new(Int8Array::from_iter_values(
                rows.iter().map(|row| row.2.code()),
            )) as _,
        ),
    ])
    .expect("a valid batch")
}

/// Opens a session of `destination` for load `load`, commits `batch` to the table, and closes it.
async fn commit(destination: &dyn Destination, load: u128, batch: RecordBatch) {
    let context = OpenContext {
        pipeline: PipelineId::parse("tombstones").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("seq", LogicalType::Binary, false),
    ])
    .expect("the schema is valid");
    let create = TableChange::Create {
        table: table(),
        schema,
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = opened
        .session
        .writer(&table())
        .await
        .expect("a writer opens");
    writer
        .write(SegmentId(1), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: [SegmentId(1)].into_iter().collect(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    opened.session.close().await.expect("the session closes");
}

/// The ids `C` publishes after one session commits inserts, a hard delete and a truncate, and a
/// second sends the changes before them again, with one new insert.
async fn replayed<C: DestinationConnector + ReadBack>(config: serde_json::Value) -> Vec<i64> {
    use ChangeOp::{Delete, Insert, Truncate};
    let first = changes(&[
        (Some(1), 1, Insert),
        (Some(2), 2, Insert),
        (Some(1), 3, Delete),
        (None, 4, Truncate),
        (Some(3), 5, Insert),
    ]);
    let again = changes(&[
        (Some(1), 1, Insert),
        (Some(2), 2, Insert),
        (Some(4), 6, Insert),
    ]);
    let (destination, reader) = readable_destination_factory::<C>()
        .connect_reading(config, ConnectContext::new())
        .await
        .expect("the destination connects");
    for (load, batch) in [(1, first), (2, again)] {
        commit(destination.as_ref(), load, batch).await;
    }
    let batches = reader
        .published(&table())
        .await
        .expect("the table reads back");
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch.column_by_name("id").expect("an id column");
            ids.as_primitive::<Int64Type>().values().to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

async fn files(root: &Path, format: &str) -> Vec<i64> {
    replayed::<FilesDestination>(json!({ "root": root, "format": format })).await
}

#[tokio::test]
async fn a_replayed_change_never_brings_back_a_row_the_memory_destination_removed() {
    let ids = replayed::<MemoryDestination>(json!({ "store": "tombstones" })).await;
    assert_eq!(ids, [3, 4]);
}

#[tokio::test]
async fn a_replayed_change_never_brings_back_a_row_the_files_destination_removed() {
    for format in ["jsonl", "arrow"] {
        let root = tempfile::tempdir().expect("a temporary directory");
        assert_eq!(files(root.path(), format).await, [3, 4], "{format}");
    }
}
