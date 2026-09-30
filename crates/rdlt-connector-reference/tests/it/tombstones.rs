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
    DestinationConnector, Field, GenerationId, LoadId, LogicalType, MergeKey, OpenContext,
    PipelineId, PublishedReader, ReadBack, SchemaVersion, SegmentId, TableChange, TablePath,
    TableRef, TableSchema, readable_destination_factory,
};
use rdlt_connector_reference::{FilesDestination, MemoryDestination, SqliteDestination};
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

/// Opens a session of `destination` for load `load`, commits `batch` to `table`, swapping in the
/// generations `finish` names, and closes it.
async fn commit_as(
    destination: &dyn Destination,
    load: u128,
    table: &TableRef,
    batch: RecordBatch,
    finish: Vec<(TablePath, GenerationId)>,
) {
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
        table: TableRef {
            generation: None,
            ..table.clone()
        },
        schema,
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = opened.session.writer(table).await.expect("a writer opens");
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
        finish_generations: finish,
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    opened.session.close().await.expect("the session closes");
}

/// Opens a session of `destination` for load `load`, commits `batch` to the table, and closes it.
async fn commit(destination: &dyn Destination, load: u128, batch: RecordBatch) {
    commit_as(destination, load, &table(), batch, Vec::new()).await;
}

/// The ids `reader` reads back from the table.
async fn ids(reader: &dyn PublishedReader) -> Vec<i64> {
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
    ids(reader.as_ref()).await
}

/// The ids `C` publishes after one session hard deletes a key, a second replaces the table whole
/// with a generation, and a third inserts the key again, sequenced before the delete: the
/// replaced table's tombstones went with it.
async fn replaced<C: DestinationConnector + ReadBack>(config: serde_json::Value) -> Vec<i64> {
    use ChangeOp::{Delete, Insert};
    let (destination, reader) = readable_destination_factory::<C>()
        .connect_reading(config, ConnectContext::new())
        .await
        .expect("the destination connects");
    let deleted = changes(&[(Some(1), 1, Insert), (Some(1), 5, Delete)]);
    commit(destination.as_ref(), 1, deleted).await;
    let generation = TableRef {
        generation: Some(GenerationId(1)),
        merge: None,
        ..table()
    };
    let replacing = changes(&[(Some(9), 0, Insert)]);
    let replacing = replacing.project(&[0, 1]).expect("the stored columns");
    let finish = vec![(table().path, GenerationId(1))];
    commit_as(destination.as_ref(), 2, &generation, replacing, finish).await;
    let inserted = changes(&[(Some(1), 2, Insert)]);
    commit(destination.as_ref(), 3, inserted).await;
    ids(reader.as_ref()).await
}

async fn files(root: &Path, format: &str) -> Vec<i64> {
    replayed::<FilesDestination>(json!({ "root": root, "format": format })).await
}

#[tokio::test]
async fn a_replayed_change_never_brings_back_a_row_the_sqlite_destination_removed() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("tombstones.db");
    let ids = replayed::<SqliteDestination>(json!({ "path": path })).await;
    assert_eq!(ids, [3, 4]);
}

#[tokio::test]
async fn a_table_replaced_whole_forgets_the_tombstones_of_the_rows_it_held() {
    let ids = replaced::<MemoryDestination>(json!({ "store": "replaced_tombstones" })).await;
    assert_eq!(ids, [1, 9], "memory");
    for format in ["jsonl", "arrow"] {
        let root = tempfile::tempdir().expect("a temporary directory");
        let config = json!({ "root": root.path(), "format": format });
        assert_eq!(
            replaced::<FilesDestination>(config).await,
            [1, 9],
            "{format}"
        );
    }
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("replaced.db");
    assert_eq!(
        replaced::<SqliteDestination>(json!({ "path": path })).await,
        [1, 9],
        "sqlite"
    );
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
