//! The memory and SQLite destinations reading back what they published, and nothing staged.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, DestinationConnector, Field, LoadId, LogicalType,
    OpenContext, OpenedSession, PipelineId, ReadBack, SchemaVersion, SegmentId, TableChange,
    TablePath, TableRef, TableSchema, readable_destination_factory,
};
use rdlt_connector_reference::{MemoryDestination, SqliteDestination, tables};
use serde_json::json;

fn table(name: &str) -> TableRef {
    TableRef {
        path: TablePath::new([name]).expect("valid table path"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

/// Creates `table` and stages `ids` in `segment`.
async fn stage(opened: &mut OpenedSession, table: &TableRef, segment: u64, ids: Vec<i64>) {
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
        .expect("the schema is valid");
    let create = TableChange::Create {
        table: table.clone(),
        schema,
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = opened.session.writer(table).await.expect("a writer opens");
    let batch = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as _)])
        .expect("the batch is valid");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

/// The ids `C` reads back from table `published` after committing ids 1 and 2 to it, with ids 3
/// and 4 staged in another segment and never committed.
async fn read_back<C: DestinationConnector + ReadBack>(config: serde_json::Value) -> Vec<i64> {
    let (destination, reader) = readable_destination_factory::<C>()
        .connect_reading(config, ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("reader").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let published = table("published");
    stage(&mut opened, &published, 1, vec![1, 2]).await;
    stage(&mut opened, &published, 2, vec![3, 4]).await;
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: [SegmentId(1)].into_iter().collect(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    let batches = reader
        .published(&published)
        .await
        .expect("the table reads back");
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn the_memory_destination_reads_back_what_it_published() {
    let ids = read_back::<MemoryDestination>(json!({ "store": "read_back" })).await;
    assert_eq!(ids, [1, 2]);
}

#[tokio::test]
async fn the_sqlite_destination_reads_back_what_it_published() {
    let directory = crate::fixtures::tempdir().expect("a temporary directory");
    let path = directory.path().join("read_back.db");
    let ids = read_back::<SqliteDestination>(json!({ "path": path })).await;
    assert_eq!(ids, [1, 2]);
}

#[tokio::test]
async fn a_memory_store_lists_every_table_created_in_it() {
    let (destination, _reader) = readable_destination_factory::<MemoryDestination>()
        .connect_reading(json!({ "store": "listed" }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("lister").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    for name in ["second", "first"] {
        stage(&mut opened, &table(name), 1, vec![1]).await;
    }
    let mut listed = tables("listed");
    listed.sort();
    assert_eq!(listed, ["first", "second"]);
}
