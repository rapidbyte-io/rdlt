//! A table's merging follows the writer that stages its rows, whatever the table was before.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, DestinationConnector, Field, LoadId, LogicalType,
    MergeKey, OpenContext, PipelineId, SchemaVersion, SegmentId, SegmentSet, Session, TableChange,
    TablePath, TableRef, TableSchema, TableWriter,
};
use rdlt_connector_reference::{
    FilesDestination, MemoryDestination, SqliteDestination, files, published, sqlite,
};
use serde_json::json;

fn table(merging: bool) -> TableRef {
    TableRef {
        path: TablePath::new(["switch"]).expect("valid table path"),
        name: "switch".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: merging.then(|| MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
        }),
    }
}

/// Rows `(id, v)` with sequences counting from `first`.
fn rows(values: &[&str], first: u8) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1; values.len()]));
    let names: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    let seqs: BinaryArray = (0..values.len())
        .map(|index| {
            let mut seq = [0_u8; 16];
            seq[15] = first + u8::try_from(index).expect("few rows");
            Some(seq.to_vec())
        })
        .collect();
    RecordBatch::try_from_iter([("id", ids), ("v", names), ("seq", Arc::new(seqs) as _)])
        .expect("the batch is valid")
}

/// Commits two appended rows of key 1, then a merged row, then another appended row.
async fn switch<C: DestinationConnector>(config: serde_json::Value) {
    let destination = C::connect(
        serde_json::from_value(config).expect("the configuration is valid"),
        &ConnectContext::new(),
    )
    .await
    .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("switch").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("the session opens");
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("v", LogicalType::Utf8, true),
        Field::new("seq", LogicalType::Binary, true),
    ])
    .expect("the schema is valid");
    let create = TableChange::Create {
        table: table(false),
        schema,
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut seq = CommitSeq::FIRST;
    for (segment, merging, batch) in [
        (1, false, rows(&["a", "b"], 1)),
        (2, true, rows(&["c"], 3)),
        (3, false, rows(&["d"], 4)),
    ] {
        let mut writer = opened
            .session
            .writer(&table(merging))
            .await
            .expect("a writer opens");
        writer
            .write(SegmentId(segment), batch)
            .await
            .expect("the write buffers");
        writer.flush().await.expect("the flush stages");
        let meta = CommitMeta {
            load_id: context.load_id,
            commit_seq: seq,
            epoch: opened.epoch,
            segments: [SegmentId(segment)].into_iter().collect::<SegmentSet>(),
            state_delta: Vec::new(),
            finish_generations: Vec::new(),
            child_tables: Vec::new(),
        };
        opened
            .session
            .commit(&meta)
            .await
            .expect("the commit lands");
        seq = seq.next();
    }
}

/// The sorted values of `v` in `batches`.
fn values(batches: &[RecordBatch]) -> Vec<String> {
    let mut values: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let column = arrow_cast::cast(
                batch.column_by_name("v").expect("a v column"),
                &DataType::Utf8,
            )
            .expect("v reads as text");
            column
                .as_string::<i32>()
                .iter()
                .flatten()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect();
    values.sort();
    values
}

#[tokio::test]
async fn merging_follows_the_writer_whatever_the_table_was_before() {
    let expected = ["c", "d"];
    switch::<MemoryDestination>(json!({ "store": "switch" })).await;
    assert_eq!(values(&published("switch", "switch")), expected, "memory");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("switch.db");
    switch::<SqliteDestination>(json!({ "path": path })).await;
    assert_eq!(
        values(&sqlite::published(&path, "switch").unwrap()),
        expected,
        "sqlite"
    );
    let root = directory.path().join("files");
    switch::<FilesDestination>(json!({ "root": root })).await;
    assert_eq!(
        values(&files::published(&root, "switch").unwrap()),
        expected,
        "files"
    );
}
