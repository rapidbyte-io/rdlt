//! Child tables of merge tables: they follow their roots in the SQLite destination, and the files
//! destination rewrites them only where their roots' rows change what they hold.

use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{
    ChildTable, CommitMeta, CommitSeq, ConnectContext, Destination, LoadId, MergeKey, OpenContext,
    OpenedSession, PipelineId, RootKey, SchemaVersion, SegmentId, TableChange, TablePath, TableRef,
    TableSchema, destination_factory,
};
use rdlt_connector_reference::{FilesDestination, SqliteDestination, sqlite};
use serde_json::json;

fn table(name: &str, merge: MergeKey) -> TableRef {
    TableRef {
        path: TablePath::new([name]).expect("valid table path"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(merge),
    }
}

/// The root table, merging by `id`, and its child table `items`.
fn family() -> (TableRef, TableRef) {
    let roots = table(
        "roots",
        MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        },
    );
    let items = table(
        "items",
        MergeKey {
            columns: vec!["root".into()],
            seq: "seq".into(),
            root: Some(RootKey {
                table: "roots".into(),
                id: "rid".into(),
                seq: "seq".into(),
            }),
            changes: None,
            history: None,
        },
    );
    (roots, items)
}

fn bytes(byte: u8) -> Vec<u8> {
    vec![byte; 16]
}

/// Root rows of `(id, id byte, seq byte)`.
fn roots(rows: &[(i64, u8, u8)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
        ),
        (
            "rid",
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| bytes(row.1)),
            )) as _,
        ),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| bytes(row.2)),
            )) as _,
        ),
    ])
    .expect("a valid batch")
}

/// Child rows of `(value, root id byte, seq byte)`.
fn items(rows: &[(&str, u8, u8)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "value",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
        ),
        (
            "root",
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| bytes(row.1)),
            )) as _,
        ),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| bytes(row.2)),
            )) as _,
        ),
    ])
    .expect("a valid batch")
}

async fn write(opened: &mut OpenedSession, table: &TableRef, segment: u64, batch: RecordBatch) {
    let schema = TableSchema::from_arrow(&batch.schema()).expect("a schema");
    let create = TableChange::Create {
        table: table.clone(),
        schema,
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = opened.session.writer(table).await.expect("a writer");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

fn meta(opened: &OpenedSession, seq: CommitSeq, segment: u64, items: &TableRef) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: seq,
        epoch: opened.epoch,
        segments: [SegmentId(segment)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: vec![ChildTable {
            table: Arc::clone(&items.name),
            merge: items.merge.clone().expect("a merge key"),
        }],
        drop_tables: Vec::new(),
        horizon: None,
    }
}

fn context() -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse("children").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    }
}

/// The paths of the files under `dir` a commit `seq` wrote for `table` by merging.
fn merged_files(dir: &Path, seq: u64, table: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let path = path.to_string_lossy().replace('\\', "/");
                // `merged/<load>-<commit>-<random>/<table>/...`
                let written = path.split_once("/merged/").and_then(|(_, rest)| {
                    let (commit, rest) = rest.split_once('/')?;
                    let number = commit.rsplit('-').nth(1)?;
                    Some(number == seq.to_string() && rest.starts_with(&format!("{table}/")))
                });
                if written == Some(true) {
                    found.push(path);
                }
            }
        }
    }
    found
}

#[tokio::test]
async fn a_child_table_none_of_whose_roots_changed_is_not_rewritten() {
    let dir = crate::fixtures::tempdir().expect("a temporary directory");
    let destination: Box<dyn Destination> = destination_factory::<FilesDestination>()
        .connect(
            json!({ "root": dir.path(), "format": "jsonl" }),
            ConnectContext::new(),
        )
        .await
        .expect("the files destination connects");
    let mut opened = destination.open(&context()).await.expect("the root opens");
    let (root_table, item_table) = family();
    write(&mut opened, &root_table, 1, roots(&[(1, 1, 1), (2, 2, 2)])).await;
    write(&mut opened, &item_table, 1, items(&[("a", 2, 2)])).await;
    let first = meta(&opened, CommitSeq::FIRST, 1, &item_table);
    opened
        .session
        .commit(&first)
        .await
        .expect("the first commit");
    write(&mut opened, &root_table, 2, roots(&[(1, 1, 3)])).await;
    let second = meta(&opened, CommitSeq::FIRST.next(), 2, &item_table);
    opened
        .session
        .commit(&second)
        .await
        .expect("the second commit");
    assert!(
        merged_files(dir.path(), 2, "items").is_empty(),
        "root 1 had no children, so items stays as it was"
    );
    assert!(
        !merged_files(dir.path(), 2, "roots").is_empty(),
        "the roots merged"
    );
}

#[tokio::test]
async fn a_sqlite_child_table_follows_a_root_that_merged_where_the_child_staged_nothing() {
    let dir = crate::fixtures::tempdir().expect("a temporary directory");
    let path = dir.path().join("children.db");
    let destination: Box<dyn Destination> = destination_factory::<SqliteDestination>()
        .connect(json!({ "path": path }), ConnectContext::new())
        .await
        .expect("the sqlite destination connects");
    let mut opened = destination
        .open(&context())
        .await
        .expect("the database opens");
    let (root_table, item_table) = family();
    write(&mut opened, &root_table, 1, roots(&[(1, 1, 1), (2, 2, 2)])).await;
    write(
        &mut opened,
        &item_table,
        1,
        items(&[("a", 1, 1), ("b", 2, 2)]),
    )
    .await;
    let first = meta(&opened, CommitSeq::FIRST, 1, &item_table);
    opened
        .session
        .commit(&first)
        .await
        .expect("the first commit");
    write(&mut opened, &root_table, 2, roots(&[(1, 1, 3)])).await;
    let second = meta(&opened, CommitSeq::FIRST.next(), 2, &item_table);
    opened
        .session
        .commit(&second)
        .await
        .expect("the second commit");
    let mut values: Vec<String> = sqlite::published(&path, "items")
        .expect("the child table reads")
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name("value")
                .expect("a value column")
                .as_string::<i32>();
            values
                .iter()
                .flatten()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect();
    values.sort();
    assert_eq!(values, ["b"], "root 1 now has no children");
}
