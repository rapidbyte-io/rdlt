//! Child tables of merge tables: they follow their roots in the SQLite destination, and the files
//! destination rewrites them only where their roots' rows change what they hold.

use std::path::Path;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{
    ChildTable, CommitKey, CommitMeta, ConnectContext, Destination, MergeKey, OpenContext,
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

/// Commit `seq` of `opened`, of `segment`, whose rows `items`, a child table, follow.
fn meta(opened: &OpenedSession, seq: u64, segment: u64, items: &TableRef) -> CommitMeta {
    CommitMeta {
        key: CommitKey {
            epoch: opened.epoch,
            seq,
        },
        after: if seq == 1 {
            opened.landed.as_ref().map(|landed| landed.key)
        } else {
            Some(CommitKey {
                epoch: opened.epoch,
                seq: seq - 1,
            })
        },
        segments: [SegmentId(segment)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: vec![ChildTable {
            table: Arc::clone(&items.name),
            merge: items.merge.clone().expect("a merge key"),
        }],
        drop_tables: Vec::new(),
    }
}

fn context() -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse("children").expect("valid pipeline id"),
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
    let first = meta(&opened, 1, 1, &item_table);
    opened
        .session
        .commit(&first)
        .await
        .expect("the first commit");
    write(&mut opened, &root_table, 2, roots(&[(1, 1, 3)])).await;
    let second = meta(&opened, 2, 2, &item_table);
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
    let first = meta(&opened, 1, 1, &item_table);
    opened
        .session
        .commit(&first)
        .await
        .expect("the first commit");
    write(&mut opened, &root_table, 2, roots(&[(1, 1, 3)])).await;
    let second = meta(&opened, 2, 2, &item_table);
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

/// Commits roots to `destination` with a commit that names their child table, which no row has
/// reached and nothing created; the commit's outcome.
async fn roots_before_any_child(destination: &dyn Destination) -> rdlt_connector::Result<()> {
    let mut opened = destination.open(&context()).await.expect("a session opens");
    let (root_table, item_table) = family();
    write(&mut opened, &root_table, 1, roots(&[(1, 1, 1)])).await;
    let commit = meta(&opened, 1, 1, &item_table);
    opened.session.commit(&commit).await.map(|_| ())
}

#[tokio::test]
async fn a_commit_naming_a_child_table_no_row_has_reached_lands_in_every_destination() {
    use rdlt_connector_reference::MemoryDestination;
    let dir = crate::fixtures::tempdir().expect("a temporary directory");
    let memory = destination_factory::<MemoryDestination>()
        .connect(
            json!({ "store": "children-unreached" }),
            ConnectContext::new(),
        )
        .await
        .expect("the memory destination connects");
    roots_before_any_child(memory.as_ref())
        .await
        .expect("memory");
    for format in ["jsonl", "arrow"] {
        let root = dir.path().join(format);
        std::fs::create_dir(&root).expect("a root");
        let files = destination_factory::<FilesDestination>()
            .connect(
                json!({ "root": root, "format": format }),
                ConnectContext::new(),
            )
            .await
            .expect("the files destination connects");
        roots_before_any_child(files.as_ref()).await.expect(format);
    }
    let sqlite = destination_factory::<SqliteDestination>()
        .connect(
            json!({ "path": dir.path().join("unreached.db") }),
            ConnectContext::new(),
        )
        .await
        .expect("the SQLite destination connects");
    roots_before_any_child(sqlite.as_ref())
        .await
        .expect("sqlite");
}
