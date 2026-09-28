//! A files commit that fails between writing its data and recording it in its manifest leaves
//! the table as it was, and its retry publishes once.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, Destination, Field, LoadId, LogicalType, MergeKey,
    OpenContext, OpenedSession, PipelineId, SchemaVersion, SegmentId, SegmentSet, TableChange,
    TablePath, TableRef, TableSchema, destination_factory,
};
use rdlt_connector_reference::{FilesDestination, files};
use serde_json::json;

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["torn"]).expect("valid table path"),
        name: "torn".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
        }),
    }
}

/// Key 1's row `v` at sequence `seq`.
fn row(v: &str, seq: u8) -> RecordBatch {
    let mut sequence = [0_u8; 16];
    sequence[15] = seq;
    let seqs: BinaryArray = std::iter::once(Some(sequence.to_vec())).collect();
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("v", Arc::new(StringArray::from(vec![v])) as ArrayRef),
        ("seq", Arc::new(seqs) as ArrayRef),
    ])
    .expect("the batch is valid")
}

/// Opens a session for `load`, and stages `batch` as segment 1 of the table, created as needed.
async fn staged(destination: &dyn Destination, load: u128, batch: RecordBatch) -> OpenedSession {
    let context = OpenContext {
        pipeline: PipelineId::parse("torn").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
    };
    let mut opened = destination.open(&context).await.expect("the session opens");
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("v", LogicalType::Utf8, true),
        Field::new("seq", LogicalType::Binary, true),
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
    opened
}

fn meta(opened: &OpenedSession, load: u128) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: [SegmentId(1)].into_iter().collect::<SegmentSet>(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
    }
}

fn values(root: &Path) -> Vec<String> {
    files::published(root, "torn")
        .expect("the table reads")
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name("v").expect("the column is published");
            column
                .as_string::<i32>()
                .iter()
                .map(|value| value.unwrap_or_default().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The directory holding the pipeline's manifests.
fn manifests(root: &Path) -> PathBuf {
    let pipelines = root.join("_rdlt").join("pipelines");
    let pipeline = std::fs::read_dir(&pipelines)
        .expect("the pipelines list")
        .flatten()
        .next()
        .expect("the pipeline has a directory");
    pipeline.path().join("manifests")
}

fn set_mode(dir: &Path, mode: u32) {
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode))
        .expect("the permissions change");
}

#[tokio::test]
async fn a_commit_failing_before_its_manifest_leaves_the_table_and_its_retry_publishes_once() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let destination = destination_factory::<FilesDestination>()
        .connect(json!({ "root": root.path() }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let mut first = staged(destination.as_ref(), 1, row("a", 1)).await;
    let meta_first = meta(&first, 1);
    first
        .session
        .commit(&meta_first)
        .await
        .expect("the first commit lands");
    let mut torn = staged(destination.as_ref(), 2, row("b", 2)).await;
    let manifests = manifests(root.path());
    set_mode(&manifests, 0o555);
    // Where permissions bind nothing, as for root, the fault cannot be made.
    if std::fs::write(manifests.join(".probe"), b"").is_ok() {
        set_mode(&manifests, 0o755);
        return;
    }
    let meta_torn = meta(&torn, 2);
    let failed = torn.session.commit(&meta_torn).await;
    set_mode(&manifests, 0o755);
    failed.expect_err("the manifest cannot be written");
    assert_eq!(values(root.path()), ["a"]);
    let mut retried = staged(destination.as_ref(), 3, row("b", 2)).await;
    let meta_retried = meta(&retried, 3);
    retried
        .session
        .commit(&meta_retried)
        .await
        .expect("the retry lands");
    assert_eq!(values(root.path()), ["b"]);
}
