//! What the files destination's tests share: sessions, tables, commits and the tree on disk.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, Destination, Field, LoadId, LogicalType, MergeKey,
    OpenContext, OpenedSession, PipelineId, PublishedReader, SchemaVersion, SegmentId, SegmentSet,
    TableChange, TablePath, TableRef, TableSchema, readable_destination_factory,
};
use rdlt_connector_reference::FilesDestination;
use serde_json::{Value, json};

/// A new temporary directory, its user's alone, in a process whose later directories are too.
///
/// The connectors refuse a directory its group or others may write, and a directory is made
/// with the modes the process's mask leaves: a test neither inherits a mask that leaves those
/// nor relies on one that does not.
pub(crate) fn tempdir() -> std::io::Result<tempfile::TempDir> {
    use std::os::unix::fs::PermissionsExt as _;
    rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o022));
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
}

/// The pipeline every fixture session belongs to.
pub(crate) const PIPELINE: &str = "files";

/// A files destination under `root` with `settings` added to its configuration, and its reader.
pub(crate) async fn connect_with(
    root: &Path,
    settings: Value,
) -> (Arc<dyn Destination>, Arc<dyn PublishedReader>) {
    let mut config = json!({ "root": root });
    for (key, value) in settings.as_object().into_iter().flatten() {
        config[key] = value.clone();
    }
    readable_destination_factory::<FilesDestination>()
        .connect_reading(config, ConnectContext::new())
        .await
        .expect("the files destination connects")
}

/// A files destination under `root` writing `format`.
pub(crate) async fn connect(root: &Path, format: &str) -> Arc<dyn Destination> {
    connect_with(root, json!({ "format": format })).await.0
}

pub(crate) fn context(load: u128) -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse(PIPELINE).expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
    }
}

pub(crate) async fn open(destination: &dyn Destination, load: u128) -> OpenedSession {
    destination
        .open(&context(load))
        .await
        .expect("the root opens")
}

/// The append table `name`.
pub(crate) fn table(name: &str) -> TableRef {
    TableRef {
        path: TablePath::new([name]).expect("valid table path"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

/// The table `name` merged by `id`, its rows ordered by `seq`.
pub(crate) fn merge_table(name: &str) -> TableRef {
    TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
        ..table(name)
    }
}

pub(crate) fn meta(
    opened: &OpenedSession,
    load: u128,
    seq: CommitSeq,
    segments: &[u64],
) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, load),
        commit_seq: seq,
        epoch: opened.epoch,
        segments: segments
            .iter()
            .copied()
            .map(SegmentId)
            .collect::<SegmentSet>(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    }
}

/// Creates `table` with `schema` and stages `batch` as segment `segment`.
pub(crate) async fn stage(
    opened: &mut OpenedSession,
    table: &TableRef,
    schema: &TableSchema,
    batch: RecordBatch,
    segment: u64,
) {
    let create = TableChange::Create {
        table: TableRef {
            generation: None,
            ..table.clone()
        },
        schema: schema.clone(),
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let mut writer = opened.session.writer(table).await.expect("a writer opens");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

/// A schema of one required `id` column and a batch of `values` in it.
pub(crate) fn ids(values: &[i64]) -> (TableSchema, RecordBatch) {
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
        .expect("the schema is valid");
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
    )])
    .expect("the batch is valid");
    (schema, batch)
}

/// A merge table's schema, and a batch of `values` in it, every row at sequence `seq`.
pub(crate) fn keyed(values: &[i64], seq: u8) -> (TableSchema, RecordBatch) {
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("seq", LogicalType::Binary, false),
    ])
    .expect("the schema is valid");
    let mut sequence = [0_u8; 16];
    sequence[15] = seq;
    let seqs = BinaryArray::from_iter_values(values.iter().map(|_| sequence));
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
        ),
        ("seq", Arc::new(seqs) as ArrayRef),
    ])
    .expect("the batch is valid");
    (schema, batch)
}

/// Every entry under `dir` that is no directory, links included and never followed.
pub(crate) fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            found.extend(files_under(&path));
        } else {
            found.push(path);
        }
    }
    found.sort();
    found
}

/// Every directory under `dir`, never following links.
pub(crate) fn dirs_under(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            found.push(entry.path());
            found.extend(dirs_under(&entry.path()));
        }
    }
    found.sort();
    found
}

/// The directory of the fixture pipeline under `root`.
pub(crate) fn pipeline_dir(root: &Path) -> PathBuf {
    let pipelines = root.join("_rdlt").join("pipelines");
    std::fs::read_dir(&pipelines)
        .expect("the pipelines list")
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(PIPELINE))
        })
        .expect("the pipeline has a directory")
}

/// The path and JSON of the pipeline's latest manifest under `root`.
pub(crate) fn latest_manifest(root: &Path) -> (PathBuf, Value) {
    let path = files_under(&pipeline_dir(root).join("manifests"))
        .into_iter()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .max()
        .expect("the pipeline has a manifest");
    let manifest = serde_json::from_slice(&std::fs::read(&path).expect("the manifest reads"))
        .expect("the manifest is JSON");
    (path, manifest)
}

/// Writes `manifest` as the version after the manifest at `latest`, as a tamperer would.
pub(crate) fn plant_manifest(latest: &Path, mut manifest: Value) {
    let version = manifest["version"].as_u64().expect("a version") + 1;
    manifest["version"] = json!(version);
    let path = latest.with_file_name(format!("{version:020}.json"));
    std::fs::write(path, serde_json::to_vec(&manifest).expect("JSON"))
        .expect("the manifest plants");
}

/// The `id` column of every published row of `table`, in order.
pub(crate) async fn published_ids(reader: &dyn PublishedReader, table: &TableRef) -> Vec<i64> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    reader
        .published(table)
        .await
        .expect("the table reads back")
        .iter()
        .flat_map(|batch| {
            let ids = batch.column_by_name("id").expect("an id column");
            ids.as_primitive::<Int64Type>().values().to_vec()
        })
        .collect()
}
