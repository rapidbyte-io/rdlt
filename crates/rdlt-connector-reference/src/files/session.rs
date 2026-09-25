//! A files session: schema changes in the table catalog, writers that stage one file per batch,
//! and commits that create the next manifest.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::{
    ChildTable, Epoch, GenerationId, LoadId, MergeKey, RootKey, SegmentId, TablePath,
};

use super::format::FileFormat;
use super::manifest::{self, Manifest};
use super::{destination, tables};
use crate::blocking::blocking;
use crate::columns::changed;

/// Where a session writes: the root, its pipeline's directory, the format, and who it is.
#[derive(Clone, Debug)]
pub(super) struct Location {
    pub(super) root: Arc<Path>,
    pub(super) dir: PathBuf,
    pub(super) format: FileFormat,
    pub(super) epoch: Epoch,
    pub(super) load_id: LoadId,
}

/// A file a writer of this session staged.
#[derive(Clone, Debug)]
struct StagedFile {
    segment: SegmentId,
    table: TableRef,
    /// The file's path relative to the root.
    path: String,
    rows: u64,
    bytes: u64,
}

/// What a session and its writers share.
#[derive(Debug, Default)]
struct Shared {
    staged: Vec<StagedFile>,
    /// The identifier of each table path the session wrote or changed, by the path's JSON.
    names: BTreeMap<String, String>,
    parts: u64,
}

/// A [`FilesDestination`](super::FilesDestination) session.
#[derive(Debug)]
pub struct FilesSession {
    location: Location,
    shared: Arc<Mutex<Shared>>,
}

impl FilesSession {
    pub(super) fn new(location: Location) -> Self {
        Self {
            location,
            shared: Arc::default(),
        }
    }

    fn learn(&self, table: &TableRef) {
        self.shared
            .lock()
            .names
            .insert(path_key(&table.path), table.name.to_string());
    }
}

impl Session for FilesSession {
    type Writer = FilesWriter;

    async fn apply_schema(&mut self, change: &TableChange) -> Result<()> {
        self.learn(change.table());
        let (root, change) = (Arc::clone(&self.location.root), change.clone());
        blocking(move || {
            tables::update(&root, &change.table().name, |current| {
                let next = changed(current, &change)?;
                Ok((current != Some(&next)).then_some(next))
            })
        })
        .await
    }

    async fn writer(&mut self, table: &TableRef) -> Result<FilesWriter> {
        self.learn(table);
        Ok(FilesWriter {
            location: self.location.clone(),
            shared: Arc::clone(&self.shared),
            table: table.clone(),
            buffered: Vec::new(),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        let location = self.location.clone();
        blocking(move || destination::discard(&location.root, &location.dir, location.epoch)).await
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        let (location, shared, meta) = (
            self.location.clone(),
            Arc::clone(&self.shared),
            meta.clone(),
        );
        blocking(move || commit(&location, &shared, &meta)).await
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}

/// The files a commit publishes, by table and generation.
type Staging<'a> = BTreeMap<(String, Option<GenerationId>), Vec<&'a StagedFile>>;

/// Publishes the files this session staged in `meta`'s segments by creating the next manifest.
fn commit(location: &Location, shared: &Mutex<Shared>, meta: &CommitMeta) -> Result<Receipt> {
    let mut manifest = manifest::latest(&location.dir)?.unwrap_or_default();
    if manifest.epoch != location.epoch || meta.epoch != location.epoch {
        return Err(ConnectorError::fenced(format!(
            "the pipeline is at epoch {}; this session opened at {}",
            manifest.epoch, location.epoch
        )));
    }
    if let Some(receipt) = manifest.receipt(meta.load_id, meta.commit_seq) {
        return Ok(receipt);
    }
    let (staged, names) = {
        let shared = shared.lock();
        let staged: Vec<StagedFile> = shared
            .staged
            .iter()
            .filter(|file| meta.segments.contains(file.segment))
            .cloned()
            .collect();
        (staged, shared.names.clone())
    };
    publish_all(location, &mut manifest, &staged, meta)?;
    manifest.paths.extend(names);
    for (path, generation) in &meta.finish_generations {
        let Some(name) = manifest.paths.get(&path_key(path)).cloned() else {
            continue;
        };
        let table = manifest.tables.entry(name).or_default();
        table.files = table.generations.remove(generation).unwrap_or_default();
        table.generations.clear();
    }
    manifest.apply(&meta.state_delta);
    let receipt = Receipt {
        load_id: meta.load_id,
        commit_seq: meta.commit_seq,
        committed_at: manifest::truncated(SystemTime::now()),
        rows: staged.iter().map(|file| file.rows).sum(),
        bytes: staged.iter().map(|file| file.bytes).sum(),
    };
    manifest.record(&receipt);
    manifest.version += 1;
    if !manifest::put(&location.dir, &manifest)? {
        return Err(ConnectorError::fenced(
            "another session published the pipeline's next manifest first",
        ));
    }
    shared
        .lock()
        .staged
        .retain(|file| !meta.segments.contains(file.segment));
    Ok(receipt)
}

/// Adds `files`, staged for the table `name` or one generation of it, to what `manifest` lists
/// for it: appended, into their generation, or merged into one new file of the table's rows.
fn publish(
    location: &Location,
    manifest: &mut Manifest,
    name: &str,
    files: &[&StagedFile],
    meta: &CommitMeta,
    staged: &Staging<'_>,
) -> Result<()> {
    let table = manifest.tables.entry(name.to_owned()).or_default();
    let first = &files[0].table;
    let paths = files.iter().map(|file| file.path.clone());
    match (&first.generation, &first.merge) {
        (Some(generation), _) => table
            .generations
            .entry(*generation)
            .or_default()
            .extend(paths),
        (None, Some(key)) => {
            let root = key.root.as_ref().map(|root| {
                let files = staged.get(&(root.table.to_string(), None));
                (root, files.map(Vec::as_slice).unwrap_or_default())
            });
            let merged = merged(location, name, &table.files, files, key, root, meta)?;
            table.files = merged.into_iter().collect();
        }
        (None, None) => table.files.extend(paths),
    }
    Ok(())
}

/// Adds the `staged` files of `meta` to what `manifest` lists for their tables, and has the
/// child tables it lists follow their roots.
fn publish_all(
    location: &Location,
    manifest: &mut Manifest,
    staged: &[StagedFile],
    meta: &CommitMeta,
) -> Result<()> {
    let mut by_table: Staging<'_> = BTreeMap::new();
    for file in staged {
        by_table
            .entry((file.table.name.to_string(), file.table.generation))
            .or_default()
            .push(file);
    }
    for ((name, _), files) in &by_table {
        publish(location, manifest, name, files, meta, &by_table)?;
    }
    for child in &meta.child_tables {
        follow_root(location, manifest, child, meta, &by_table)?;
    }
    Ok(())
}

/// Replaces, in the child table `child` the commit staged nothing for, the children of the roots
/// its root's staged files publish.
fn follow_root(
    location: &Location,
    manifest: &mut Manifest,
    child: &ChildTable,
    meta: &CommitMeta,
    staged: &Staging<'_>,
) -> Result<()> {
    let Some(root) = &child.merge.root else {
        return Ok(());
    };
    let name = child.table.to_string();
    let Some(root_files) = staged.get(&(root.table.to_string(), None)) else {
        return Ok(());
    };
    if staged.contains_key(&(name.clone(), None)) {
        return Ok(());
    }
    let table = manifest.tables.entry(name.clone()).or_default();
    let root = Some((root, root_files.as_slice()));
    let merged = merged(location, &name, &table.files, &[], &child.merge, root, meta)?;
    table.files = merged.into_iter().collect();
    Ok(())
}

/// Writes the rows of the table `name` once `files` are merged into its `published` files by
/// `key`, or for a child table once they replace the children of the roots its root's `files`
/// publish, to one new file; returns its path, or none when the table is empty.
fn merged(
    location: &Location,
    name: &str,
    published: &[String],
    files: &[&StagedFile],
    key: &MergeKey,
    root: Option<(&RootKey, &[&StagedFile])>,
    meta: &CommitMeta,
) -> Result<Option<String>> {
    let schema = tables::read(&location.root, name)?
        .ok_or_else(|| ConnectorError::data(format!("table {name} does not exist")))?;
    let schema = Arc::new(schema.to_arrow());
    let read = |paths: &mut dyn Iterator<Item = &String>| -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for path in paths {
            batches.extend(location.format.read(&location.root.join(path), &schema)?);
        }
        Ok(batches)
    };
    let published = read(&mut published.iter())?;
    let incoming = read(&mut files.iter().map(|file| &file.path))?;
    let merged = match root {
        Some((root, root_files)) => {
            let root_schema = tables::read(&location.root, &root.table)?.ok_or_else(|| {
                ConnectorError::data(format!("root table {} does not exist", root.table))
            })?;
            let root_schema = Arc::new(root_schema.to_arrow());
            let mut roots = Vec::new();
            for file in root_files {
                roots.extend(
                    location
                        .format
                        .read(&location.root.join(&file.path), &root_schema)?,
                );
            }
            crate::merge::merge_children(&schema, &published, &incoming, key, root, &roots)
        }
        None => crate::merge::merge(&schema, &published, &incoming, key),
    };
    let merged = merged
        .and_then(|batches| arrow_select::concat::concat_batches(&schema, &batches))
        .map_err(|error| ConnectorError::data(format!("merging table {name}: {error}")))?;
    if merged.num_rows() == 0 {
        return Ok(None);
    }
    let path = location.staged(&format!("merged/{}", meta.commit_seq.get()), name, None, 0);
    location.format.write(&location.root.join(&path), &merged)?;
    Ok(Some(path))
}

impl Location {
    /// The path, relative to the root, of `part` of `table` staged under `segment` for
    /// `generation`.
    fn staged(
        &self,
        segment: &str,
        table: &str,
        generation: Option<GenerationId>,
        part: u64,
    ) -> String {
        let dir = self
            .dir
            .strip_prefix(&self.root)
            .unwrap_or(&self.dir)
            .to_string_lossy()
            .replace('\\', "/");
        let generation = generation.map_or_else(|| "table".to_owned(), |g| format!("g{g}"));
        format!(
            "{dir}/staging/{}/{}/{segment}/{table}/{generation}/{part}.{}",
            self.epoch,
            self.load_id,
            self.format.extension()
        )
    }
}

/// Buffers a table's batches and writes each to its own staged file on flush.
#[derive(Debug)]
pub struct FilesWriter {
    location: Location,
    shared: Arc<Mutex<Shared>>,
    table: TableRef,
    buffered: Vec<(SegmentId, RecordBatch)>,
}

impl TableWriter for FilesWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        self.buffered.push((segment, batch));
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        let buffered = std::mem::take(&mut self.buffered);
        let (location, shared, table) = (
            self.location.clone(),
            Arc::clone(&self.shared),
            self.table.clone(),
        );
        blocking(move || {
            let mut stats = WriteStats::default();
            for (segment, batch) in buffered {
                if batch.num_rows() == 0 {
                    continue;
                }
                let part = {
                    let mut shared = shared.lock();
                    shared.parts += 1;
                    shared.parts
                };
                let path =
                    location.staged(&segment.to_string(), &table.name, table.generation, part);
                let bytes = location.format.write(&location.root.join(&path), &batch)?;
                let rows = batch.num_rows() as u64;
                shared.lock().staged.push(StagedFile {
                    segment,
                    table: table.clone(),
                    path,
                    rows,
                    bytes,
                });
                stats.rows += rows;
                stats.bytes += bytes;
            }
            Ok(stats)
        })
        .await
    }
}

/// How the manifest keys a table path: its segments as a JSON array.
fn path_key(path: &TablePath) -> String {
    serde_json::to_string(path).expect("table paths serialize")
}
