//! A files session: schema changes in the table catalog, writers that stage one file per batch,
//! and commits that create the next manifest.

mod merged;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::{Epoch, GenerationId, LoadId, PipelineId, SegmentId, TablePath};

use super::format::FileFormat;
use super::manifest::{self, Manifest};
use super::{destination, tables};
use crate::blocking::blocking;
use crate::columns::changed;
use merged::{follow_root, merged_rows, written};

/// Where a session writes: the root, its pipeline's directory, the format, and who it is.
#[derive(Clone, Debug)]
pub(super) struct Location {
    pub(super) root: Arc<Path>,
    /// The pipeline the session belongs to, which owns the tables it creates.
    pub(super) pipeline: PipelineId,
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
        let (location, table) = (self.location.clone(), change.table().clone());
        let change = change.clone();
        blocking(move || {
            let (root, name) = (&location.root, &change.table().name);
            // Claimed and changed under one lock, so no release lands between them.
            tables::locked(root, name, || {
                claim(&location, name)?;
                tables::update(root, name, |current| {
                    let next = changed(current, &change)?;
                    Ok((current != Some(&next)).then_some(next))
                })
            })
        })
        .await?;
        self.learn(&table);
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<FilesWriter> {
        crate::merge::refuse_history_generation(table)?;
        let (location, name) = (self.location.clone(), table.name.clone());
        blocking(move || tables::locked(&location.root, &name, || claim(&location, &name))).await?;
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

/// Claims the table `name` for `location`'s pipeline where no pipeline owns it; another
/// pipeline's table is refused as `table_owned`, and a claim by a session a newer one fenced as
/// fenced, since a drop may have released the table from it.
///
/// The catalog is outside the manifest, so the session is checked again once it claimed: a
/// claim a drop overtook is undone. The caller holds the catalog's lock.
fn claim(location: &Location, name: &str) -> Result<()> {
    let (root, pipeline) = (&location.root, &location.pipeline);
    let unowned = tables::owner(root, name)?.is_none();
    let fenced = || -> Result<Option<ConnectorError>> {
        let epoch = manifest::latest(&location.dir)?.map(|manifest| manifest.epoch);
        Ok((epoch != Some(location.epoch)).then(|| {
            ConnectorError::fenced(format!(
                "pipeline {pipeline} has a session newer than epoch {}",
                location.epoch
            ))
        }))
    };
    if unowned && let Some(error) = fenced()? {
        return Err(error);
    }
    tables::claim(root, name, pipeline)?;
    if unowned && let Some(error) = fenced()? {
        tables::release_held(root, name, pipeline)?;
        return Err(error);
    }
    Ok(())
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
    finish(location, &mut manifest, meta)?;
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
    // The manifest is the truth: a catalog left behind here is removed by the next open.
    for name in &manifest.dropped {
        drop(tables::release(&location.root, name, &location.pipeline));
    }
    Ok(receipt)
}

/// Swaps into `manifest` the generations `meta` finishes, and drops from it the tables `meta`
/// drops, each of which another pipeline must not own.
fn finish(location: &Location, manifest: &mut Manifest, meta: &CommitMeta) -> Result<()> {
    for dropped in &meta.drop_tables {
        if let Some(owner) = tables::owner(&location.root, &dropped.name)?
            && owner != location.pipeline.as_str()
        {
            return Err(ConnectorError::table_owned(&dropped.name, &owner));
        }
    }
    for (path, generation) in &meta.finish_generations {
        let Some(name) = manifest.paths.get(&path_key(path)).cloned() else {
            continue;
        };
        let table = manifest.tables.entry(name).or_default();
        table.files = table.generations.remove(generation).unwrap_or_default();
        table.generations.clear();
        table.tombstones.clear();
    }
    for dropped in &meta.drop_tables {
        manifest.tables.remove(&*dropped.name);
        manifest.paths.remove(&path_key(&dropped.path));
        manifest.dropped.insert(dropped.name.to_string());
    }
    Ok(())
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
            let merged = merged_rows(location, name, table, files, key, root)?;
            table.files = written(location, name, "merged", &merged.rows, meta)?
                .into_iter()
                .collect();
            table.tombstones = match &merged.tombstones {
                Some(tombstones) => written(location, name, "tombstones", tombstones, meta)?
                    .into_iter()
                    .collect(),
                None => Vec::new(),
            };
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
