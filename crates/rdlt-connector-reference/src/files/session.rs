//! A files session: schema changes in the table catalog, writers that stage one file per batch,
//! and commits that create the next manifest.

mod commit;
mod compact;
mod merged;
#[cfg(test)]
pub(super) mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::{Epoch, GenerationId, LoadId, PipelineId, SegmentId, TablePath};

use super::format::FileFormat;
use super::manifest::{self, Listed, STAGING};
use super::{destination, io, tables};
use crate::blocking::blocking;
use crate::columns::changed;
use crate::rooted::Dir;

/// Where a session writes: the destination's directories, the format, and who it is.
#[derive(Clone, Debug)]
pub(super) struct Location {
    /// The destination's private directory, which holds the catalog.
    pub(super) rdlt: Arc<Dir>,
    /// The pipeline the session belongs to, which owns the tables it creates.
    pub(super) pipeline: PipelineId,
    /// The pipeline's directory, which its manifests list files relative to.
    pub(super) dir: Arc<Dir>,
    pub(super) format: FileFormat,
    pub(super) epoch: Epoch,
    pub(super) load_id: LoadId,
    /// How long the session waits for a table's lock.
    pub(super) lock_wait: Duration,
}

/// A file a writer of this session staged.
#[derive(Clone, Debug)]
struct StagedFile {
    segment: SegmentId,
    table: TableRef,
    file: Listed,
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
            let (rdlt, name) = (&location.rdlt, &change.table().name);
            // Claimed and changed under one lock, so no release lands between them.
            tables::locked(rdlt, name, location.lock_wait, || {
                claim(&location, name)?;
                tables::update(rdlt, name, |current| {
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
        blocking(move || {
            let wait = location.lock_wait;
            tables::locked(&location.rdlt, &name, wait, || claim(&location, &name))
        })
        .await?;
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
        blocking(move || destination::discard(&location.dir, location.epoch)).await
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        let (location, shared, meta) = (
            self.location.clone(),
            Arc::clone(&self.shared),
            meta.clone(),
        );
        blocking(move || commit::commit(&location, &shared, &meta)).await
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
    let (rdlt, pipeline) = (&location.rdlt, &location.pipeline);
    let unowned = tables::owner(rdlt, name)?.is_none();
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
    tables::claim(rdlt, name, pipeline)?;
    if unowned && let Some(error) = fenced()? {
        tables::release_held(rdlt, name, pipeline)?;
        return Err(error);
    }
    Ok(())
}

impl Location {
    /// Where `part` of `table` is staged under `segment`, a segment's id or what a commit writes
    /// in a segment's place, for `generation`: the names leading to its directory under the
    /// pipeline's, and its file's name.
    fn staged(
        &self,
        segment: &[String],
        table: &str,
        generation: Option<GenerationId>,
        part: u64,
    ) -> (Vec<String>, String) {
        let generation = generation.map_or_else(|| "table".to_owned(), |g| format!("g{g}"));
        let mut names = vec![
            STAGING.to_owned(),
            self.epoch.to_string(),
            self.load_id.to_string(),
        ];
        names.extend(segment.iter().cloned());
        names.extend([table.to_owned(), generation]);
        (names, format!("{part}.{}", self.format.extension()))
    }

    /// Opens the directory the `names` lead to under the pipeline's, creating what is missing.
    fn staging(&self, names: &[String]) -> Result<Dir> {
        self.dir
            .walk_created(names)
            .map_err(io::failed("creating", &self.dir.at(names.join("/"))))
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
                let (names, file) =
                    location.staged(&[segment.to_string()], &table.name, table.generation, part);
                let dir = location.staging(&names)?;
                let written = location.format.write(&dir, &file, &[batch])?;
                let file = Listed {
                    path: format!("{}/{file}", names.join("/")),
                    rows: written.rows,
                    bytes: written.bytes,
                };
                shared.lock().staged.push(StagedFile {
                    segment,
                    table: table.clone(),
                    file,
                });
                stats.rows += written.rows;
                stats.bytes += written.bytes;
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
