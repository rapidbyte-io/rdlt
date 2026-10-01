//! A destination that writes files under a root directory and publishes them with manifests.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::io::ErrorKind;
use std::num::NonZeroU16;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::{
    CommitKind, DeleteModes, Epoch, IdentifierCase, IdentifierChars, IdentifierRules,
    NestedSupport, PipelineId, SchemaChanges, TypeKind, WriteModes,
};
use schemars::JsonSchema;
use serde::Deserialize;

use super::format::FileFormat;
use super::manifest::{self, Manifest, STAGING};
use super::session::{FilesSession, Location};
use super::{io, tables};
use crate::blocking::blocking;
use crate::limits::{LOCK_WAIT, TABLE_NAME_BYTES, TREE_DEPTH};
use crate::rooted::{Dir, Kind};

/// The destination's private directory under its root: catalogs, locks, manifests and files.
const PRIVATE: &str = "_rdlt";

/// The directory of the pipelines' directories, in the private directory.
const PIPELINES: &str = "pipelines";

/// Configuration of [`FilesDestination`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FilesDestinationConfig {
    /// The directory the destination writes under, created when missing.
    pub root: PathBuf,
    /// How files store rows.
    #[serde(default)]
    pub format: FileFormat,
    /// How long a schema change, a writer or a release waits for a table's lock before it fails
    /// as a transient error, as seconds and nanoseconds (`{"secs": 30, "nanos": 0}`); 30 seconds
    /// where unset.
    #[serde(default = "default_lock_wait")]
    pub lock_wait: Duration,
}

fn default_lock_wait() -> Duration {
    LOCK_WAIT
}

/// Writes each flushed batch to its own file and publishes a commit by creating the pipeline's
/// next manifest, which lists every published file and the pipeline's state.
///
/// Creating a manifest version fails when another writer created it first, which fences older
/// sessions: a commit is atomic and happens at most once. Readers read only the files the latest
/// manifest lists. A merge table's commit rewrites the table as one file.
///
/// Everything lives in the directory `_rdlt` under the root, which belongs to the user the
/// destination runs as and is that user's alone: directories are created with mode 0700 and
/// files with 0600. Nothing is opened but by name beneath that directory, and no link is
/// followed.
#[derive(Debug)]
pub struct FilesDestination {
    root: Arc<Path>,
    format: FileFormat,
    lock_wait: Duration,
    /// The private directory, opened once: the root's path is resolved no second time.
    rdlt: Arc<Held>,
}

/// A destination's root and the private directory in it, once they were opened.
type Held = Mutex<Option<(Arc<Dir>, Arc<Dir>)>>;

#[destination(id = "io.rapidbyte.files")]
impl DestinationConnector for FilesDestination {
    type Config = FilesDestinationConfig;
    type Session = FilesSession;

    fn capabilities(&self) -> Capabilities {
        capabilities(self.format)
    }

    async fn connect(config: FilesDestinationConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            root: config.root.into(),
            format: config.format,
            lock_wait: config.lock_wait,
            rdlt: Arc::default(),
        })
    }

    async fn check(&self) -> Result<()> {
        let (root, held) = (Arc::clone(&self.root), Arc::clone(&self.rdlt));
        blocking(move || checked(&*held_or_opened(&root, &held)?)).await
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<FilesSession>> {
        let (root, pipeline, wait) = (
            Arc::clone(&self.root),
            context.pipeline.clone(),
            self.lock_wait,
        );
        let held = Arc::clone(&self.rdlt);
        let (rdlt, dir, manifest) = blocking(move || {
            let rdlt = held_or_opened(&root, &held)?;
            tables::empty_trash(&rdlt)?;
            let name = manifest::pipeline_dir(&pipeline);
            let dir = rdlt
                .walk_created([PIPELINES, name.as_str()])
                .map_err(io::failed("creating", &rdlt.at(PIPELINES).join(&name)))?;
            manifest::sweep(&dir)?;
            let manifest = next_epoch(&dir, &rdlt, &pipeline, wait)?;
            Ok((rdlt, dir, manifest))
        })
        .await?;
        let location = Location {
            rdlt,
            pipeline: context.pipeline.clone(),
            dir: Arc::new(dir),
            format: self.format,
            epoch: manifest.epoch,
            load_id: context.load_id,
            lock_wait: self.lock_wait,
        };
        Ok(Opened {
            state: manifest.records()?,
            epoch: manifest.epoch,
            session: FilesSession::new(location),
        })
    }
}

/// Opens the destination's private directory under `root`, creating both where missing; a root
/// or a private directory another user made, or others may write, is refused.
fn private(root: &Path) -> Result<(Dir, Dir)> {
    let opened = Dir::ambient_created(root).map_err(io::failed("creating", root))?;
    let path = opened.at(PRIVATE);
    let rdlt = opened
        .dir_created(PRIVATE)
        .map_err(io::failed("creating", &path))?;
    Ok((opened, rdlt))
}

/// Opens the private directory under `root` to read, if the destination ever wrote there.
fn existing(root: &Path) -> Result<Option<Dir>> {
    let missing = |error: &std::io::Error| error.kind() == ErrorKind::NotFound;
    let opened = match Dir::ambient(root) {
        Ok(opened) => opened,
        Err(error) if missing(&error) => return Ok(None),
        Err(error) => return Err(io::failed("opening", root)(error)),
    };
    let path = opened.at(PRIVATE);
    match opened.dir(PRIVATE) {
        Ok(rdlt) => Ok(Some(rdlt)),
        Err(error) if missing(&error) => Ok(None),
        Err(error) => Err(io::failed("opening", &path)(error)),
    }
}

/// Creates the next manifest of the pipeline whose directory `dir` is, with the next epoch,
/// trying again a bounded number of times while other sessions create versions first.
///
/// The catalogs of tables the pipeline dropped are removed first, before this session can create
/// any of them again.
pub(super) fn next_epoch(
    dir: &Dir,
    rdlt: &Dir,
    pipeline: &PipelineId,
    wait: Duration,
) -> Result<Manifest> {
    io::retried(&format!("opening pipeline {pipeline}"), || {
        let mut manifest = manifest::latest(dir)?.unwrap_or_default();
        for name in &manifest.dropped {
            tables::release(rdlt, name, pipeline, wait, || still_dropped(dir, name))?;
        }
        manifest.dropped.clear();
        let (version, epoch) = (manifest.version.checked_add(1), manifest.epoch.next());
        let Some(version) = version.filter(|_| epoch != manifest.epoch) else {
            return Err(ConnectorError::data(format!(
                "pipeline {pipeline} holds the last manifest version or epoch there is"
            )));
        };
        (manifest.version, manifest.epoch) = (version, epoch);
        Ok(manifest::put(dir, &manifest)?.then_some(manifest))
    })
}

/// Whether the latest manifest of the pipeline whose directory `dir` is still lists the table
/// `name` as dropped and lists nothing for it: a newer session that removed its catalog no
/// longer lists it as dropped, and a table that was created again is published.
pub(super) fn still_dropped(dir: &Dir, name: &str) -> Result<bool> {
    Ok(manifest::latest(dir)?
        .is_some_and(|latest| latest.dropped.contains(name) && !latest.tables.contains_key(name)))
}

/// Removes what sessions of the pipeline whose directory `dir` is, older than `epoch`, staged
/// that the latest manifest does not list.
///
/// The latest manifest's name is made durable first: a manifest a power loss could take back
/// decides nothing about the files of the manifest before it.
pub(super) fn discard(dir: &Dir, epoch: Epoch) -> Result<()> {
    manifest::settle(dir)?;
    discard_settled(dir, epoch)
}

/// Removes what [`discard`] does, the latest manifest's name already durable.
fn discard_settled(dir: &Dir, epoch: Epoch) -> Result<()> {
    let listed: BTreeSet<String> = manifest::latest(dir)?
        .unwrap_or_default()
        .files()
        .map(|file| file.path.clone())
        .collect();
    let staging = match dir.dir(STAGING) {
        Ok(staging) => staging,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io::failed("opening", &dir.at(STAGING))(error)),
    };
    let entries = staging
        .entries()
        .map_err(io::failed("listing", staging.path()))?;
    for (name, kind) in entries {
        let older = name
            .to_str()
            .and_then(|name| name.parse::<u64>().ok())
            .is_some_and(|staged| staged < epoch.0);
        if older {
            let path = format!("{STAGING}/{}", name.to_string_lossy());
            remove_unlisted(&staging, &name, kind, &path, &listed, TREE_DEPTH)?;
        }
    }
    Ok(())
}

/// The directories, beside a load's segments, that hold what the load's commits wrote.
const WRITTEN: [&str; 3] = ["merged", "compacted", "tombstones"];

/// Removes what commits of the session at `epoch` of the pipeline whose directory `dir` is, and
/// what older sessions, left that the latest manifest does not list.
///
/// Only commits write under those directories, one at a time, so nothing a writer is staging
/// is among them. The caller made the latest manifest's name durable.
pub(super) fn discard_superseded(dir: &Dir, epoch: Epoch) -> Result<()> {
    discard_settled(dir, epoch)?;
    let listed: BTreeSet<String> = manifest::latest(dir)?
        .unwrap_or_default()
        .files()
        .map(|file| file.path.clone())
        .collect();
    let own = [STAGING.to_owned(), epoch.to_string()];
    let Ok(staged) = dir.walk(&own) else {
        return Ok(());
    };
    let listing = io::failed("listing", staged.path());
    for (load, kind) in staged.entries().map_err(&listing)? {
        let Some(load) = load.to_str().filter(|_| kind == Kind::Dir) else {
            continue;
        };
        let loaded = staged.dir(load).map_err(&listing)?;
        for written in WRITTEN {
            if loaded.kind(written).map_err(&listing)? == Some(Kind::Dir) {
                let path = format!("{}/{load}/{written}", own.join("/"));
                remove_unlisted(
                    &loaded,
                    written.as_ref(),
                    Kind::Dir,
                    &path,
                    &listed,
                    TREE_DEPTH,
                )?;
            }
        }
    }
    Ok(())
}

/// Removes the entry `name` of `parent`, at `path` under the pipeline's directory, unless it is
/// a `listed` file or a directory holding one; returns whether it is gone.
///
/// A link is removed itself: only directories are entered, each opened by name in its parent.
fn remove_unlisted(
    parent: &Dir,
    name: &OsStr,
    kind: Kind,
    path: &str,
    listed: &BTreeSet<String>,
    depth: usize,
) -> Result<bool> {
    let removing = |error| io::failed("removing", &parent.at(name))(error);
    if kind != Kind::Dir {
        if kind == Kind::File && listed.contains(path) {
            return Ok(false);
        }
        parent.remove_file(name).map_err(removing)?;
        return Ok(true);
    }
    let entered = depth
        .checked_sub(1)
        .ok_or_else(|| std::io::Error::from(crate::rooted::Refusal::TooDeep { limit: TREE_DEPTH }));
    let depth = entered.map_err(io::failed("listing", &parent.at(name)))?;
    let dir = parent
        .dir(name)
        .map_err(io::failed("listing", &parent.at(name)))?;
    let mut empty = true;
    for (child, kind) in dir.entries().map_err(io::failed("listing", dir.path()))? {
        let below = format!("{path}/{}", child.to_string_lossy());
        empty &= remove_unlisted(&dir, &child, kind, &below, listed, depth)?;
    }
    if empty {
        parent.remove_dir(name).map_err(removing)?;
    }
    Ok(empty)
}

/// The destination's private directory under `root`: `held` since it was first opened, or
/// opened now and held from here on.
///
/// Held directories are checked again each time: they must still be their user's alone.
fn held_or_opened(root: &Path, held: &Held) -> Result<Arc<Dir>> {
    let mut held = held.lock();
    if let Some((opened, rdlt)) = held.as_ref() {
        still_at(root, opened)?;
        for dir in [opened, rdlt] {
            dir.private().map_err(io::failed("opening", dir.path()))?;
        }
        return Ok(Arc::clone(rdlt));
    }
    let (opened, rdlt) = private(root)?;
    let rdlt = Arc::new(rdlt);
    *held = Some((Arc::new(opened), Arc::clone(&rdlt)));
    Ok(rdlt)
}

/// The code of an error for a root whose path no longer leads to the directory first opened.
const ROOT_REPLACED: &str = "root_replaced";

/// Checks that the path `root` still leads to the directory `opened` there.
///
/// A root moved aside or removed, with or without another directory in its place, would
/// otherwise keep receiving every write where nobody looks for it.
fn still_at(root: &Path, opened: &Dir) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let found = std::fs::metadata(root)
        .ok()
        .map(|metadata| (metadata.dev(), metadata.ino()));
    let held = opened
        .identity()
        .map_err(io::failed("opening", opened.path()))?;
    if found == Some(held) {
        return Ok(());
    }
    Err(ConnectorError::config(format!(
        "the root {} is no longer the directory the destination opened: it was moved, removed \
         or replaced; start the destination again once the root is in place",
        root.display()
    ))
    .with_code(ROOT_REPLACED))
}

/// Checks that the destination can write in its private directory `rdlt`.
fn checked(rdlt: &Dir) -> Result<()> {
    // The probe goes as it leaves scope, whichever check made it.
    rdlt.temporary()
        .map(drop)
        .map_err(io::failed("writing in", rdlt.path()))
}

/// What the files destination stores: every type its format keeps, any schema change, and
/// identifiers that are safe file names everywhere.
fn capabilities(format: FileFormat) -> Capabilities {
    let types = format.types();
    let mut capabilities = Capabilities::minimal();
    capabilities.commit = CommitKind::Manifest;
    capabilities.write_modes = WriteModes {
        append: true,
        replace: true,
        merge: true,
        history: true,
    };
    capabilities.delete_modes = DeleteModes {
        hard: true,
        soft: true,
    };
    capabilities.partial_updates = true;
    capabilities.merge_changes = true;
    capabilities.drop_tables = true;
    capabilities.nested = NestedSupport {
        structs: true,
        lists: true,
        json: types.contains(&TypeKind::Json),
    };
    capabilities.types = types;
    capabilities.schema_changes = SchemaChanges::all();
    capabilities.identifiers = IdentifierRules {
        case: IdentifierCase::Lower,
        max_len: NonZeroU16::new(TABLE_NAME_BYTES).expect("the limit is not zero"),
        chars: IdentifierChars::AsciiWord,
        reserved: BTreeSet::new(),
        reserved_table_prefixes: BTreeSet::new(),
    };
    capabilities.max_parallel_writers = NonZeroU16::new(4).expect("4 is non-zero");
    capabilities
}

#[cfg(feature = "certify")]
impl ReadBack for FilesDestination {
    async fn published(&self, table: &TableRef, rows: PublishedRows) -> Result<()> {
        let (root, name) = (self.root.to_path_buf(), table.name.clone());
        let held = Arc::clone(&self.rdlt);
        blocking(move || {
            let mut send = |batch| rows.blocking_send(batch);
            // A destination that never opened reads what is there, and creates nothing.
            if held.lock().is_none() {
                return published_each(&root, &name, &mut send);
            }
            published_in(&*held_or_opened(&root, &held)?, &name, &mut send)
        })
        .await
    }
}

/// Every published batch of `table` under `root`, over every pipeline's latest manifest.
pub fn published(root: impl Into<PathBuf>, table: &str) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    published_each(&root.into(), table, &mut |batch| {
        batches.push(batch);
        Ok(())
    })?;
    Ok(batches)
}

/// Gives `each` every batch [`published`] answers of `table` under `root`.
fn published_each(
    root: &Path,
    table: &str,
    each: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    tables::named(table)?;
    match existing(root)? {
        Some(rdlt) => published_in(&rdlt, table, each),
        None => Ok(()),
    }
}

/// Gives `each` every published batch of `table` under the private directory `rdlt`, a
/// pipeline's at a time: what one pipeline publishes of the table is read whole, since it is
/// read again where a commit removed a file meanwhile.
fn published_in(
    rdlt: &Dir,
    table: &str,
    each: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    tables::named(table)?;
    let schema = Arc::new(
        tables::read(rdlt, table)?
            .map_or_else(arrow_schema::Schema::empty, |schema| schema.to_arrow()),
    );
    let pipelines = match rdlt.dir(PIPELINES) {
        Ok(pipelines) => pipelines,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io::failed("opening", &rdlt.at(PIPELINES))(error)),
    };
    let listing = io::failed("listing", pipelines.path());
    for (name, kind) in pipelines.entries().map_err(&listing)? {
        if kind != Kind::Dir {
            continue;
        }
        let dir = pipelines.dir(&name).map_err(&listing)?;
        for batch in published_by(&dir, table, &schema, manifest::latest)? {
            each(batch)?;
        }
    }
    Ok(())
}

/// Every batch the pipeline whose directory `dir` is publishes of `table`, as the manifest
/// `latest` reads lists them.
///
/// A commit removes the files it supersedes once its manifest is durable, so a file listed by
/// the manifest just read may be gone: when a newer manifest exists by then, the table is read
/// again from it, a bounded number of times. A file missing under the manifest that is still
/// the latest is lost.
fn published_by(
    dir: &Dir,
    table: &str,
    schema: &arrow_schema::SchemaRef,
    mut latest: impl FnMut(&Dir) -> Result<Option<Manifest>>,
) -> Result<Vec<RecordBatch>> {
    io::retried(&format!("reading table {table}"), || {
        let Some(manifest) = latest(dir)? else {
            return Ok(Some(Vec::new()));
        };
        let files = manifest.tables.get(table);
        let mut batches = Vec::new();
        for file in files.into_iter().flat_map(|files| &files.files) {
            match manifest::read(dir, &file.path, schema) {
                Ok(read) => batches.extend(read),
                Err(error) if error.code() == Some(io::FILE_MISSING) => {
                    let newer = latest(dir)?.is_some_and(|now| now.version != manifest.version);
                    return if newer { Ok(None) } else { Err(error) };
                }
                Err(error) => return Err(error),
            }
        }
        Ok(Some(batches))
    })
}
