//! A destination that writes files under a root directory and publishes them with manifests.

use std::collections::BTreeSet;
use std::fs;
use std::num::NonZeroU16;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;
use rdlt_connector::{
    CommitKind, Epoch, IdentifierCase, IdentifierChars, IdentifierRules, NestedSupport,
    SchemaChanges, TypeKind, WriteModes,
};
use schemars::JsonSchema;
use serde::Deserialize;

use super::format::FileFormat;
use super::manifest::{self, Manifest};
use super::session::{FilesSession, Location};
use super::{io, tables};
use crate::blocking::blocking;

/// Configuration of [`FilesDestination`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FilesDestinationConfig {
    /// The directory the destination writes under, created when missing.
    pub root: PathBuf,
    /// How files store rows.
    #[serde(default)]
    pub format: FileFormat,
}

/// Writes each flushed batch to its own file and publishes a commit by creating the pipeline's
/// next manifest, which lists every published file and the pipeline's state.
///
/// Creating a manifest version fails when another writer created it first, which fences older
/// sessions: a commit is atomic and happens at most once. Readers read only the files the latest
/// manifest lists. A merge table's commit rewrites the table as one file.
#[derive(Debug)]
pub struct FilesDestination {
    root: Arc<Path>,
    format: FileFormat,
}

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
        })
    }

    async fn check(&self) -> Result<()> {
        let root = Arc::clone(&self.root);
        blocking(move || {
            let dir = root.join("_rdlt");
            fs::create_dir_all(&dir).map_err(io::failed("creating a directory", &dir))?;
            let probe = dir.join(".check");
            fs::write(&probe, b"").map_err(io::failed("writing", &probe))?;
            fs::remove_file(&probe).map_err(io::failed("removing", &probe))
        })
        .await
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<FilesSession>> {
        let dir = manifest::pipeline_dir(&self.root, &context.pipeline);
        let opening = dir.clone();
        let manifest = blocking(move || next_epoch(&opening)).await?;
        let location = Location {
            root: Arc::clone(&self.root),
            dir,
            format: self.format,
            epoch: manifest.epoch,
            load_id: context.load_id,
        };
        Ok(Opened {
            state: manifest.records()?,
            epoch: manifest.epoch,
            session: FilesSession::new(location),
        })
    }
}

/// Creates the pipeline's next manifest with the next epoch, retrying while other sessions
/// create versions first.
fn next_epoch(dir: &Path) -> Result<Manifest> {
    loop {
        let mut manifest = manifest::latest(dir)?.unwrap_or_default();
        manifest.version += 1;
        manifest.epoch = manifest.epoch.next();
        if manifest::put(dir, &manifest)? {
            return Ok(manifest);
        }
    }
}

/// Removes the files sessions of the pipeline in `dir` older than `epoch` staged that the latest
/// manifest does not list; listed paths are relative to `root`.
pub(super) fn discard(root: &Path, dir: &Path, epoch: Epoch) -> Result<()> {
    let listed: BTreeSet<PathBuf> = manifest::latest(dir)?
        .unwrap_or_default()
        .files()
        .map(PathBuf::from)
        .collect();
    let staging = dir.join("staging");
    let Ok(epochs) = fs::read_dir(&staging) else {
        return Ok(());
    };
    for entry in epochs.flatten() {
        let older = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u64>().ok())
            .is_some_and(|staged| staged < epoch.0);
        if older {
            remove_unlisted(&entry.path(), root, &listed)?;
        }
    }
    Ok(())
}

/// Removes the files under `path` whose path relative to `root` is not `listed`, and the
/// directories left empty; returns whether `path` is gone.
fn remove_unlisted(path: &Path, root: &Path, listed: &BTreeSet<PathBuf>) -> Result<bool> {
    if path.is_dir() {
        let entries = fs::read_dir(path).map_err(io::failed("listing", path))?;
        let mut empty = true;
        for entry in entries {
            let entry = entry.map_err(io::failed("listing", path))?;
            empty &= remove_unlisted(&entry.path(), root, listed)?;
        }
        if empty {
            fs::remove_dir(path).map_err(io::failed("removing", path))?;
        }
        return Ok(empty);
    }
    let relative = path.strip_prefix(root).unwrap_or(path);
    if listed.contains(relative) {
        return Ok(false);
    }
    fs::remove_file(path).map_err(io::failed("removing", path))?;
    Ok(true)
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
        history: false,
    };
    capabilities.nested = NestedSupport {
        structs: true,
        lists: true,
        json: types.contains(&TypeKind::Json),
    };
    capabilities.types = types;
    capabilities.schema_changes = SchemaChanges::all();
    capabilities.identifiers = IdentifierRules {
        case: IdentifierCase::Lower,
        max_len: NonZeroU16::new(128).expect("128 is non-zero"),
        chars: IdentifierChars::AsciiWord,
        reserved: BTreeSet::new(),
    };
    capabilities.max_parallel_writers = NonZeroU16::new(4).expect("4 is non-zero");
    capabilities
}

/// Every published batch of `table` under `root`, over every pipeline's latest manifest.
pub fn published(root: impl Into<PathBuf>, table: &str) -> Result<Vec<RecordBatch>> {
    let root = root.into();
    let schema = Arc::new(
        tables::read(&root, table)?
            .map_or_else(arrow_schema::Schema::empty, |schema| schema.to_arrow()),
    );
    let pipelines = root.join("_rdlt").join("pipelines");
    let Ok(entries) = fs::read_dir(&pipelines) else {
        return Ok(Vec::new());
    };
    let mut batches = Vec::new();
    for entry in entries.flatten() {
        let Some(manifest) = manifest::latest(&entry.path())? else {
            continue;
        };
        let Some(files) = manifest.tables.get(table) else {
            continue;
        };
        for file in &files.files {
            let path = root.join(file);
            let format = path
                .extension()
                .and_then(|extension| extension.to_str())
                .and_then(FileFormat::of)
                .unwrap_or_default();
            batches.extend(format.read(&path, &schema)?);
        }
    }
    Ok(batches)
}
