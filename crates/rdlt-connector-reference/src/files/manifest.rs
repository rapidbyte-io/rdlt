//! A pipeline's manifests: each version lists the published files and the pipeline's state, and
//! is created only if no other writer created that version first.
//!
//! A manifest is checked when it is read: its version is its file's, the tables it names are
//! identifiers, and the files it lists are staged files of its own pipeline.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rdlt_connector::{
    CommitSeq, ConnectorError, Epoch, GenerationId, LoadId, PipelineId, Receipt, Result,
    StateChange, StateRecord,
};
use serde::{Deserialize, Serialize};

use super::format::FileFormat;
use super::{io, tables, versions};
use crate::limits::{MANIFEST_BYTES, RECEIPT_LOADS, TEMPORARY_AGE};
use crate::rooted::{self, Dir, Limit};

/// The code of an error for a manifest that is not what the destination writes.
pub(super) const MANIFEST_INVALID: &str = "manifest_invalid";

/// The directory of a pipeline's manifests, in the pipeline's directory.
const MANIFESTS: &str = "manifests";

/// The directory of a pipeline's staged and published files, in the pipeline's directory.
pub(super) const STAGING: &str = "staging";

const LIMIT: Limit = Limit {
    name: "manifest bytes",
    bytes: MANIFEST_BYTES,
};

/// One version of a pipeline's manifest.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Manifest {
    /// Grows by one with every open and commit.
    pub(super) version: u64,
    /// The epoch of the latest session.
    pub(super) epoch: Epoch,
    /// The pipeline's state records, their values in base64.
    pub(super) state: BTreeMap<String, String>,
    /// The receipts of the most recent loads' commits.
    pub(super) receipts: Vec<StoredReceipt>,
    /// Each table's published files and replace generations, by identifier.
    pub(super) tables: BTreeMap<String, TableFiles>,
    /// The identifier of each table path the pipeline wrote, by the path's JSON.
    pub(super) paths: BTreeMap<String, String>,
    /// Tables the pipeline dropped whose catalogs may remain: the next open removes them, before
    /// anything can create the tables again.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(super) dropped: BTreeSet<String>,
}

/// A commit's receipt, its commit time in microseconds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct StoredReceipt {
    load_id: LoadId,
    commit_seq: CommitSeq,
    committed_at: u64,
    rows: u64,
    bytes: u64,
}

/// A published file: its path relative to the pipeline's directory, and what it holds.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct Listed {
    pub(super) path: String,
    pub(super) rows: u64,
    pub(super) bytes: u64,
}

/// A table's published files and those of generations not yet swapped in.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct TableFiles {
    pub(super) files: Vec<Listed>,
    pub(super) generations: BTreeMap<GenerationId, Vec<Listed>>,
    /// A change stream's tombstones: the rows it removed outright, which no earlier change
    /// brings back.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) tombstones: Vec<Listed>,
}

impl Manifest {
    /// The state records.
    pub(super) fn records(&self) -> Result<Vec<StateRecord>> {
        self.state
            .iter()
            .map(|(key, value)| {
                // A manifest read from disk was checked for this; one built here holds none.
                let value = STANDARD.decode(value).map_err(|error| {
                    let message = format!("state record {key} is not base64: {error}");
                    ConnectorError::data(message).with_code(MANIFEST_INVALID)
                })?;
                Ok(StateRecord {
                    key: key.clone(),
                    value: value.into(),
                })
            })
            .collect()
    }

    /// Applies `changes` to the state records.
    pub(super) fn apply(&mut self, changes: &[StateChange]) {
        for change in changes {
            match change {
                StateChange::Put(record) => {
                    self.state
                        .insert(record.key.clone(), STANDARD.encode(&record.value));
                }
                StateChange::Delete(key) => {
                    self.state.remove(key);
                }
            }
        }
    }

    /// The stored receipt of `(load_id, commit_seq)`, if that commit happened.
    pub(super) fn receipt(&self, load_id: LoadId, commit_seq: CommitSeq) -> Option<Receipt> {
        self.receipts
            .iter()
            .find(|stored| stored.load_id == load_id && stored.commit_seq == commit_seq)
            .map(|stored| Receipt {
                load_id: stored.load_id,
                commit_seq: stored.commit_seq,
                committed_at: UNIX_EPOCH + Duration::from_micros(stored.committed_at),
                rows: stored.rows,
                bytes: stored.bytes,
            })
    }

    /// Records `receipt`, forgetting the receipts of loads older than the most recent ones.
    ///
    /// Every receipt of a load that is kept is kept: a replay repeats a commit however far back
    /// in its load, and is answered with its receipt.
    pub(super) fn record(&mut self, receipt: &Receipt) {
        self.receipts.push(StoredReceipt {
            load_id: receipt.load_id,
            commit_seq: receipt.commit_seq,
            committed_at: micros(receipt.committed_at),
            rows: receipt.rows,
            bytes: receipt.bytes,
        });
        let mut loads: Vec<LoadId> = Vec::new();
        for stored in self.receipts.iter().rev() {
            if !loads.contains(&stored.load_id) {
                loads.push(stored.load_id);
            }
        }
        loads.truncate(RECEIPT_LOADS);
        self.receipts
            .retain(|stored| loads.contains(&stored.load_id));
    }

    /// Every file the manifest lists.
    pub(super) fn files(&self) -> impl Iterator<Item = &Listed> {
        self.tables.values().flat_map(|table| {
            table
                .files
                .iter()
                .chain(table.generations.values().flatten())
                .chain(&table.tombstones)
        })
    }

    /// What keeps the manifest, read or to be written as `version`, from being one the
    /// destination writes, if anything does.
    fn fault(&self, version: u64) -> Option<String> {
        if self.version != version {
            return Some(format!("it holds version {}", self.version));
        }
        let names = self
            .tables
            .keys()
            .chain(&self.dropped)
            .chain(self.paths.values());
        for name in names {
            if tables::named(name).is_err() {
                return Some(format!("{name:?} is no table identifier"));
            }
        }
        if let Some(file) = self.files().find(|file| staged(&file.path).is_err()) {
            return Some(format!("{:?} is no staged file", file.path));
        }
        self.state
            .iter()
            .find(|(_, value)| STANDARD.decode(value).is_err())
            .map(|(key, _)| format!("state record {key:?} is not base64"))
    }
}

/// The names leading to the file at `path`, a path relative to a pipeline's directory, which
/// lies under the pipeline's staging.
pub(super) fn staged(path: &str) -> std::io::Result<Vec<&str>> {
    let names = rooted::components(path)?;
    if names.len() < 2 || names[0] != STAGING {
        return Err(rooted::Refusal::Name.into());
    }
    Ok(names)
}

/// The directory holding the file listed at `path` under the pipeline's directory `dir`, and
/// the file's name in it.
pub(super) fn located<'a>(dir: &Dir, path: &'a str) -> Result<(Dir, &'a str)> {
    let reached = dir.at(path);
    let names = staged(path).map_err(io::failed("opening", &reached))?;
    let (file, parents) = names.split_last().expect("a staged path has two names");
    let parent = dir.walk(parents).map_err(io::listed("opening", &reached))?;
    Ok((parent, file))
}

/// The rows of the file listed at `path` under the pipeline's directory `dir`, in the format
/// its name says; JSON lines are read as `schema`.
pub(super) fn read(dir: &Dir, path: &str, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    let (parent, file) = located(dir, path)?;
    format_of(dir, path)?.read(&parent, file, schema)
}

/// The format of the file listed at `path` under `dir`, which its name's extension says.
pub(super) fn format_of(dir: &Dir, path: &str) -> Result<FileFormat> {
    FileFormat::named(path).ok_or_else(|| {
        ConnectorError::data(format!(
            "{}: no format has this name",
            dir.at(path).display()
        ))
    })
}

/// `at` to the microsecond, the precision receipts keep, so a receipt reads back equal.
pub(super) fn truncated(at: SystemTime) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(micros(at))
}

fn micros(at: SystemTime) -> u64 {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since.as_micros()).unwrap_or(u64::MAX)
}

/// The name of the directory holding `pipeline`'s manifests and staged files.
///
/// The name holds a hash of the exact id, so ids that differ only in case never share a
/// directory on a filesystem that ignores case.
pub(super) fn pipeline_dir(pipeline: &PipelineId) -> String {
    let hash = xxhash_rust::xxh3::xxh3_64(pipeline.as_str().as_bytes());
    format!("{pipeline}-{hash:016x}")
}

/// The manifests of the pipeline whose directory `dir` is, if it has any.
fn manifests(dir: &Dir) -> Result<Option<Dir>> {
    match dir.dir(MANIFESTS) {
        Ok(manifests) => Ok(Some(manifests)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io::failed("opening", &dir.at(MANIFESTS))(error)),
    }
}

/// The latest manifest of the pipeline whose directory `dir` is, if any.
pub(super) fn latest(dir: &Dir) -> Result<Option<Manifest>> {
    let Some(manifests) = manifests(dir)? else {
        return Ok(None);
    };
    let read = versions::newest(&manifests, LIMIT);
    let Some((version, bytes)) =
        read.map_err(io::failed("reading a manifest of", manifests.path()))?
    else {
        return Ok(None);
    };
    let invalid = |reason: String| {
        let path = manifests.at(versions::name(version));
        ConnectorError::data(format!("manifest {}: {reason}", path.display()))
            .with_code(MANIFEST_INVALID)
    };
    let manifest: Manifest =
        serde_json::from_slice(&bytes).map_err(|error| invalid(error.to_string()))?;
    match manifest.fault(version) {
        Some(fault) => Err(invalid(fault)),
        None => Ok(Some(manifest)),
    }
}

/// Makes the names of the manifests of the pipeline whose directory `dir` is durable.
pub(super) fn settle(dir: &Dir) -> Result<()> {
    match manifests(dir)? {
        Some(manifests) => manifests
            .sync()
            .map_err(io::failed("syncing", manifests.path())),
        None => Ok(()),
    }
}

/// Removes the temporaries writers that died left among the manifests of the pipeline whose
/// directory `dir` is.
pub(super) fn sweep(dir: &Dir) -> Result<()> {
    let Some(manifests) = manifests(dir)? else {
        return Ok(());
    };
    manifests
        .sweep(TEMPORARY_AGE)
        .map_err(io::failed("sweeping", manifests.path()))
}

/// Creates `manifest` as its version in the pipeline's directory `dir`, unless that version or
/// a newer one exists; returns whether it did, removing versions older than those kept.
///
/// A version older than the newest loses even where garbage collection removed it: its writer
/// read a manifest others have moved past.
pub(super) fn put(dir: &Dir, manifest: &Manifest) -> Result<bool> {
    // A manifest no reader accepts is never written.
    if let Some(fault) = manifest.fault(manifest.version) {
        let message = format!("the next manifest: {fault}");
        return Err(ConnectorError::data(message).with_code(MANIFEST_INVALID));
    }
    let json = serde_json::to_vec_pretty(manifest).expect("manifests serialize to JSON");
    let failed = io::failed("writing a manifest of", dir.path());
    // Nor one larger than a reader accepts.
    LIMIT
        .admit(u64::try_from(json.len()).unwrap_or(u64::MAX))
        .map_err(|refusal| failed(refusal.into()))?;
    let manifests = dir
        .dir_created(MANIFESTS)
        .map_err(io::failed("creating", &dir.at(MANIFESTS)))?;
    if !versions::create(&manifests, manifest.version, &json).map_err(&failed)? {
        return Ok(false);
    }
    let listed = versions::listed(&manifests).map_err(&failed)?;
    if listed
        .last()
        .is_some_and(|newest| *newest > manifest.version)
    {
        drop(manifests.remove_file(versions::name(manifest.version)));
        return Ok(false);
    }
    versions::prune(&manifests, &listed, manifest.version);
    Ok(true)
}
