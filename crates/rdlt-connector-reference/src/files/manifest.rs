//! A pipeline's manifests: each version lists the published files and the pipeline's state, and
//! is created only if no other writer created that version first.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rdlt_connector::{
    CommitSeq, ConnectorError, Epoch, GenerationId, LoadId, PipelineId, Receipt, Result,
    StateChange, StateRecord,
};
use serde::{Deserialize, Serialize};

use super::io;

/// Loads whose receipts a manifest keeps, most recent last; a commit of an older load is not
/// recognized again.
const RECEIPT_LOADS: usize = 16;

/// Manifest versions kept on disk besides the latest, for readers still reading them.
const KEPT_VERSIONS: u64 = 8;

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

/// A table's published files, relative to the root, and those of generations not yet swapped in.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct TableFiles {
    pub(super) files: Vec<String>,
    pub(super) generations: BTreeMap<GenerationId, Vec<String>>,
}

impl Manifest {
    /// The state records.
    pub(super) fn records(&self) -> Result<Vec<StateRecord>> {
        self.state
            .iter()
            .map(|(key, value)| {
                let value = STANDARD.decode(value).map_err(|error| {
                    ConnectorError::internal(format!("state record {key} is not base64: {error}"))
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
    pub(super) fn files(&self) -> impl Iterator<Item = &String> {
        self.tables.values().flat_map(|table| {
            table
                .files
                .iter()
                .chain(table.generations.values().flatten())
        })
    }
}

/// `at` to the microsecond, the precision receipts keep, so a receipt reads back equal.
pub(super) fn truncated(at: SystemTime) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(micros(at))
}

fn micros(at: SystemTime) -> u64 {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since.as_micros()).unwrap_or(u64::MAX)
}

/// The directory holding `pipeline`'s manifests and staged files under `root`.
///
/// The name holds a hash of the exact id, so ids that differ only in case never share a
/// directory on a filesystem that ignores case.
pub(super) fn pipeline_dir(root: &Path, pipeline: &PipelineId) -> PathBuf {
    let hash = xxhash_rust::xxh3::xxh3_64(pipeline.as_str().as_bytes());
    root.join("_rdlt")
        .join("pipelines")
        .join(format!("{pipeline}-{hash:016x}"))
}

/// The latest manifest in `dir`, if any.
pub(super) fn latest(dir: &Path) -> Result<Option<Manifest>> {
    let Some(version) = versions(&dir.join("manifests"))?.last().copied() else {
        return Ok(None);
    };
    let path = manifest_path(dir, version);
    let bytes = fs::read(&path).map_err(io::failed("reading a manifest", &path))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| ConnectorError::internal(format!("manifest {}: {error}", path.display())))
}

/// Creates `manifest` as its version in `dir`, unless that version or a newer one exists;
/// returns whether it did, removing versions older than those kept.
///
/// A version older than the newest loses even where garbage collection removed it: its writer
/// read a manifest others have moved past.
pub(super) fn put(dir: &Path, manifest: &Manifest) -> Result<bool> {
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let manifests = dir.join("manifests");
    fs::create_dir_all(&manifests).map_err(io::failed("creating a directory", &manifests))?;
    let temporary = manifests.join(format!(
        ".{}-{}-{}.tmp",
        manifest.version,
        std::process::id(),
        WRITES.fetch_add(1, Ordering::Relaxed)
    ));
    let json = serde_json::to_vec_pretty(manifest).expect("manifests serialize to JSON");
    let written = (|| {
        let mut file = fs::File::create_new(&temporary)?;
        file.write_all(&json)?;
        file.sync_all()
    })();
    written.map_err(io::failed("writing a manifest", &temporary))?;
    let path = manifest_path(dir, manifest.version);
    let linked = fs::hard_link(&temporary, &path);
    drop(fs::remove_file(&temporary));
    match linked {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::AlreadyExists => return Ok(false),
        Err(error) => return Err(io::failed("publishing a manifest", &path)(error)),
    }
    io::sync_dir(&manifests)?;
    let versions = versions(&manifests)?;
    if versions
        .last()
        .is_some_and(|newest| *newest > manifest.version)
    {
        drop(fs::remove_file(&path));
        return Ok(false);
    }
    for old in versions {
        if old + KEPT_VERSIONS < manifest.version {
            drop(fs::remove_file(manifest_path(dir, old)));
        }
    }
    Ok(true)
}

fn manifest_path(dir: &Path, version: u64) -> PathBuf {
    dir.join("manifests").join(format!("{version:020}.json"))
}

/// The versions of the manifests in `manifests`, ascending.
fn versions(manifests: &Path) -> Result<Vec<u64>> {
    let entries = match fs::read_dir(manifests) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io::failed("listing manifests", manifests)(error)),
    };
    let mut versions = Vec::new();
    for entry in entries {
        let entry = entry.map_err(io::failed("listing manifests", manifests))?;
        let name = entry.file_name();
        let version = name
            .to_str()
            .and_then(|name| name.strip_suffix(".json"))
            .and_then(|stem| stem.parse::<u64>().ok());
        versions.extend(version);
    }
    versions.sort_unstable();
    Ok(versions)
}
