//! Numbered versions of one JSON document in a directory: each created at most once, the newest
//! read, the oldest removed.

use std::ffi::OsStr;
use std::io::{self, ErrorKind, Write as _};

use crate::limits::{KEPT_VERSIONS, PUBLISH_ATTEMPTS};
use crate::rooted::{Dir, Limit};

/// The file holding `version`.
pub(super) fn name(version: u64) -> String {
    format!("{version:020}.json")
}

/// The version the file `name` holds, if it is named as [`name`] names one.
fn version(name: &OsStr) -> Option<u64> {
    let stem = name.to_str()?.strip_suffix(".json")?;
    let canonical = stem.len() == 20 && stem.bytes().all(|byte| byte.is_ascii_digit());
    canonical.then(|| stem.parse().ok()).flatten()
}

/// The versions in `dir`, ascending.
pub(super) fn listed(dir: &Dir) -> io::Result<Vec<u64>> {
    let mut versions: Vec<u64> = dir
        .entries()?
        .iter()
        .filter_map(|(name, _)| version(name))
        .collect();
    versions.sort_unstable();
    Ok(versions)
}

/// The newest version in `dir` and its bytes, at most `limit` of them, if there is one.
///
/// A version removed between being listed and being read was superseded: the versions are
/// listed again.
pub(super) fn newest(dir: &Dir, limit: Limit) -> io::Result<Option<(u64, Vec<u8>)>> {
    let mut gone = None;
    for _ in 0..PUBLISH_ATTEMPTS {
        let Some(version) = listed(dir)?.last().copied() else {
            return Ok(None);
        };
        match dir.read(name(version), limit) {
            Ok(bytes) => return Ok(Some((version, bytes))),
            Err(error) if error.kind() == ErrorKind::NotFound => gone = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(gone.unwrap_or_else(|| ErrorKind::NotFound.into()))
}

/// Creates `version` in `dir` holding `bytes`, durably, unless it exists; returns whether it
/// did, and leaves nothing behind where it could not.
pub(super) fn create(dir: &Dir, version: u64, bytes: &[u8]) -> io::Result<bool> {
    let mut temporary = dir.temporary()?;
    temporary.file().write_all(bytes)?;
    temporary.publish(name(version))
}

/// Removes the `versions` of `dir` older than those kept behind `newest`.
pub(super) fn prune(dir: &Dir, versions: &[u64], newest: u64) {
    for old in versions {
        if old.saturating_add(KEPT_VERSIONS) < newest {
            drop(dir.remove_file(name(*old)));
        }
    }
}
