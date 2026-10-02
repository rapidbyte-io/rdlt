//! What each connector of a provider was granted, held while it runs: one pipeline's
//! connector is not granted what another's was, unless both grants say they may be shared.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use super::sandbox::{Grants, SandboxError};

#[cfg(test)]
mod tests;

/// A grant as it is held: its paths with every link resolved.
#[derive(Debug)]
struct Held {
    id: u64,
    read: Vec<PathBuf>,
    write: Vec<PathBuf>,
    shared: bool,
}

/// The grants of every connector a provider runs, shared by the provider's clones.
#[derive(Clone, Debug, Default)]
pub(crate) struct Leases {
    held: Arc<Mutex<Vec<Held>>>,
    next: Arc<AtomicU64>,
}

/// A connector's grants, held until it is dropped, its placement's last spawn with it.
#[derive(Debug)]
pub(crate) struct Lease {
    leases: Leases,
    id: u64,
    /// The paths it may write, every link resolved.
    pub(crate) write: Vec<PathBuf>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut held = self
            .leases
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        held.retain(|held| held.id != self.id);
    }
}

/// `path` with every link resolved, as a grant names it: absolute, and there.
fn resolved(path: &Path) -> Result<PathBuf, SandboxError> {
    let refused = || SandboxError::Grant {
        path: path.to_owned(),
    };
    if !path.is_absolute() {
        return Err(refused());
    }
    std::fs::canonicalize(path).map_err(|_| refused())
}

/// Whether one of `one` and `other` holds the other, or they are the same.
fn overlaps(one: &Path, other: &Path) -> bool {
    one.starts_with(other) || other.starts_with(one)
}

impl Leases {
    /// Holds `grants` for a connector, refusing paths another connector holds now, unless
    /// both grants are shared.
    pub(crate) fn take(&self, grants: &Grants) -> Result<Lease, SandboxError> {
        let read = grants
            .read
            .iter()
            .map(|path| resolved(path))
            .collect::<Result<Vec<_>, _>>()?;
        let write = grants
            .write
            .iter()
            .map(|path| resolved(path))
            .collect::<Result<Vec<_>, _>>()?;
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        for other in held.iter().filter(|other| !(other.shared && grants.shared)) {
            let theirs = other.write.iter().chain(&other.read);
            let written = write
                .iter()
                .flat_map(|path| theirs.clone().map(move |them| (path, them)));
            let read_written = read
                .iter()
                .flat_map(|path| other.write.iter().map(move |them| (path, them)));
            if let Some((path, _)) = written
                .chain(read_written)
                .find(|(ours, theirs)| overlaps(ours, theirs))
            {
                return Err(SandboxError::Overlap { path: path.clone() });
            }
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        held.push(Held {
            id,
            read,
            write: write.clone(),
            shared: grants.shared,
        });
        Ok(Lease {
            leases: self.clone(),
            id,
            write,
        })
    }
}

/// Refuses a path `lease` may write that holds one of `programs`, or lies within one of
/// `directories`: whoever may write there decides what a host runs.
pub(crate) fn guarding(
    lease: &Lease,
    programs: &[&Path],
    directories: &[PathBuf],
) -> Result<(), SandboxError> {
    let resolve = |path: &Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_owned());
    let programs: Vec<PathBuf> = programs.iter().map(|path| resolve(path)).collect();
    let directories: Vec<PathBuf> = directories.iter().map(|path| resolve(path)).collect();
    for written in &lease.write {
        let holds = programs.iter().any(|program| program.starts_with(written));
        let within = directories
            .iter()
            .any(|directory| overlaps(written, directory));
        if holds || within {
            return Err(SandboxError::Covers {
                path: written.clone(),
            });
        }
    }
    Ok(())
}
