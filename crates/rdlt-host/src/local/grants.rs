//! What each placement of this process holds while a connector of it runs: its grants, each
//! opened once and known by what was opened, and the programs it runs.
//!
//! A grant must lie within a root its operator lets grants be made in, and a root that may be
//! written must hold nothing that decides what the host runs or keeps. One pipeline's connector
//! is not granted what another's running now was, unless both grants say they may be shared;
//! no grant may write a program any placement runs; and no placement runs a program a grant
//! held now may write.

mod opened;
#[cfg(test)]
mod tests;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

pub(crate) use opened::{Chain, Opened};

use super::sandbox::{Grants, SandboxError};

/// What a placement holds, by identity.
#[derive(Debug)]
struct Held {
    id: u64,
    read: Vec<Chain>,
    write: Vec<Chain>,
    programs: Vec<Chain>,
    shared: bool,
}

/// What the placements of a process hold: one registry for the whole process, whichever
/// provider placed them.
#[derive(Clone, Debug, Default)]
pub(crate) struct Leases {
    held: Arc<Mutex<Vec<Held>>>,
    next: Arc<AtomicU64>,
}

/// A path granted, opened once: what a sandbox binds, at the path the grant names.
#[derive(Debug)]
pub(crate) struct Bound {
    /// What was opened and checked.
    pub(crate) file: File,
    /// Where the connector finds it: the path as granted.
    pub(crate) at: PathBuf,
    /// Whether the connector may write it.
    pub(crate) write: bool,
}

/// What a placement holds, until it is dropped: the placement, and each connector spawned
/// from it until that connector is reaped, hold it.
#[derive(Debug)]
pub(crate) struct Lease {
    leases: Leases,
    id: u64,
    /// What the connector is granted, the paths it reads first.
    pub(crate) bound: Vec<Bound>,
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

/// The roots its operator lets a provider's grants be made in.
#[derive(Clone, Debug, Default)]
pub(crate) struct Roots {
    /// Where a grant may read.
    pub(crate) read: Vec<PathBuf>,
    /// Where a grant may read and write.
    pub(crate) write: Vec<PathBuf>,
}

/// What no root that may be written may hold.
#[derive(Clone, Debug, Default)]
pub(crate) struct Guarded {
    /// Directories it may neither hold nor lie within: where connectors are found, the host's
    /// executable is, the host keeps its state, and secrets are read from.
    pub(crate) directories: Vec<PathBuf>,
    /// Files it may not hold: what confines a connector.
    pub(crate) files: Vec<PathBuf>,
}

/// A program a placement runs, where it was found, and where it lies.
#[derive(Clone, Debug)]
pub(crate) struct Program {
    pub(crate) path: PathBuf,
    pub(crate) chain: Chain,
}

/// What a placement asks to hold.
#[derive(Debug)]
pub(crate) struct Claim<'a> {
    /// What the reference grants.
    pub(crate) grants: &'a Grants,
    /// What every connector of the provider may read.
    pub(crate) shared_reads: &'a [PathBuf],
    /// Where the provider's operator lets grants be made.
    pub(crate) roots: &'a Roots,
    /// What no root that may be written may hold.
    pub(crate) guarded: &'a Guarded,
    /// The programs the placement runs: its binary, and a script's interpreter.
    pub(crate) programs: &'a [Program],
}

/// `path`, opened, as a grant or a root names it: absolute, and there.
fn opened(path: &Path) -> Result<Opened, SandboxError> {
    Opened::at(path).map_err(|_| SandboxError::Grant {
        path: path.to_owned(),
    })
}

/// Each of `paths`, opened, with its path.
fn all_opened(paths: &[PathBuf]) -> Result<Vec<(&Path, Opened)>, SandboxError> {
    paths
        .iter()
        .map(|path| Ok((path.as_path(), opened(path)?)))
        .collect()
}

impl Leases {
    /// The registry of this process.
    pub(crate) fn process() -> Self {
        static PROCESS: LazyLock<Leases> = LazyLock::new(Leases::default);
        PROCESS.clone()
    }

    /// Holds what `claim` asks for, once each path is seen to lie where it may.
    ///
    /// # Errors
    ///
    /// A [`SandboxError`] naming the first path that may not be held.
    pub(crate) fn take(&self, claim: &Claim<'_>) -> Result<Lease, SandboxError> {
        let read_roots = all_opened(&claim.roots.read)?;
        let write_roots = all_opened(&claim.roots.write)?;
        guarding(&write_roots, claim.guarded, claim.programs)?;
        let writable: Vec<&Chain> = write_roots.iter().map(|(_, root)| &root.chain).collect();
        let readable: Vec<&Chain> = read_roots.iter().map(|(_, root)| &root.chain).collect();
        let within = |granted: &Opened, roots: &[&Chain]| {
            roots.iter().any(|root| granted.chain.within(root))
        };
        let reads = all_opened(&claim.grants.read)?;
        let writes = all_opened(&claim.grants.write)?;
        let outside = reads
            .iter()
            .find(|(_, read)| !within(read, &readable) && !within(read, &writable))
            .or_else(|| {
                let mut writes = writes.iter();
                writes.find(|(_, write)| !within(write, &writable))
            });
        if let Some((path, _)) = outside {
            return Err(SandboxError::Outside {
                path: path.to_path_buf(),
            });
        }
        let shared = all_opened(claim.shared_reads)?;
        let read_by_all = writes.iter().find(|(_, write)| {
            shared
                .iter()
                .any(|(_, read)| write.chain.overlaps(&read.chain))
        });
        if let Some((path, _)) = read_by_all {
            return Err(SandboxError::Overlap {
                path: path.to_path_buf(),
            });
        }
        let reads: Vec<_> = shared.into_iter().chain(reads).collect();
        self.held(claim, reads, writes)
    }

    /// Holds `reads` and `writes`, and `claim`'s programs, unless what another placement holds
    /// now refuses them.
    fn held(
        &self,
        claim: &Claim<'_>,
        reads: Vec<(&Path, Opened)>,
        writes: Vec<(&Path, Opened)>,
    ) -> Result<Lease, SandboxError> {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        for other in held.iter() {
            refused_by(other, claim, &reads, &writes)?;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        held.push(Held {
            id,
            read: reads.iter().map(|(_, read)| read.chain.clone()).collect(),
            write: writes
                .iter()
                .map(|(_, write)| write.chain.clone())
                .collect(),
            programs: claim
                .programs
                .iter()
                .map(|program| program.chain.clone())
                .collect(),
            shared: claim.grants.shared,
        });
        let bound = |write: bool| {
            move |(at, opened): (&Path, Opened)| Bound {
                file: opened.file,
                at: at.to_owned(),
                write,
            }
        };
        let bound = reads
            .into_iter()
            .map(bound(false))
            .chain(writes.into_iter().map(bound(true)))
            .collect();
        Ok(Lease {
            leases: self.clone(),
            id,
            bound,
        })
    }
}

/// Refuses `claim`, whose paths are `reads` and `writes`, where `other` holds what they overlap,
/// unless both are shared; where they may write a program `other` runs; and where `other` may
/// write a program `claim` runs.
fn refused_by(
    other: &Held,
    claim: &Claim<'_>,
    reads: &[(&Path, Opened)],
    writes: &[(&Path, Opened)],
) -> Result<(), SandboxError> {
    let path = |path: &Path| path.to_path_buf();
    if !(other.shared && claim.grants.shared) {
        let theirs: Vec<&Chain> = other.write.iter().chain(&other.read).collect();
        let written = writes
            .iter()
            .find(|(_, ours)| theirs.iter().any(|theirs| ours.chain.overlaps(theirs)));
        let read = || {
            let mut reads = reads.iter();
            reads.find(|(_, ours)| other.write.iter().any(|theirs| ours.chain.overlaps(theirs)))
        };
        if let Some((overlapping, _)) = written.or_else(read) {
            return Err(SandboxError::Overlap {
                path: path(overlapping),
            });
        }
    }
    let covering = writes.iter().find(|(_, ours)| {
        other
            .programs
            .iter()
            .any(|program| program.within(&ours.chain))
    });
    if let Some((covering, _)) = covering {
        return Err(SandboxError::Covers {
            path: path(covering),
        });
    }
    let exposed = claim.programs.iter().find(|program| {
        other
            .write
            .iter()
            .any(|theirs| program.chain.within(theirs))
    });
    if let Some(exposed) = exposed {
        return Err(SandboxError::Exposed {
            path: exposed.path.clone(),
        });
    }
    Ok(())
}

/// Refuses a root of `roots`, each one that may be written, that holds or lies within one of
/// `guarded`'s directories, or holds the directory of one of `programs`, one of `programs`, or
/// one of `guarded`'s files: whoever may write there decides what a host runs, or keeps.
fn guarding(
    roots: &[(&Path, Opened)],
    guarded: &Guarded,
    programs: &[Program],
) -> Result<(), SandboxError> {
    if roots.is_empty() {
        return Ok(());
    }
    let refused = |root: &Path| SandboxError::Guarded {
        path: root.to_owned(),
    };
    // Each with whether a root may not lie within it either.
    let mut directories = Vec::new();
    for directory in &guarded.directories {
        // One that is not there yet is guarded by the nearest directory above it that is.
        let (opened, there) = Opened::nearest(directory).map_err(|_| SandboxError::Grant {
            path: directory.clone(),
        })?;
        directories.push((opened.chain, there));
    }
    let directories_of = programs.iter().filter_map(|program| program.chain.parent());
    directories.extend(directories_of.map(|directory| (directory, false)));
    let mut files: Vec<Chain> = programs
        .iter()
        .map(|program| program.chain.clone())
        .collect();
    for file in &guarded.files {
        if let Ok((opened, _)) = Opened::nearest(file) {
            files.push(opened.chain);
        }
    }
    for (path, root) in roots {
        let touches = directories.iter().any(|(directory, both_ways)| {
            directory.within(&root.chain) || (*both_ways && root.chain.within(directory))
        });
        if touches || files.iter().any(|file| file.within(&root.chain)) {
            return Err(refused(path));
        }
    }
    Ok(())
}
