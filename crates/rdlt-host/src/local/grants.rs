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
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;

pub(crate) use opened::{Chain, Opened, located};

use super::binary::Tree;
use super::sandbox::{Access, SandboxError};

/// What a placement holds, by identity.
#[derive(Debug)]
struct Held {
    id: u64,
    read: Vec<Chain>,
    write: Vec<Chain>,
    /// What it runs: its binary, a script's interpreter, and the sandbox's launcher.
    programs: Vec<Program>,
    shared: bool,
    /// Alive while the placement that took it is: once it is not, only connectors told to stop
    /// hold what it holds, each until it is reaped.
    placed: Weak<()>,
}

/// What the placements of a process hold: one registry for the whole process, whichever
/// provider placed them.
#[derive(Clone, Debug, Default)]
pub(crate) struct Leases {
    held: Arc<Mutex<Vec<Held>>>,
    next: Arc<AtomicU64>,
    /// Told each time what a placement held is released.
    released: Arc<Notify>,
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

/// What a placement holds: held while the placement is, and while each connector spawned from
/// it holds its [`Holding`], until that connector is reaped.
#[derive(Debug)]
pub(crate) struct Lease {
    /// The placement's own, dropped before `holding`: once it is, what the lease holds is held
    /// by connectors told to stop alone, each until it is reaped.
    _placed: Arc<()>,
    holding: Holding,
    /// What the connector is granted, the paths it reads first.
    pub(crate) bound: Vec<Bound>,
}

/// What a lease holds, held by its placement and by each connector spawned from it until that
/// connector is reaped, and released once none holds it.
#[derive(Clone, Debug)]
pub(crate) struct Holding(Arc<Entry>);

/// A lease's place in its registry.
#[derive(Debug)]
struct Entry {
    leases: Leases,
    id: u64,
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.leases.held.lock().retain(|held| held.id != self.id);
        self.leases.released.notify_waiters();
    }
}

impl Lease {
    /// What a connector spawned from the placement holds until it is reaped.
    pub(crate) fn holding(&self) -> Holding {
        self.holding.clone()
    }

    /// A program its placement runs that was held as having one name and has another now, by
    /// the name it is run by; not `binary`, which is run from the file placed, whatever its
    /// name leads to now.
    pub(crate) fn exposed(&self, binary: &Path) -> Option<PathBuf> {
        let Entry { leases, id } = &*self.holding.0;
        let held = leases.held.lock();
        let held = held.iter().find(|held| held.id == *id)?;
        let named = |path: &Path| std::fs::metadata(path).is_ok_and(|now| now.nlink() > 1);
        held.programs
            .iter()
            .filter(|program| program.path != binary)
            .find(|program| !program.linked && named(&program.path))
            .map(|program| program.path.clone())
    }
}

/// Why a claim was not held: the first path refused, and whether only what placements that are
/// gone hold refused it, which their connectors, told to stop, release once they are reaped.
#[derive(Debug)]
struct Refused {
    error: SandboxError,
    ending: bool,
}

impl From<SandboxError> for Refused {
    fn from(error: SandboxError) -> Self {
        Self {
            error,
            ending: false,
        }
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
    /// Whether another name led to it when it was opened: a hard link, which may lie
    /// anywhere on its file system, where no chain shows it.
    pub(crate) linked: bool,
}

/// What a placement asks to hold.
#[derive(Debug)]
pub(crate) struct Claim<'a> {
    /// The access the reference asks for.
    pub(crate) access: &'a Access,
    /// What every connector of the provider may read.
    pub(crate) shared_reads: &'a [PathBuf],
    /// Where the provider's operator lets access be given.
    pub(crate) roots: &'a Roots,
    /// What no root that may be written may hold.
    pub(crate) guarded: &'a Guarded,
    /// The programs the placement runs: its binary, and a script's interpreter.
    pub(crate) programs: &'a [Program],
    /// The tree of the directory connector the binary launches, where it launches one: held as
    /// a read whether its connector is confined or trusted, so no grant held now writes it, and
    /// guarded as a connector directory is, so no root that may be written holds it or lies
    /// within it.
    pub(crate) tree: Option<&'a Tree>,
}

/// `path`, opened, as a grant or a root names it: absolute, and there.
fn opened(path: &Path) -> Result<Opened, SandboxError> {
    Opened::at(path).map_err(|_| SandboxError::Grant {
        path: path.to_owned(),
    })
}

/// The directory `tree` was opened as, where its binary was checked, as a read holds it.
fn tree_held(tree: &Tree) -> Result<Opened, SandboxError> {
    let refused = |_| SandboxError::Grant {
        path: tree.path.clone(),
    };
    let file = tree.directory.try_clone().map_err(refused)?;
    let chain = Chain::of(&file, &tree.path).map_err(refused)?;
    Ok(Opened { file, chain })
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

    /// Holds what `claim` asks for as `claimed` does, refused at once whatever refuses it.
    #[cfg(test)]
    pub(crate) fn take(&self, claim: &Claim<'_>) -> Result<Lease, SandboxError> {
        self.claimed(claim).map_err(|refused| refused.error)
    }

    /// Holds what `claim` asks for as `claimed` does, waiting up to `patience` while only what
    /// placements that are gone hold refuses it: their connectors, told to stop, release it once
    /// they are reaped.
    ///
    /// # Errors
    ///
    /// A [`SandboxError`] naming the first path that may not be held, once nothing is left to
    /// wait for or `patience` has passed.
    pub(crate) async fn take_within(
        &self,
        claim: &Claim<'_>,
        patience: Duration,
    ) -> Result<Lease, SandboxError> {
        let until = tokio::time::Instant::now().checked_add(patience);
        loop {
            // Asked for before the claim is tried, so a release after the try is not missed.
            let mut released = std::pin::pin!(self.released.notified());
            released.as_mut().enable();
            match self.claimed(claim) {
                Err(Refused {
                    error,
                    ending: true,
                }) => {
                    let waited = if let Some(until) = until {
                        tokio::time::timeout_at(until, released).await.is_ok()
                    } else {
                        released.await;
                        true
                    };
                    if !waited {
                        return Err(error);
                    }
                }
                claimed => return claimed.map_err(|refused| refused.error),
            }
        }
    }

    /// Holds what `claim` asks for, once each path is seen to lie where it may.
    fn claimed(&self, claim: &Claim<'_>) -> Result<Lease, Refused> {
        let read_roots = all_opened(&claim.roots.read)?;
        let write_roots = all_opened(&claim.roots.write)?;
        let mut programs = claim.programs.to_vec();
        programs.extend(launchers(&claim.guarded.files)?);
        // The tree as it was checked, by its descriptor, not by what its path names now.
        let tree = match claim.tree {
            Some(tree) => Some((tree.path.as_path(), tree_held(tree)?)),
            None => None,
        };
        let chains: Vec<&Chain> = tree.iter().map(|(_, held)| &held.chain).collect();
        guarding(&write_roots, &claim.guarded.directories, &chains, &programs)?;
        let writable: Vec<&Chain> = write_roots.iter().map(|(_, root)| &root.chain).collect();
        let readable: Vec<&Chain> = read_roots.iter().map(|(_, root)| &root.chain).collect();
        let within = |granted: &Opened, roots: &[&Chain]| {
            roots.iter().any(|root| granted.chain.within(root))
        };
        let reads = all_opened(&claim.access.read)?;
        let writes = all_opened(&claim.access.write)?;
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
            }
            .into());
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
            }
            .into());
        }
        // The operator's, as the system's directories are: no root need let it be read. A
        // trusted connector's reads are held and bound nowhere.
        let reads: Vec<_> = shared.into_iter().chain(tree).chain(reads).collect();
        self.held(claim, programs, reads, writes)
    }

    /// Holds `reads` and `writes`, and `programs`, those `claim` runs, unless what another
    /// placement holds now refuses them: refused at once where that placement is still there,
    /// and as ending where only connectors told to stop hold it.
    fn held(
        &self,
        claim: &Claim<'_>,
        programs: Vec<Program>,
        reads: Vec<(&Path, Opened)>,
        writes: Vec<(&Path, Opened)>,
    ) -> Result<Lease, Refused> {
        let mut held = self.held.lock();
        let mut ending = None;
        for other in held.iter() {
            if let Err(error) = refused_by(other, claim, &programs, &reads, &writes) {
                if other.placed.strong_count() > 0 {
                    return Err(error.into());
                }
                ending.get_or_insert(error);
            }
        }
        if let Some(error) = ending {
            return Err(Refused {
                error,
                ending: true,
            });
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let placed = Arc::new(());
        held.push(Held {
            id,
            read: reads.iter().map(|(_, read)| read.chain.clone()).collect(),
            write: writes
                .iter()
                .map(|(_, write)| write.chain.clone())
                .collect(),
            programs,
            shared: claim.access.shared,
            placed: Arc::downgrade(&placed),
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
            _placed: placed,
            holding: Holding(Arc::new(Entry {
                leases: self.clone(),
                id,
            })),
            bound,
        })
    }
}

impl Program {
    /// The program `opened` at `path`.
    pub(crate) fn opened(path: PathBuf, opened: Opened) -> io::Result<Self> {
        let linked = opened.file.metadata()?.nlink() > 1;
        Ok(Self {
            path,
            chain: opened.chain,
            linked,
        })
    }

    /// Whether whoever may write `written` may write this program.
    ///
    /// It lies within `written`, or has a second name, which may lie within anything on its
    /// file system.
    fn written_through(&self, written: &Chain) -> bool {
        self.chain.within(written) || (self.linked && self.chain.shares_device(written))
    }
}

/// Refuses `claim`, whose paths are `reads` and `writes`, where `other` holds what they overlap,
/// unless both are shared; where they may write a program `other` runs; and where `other` may
/// write one of `programs`, those `claim` runs.
///
/// A program with a second name may be written through any path on its file system.
fn refused_by(
    other: &Held,
    claim: &Claim<'_>,
    programs: &[Program],
    reads: &[(&Path, Opened)],
    writes: &[(&Path, Opened)],
) -> Result<(), SandboxError> {
    let path = |path: &Path| path.to_path_buf();
    if !(other.shared && claim.access.shared) {
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
            .any(|program| program.written_through(&ours.chain))
    });
    if let Some((covering, _)) = covering {
        return Err(SandboxError::Covers {
            path: path(covering),
        });
    }
    let exposed = programs.iter().find(|program| {
        other
            .write
            .iter()
            .any(|theirs| program.written_through(theirs))
    });
    if let Some(exposed) = exposed {
        return Err(SandboxError::Exposed {
            path: exposed.path.clone(),
        });
    }
    Ok(())
}

/// The launchers among `files`, what confines a connector, each as a program a placement
/// runs: one not there yet lies where the nearest directory above it is.
fn launchers(files: &[PathBuf]) -> Result<Vec<Program>, SandboxError> {
    let mut launchers = Vec::new();
    for file in files {
        if let Ok((opened, there)) = Opened::nearest(file) {
            let mut launcher = Program::opened(file.clone(), opened)
                .map_err(|_| SandboxError::Grant { path: file.clone() })?;
            // A directory above a file not there yet has names of its own.
            launcher.linked &= there;
            launchers.push(launcher);
        }
    }
    Ok(launchers)
}

/// Refuses a root of `roots`, each one that may be written, that holds or lies within one of
/// `guarded`, or of `checked`, guarded directories already opened, or holds one of `programs` or
/// its directory, or lies on the file system of one of `programs` that has a second name:
/// whoever may write there decides what a host runs, or keeps.
fn guarding(
    roots: &[(&Path, Opened)],
    guarded: &[PathBuf],
    checked: &[&Chain],
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
    for directory in guarded {
        // One that is not there yet is guarded by the nearest directory above it that is.
        let (opened, there) = Opened::nearest(directory).map_err(|_| SandboxError::Grant {
            path: directory.clone(),
        })?;
        directories.push((opened.chain, there));
    }
    directories.extend(checked.iter().map(|chain| ((*chain).clone(), true)));
    let directories_of = programs.iter().filter_map(|program| program.chain.parent());
    directories.extend(directories_of.map(|directory| (directory, false)));
    for (path, root) in roots {
        let touches = directories.iter().any(|(directory, both_ways)| {
            directory.within(&root.chain) || (*both_ways && root.chain.within(directory))
        });
        let writes = programs
            .iter()
            .any(|program| program.written_through(&root.chain));
        if touches || writes {
            return Err(refused(path));
        }
    }
    Ok(())
}
