//! Positions a source keeps outside the engine, by name, as a replication slot or a consumer
//! group keeps them beyond a connection: the position the engine last told it is committed, per
//! stream and partition, never moving back.
//!
//! A keeper given a file outlives its process there, as a replication slot outlives its server's
//! clients.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, ErrorKind, Write as _};
use std::path::Path;
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rdlt_connector::PartitionId;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::Instant;

use crate::limits::{KEEPER_BYTES, KEEPER_POSITIONS};
use crate::rooted::{Dir, Limit};

const LIMIT: Limit = Limit {
    name: "keeper bytes",
    bytes: KEEPER_BYTES,
};

/// Each partition's acknowledged position, by stream and partition.
type Positions<P> = BTreeMap<(String, PartitionId), P>;

/// One keeper of positions, and the file it keeps them in, if any.
#[derive(Debug)]
pub(crate) struct Kept<P> {
    positions: Mutex<Positions<P>>,
    file: Option<KeptFile>,
    /// When the process opened the keeper, on the clock tasks sleep by and on the calendar's.
    opened: (Instant, SystemTime),
    /// When a keeper kept in a file was first kept there, in the calendar's milliseconds.
    began: Option<u64>,
}

/// Where a keeper's file is: its directory, opened once, and its name there.
#[derive(Debug)]
struct KeptFile {
    dir: Dir,
    name: OsString,
    /// The lock that makes the file this keeper's alone, held for as long as the keeper is.
    _lock: File,
}

impl<P> Default for Kept<P> {
    fn default() -> Self {
        Self {
            positions: Mutex::default(),
            file: None,
            opened: (Instant::now(), SystemTime::now()),
            began: None,
        }
    }
}

impl<P> Kept<P> {
    /// When the process opened the keeper: every source that shares it shares that moment, on
    /// the clock tasks sleep by and on the calendar's.
    pub(crate) fn opened(&self) -> (Instant, SystemTime) {
        self.opened
    }

    /// How long a keeper kept in a file had been kept there when this process opened it, by the
    /// calendar; none for a keeper kept in no file, which begins with its process.
    ///
    /// No time where the calendar says the file was first kept later than now.
    pub(crate) fn kept_for(&self) -> Option<Duration> {
        let opened = millis(self.opened.1);
        let began = self.began?;
        Some(Duration::from_millis(opened.saturating_sub(began)))
    }
}

/// `at` in the calendar's milliseconds.
fn millis(at: SystemTime) -> u64 {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
}

/// When a keeper file was first kept, in the calendar's milliseconds, as its `lock` says: the
/// lock file is made with the keeper's first process and never written, where the keeper's own
/// file is put anew by every write.
fn began(lock: &File) -> io::Result<u64> {
    Ok(millis(lock.metadata()?.modified()?))
}

impl<P: Copy + Ord + Serialize + DeserializeOwned> Kept<P> {
    /// What the keeper file at `path` holds, as its next process finds it, without becoming the
    /// file's keeper: nothing acknowledged here is written.
    ///
    /// # Errors
    ///
    /// As [`Kept::keeping`].
    #[cfg(test)]
    pub(crate) fn at(path: &Path) -> io::Result<Self> {
        let (dir, name) = place(path)?;
        Ok(Self {
            positions: Mutex::new(read(&dir, &name)?),
            file: None,
            opened: (Instant::now(), SystemTime::now()),
            began: None,
        })
    }

    /// The keeper kept in the file at `path`: what the file holds, none where there is no file.
    ///
    /// The file's directory must be this user's alone, and so must the file: a regular file in
    /// the directory `path` names it in, of at most a keeper's size. A link there is refused,
    /// read or written, and so is a file another keeper holds.
    ///
    /// # Errors
    ///
    /// A file that cannot be read, or holds no keeper, as one a disk damaged would.
    #[cfg(test)]
    pub(crate) fn keeping(path: &Path) -> io::Result<Self> {
        let (dir, name) = place(path)?;
        Self::open(dir, name)
    }

    /// The keeper kept in the file `name` of `dir`, as [`Kept::keeping`] reads it.
    fn open(dir: Dir, name: OsString) -> io::Result<Self> {
        let lock = alone(&dir, &name)?;
        let positions = read(&dir, &name)?;
        // No other writer shares the file: every temporary of it is one a crash left.
        dir.sweep_of(&name.to_string_lossy(), Duration::ZERO)?;
        let began = began(&lock)?;
        let opened = (Instant::now(), SystemTime::now());
        Ok(Self {
            positions: Mutex::new(positions),
            file: Some(KeptFile {
                dir,
                name,
                _lock: lock,
            }),
            opened,
            began: Some(began),
        })
    }

    /// Acknowledges `position` of `partition` of `stream`, unless the keeper stands at or past
    /// it: writes the keeper as it then stands to its file, whole, in place of what it held.
    ///
    /// The keeper stands at a position only once its file durably does: an acknowledgement
    /// whose write fails leaves the keeper where it stood, to be acknowledged again.
    ///
    /// # Errors
    ///
    /// The file cannot be written, or the partition is one more than a keeper holds.
    pub(crate) fn advance(
        &self,
        stream: &str,
        partition: &PartitionId,
        position: P,
    ) -> io::Result<()> {
        let mut positions = self.positions.lock();
        let key = (stream.to_owned(), partition.clone());
        match positions.get(&key) {
            Some(standing) if *standing >= position => return Ok(()),
            None if positions.len() >= KEEPER_POSITIONS => {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("a keeper holds at most {KEEPER_POSITIONS} positions"),
                ));
            }
            _ => {}
        }
        let Some(file) = &self.file else {
            positions.insert(key, position);
            return Ok(());
        };
        let mut moved = positions.clone();
        moved.insert(key, position);
        write(file, &moved)?;
        *positions = moved;
        Ok(())
    }

    /// Where `partition` of `stream` stands.
    pub(crate) fn position(&self, stream: &str, partition: &PartitionId) -> Option<P> {
        self.positions
            .lock()
            .get(&(stream.to_owned(), partition.clone()))
            .copied()
    }
}

/// The directory of the keeper file at `path`, opened, and the file's name in it.
fn place(path: &Path) -> io::Result<(Dir, OsString)> {
    // The file's temporaries and its lock are named after it, as text.
    let name = path
        .file_name()
        .filter(|name| name.to_str().is_some())
        .ok_or_else(|| {
            let message = format!("{} names no file by a name that is text", path.display());
            io::Error::new(ErrorKind::InvalidInput, message)
        })?;
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok((Dir::ambient(directory)?, name.to_owned()))
}

/// The positions the keeper file `name` of `dir` holds, none where there is no file.
fn read<P: Ord + DeserializeOwned>(dir: &Dir, name: &OsStr) -> io::Result<Positions<P>> {
    let bytes = match dir.read_private(name, LIMIT) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error),
    };
    let listed: Vec<(String, PartitionId, P)> = serde_json::from_slice(&bytes)?;
    if listed.len() > KEEPER_POSITIONS {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("the file holds more than {KEEPER_POSITIONS} positions"),
        ));
    }
    Ok(listed
        .into_iter()
        .map(|(stream, partition, position)| ((stream, partition), position))
        .collect())
}

/// Takes the lock that makes the keeper file `name` of `dir` one keeper's alone.
///
/// The lock is on a file beside the keeper's, since each write puts a new file in its place.
/// Two keepers of one file would each write what it holds over what the other wrote, and move
/// positions back: the second is refused for as long as the first is held, in this process or
/// another.
fn alone(dir: &Dir, name: &OsStr) -> io::Result<File> {
    let mut lock = OsString::from(".");
    lock.push(name);
    lock.push(".lock");
    let file = dir.lock_file(&lock)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(io::Error::new(
            ErrorKind::ResourceBusy,
            format!(
                "another keeper holds {}: a keeper file is one process's",
                dir.at(name).display()
            ),
        )),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

/// Writes `positions` to `file` through a temporary beside it, created under a name nobody can
/// guess and renamed over it once durable, the rename made durable too: a crash leaves the file
/// as it was or as it is now, never torn.
fn write<P: Serialize>(file: &KeptFile, positions: &Positions<P>) -> io::Result<()> {
    let listed: Vec<(&String, &PartitionId, &P)> = positions
        .iter()
        .map(|((stream, partition), position)| (stream, partition, position))
        .collect();
    let bytes = serde_json::to_vec(&listed)?;
    let mut temporary = file.dir.temporary_of(&file.name.to_string_lossy())?;
    temporary.file().write_all(&bytes)?;
    temporary.replace(&file.name)
}

/// What names a keeper of a process: the host it is kept for, the process's own where none is
/// named, and a name sources share or a file.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Named {
    Group(Option<String>, String),
    /// The file system and the directory on it that hold the file, and its name there.
    File(Option<String>, u64, u64, OsString),
}

/// Keepers by host and name.
///
/// A keeper is its host's: where a connector listens for hosts, two hosts that name one group
/// have a keeper each, and a file one of them keeps is refused the other while it is held.
///
/// A keeper is held for as long as a source holds it, and one the process's own host names for
/// as long as the process runs, as a broker keeps a group between its consumers. A keeper kept
/// for a host named to a listening connector, or in a file, is freed with its last source, its
/// file's lock with it, and its name is forgotten when a keeper is next asked for: what hosts
/// name costs the process nothing once they are gone, and a name freed is a new keeper's when
/// named again.
pub(crate) struct Registry<P>(LazyLock<Mutex<Keepers<P>>>);

/// The keepers of a registry: each by what names it, and those kept for the process.
struct Keepers<P> {
    named: BTreeMap<Named, Weak<Kept<P>>>,
    own: Vec<Arc<Kept<P>>>,
}

impl<P> Keepers<P> {
    /// The keeper `key` names, where a source or the process holds one; the names of keepers
    /// nothing holds any more are forgotten.
    fn held(&mut self, key: &Named) -> Option<Arc<Kept<P>>> {
        self.named.retain(|_, kept| kept.strong_count() != 0);
        self.named.get(key).and_then(Weak::upgrade)
    }
}

impl<P> Registry<P> {
    pub(crate) const fn new() -> Self {
        Self(LazyLock::new(|| {
            Mutex::new(Keepers {
                named: BTreeMap::new(),
                own: Vec::new(),
            })
        }))
    }

    /// The keeper `host` names `name`, shared by every source of this process connected for
    /// that host that names it; the host's default keeper, which its sources naming none share,
    /// where `name` is none.
    pub(crate) fn named(&self, host: Option<&str>, name: Option<&str>) -> Arc<Kept<P>> {
        let key = Named::Group(host.map(str::to_owned), name.unwrap_or_default().to_owned());
        let mut keepers = self.0.lock();
        if let Some(kept) = keepers.held(&key) {
            return kept;
        }
        let kept = Arc::new(Kept::default());
        keepers.named.insert(key, Arc::downgrade(&kept));
        if host.is_none() {
            keepers.own.push(Arc::clone(&kept));
        }
        kept
    }

    /// How many names the registry held when a keeper was last asked for.
    #[cfg(test)]
    pub(crate) fn kept(&self) -> usize {
        self.0.lock().named.len()
    }
}

impl<P: Copy + Ord + Serialize + DeserializeOwned> Registry<P> {
    /// The keeper kept for `host` in the file at `path`, shared by every source of this process
    /// connected for that host that names the file.
    ///
    /// # Errors
    ///
    /// The file cannot be read, holds no keeper, or is held for another host.
    pub(crate) fn at(&self, host: Option<&str>, path: &Path) -> io::Result<Arc<Kept<P>>> {
        // Keyed by the directory itself and the name in it, so every path to one file names one
        // keeper.
        let (dir, name) = place(path)?;
        let (device, file) = dir.identity()?;
        let key = Named::File(host.map(str::to_owned), device, file, name.clone());
        let mut keepers = self.0.lock();
        // A keeper no source holds has let its file go by now.
        if let Some(kept) = keepers.held(&key) {
            return Ok(kept);
        }
        let kept = Arc::new(Kept::open(dir, name)?);
        keepers.named.insert(key, Arc::downgrade(&kept));
        Ok(kept)
    }
}
