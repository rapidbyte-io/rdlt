//! Positions a source keeps outside the engine, by name, as a replication slot or a consumer
//! group keeps them beyond a connection: the position the engine last told it is committed, per
//! stream and partition, never moving back.
//!
//! A keeper given a file outlives its process there, as a replication slot outlives its server's
//! clients.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::{self, ErrorKind, Write as _};
use std::path::Path;
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use rdlt_connector::PartitionId;
use serde::Serialize;
use serde::de::DeserializeOwned;

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
}

/// Where a keeper's file is: its directory, opened once, and its name there.
#[derive(Debug)]
struct KeptFile {
    dir: Dir,
    name: OsString,
}

impl<P> Default for Kept<P> {
    fn default() -> Self {
        Self {
            positions: Mutex::default(),
            file: None,
        }
    }
}

impl<P: Copy + Ord + Serialize + DeserializeOwned> Kept<P> {
    /// The keeper kept in the file at `path`: what the file holds, none where there is no file.
    ///
    /// The file's directory must be this user's alone, and so must the file: a regular file in
    /// the directory `path` names it in, of at most a keeper's size. A link there is refused,
    /// read or written.
    ///
    /// # Errors
    ///
    /// A file that cannot be read, or holds no keeper, as one a disk damaged would.
    #[cfg(test)]
    pub(crate) fn at(path: &Path) -> io::Result<Self> {
        let (dir, name) = place(path)?;
        Self::open(dir, name)
    }

    /// The keeper kept in the file `name` of `dir`, as [`Kept::at`] reads it.
    fn open(dir: Dir, name: OsString) -> io::Result<Self> {
        let positions = match dir.read_private(&name, LIMIT) {
            Ok(bytes) => {
                let listed: Vec<(String, PartitionId, P)> = serde_json::from_slice(&bytes)?;
                if listed.len() > KEEPER_POSITIONS {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        format!("the file holds more than {KEEPER_POSITIONS} positions"),
                    ));
                }
                listed
                    .into_iter()
                    .map(|(stream, partition, position)| ((stream, partition), position))
                    .collect()
            }
            Err(error) if error.kind() == ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(error),
        };
        // No other writer shares the file: every temporary of it is one a crash left.
        dir.sweep_of(&name.to_string_lossy(), std::time::Duration::ZERO)?;
        Ok(Self {
            positions: Mutex::new(positions),
            file: Some(KeptFile { dir, name }),
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
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::from(ErrorKind::InvalidInput))?;
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok((Dir::ambient(directory)?, name.to_owned()))
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

/// Keepers by name, for as long as the process runs.
pub(crate) struct Registry<P>(LazyLock<Mutex<BTreeMap<String, Arc<Kept<P>>>>>);

impl<P> Registry<P> {
    pub(crate) const fn new() -> Self {
        Self(LazyLock::new(|| Mutex::new(BTreeMap::new())))
    }

    /// The keeper named `name`, shared by every source of this process that names it; the
    /// default keeper, which every source naming none shares, where `name` is none.
    pub(crate) fn named(&self, name: Option<&str>) -> Arc<Kept<P>> {
        let name = name.unwrap_or_default();
        Arc::clone(self.0.lock().entry(name.to_owned()).or_default())
    }
}

impl<P: Copy + Ord + Serialize + DeserializeOwned> Registry<P> {
    /// The keeper kept in the file at `path`, shared by every source of this process that names
    /// the file.
    ///
    /// # Errors
    ///
    /// The file cannot be read, or holds no keeper.
    pub(crate) fn at(&self, path: &Path) -> io::Result<Arc<Kept<P>>> {
        // Keyed by the directory itself and the name in it, so every path to one file names one
        // keeper.
        let (dir, name) = place(path)?;
        let (device, file) = dir.identity()?;
        let key = format!("file:{device}:{file}:{}", name.to_string_lossy());
        let mut keepers = self.0.lock();
        if let Some(kept) = keepers.get(&key) {
            return Ok(Arc::clone(kept));
        }
        let kept = Arc::new(Kept::open(dir, name)?);
        keepers.insert(key, Arc::clone(&kept));
        Ok(kept)
    }
}
