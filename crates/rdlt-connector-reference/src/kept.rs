//! Positions a source keeps outside the engine, by name, as a replication slot or a consumer
//! group keeps them beyond a connection: the position the engine last told it is committed, per
//! stream and partition, never moving back.
//!
//! A keeper given a file outlives its process there, as a replication slot outlives its server's
//! clients.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use rdlt_connector::PartitionId;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Each partition's acknowledged position, by stream and partition.
type Positions<P> = BTreeMap<(String, PartitionId), P>;

/// One keeper of positions, and the file it keeps them in, if any.
#[derive(Debug)]
pub(crate) struct Kept<P> {
    positions: Mutex<Positions<P>>,
    path: Option<PathBuf>,
}

impl<P> Default for Kept<P> {
    fn default() -> Self {
        Self {
            positions: Mutex::default(),
            path: None,
        }
    }
}

impl<P: Copy + Ord + Serialize + DeserializeOwned> Kept<P> {
    /// The keeper kept in the file at `path`: what the file holds, none where there is no file.
    ///
    /// # Errors
    ///
    /// A file that cannot be read, or holds no keeper, as one a disk damaged would.
    pub(crate) fn at(path: &Path) -> std::io::Result<Self> {
        let positions = match std::fs::read(path) {
            Ok(bytes) => {
                let listed: Vec<(String, PartitionId, P)> = serde_json::from_slice(&bytes)?;
                listed
                    .into_iter()
                    .map(|(stream, partition, position)| ((stream, partition), position))
                    .collect()
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(error),
        };
        Ok(Self {
            positions: Mutex::new(positions),
            path: Some(path.to_owned()),
        })
    }

    /// Acknowledges `position` of `partition` of `stream`, unless the keeper stands past it, and
    /// writes the keeper to its file, whole, in place of what it held.
    ///
    /// # Errors
    ///
    /// The file cannot be written.
    pub(crate) fn advance(
        &self,
        stream: &str,
        partition: &PartitionId,
        position: P,
    ) -> std::io::Result<()> {
        let mut positions = self.positions.lock();
        let standing = positions
            .entry((stream.to_owned(), partition.clone()))
            .or_insert(position);
        *standing = (*standing).max(position);
        match &self.path {
            Some(path) => write(path, &positions),
            None => Ok(()),
        }
    }

    /// Where `partition` of `stream` stands.
    pub(crate) fn position(&self, stream: &str, partition: &PartitionId) -> Option<P> {
        self.positions
            .lock()
            .get(&(stream.to_owned(), partition.clone()))
            .copied()
    }
}

/// Writes `positions` to `path` through a file beside it, renamed over it once durable and the
/// rename made durable too, so a crash leaves the file as it was or as it is now, never torn.
fn write<P: Serialize>(path: &Path, positions: &Positions<P>) -> std::io::Result<()> {
    let listed: Vec<(&String, &PartitionId, &P)> = positions
        .iter()
        .map(|((stream, partition), position)| (stream, partition, position))
        .collect();
    let bytes = serde_json::to_vec(&listed)?;
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".writing");
    let temporary = PathBuf::from(temporary);
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    // The rename outlives a power loss only once the directory holding it is synced.
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::File::open(directory)?.sync_all()
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
    pub(crate) fn at(&self, path: &Path) -> std::io::Result<Arc<Kept<P>>> {
        let name = format!("file:{}", path.display());
        let mut keepers = self.0.lock();
        if let Some(kept) = keepers.get(&name) {
            return Ok(Arc::clone(kept));
        }
        let kept = Arc::new(Kept::at(path)?);
        keepers.insert(name, Arc::clone(&kept));
        Ok(kept)
    }
}
