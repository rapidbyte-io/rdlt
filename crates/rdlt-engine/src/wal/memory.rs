//! A write-ahead log store in memory, for tests: each published chunk, and what can make each
//! operation fail.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

use super::store::{Chunk, StagedChunk, WalStore};

/// Published chunks by pipeline and chunk; `failing` makes every call fail, `interrupted` every
/// call fail as a transient failure does, `unpublishable` every publish and `unremovable` every
/// deletion of a chunk.
#[derive(Debug, Default)]
pub(crate) struct MemoryWal {
    pub(crate) chunks: Arc<Mutex<BTreeMap<(PipelineId, Chunk), Bytes>>>,
    /// Each log ever opened, and whether it is still open.
    pub(crate) logs: Logs,
    pub(crate) failing: Arc<Mutex<bool>>,
    /// Whether every call is interrupted, as a transient failure a retry may not meet.
    pub(crate) interrupted: Arc<Mutex<bool>>,
    pub(crate) unpublishable: Arc<Mutex<bool>>,
    pub(crate) unremovable: Mutex<bool>,
    /// Every chunk published, in order, deleted ones included.
    pub(crate) published: Arc<Mutex<Vec<Bytes>>>,
    /// Whether each append waits a few turns of the scheduler first, as a slow disk does.
    pub(crate) slow: bool,
    /// What its stagings hold, and how much it may hold in all.
    pub(crate) disk: Arc<Disk>,
}

/// What a [`MemoryWal`]'s stagings hold, and how much the store may hold in all.
#[derive(Debug, Default)]
pub(crate) struct Disk {
    /// Bytes the store may hold, staged and published; unbounded where none.
    pub(crate) capacity: Option<usize>,
    /// Each staging not yet published or deleted: its log and the bytes it holds.
    staged: Mutex<BTreeMap<u64, (Log, usize)>>,
    next: AtomicU64,
}

impl Disk {
    /// A disk of `capacity` bytes.
    #[cfg(test)]
    pub(crate) fn of(capacity: usize) -> Self {
        Self {
            capacity: Some(capacity),
            ..Self::default()
        }
    }

    /// Bytes its stagings hold.
    #[cfg(test)]
    pub(crate) fn staged(&self) -> usize {
        self.staged.lock().values().map(|(_, len)| len).sum()
    }
}

/// A log, by its pipeline and load.
type Log = (PipelineId, LoadId);

/// Each log ever opened, and whether it is open still.
pub(crate) type Logs = Arc<Mutex<BTreeMap<Log, bool>>>;

/// What makes a [`MemoryWal`]'s calls fail, shared with the chunks it stages.
#[derive(Clone, Debug)]
struct Faults {
    failing: Arc<Mutex<bool>>,
    interrupted: Arc<Mutex<bool>>,
}

impl Faults {
    fn check(&self) -> io::Result<()> {
        if *self.failing.lock() {
            Err(io::Error::other("the disk is full"))
        } else if *self.interrupted.lock() {
            Err(io::Error::from(io::ErrorKind::Interrupted))
        } else {
            Ok(())
        }
    }
}

/// A chunk a [`MemoryWal`] stages: its bytes, until it is published.
struct Staged {
    chunks: Arc<Mutex<BTreeMap<(PipelineId, Chunk), Bytes>>>,
    logs: Logs,
    disk: Arc<Disk>,
    /// Which staging of the disk's it is.
    id: u64,
    key: (PipelineId, Chunk),
    bytes: Vec<u8>,
    faults: Faults,
    unpublishable: Arc<Mutex<bool>>,
    published: Arc<Mutex<Vec<Bytes>>>,
    slow: bool,
}

impl StagedChunk for Staged {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async move {
            if self.slow {
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
            }
            self.faults.check()?;
            let published: usize = self.chunks.lock().values().map(Bytes::len).sum();
            let mut staged = self.disk.staged.lock();
            let held: usize = staged.values().map(|(_, len)| len).sum();
            if self
                .disk
                .capacity
                .is_some_and(|capacity| published + held + bytes.len() > capacity)
            {
                return Err(io::Error::from(io::ErrorKind::StorageFull));
            }
            let Some((_, len)) = staged.get_mut(&self.id) else {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            };
            *len += bytes.len();
            self.bytes.extend_from_slice(&bytes);
            Ok(())
        })
    }

    fn discard(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        self.disk.staged.lock().remove(&self.id);
        Box::pin(async { Ok(()) })
    }

    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        let staged = self.disk.staged.lock().remove(&self.id).is_some();
        let published = self.faults.check().and_then(|()| {
            if !staged {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
            if *self.unpublishable.lock() {
                return Err(io::Error::other("the disk failed to flush"));
            }
            let open = self.logs.lock().get(&(self.key.0.clone(), self.key.1.load)) == Some(&true);
            if !open {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
            let mut chunks = self.chunks.lock();
            if chunks.contains_key(&self.key) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            let bytes = Bytes::from(self.bytes);
            chunks.insert(self.key.clone(), bytes.clone());
            self.published.lock().push(bytes);
            Ok(())
        });
        Box::pin(async move { published })
    }
}

impl MemoryWal {
    fn faults(&self) -> Faults {
        Faults {
            failing: Arc::clone(&self.failing),
            interrupted: Arc::clone(&self.interrupted),
        }
    }

    fn check(&self) -> io::Result<()> {
        self.faults().check()
    }

    /// Opens `load`'s log of `pipeline` where it was never opened, as a load does when it starts.
    #[cfg(test)]
    pub(crate) fn open(&self, pipeline: &PipelineId, load: LoadId) {
        self.logs
            .lock()
            .entry((pipeline.clone(), load))
            .or_insert(true);
    }

    /// The frames of every chunk published, in order, deleted ones included.
    #[cfg(test)]
    pub(crate) fn published_frames(&self) -> Vec<super::frame::Frame> {
        let limits = super::frame::limits(1 << 30);
        self.published
            .lock()
            .iter()
            .flat_map(|chunk| super::frame::frames(chunk, limits).expect("a chunk reads"))
            .collect()
    }

    /// The published chunks of `pipeline`'s logs, in order.
    pub(crate) fn stored(&self, pipeline: &PipelineId) -> Vec<(Chunk, Bytes)> {
        self.chunks
            .lock()
            .iter()
            .filter(|((owner, _), _)| owner == pipeline)
            .map(|((_, chunk), bytes)| (*chunk, bytes.clone()))
            .collect()
    }
}

fn ready<T: Send + 'static>(value: io::Result<T>) -> BoxFuture<'static, io::Result<T>> {
    Box::pin(async move { value })
}

impl WalStore for MemoryWal {
    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let opened = self.check().and_then(|()| {
            let mut logs = self.logs.lock();
            if logs.contains_key(&(pipeline.clone(), load)) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            logs.insert((pipeline.clone(), load), true);
            Ok(())
        });
        ready(opened)
    }

    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        let open = self.logs.lock().get(&(pipeline.clone(), chunk.load)) == Some(&true);
        let staged = self.check().and_then(|()| {
            if !open {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
            let id = self.disk.next.fetch_add(1, Ordering::Relaxed);
            let log = (pipeline.clone(), chunk.load);
            self.disk.staged.lock().insert(id, (log, 0));
            Ok(Box::new(Staged {
                chunks: Arc::clone(&self.chunks),
                logs: Arc::clone(&self.logs),
                disk: Arc::clone(&self.disk),
                id,
                key: (pipeline.clone(), chunk),
                bytes: Vec::new(),
                faults: self.faults(),
                unpublishable: Arc::clone(&self.unpublishable),
                published: Arc::clone(&self.published),
                slow: self.slow,
            }) as Box<dyn StagedChunk>)
        });
        ready(staged)
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let loads = self.check().map(|()| {
            self.logs
                .lock()
                .iter()
                .filter(|((owner, _), open)| owner == pipeline && **open)
                .map(|((_, load), _)| *load)
                .collect()
        });
        ready(loads)
    }

    /// Every removal here is whole, so none leaves anything behind.
    fn leftovers<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let logs = self.logs.lock();
        let mut leftovers: Vec<LoadId> = self
            .stored(pipeline)
            .iter()
            .map(|(chunk, _)| chunk.load)
            .filter(|load| logs.get(&(pipeline.clone(), *load)) != Some(&true))
            .collect();
        leftovers.dedup();
        ready(self.check().map(|()| leftovers))
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        let chunks = self.check().map(|()| {
            self.stored(pipeline)
                .iter()
                .filter(|(chunk, _)| chunk.load == load)
                .map(|(chunk, bytes)| (chunk.number, bytes.len() as u64))
                .collect()
        });
        ready(chunks)
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        let read = self.check().and_then(|()| {
            let chunks = self.chunks.lock();
            let bytes = chunks
                .get(&(pipeline.clone(), chunk))
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            let end = start
                .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
                .min(bytes.len());
            Ok(bytes.slice(start..end))
        });
        ready(read)
    }

    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let removed = self.check().map(|()| {
            let log = (pipeline.clone(), load);
            self.disk
                .staged
                .lock()
                .retain(|_, (staged, _)| *staged != log);
        });
        ready(removed)
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        let removed = self.check().and_then(|()| {
            if *self.unremovable.lock() {
                return Err(io::Error::other("the disk failed to remove a file"));
            }
            self.chunks.lock().remove(&(pipeline.clone(), chunk));
            Ok(())
        });
        ready(removed)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let removed = self.check().map(|()| {
            if let Some(open) = self.logs.lock().get_mut(&(pipeline.clone(), load)) {
                *open = false;
            }
            let log = (pipeline.clone(), load);
            self.disk
                .staged
                .lock()
                .retain(|_, (staged, _)| *staged != log);
            self.chunks
                .lock()
                .retain(|(owner, chunk), _| owner != pipeline || chunk.load != load);
        });
        ready(removed)
    }
}

#[cfg(test)]
mod tests;
