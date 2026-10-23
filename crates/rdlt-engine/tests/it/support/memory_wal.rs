//! A write-ahead log store in memory, keeping the contract a local one keeps, for runs that
//! share one store between engines, on a disk of a size a test chooses, which charges each open
//! log the block a file system takes for a directory.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};
use rdlt_engine::{Chunk, StagedChunk, WalStore};

/// What a [`Memory`] store and the chunks it stages share.
#[derive(Debug, Default)]
struct Held {
    chunks: BTreeMap<(PipelineId, Chunk), Bytes>,
    /// Each log ever opened, and whether it is open still.
    logs: BTreeMap<(PipelineId, LoadId), bool>,
    /// Each staging not yet published or deleted, by number: its log and its bytes.
    staged: BTreeMap<u64, ((PipelineId, LoadId), Vec<u8>)>,
    next: u64,
    /// Bytes the disk holds at most, staged and published; unbounded where none.
    capacity: Option<usize>,
    /// Its identity, once it was asked for.
    identity: Option<LoadId>,
}

/// Bytes: what an open log takes of the disk before it holds anything, as a directory's block.
const DIRECTORY: usize = 1 << 10;

impl Held {
    /// Bytes the disk holds.
    fn used(&self) -> usize {
        let published: usize = self.chunks.values().map(Bytes::len).sum();
        let open = self.logs.values().filter(|open| **open).count();
        published + self.staged() + open * DIRECTORY
    }

    /// Whether the disk has no room for `bytes` more.
    fn full(&self, bytes: usize) -> bool {
        self.capacity
            .is_some_and(|capacity| self.used() + bytes > capacity)
    }

    fn staged(&self) -> usize {
        self.staged.values().map(|(_, bytes)| bytes.len()).sum()
    }
}

/// Logs in memory.
#[derive(Debug, Default)]
pub(crate) struct Memory {
    held: Arc<Mutex<Held>>,
}

impl Memory {
    /// Logs on a disk of `capacity` bytes.
    pub(crate) fn of(capacity: usize) -> Self {
        let held = Held {
            capacity: Some(capacity),
            ..Held::default()
        };
        Self {
            held: Arc::new(Mutex::new(held)),
        }
    }

    /// Bytes the stagings not yet published or deleted hold.
    pub(crate) fn staged(&self) -> usize {
        self.held.lock().staged()
    }
}

struct Staged {
    held: Arc<Mutex<Held>>,
    key: (PipelineId, Chunk),
    id: u64,
}

fn ready<T: Send + 'static>(value: io::Result<T>) -> BoxFuture<'static, io::Result<T>> {
    Box::pin(async move { value })
}

impl StagedChunk for Staged {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        let mut held = self.held.lock();
        let full = held.full(bytes.len());
        let appended = match held.staged.get_mut(&self.id) {
            _ if full => Err(io::Error::from(io::ErrorKind::StorageFull)),
            Some((_, staged)) => {
                staged.extend_from_slice(&bytes);
                Ok(())
            }
            None => Err(io::Error::from(io::ErrorKind::NotFound)),
        };
        ready(appended)
    }

    fn discard(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        self.held.lock().staged.remove(&self.id);
        ready(Ok(()))
    }

    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        let mut held = self.held.lock();
        let log = (self.key.0.clone(), self.key.1.load);
        let staged = held.staged.remove(&self.id);
        let published =
            if let Some((_, bytes)) = staged.filter(|_| held.logs.get(&log) == Some(&true)) {
                if held.chunks.contains_key(&self.key) {
                    Err(io::Error::from(io::ErrorKind::AlreadyExists))
                } else {
                    held.chunks.insert(self.key.clone(), Bytes::from(bytes));
                    Ok(())
                }
            } else {
                Err(io::Error::from(io::ErrorKind::NotFound))
            };
        ready(published)
    }
}

impl WalStore for Memory {
    fn identity(&self, proposed: LoadId) -> BoxFuture<'_, io::Result<LoadId>> {
        ready(Ok(*self.held.lock().identity.get_or_insert(proposed)))
    }

    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let mut held = self.held.lock();
        let key = (pipeline.clone(), load);
        let opened = if held.logs.get(&key) == Some(&true) {
            Err(io::Error::from(io::ErrorKind::AlreadyExists))
        } else if held.full(DIRECTORY) {
            Err(io::Error::from(io::ErrorKind::StorageFull))
        } else {
            held.logs.insert(key, true);
            Ok(())
        };
        ready(opened)
    }

    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        let open = self.held.lock().logs.get(&(pipeline.clone(), chunk.load)) == Some(&true);
        if !open {
            return ready(Err(io::Error::from(io::ErrorKind::NotFound)));
        }
        let mut held = self.held.lock();
        let id = held.next;
        held.next += 1;
        held.staged
            .insert(id, ((pipeline.clone(), chunk.load), Vec::new()));
        ready(Ok(Box::new(Staged {
            held: Arc::clone(&self.held),
            key: (pipeline.clone(), chunk),
            id,
        }) as Box<dyn StagedChunk>))
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let loads = self
            .held
            .lock()
            .logs
            .iter()
            .filter(|((owner, _), open)| owner == pipeline && **open)
            .map(|((_, load), _)| *load)
            .collect();
        ready(Ok(loads))
    }

    /// Every removal here is whole, so none leaves anything behind.
    fn leftovers<'a>(
        &'a self,
        _pipeline: &'a PipelineId,
    ) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        ready(Ok(Vec::new()))
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        let chunks = self
            .held
            .lock()
            .chunks
            .iter()
            .filter(|((owner, chunk), _)| owner == pipeline && chunk.load == load)
            .map(|((_, chunk), bytes)| (chunk.number, bytes.len() as u64))
            .collect();
        ready(Ok(chunks))
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        let held = self.held.lock();
        let read = held
            .chunks
            .get(&(pipeline.clone(), chunk))
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
            .map(|bytes| {
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(bytes.len());
                let end = start
                    .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
                    .min(bytes.len());
                bytes.slice(start..end)
            });
        ready(read)
    }

    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let log = (pipeline.clone(), load);
        self.held
            .lock()
            .staged
            .retain(|_, (staged, _)| *staged != log);
        ready(Ok(()))
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.held.lock().chunks.remove(&(pipeline.clone(), chunk));
        ready(Ok(()))
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let mut held = self.held.lock();
        if let Some(open) = held.logs.get_mut(&(pipeline.clone(), load)) {
            *open = false;
        }
        let log = (pipeline.clone(), load);
        held.staged.retain(|_, (staged, _)| *staged != log);
        held.chunks
            .retain(|(owner, chunk), _| owner != pipeline || chunk.load != load);
        ready(Ok(()))
    }
}
