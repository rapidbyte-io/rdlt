//! A write-ahead log store in memory, for tests: what each chunk was appended and how much of it
//! was made durable.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

use super::store::{Chunk, Claim, WalStore};

/// A chunk's bytes and how many of them are durable.
#[derive(Clone, Debug, Default)]
pub(crate) struct Stored {
    pub(crate) bytes: Vec<u8>,
    pub(crate) synced: usize,
}

/// Chunks by pipeline and chunk; `failing` makes every call fail, `unsyncable` every sync and
/// `unremovable` every removal of a chunk.
#[derive(Debug, Default)]
pub(crate) struct MemoryWal {
    pub(crate) chunks: Mutex<BTreeMap<(PipelineId, Chunk), Stored>>,
    pub(crate) failing: Mutex<bool>,
    /// Whether every call is interrupted, as a transient failure a retry may not meet.
    pub(crate) interrupted: Mutex<bool>,
    pub(crate) unsyncable: Mutex<bool>,
    pub(crate) unremovable: Mutex<bool>,
    /// The logs claimed, each until its claim is dropped.
    pub(crate) claimed: Arc<Mutex<BTreeSet<(PipelineId, LoadId)>>>,
    /// Every append, in order, removed chunks' included.
    pub(crate) appended: Mutex<Vec<Bytes>>,
    /// Whether each append waits a few turns of the scheduler first, as a slow disk does.
    pub(crate) slow: bool,
}

/// A claim on a log in memory, let go when dropped.
struct Held {
    claimed: Arc<Mutex<BTreeSet<(PipelineId, LoadId)>>>,
    log: (PipelineId, LoadId),
}

impl Drop for Held {
    fn drop(&mut self) {
        self.claimed.lock().remove(&self.log);
    }
}

impl MemoryWal {
    fn check(&self) -> io::Result<()> {
        if *self.failing.lock() {
            Err(io::Error::other("the disk is full"))
        } else if *self.interrupted.lock() {
            Err(io::Error::from(io::ErrorKind::Interrupted))
        } else {
            Ok(())
        }
    }

    /// The chunks of `pipeline`'s logs, in order, with their stored state.
    pub(crate) fn stored(&self, pipeline: &PipelineId) -> Vec<(Chunk, Stored)> {
        self.chunks
            .lock()
            .iter()
            .filter(|((owner, _), _)| owner == pipeline)
            .map(|((_, chunk), stored)| (*chunk, stored.clone()))
            .collect()
    }

    /// Keeps only what each chunk made durable, as a crash does.
    #[cfg(test)]
    pub(crate) fn crash(&self) {
        for stored in self.chunks.lock().values_mut() {
            stored.bytes.truncate(stored.synced);
        }
    }
}

fn ready<T: Send + 'static>(value: io::Result<T>) -> BoxFuture<'static, io::Result<T>> {
    Box::pin(async move { value })
}

impl WalStore for MemoryWal {
    fn claim<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Option<Claim>>> {
        let claimed = self.check().map(|()| {
            let log = (pipeline.clone(), load);
            self.claimed.lock().insert(log.clone()).then(|| {
                Box::new(Held {
                    claimed: Arc::clone(&self.claimed),
                    log,
                }) as Claim
            })
        });
        ready(claimed)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let removed = self.check().map(|()| {
            self.chunks
                .lock()
                .retain(|(owner, chunk), _| owner != pipeline || chunk.load != load);
        });
        ready(removed)
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let loads = self.check().map(|()| {
            let mut loads: Vec<LoadId> = self
                .stored(pipeline)
                .iter()
                .map(|(chunk, _)| chunk.load)
                .collect();
            loads.dedup();
            loads
        });
        ready(loads)
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
                .map(|(chunk, stored)| (chunk.number, stored.bytes.len() as u64))
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
            let stored = chunks
                .get(&(pipeline.clone(), chunk))
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(stored.bytes.len());
            let end = start
                .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
                .min(stored.bytes.len());
            Ok(Bytes::copy_from_slice(&stored.bytes[start..end]))
        });
        ready(read)
    }

    fn append<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        bytes: Bytes,
    ) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            if self.slow {
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
            }
            self.check()?;
            let mut chunks = self.chunks.lock();
            let stored = chunks.entry((pipeline.clone(), chunk)).or_default();
            stored.bytes.extend_from_slice(&bytes);
            self.appended.lock().push(bytes);
            Ok(())
        })
    }

    fn sync<'a>(&'a self, pipeline: &'a PipelineId, chunk: Chunk) -> BoxFuture<'a, io::Result<()>> {
        let synced = self.check().and_then(|()| {
            if *self.unsyncable.lock() {
                return Err(io::Error::other("the disk failed to flush"));
            }
            if let Some(stored) = self.chunks.lock().get_mut(&(pipeline.clone(), chunk)) {
                stored.synced = stored.bytes.len();
            }
            Ok(())
        });
        ready(synced)
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
}
