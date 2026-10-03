//! The simulation's write-ahead log store: logs in memory that outlive runs, of which a crash
//! keeps every chunk published and nothing staged.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};
use rdlt_engine::{Chunk, StagedChunk, WalStore};

use crate::rng::SplitMix64;

/// Published chunks by pipeline and chunk.
type Chunks = Arc<Mutex<BTreeMap<(PipelineId, Chunk), Bytes>>>;

/// Each log ever opened, by pipeline and load, and whether it is open still.
type Logs = Arc<Mutex<BTreeMap<(PipelineId, LoadId), bool>>>;

/// A count for each log, by pipeline and load.
type Counts = Arc<Mutex<BTreeMap<(PipelineId, LoadId), u64>>>;

/// Logs in memory, by pipeline and chunk.
#[derive(Debug, Default)]
pub struct SimWal {
    chunks: Chunks,
    logs: Logs,
    /// How many times each log's stagings were deleted: a chunk staged before is lost.
    cleared: Counts,
    /// How many times each pipeline's worker crashed: a chunk staged before a crash is lost.
    crashes: Arc<Mutex<BTreeMap<PipelineId, u64>>>,
    /// Where stagings, publishes and deletions fail now and then, the draws deciding when.
    faults: Arc<Mutex<Option<SplitMix64>>>,
}

/// Failures per thousand stagings, publishes or deletions, while faulty.
const FAULTS: u64 = 10;

/// A chunk a [`SimWal`] stages: its bytes, until it is published, and the crash it follows.
struct Staged {
    chunks: Chunks,
    logs: Logs,
    cleared: Counts,
    /// How many times the log's stagings were deleted when this one began.
    generation: u64,
    crashes: Arc<Mutex<BTreeMap<PipelineId, u64>>>,
    faults: Arc<Mutex<Option<SplitMix64>>>,
    key: (PipelineId, Chunk),
    crashed: u64,
    bytes: Vec<u8>,
}

impl StagedChunk for Staged {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        self.bytes.extend_from_slice(&bytes);
        Box::pin(async { Ok(()) })
    }

    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        let published = fault(&self.faults).and_then(|()| {
            let crashed = self.crashes.lock().get(&self.key.0).copied();
            if crashed.unwrap_or(0) != self.crashed {
                return Err(io::Error::other("the worker that staged the chunk crashed"));
            }
            let log = (self.key.0.clone(), self.key.1.load);
            let cleared = self.cleared.lock().get(&log).copied().unwrap_or(0);
            if self.logs.lock().get(&log) != Some(&true) || cleared != self.generation {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
            let mut chunks = self.chunks.lock();
            if chunks.contains_key(&self.key) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            chunks.insert(self.key.clone(), Bytes::from(self.bytes));
            Ok(())
        });
        Box::pin(async move { published })
    }

    fn discard(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// A transient failure, where `faults` draws one.
fn fault(faults: &Mutex<Option<SplitMix64>>) -> io::Result<()> {
    let failed = faults.lock().as_mut().is_some_and(|rng| rng.chance(FAULTS));
    if failed {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "the simulated disk hiccuped",
        ));
    }
    Ok(())
}

impl SimWal {
    /// Crashes the worker running `pipeline`: every chunk it staged and did not publish is lost;
    /// other pipelines' logs run on.
    pub(crate) fn crash(&self, pipeline: &PipelineId) {
        *self.crashes.lock().entry(pipeline.clone()).or_default() += 1;
    }

    /// Makes stagings, publishes and deletions of chunks fail now and then, as `faults` draws, or
    /// never again.
    pub(crate) fn set_faults(&self, faults: Option<SplitMix64>) {
        *self.faults.lock() = faults;
    }

    /// Whether it holds any log: a load's that has not closed it with every receipt, or one no
    /// replay has taken yet.
    pub(crate) fn holds_logs(&self) -> bool {
        !self.chunks.lock().is_empty() || self.logs.lock().values().any(|open| *open)
    }
}

fn ready<T: Send + 'static>(value: T) -> BoxFuture<'static, io::Result<T>> {
    Box::pin(async move { Ok(value) })
}

impl WalStore for SimWal {
    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let opened = fault(&self.faults).and_then(|()| {
            let mut logs = self.logs.lock();
            if logs.contains_key(&(pipeline.clone(), load)) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            logs.insert((pipeline.clone(), load), true);
            Ok(())
        });
        Box::pin(async move { opened })
    }

    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        if let Err(error) = fault(&self.faults) {
            return Box::pin(async { Err(error) });
        }
        if self.logs.lock().get(&(pipeline.clone(), chunk.load)) != Some(&true) {
            return Box::pin(async { Err(io::Error::from(io::ErrorKind::NotFound)) });
        }
        let crashed = self.crashes.lock().get(pipeline).copied().unwrap_or(0);
        let log = (pipeline.clone(), chunk.load);
        let generation = self.cleared.lock().get(&log).copied().unwrap_or(0);
        let staged = Staged {
            chunks: Arc::clone(&self.chunks),
            logs: Arc::clone(&self.logs),
            cleared: Arc::clone(&self.cleared),
            generation,
            crashes: Arc::clone(&self.crashes),
            faults: Arc::clone(&self.faults),
            key: (pipeline.clone(), chunk),
            crashed,
            bytes: Vec::new(),
        };
        ready(Box::new(staged) as Box<dyn StagedChunk>)
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let loads = self
            .logs
            .lock()
            .iter()
            .filter(|((owner, _), open)| owner == pipeline && **open)
            .map(|((_, load), _)| *load)
            .collect();
        ready(loads)
    }

    fn leftovers<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let logs = self.logs.lock();
        let leftovers: BTreeSet<LoadId> = self
            .chunks
            .lock()
            .keys()
            .filter(|(owner, chunk)| {
                owner == pipeline && logs.get(&(pipeline.clone(), chunk.load)) != Some(&true)
            })
            .map(|(_, chunk)| chunk.load)
            .collect();
        ready(leftovers.into_iter().collect())
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        let chunks = self
            .chunks
            .lock()
            .iter()
            .filter(|((owner, chunk), _)| owner == pipeline && chunk.load == load)
            .map(|((_, chunk), bytes)| (chunk.number, bytes.len() as u64))
            .collect();
        ready(chunks)
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        let chunks = self.chunks.lock();
        let Some(bytes) = chunks.get(&(pipeline.clone(), chunk)) else {
            return Box::pin(async { Err(io::Error::from(io::ErrorKind::NotFound)) });
        };
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let end = start
            .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
            .min(bytes.len());
        ready(bytes.slice(start..end))
    }

    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        *self
            .cleared
            .lock()
            .entry((pipeline.clone(), load))
            .or_default() += 1;
        ready(())
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        if let Err(error) = fault(&self.faults) {
            return Box::pin(async { Err(error) });
        }
        self.chunks.lock().remove(&(pipeline.clone(), chunk));
        ready(())
    }

    /// Closes the log, then deletes its chunks one at a time, yielding between them, so a crash
    /// may land part way and leave some behind.
    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            if let Some(open) = self.logs.lock().get_mut(&(pipeline.clone(), load)) {
                *open = false;
            }
            let numbers: Vec<Chunk> = self
                .chunks
                .lock()
                .keys()
                .filter(|(owner, chunk)| owner == pipeline && chunk.load == load)
                .map(|(_, chunk)| *chunk)
                .collect();
            for chunk in numbers {
                self.chunks.lock().remove(&(pipeline.clone(), chunk));
                tokio::task::yield_now().await;
            }
            Ok(())
        })
    }
}
