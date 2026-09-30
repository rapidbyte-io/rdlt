//! The simulation's write-ahead log store: logs in memory that outlive runs, of which a crash
//! keeps what was made durable and, drawn, a torn part of the rest.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};
use rdlt_engine::{Chunk, Claim, WalStore};

use crate::rng::SplitMix64;

/// A chunk's bytes and how many of them are durable.
#[derive(Clone, Debug, Default)]
struct Stored {
    bytes: Vec<u8>,
    synced: usize,
}

type Claims = Arc<Mutex<BTreeSet<(PipelineId, LoadId)>>>;

/// Logs in memory, by pipeline and chunk.
#[derive(Debug, Default)]
pub struct SimWal {
    chunks: Mutex<BTreeMap<(PipelineId, Chunk), Stored>>,
    claims: Claims,
    /// Where appends and syncs fail now and then, the draws deciding when.
    faults: Mutex<Option<SplitMix64>>,
}

/// Failures per thousand appends or syncs, while faulty.
const FAULTS: u64 = 10;

/// A claim on a log, let go when dropped, as a process's lock goes with it.
struct Held {
    claims: Claims,
    log: (PipelineId, LoadId),
}

impl Drop for Held {
    fn drop(&mut self) {
        self.claims.lock().remove(&self.log);
    }
}

impl SimWal {
    /// Keeps of each chunk of `pipeline`'s logs what was made durable, then, as `rng` draws, a
    /// part of the rest, whose last byte may be garbled, as a crash of the worker running it
    /// leaves them; other pipelines' logs run on.
    pub(crate) fn crash(&self, pipeline: &PipelineId, rng: &mut SplitMix64) {
        let mut chunks = self.chunks.lock();
        let crashed = chunks
            .iter_mut()
            .filter(|((owner, _), _)| owner == pipeline)
            .map(|(_, stored)| stored);
        for stored in crashed {
            let unsynced = stored.bytes.len() - stored.synced;
            let kept = stored.synced + usize::try_from(rng.below(unsynced as u64 + 1)).unwrap_or(0);
            stored.bytes.truncate(kept);
            if kept > stored.synced && rng.chance(250) {
                stored.bytes[kept - 1] ^= 0x5a;
            }
        }
    }

    /// Makes appends and syncs fail now and then, as `faults` draws, or never again.
    pub(crate) fn set_faults(&self, faults: Option<SplitMix64>) {
        *self.faults.lock() = faults;
    }

    /// Whether it holds any log: a load's that has not closed it with every receipt, or one no
    /// replay has taken yet.
    pub(crate) fn holds_logs(&self) -> bool {
        !self.chunks.lock().is_empty()
    }

    /// A transient failure, where one is drawn.
    fn fault(&self) -> io::Result<()> {
        let failed = self
            .faults
            .lock()
            .as_mut()
            .is_some_and(|rng| rng.chance(FAULTS));
        if failed {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "the simulated disk hiccuped",
            ));
        }
        Ok(())
    }
}

fn ready<T: Send + 'static>(value: T) -> BoxFuture<'static, io::Result<T>> {
    Box::pin(async move { Ok(value) })
}

impl WalStore for SimWal {
    fn claim<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Option<Claim>>> {
        let log = (pipeline.clone(), load);
        let claimed = self.claims.lock().insert(log.clone()).then(|| {
            Box::new(Held {
                claims: Arc::clone(&self.claims),
                log,
            }) as Claim
        });
        ready(claimed)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.chunks
            .lock()
            .retain(|(owner, chunk), _| owner != pipeline || chunk.load != load);
        ready(())
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let loads: BTreeSet<LoadId> = self
            .chunks
            .lock()
            .keys()
            .filter(|(owner, _)| owner == pipeline)
            .map(|(_, chunk)| chunk.load)
            .collect();
        ready(loads.into_iter().collect())
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
            .map(|((_, chunk), stored)| (chunk.number, stored.bytes.len() as u64))
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
        let Some(stored) = chunks.get(&(pipeline.clone(), chunk)) else {
            return Box::pin(async { Err(io::Error::from(io::ErrorKind::NotFound)) });
        };
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(stored.bytes.len());
        let end = start
            .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
            .min(stored.bytes.len());
        ready(Bytes::copy_from_slice(&stored.bytes[start..end]))
    }

    fn append<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        bytes: Bytes,
    ) -> BoxFuture<'a, io::Result<()>> {
        if let Err(error) = self.fault() {
            return Box::pin(async { Err(error) });
        }
        let mut chunks = self.chunks.lock();
        let stored = chunks.entry((pipeline.clone(), chunk)).or_default();
        stored.bytes.extend_from_slice(&bytes);
        ready(())
    }

    fn sync<'a>(&'a self, pipeline: &'a PipelineId, chunk: Chunk) -> BoxFuture<'a, io::Result<()>> {
        if let Err(error) = self.fault() {
            return Box::pin(async { Err(error) });
        }
        if let Some(stored) = self.chunks.lock().get_mut(&(pipeline.clone(), chunk)) {
            stored.synced = stored.bytes.len();
        }
        ready(())
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.chunks.lock().remove(&(pipeline.clone(), chunk));
        ready(())
    }
}
