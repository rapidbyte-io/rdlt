//! A write-ahead log store in memory, keeping the contract a local one keeps, for runs that
//! share one store between engines.

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
}

/// Logs in memory.
#[derive(Debug, Default)]
pub(crate) struct Memory {
    held: Arc<Mutex<Held>>,
}

struct Staged {
    held: Arc<Mutex<Held>>,
    key: (PipelineId, Chunk),
    bytes: Vec<u8>,
}

fn ready<T: Send + 'static>(value: io::Result<T>) -> BoxFuture<'static, io::Result<T>> {
    Box::pin(async move { value })
}

impl StagedChunk for Staged {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        self.bytes.extend_from_slice(&bytes);
        ready(Ok(()))
    }

    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        let mut held = self.held.lock();
        let log = (self.key.0.clone(), self.key.1.load);
        let published = if held.logs.get(&log) != Some(&true) {
            Err(io::Error::from(io::ErrorKind::NotFound))
        } else if held.chunks.contains_key(&self.key) {
            Err(io::Error::from(io::ErrorKind::AlreadyExists))
        } else {
            held.chunks
                .insert(self.key.clone(), Bytes::from(self.bytes));
            Ok(())
        };
        ready(published)
    }
}

impl WalStore for Memory {
    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let mut held = self.held.lock();
        let key = (pipeline.clone(), load);
        let opened = if held.logs.get(&key) == Some(&true) {
            Err(io::Error::from(io::ErrorKind::AlreadyExists))
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
        ready(Ok(Box::new(Staged {
            held: Arc::clone(&self.held),
            key: (pipeline.clone(), chunk),
            bytes: Vec::new(),
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
        held.chunks
            .retain(|(owner, chunk), _| owner != pipeline || chunk.load != load);
        ready(Ok(()))
    }
}
