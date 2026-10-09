//! A run's write-ahead log store, each request it makes counted by operation, with the bytes
//! appended and read.

use std::io;
use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

use super::store::{Chunk, StagedChunk, WalStore};
use crate::report::{LogCounters, StoreRequests, Tally};

/// A store whose requests are counted into a run's tally.
#[derive(Debug)]
pub(crate) struct CountedStore {
    store: Arc<dyn WalStore>,
    tally: Arc<Tally>,
}

/// A chunk a [`CountedStore`] staged, its requests counted as the store's are.
struct CountedChunk {
    staged: Box<dyn StagedChunk>,
    tally: Arc<Tally>,
}

impl CountedStore {
    /// `store`, counting into `tally`.
    pub(crate) fn new(store: Arc<dyn WalStore>, tally: Arc<Tally>) -> Self {
        Self { store, tally }
    }
}

/// Counts a request of the operation `of` picks into `tally`, and what `more` adds.
fn count(
    tally: &Tally,
    of: fn(&mut StoreRequests) -> &mut u64,
    more: impl FnOnce(&mut LogCounters),
) {
    tally.add(|counters| {
        let requests = of(&mut counters.log.requests);
        *requests = requests.saturating_add(1);
        more(&mut counters.log);
    });
}

/// A count of bytes as the counters hold it.
fn bytes(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

impl StagedChunk for CountedChunk {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        let len = self::bytes(bytes.len());
        count(
            &self.tally,
            |requests| &mut requests.append,
            |log| log.appended = log.appended.saturating_add(len),
        );
        self.staged.append(bytes)
    }

    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        count(&self.tally, |requests| &mut requests.publish, |_| {});
        self.staged.publish()
    }

    fn discard(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        count(&self.tally, |requests| &mut requests.discard, |_| {});
        self.staged.discard()
    }
}

impl WalStore for CountedStore {
    fn chunk_bytes(&self) -> Option<NonZeroU64> {
        self.store.chunk_bytes()
    }

    fn staging_bytes(&self) -> u64 {
        self.store.staging_bytes()
    }

    fn identity(&self, proposed: LoadId) -> BoxFuture<'_, io::Result<LoadId>> {
        count(&self.tally, |requests| &mut requests.identity, |_| {});
        self.store.identity(proposed)
    }

    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        count(&self.tally, |requests| &mut requests.open_log, |_| {});
        self.store.open_log(pipeline, load)
    }

    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        count(&self.tally, |requests| &mut requests.stage, |_| {});
        Box::pin(async move {
            let staged = self.store.stage(pipeline, chunk).await?;
            let tally = Arc::clone(&self.tally);
            Ok(Box::new(CountedChunk { staged, tally }) as Box<dyn StagedChunk>)
        })
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        count(&self.tally, |requests| &mut requests.loads, |_| {});
        self.store.loads(pipeline)
    }

    fn leftovers<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        count(&self.tally, |requests| &mut requests.leftovers, |_| {});
        self.store.leftovers(pipeline)
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        count(&self.tally, |requests| &mut requests.chunks, |_| {});
        self.store.chunks(pipeline, load)
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        count(&self.tally, |requests| &mut requests.read, |_| {});
        Box::pin(async move {
            let read = self.store.read(pipeline, chunk, offset, len).await?;
            let len = bytes(read.len());
            self.tally
                .add(|counters| counters.log.read = counters.log.read.saturating_add(len));
            Ok(read)
        })
    }

    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        count(&self.tally, |requests| &mut requests.remove_staged, |_| {});
        self.store.remove_staged(pipeline, load)
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        count(&self.tally, |requests| &mut requests.remove, |_| {});
        self.store.remove(pipeline, chunk)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        count(&self.tally, |requests| &mut requests.remove_log, |_| {});
        self.store.remove_log(pipeline, load)
    }
}
