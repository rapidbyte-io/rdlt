//! A write-ahead log store that counts what it is asked to do, over a local one.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};
use rdlt_engine::{Chunk, LocalWal, StagedChunk, WalStore};

/// A [`LocalWal`] counting the bytes appended to the chunks it stages, and the chunks published.
#[derive(Debug)]
pub(crate) struct Counted {
    pub(crate) local: LocalWal,
    pub(crate) appends: Arc<AtomicUsize>,
    pub(crate) publishes: Arc<AtomicUsize>,
}

/// A chunk a [`Counted`] stages, counting as it goes.
struct Counting {
    staged: Box<dyn StagedChunk>,
    appends: Arc<AtomicUsize>,
    publishes: Arc<AtomicUsize>,
}

impl StagedChunk for Counting {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        self.appends.fetch_add(1, Ordering::SeqCst);
        self.staged.append(bytes)
    }

    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        self.publishes.fetch_add(1, Ordering::SeqCst);
        self.staged.publish()
    }
}

impl Counted {
    pub(crate) fn new(local: LocalWal) -> Self {
        Self {
            local,
            appends: Arc::default(),
            publishes: Arc::default(),
        }
    }

    pub(crate) fn appends(&self) -> usize {
        self.appends.load(Ordering::SeqCst)
    }

    pub(crate) fn publishes(&self) -> usize {
        self.publishes.load(Ordering::SeqCst)
    }
}

impl WalStore for Counted {
    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        Box::pin(async move {
            let staged = self.local.stage(pipeline, chunk).await?;
            Ok(Box::new(Counting {
                staged,
                appends: Arc::clone(&self.appends),
                publishes: Arc::clone(&self.publishes),
            }) as Box<dyn StagedChunk>)
        })
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        self.local.loads(pipeline)
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        self.local.chunks(pipeline, load)
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        self.local.read(pipeline, chunk, offset, len)
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.local.remove(pipeline, chunk)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.local.remove_log(pipeline, load)
    }
}
