//! A write-ahead log store that counts what it is asked to do, over a local one.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};
use rdlt_engine::{Chunk, Claim, LocalWal, WalStore};

/// A [`LocalWal`] counting its appends and syncs.
#[derive(Debug)]
pub(crate) struct Counted {
    pub(crate) local: LocalWal,
    pub(crate) appends: AtomicUsize,
    pub(crate) syncs: AtomicUsize,
}

impl Counted {
    pub(crate) fn new(local: LocalWal) -> Self {
        Self {
            local,
            appends: AtomicUsize::new(0),
            syncs: AtomicUsize::new(0),
        }
    }

    pub(crate) fn appends(&self) -> usize {
        self.appends.load(Ordering::SeqCst)
    }

    pub(crate) fn syncs(&self) -> usize {
        self.syncs.load(Ordering::SeqCst)
    }
}

impl WalStore for Counted {
    fn claim<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Option<Claim>>> {
        self.local.claim(pipeline, load)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.local.remove_log(pipeline, load)
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

    fn append<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        bytes: Bytes,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.appends.fetch_add(1, Ordering::SeqCst);
        self.local.append(pipeline, chunk, bytes)
    }

    fn sync<'a>(&'a self, pipeline: &'a PipelineId, chunk: Chunk) -> BoxFuture<'a, io::Result<()>> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        self.local.sync(pipeline, chunk)
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.local.remove(pipeline, chunk)
    }
}
