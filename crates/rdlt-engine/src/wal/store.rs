//! Where write-ahead logs are kept: one log per load of a pipeline, in numbered chunks, each an
//! object written whole.

#[cfg(test)]
pub(crate) mod conformance;

use std::io;

use bytes::Bytes;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

/// One chunk of a load's log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Chunk {
    /// The load whose log it is.
    pub load: LoadId,
    /// Its place in the log, from 0.
    pub number: u64,
}

/// A chunk being written: no reader sees it until it is published, and once published it never
/// changes.
pub trait StagedChunk: Send + Sync {
    /// Adds `bytes` to the end of the chunk.
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>>;

    /// Writes the chunk durably under its name where no chunk of that name exists, and is
    /// answered once it is durable: where one exists, the error is
    /// [`io::ErrorKind::AlreadyExists`] and the chunk there is unchanged.
    ///
    /// The name is taken by whoever publishes it first, which is how a log is fenced: a replay
    /// publishes the chunk a writer would publish next, and the writer finds it taken.
    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>>;
}

/// Keeps each load's write-ahead log as numbered chunks, each an object written whole.
///
/// The operations are those an object store offers: a chunk is staged and published whole where
/// its name is free, listed by its log, read by range, and deleted. Nothing is appended to a
/// published chunk, renamed over another, or held locked. A crash loses what was staged and not
/// published, and nothing else: a published chunk is whole, and a deletion that returned is
/// durable.
pub trait WalStore: std::fmt::Debug + Send + Sync + 'static {
    /// Begins chunk `chunk` of `pipeline`'s log, which no reader sees until it is published.
    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>>;

    /// The loads of `pipeline` with a published chunk, and perhaps some with only staged ones.
    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>>;

    /// The published chunks of `load`'s log, by number, each with its length.
    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>>;

    /// `len` bytes of the published chunk `chunk` from `offset`, fewer where the chunk ends first.
    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>>;

    /// Deletes the published chunk `chunk`, durably; one that is gone is no error.
    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>>;

    /// Deletes `load`'s log whole, durably: what it staged, then its chunks in the order of their
    /// numbers, so a crash part way leaves its highest chunks.
    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>>;
}
