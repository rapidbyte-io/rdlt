//! Where write-ahead logs are kept: one log per load of a pipeline, in numbered chunks, each an
//! object written whole.

use std::io;
use std::num::NonZeroU64;

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

    /// Writes the chunk durably under its name where no chunk of that name exists and its log is
    /// still open, and is answered once it is durable.
    ///
    /// Where a chunk of the name exists, the error is [`io::ErrorKind::AlreadyExists`] and the
    /// chunk there is unchanged; where the log was removed before the chunk could be published,
    /// it is [`io::ErrorKind::NotFound`], and no listing ever names the chunk.
    ///
    /// The name is taken by whoever publishes it first, which is how a log is fenced: a replay
    /// publishes the chunk a writer would publish next, and the writer finds it taken; once the
    /// replay removes the log, the writer finds it gone. A chunk deleted frees its name, so a
    /// fence holds only where no chunk above it is listed once it is published: a writer deletes
    /// only chunks below its last. A store creates the chunk first, where
    /// its name is free, and only then asks whether the log is still open, deleting the chunk
    /// where it is not: asked first, a removal could close and list the log between the answer
    /// and the chunk.
    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>>;

    /// Deletes what was staged, which is never published: a write that failed gives back the
    /// room it took.
    fn discard(self: Box<Self>) -> BoxFuture<'static, io::Result<()>>;
}

/// Keeps each load's write-ahead log as numbered chunks, each an object written whole.
///
/// The operations are those an object store offers: a log is opened by creating an object where
/// none of its name exists, a chunk is staged and published whole where its name is free, listed
/// by its log, read by range, and deleted. Nothing is appended to a published chunk, renamed over
/// another, or held locked. A crash loses what was staged and not published, and nothing else: a
/// published chunk is whole, and a deletion that returned is durable.
///
/// A log is open from [`WalStore::open_log`] until [`WalStore::remove_log`] begins, and never
/// again: only while it is open is a chunk of it published and the log listed.
pub trait WalStore: std::fmt::Debug + Send + Sync + 'static {
    /// Bytes: the most a chunk of this store may hold, which bounds a load's log as
    /// [`GrowthLimits::log_bytes`](crate::GrowthLimits::log_bytes) does, the lower of the two
    /// applying; none where a chunk is bounded only by that.
    fn chunk_bytes(&self) -> Option<NonZeroU64> {
        None
    }

    /// Bytes: what a chunk this store stages holds in memory at most, beside the frames the
    /// engine charges until each is appended, which the engine charges to its memory budget for
    /// as long as a load's log is written; none where a store stages nothing in memory.
    fn staging_bytes(&self) -> u64 {
        0
    }

    /// The store's identity, durable: the first caller's `proposed`, which every later call, of
    /// any process, answers with.
    fn identity(&self, proposed: LoadId) -> BoxFuture<'_, io::Result<LoadId>>;

    /// Opens `load`'s log of `pipeline`, durably, as a load does once, before it logs anything.
    ///
    /// A log open already, or one whose removal left something behind, is refused with
    /// [`io::ErrorKind::AlreadyExists`].
    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>>;

    /// Begins chunk `chunk` of `pipeline`'s log, which no reader sees until it is published; a
    /// log that is not open is refused with [`io::ErrorKind::NotFound`].
    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>>;

    /// The loads of `pipeline` whose logs are open, in order.
    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>>;

    /// The loads of `pipeline` whose logs were removed and left something behind, as a crash
    /// part way through a removal does, in order: what they hold is never read, only removed.
    fn leftovers<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>>;

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

    /// Deletes what `load`'s log staged and has not published, whoever staged it, needing no room
    /// to do so.
    ///
    /// A publish of what was deleted is refused with [`io::ErrorKind::NotFound`].
    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>>;

    /// Deletes the published chunk `chunk`, durably; one that is gone is no error.
    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>>;

    /// Removes `load`'s log, durably; removing a log removed is no error.
    ///
    /// The log is no longer open once this begins, so no chunk of it is published after; then
    /// what it staged and published is deleted, and a crash part way leaves what
    /// [`WalStore::leftovers`] lists.
    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>>;
}
