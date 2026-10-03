//! Where write-ahead logs are kept: one log per load of a pipeline, in numbered chunks.

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

/// A held claim on a load's log; dropping it lets the log go.
pub type Claim = Box<dyn std::any::Any + Send + Sync>;

/// Keeps each load's write-ahead log as numbered chunks of frames.
///
/// A log is written by one load at a time: appends to one chunk, then that chunk made durable,
/// then the next chunk. Reads are by range, so replaying a log never holds more than a frame.
///
/// A log has one claimant at a time: the load writing it, then whoever replays it once that load
/// is gone. A claim outlives nothing that holds it, so a load whose process died leaves its log
/// free for the next to replay.
pub trait WalStore: std::fmt::Debug + Send + Sync + 'static {
    /// A claim on `load`'s log of `pipeline`, or none where another holds it.
    fn claim<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Option<Claim>>>;

    /// Removes `load`'s log of `pipeline` whole: its chunks and what marks its claim.
    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>>;

    /// The loads of `pipeline` that have a log, or a claim's mark left behind.
    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>>;

    /// The chunks of `load`'s log, in order, each with its length.
    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>>;

    /// `len` bytes of chunk `chunk` of `load`'s log, from `offset`.
    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>>;

    /// Appends `bytes` to chunk `chunk` of `load`'s log, creating it where it is missing.
    fn append<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        bytes: Bytes,
    ) -> BoxFuture<'a, io::Result<()>>;

    /// Makes everything appended to chunk `chunk` of `load`'s log durable; a chunk once synced is
    /// finished, and appended to again only after a failure left it unknown.
    fn sync<'a>(&'a self, pipeline: &'a PipelineId, chunk: Chunk) -> BoxFuture<'a, io::Result<()>>;

    /// Removes chunk `chunk` of `load`'s log, durably, so no crash after it brings the chunk
    /// back; the log goes with its last chunk.
    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>>;
}
