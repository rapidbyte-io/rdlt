//! Write-ahead logs kept in an object store: S3, and stores that answer as it does.

mod calls;
mod fault;
mod head;
mod keys;
mod ops;
mod probe;
mod staged;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::io;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::multipart::MultipartStore;
use parking_lot::Mutex;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

use self::calls::Calls;
pub(crate) use self::fault::code;
use self::head::Heads;
use self::keys::Keys;
use crate::env::Clock;
use crate::error::Error;
use crate::limits::{
    OBJECT_ATTEMPTS, OBJECT_BACKOFF, OBJECT_BACKOFF_MOST, OBJECT_PART_BYTES, OBJECT_PARTS,
    OBJECT_REQUEST, OBJECT_REQUEST_PER_MIB,
};
use crate::wal::{Chunk, StagedChunk, WalStore};

/// What an object-store log needs of its store: objects, and uploads in parts, as
/// `object_store`'s S3 and in-memory stores have.
pub trait WalObjects: ObjectStore + MultipartStore {}

impl<T: ObjectStore + MultipartStore + ?Sized> WalObjects for T {}

/// How an object-store log uses its store: the length of a part, and how each request is tried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreOptions {
    part_bytes: NonZeroUsize,
    attempts: NonZeroU32,
    backoff: Duration,
    backoff_most: Duration,
    request: Duration,
    request_per_mib: Duration,
}

impl Default for ObjectStoreOptions {
    /// Parts of 8 MiB; five attempts a request, waiting up to 100 ms before the first retry, each
    /// up to twice the last, at most 5 s; 30 s a request and a second more for each MiB it moves.
    fn default() -> Self {
        Self {
            part_bytes: NonZeroUsize::new(OBJECT_PART_BYTES).unwrap_or(NonZeroUsize::MIN),
            attempts: NonZeroU32::new(OBJECT_ATTEMPTS).unwrap_or(NonZeroU32::MIN),
            backoff: OBJECT_BACKOFF,
            backoff_most: OBJECT_BACKOFF_MOST,
            request: OBJECT_REQUEST,
            request_per_mib: OBJECT_REQUEST_PER_MIB,
        }
    }
}

impl ObjectStoreOptions {
    /// Chunks longer than `bytes` uploaded in parts of `bytes`; S3 takes parts of 5 MiB or more.
    #[must_use]
    pub fn with_part_bytes(mut self, bytes: NonZeroUsize) -> Self {
        self.part_bytes = bytes;
        self
    }

    /// Each request tried at most `attempts` times.
    #[must_use]
    pub fn with_attempts(mut self, attempts: NonZeroU32) -> Self {
        self.attempts = attempts;
        self
    }

    /// A random wait of up to `first` before the first retry, up to twice the last before each
    /// after, and never more than `most`.
    #[must_use]
    pub fn with_backoff(mut self, first: Duration, most: Duration) -> Self {
        self.backoff = first;
        self.backoff_most = most;
        self
    }

    /// Each attempt given `base`, and `per_mib` more for each MiB it moves, before it is given up.
    #[must_use]
    pub fn with_deadline(mut self, base: Duration, per_mib: Duration) -> Self {
        self.request = base;
        self.request_per_mib = per_mib;
        self
    }

    /// Bytes: the length of a part, and the longest chunk published by one request.
    pub fn part_bytes(&self) -> NonZeroUsize {
        self.part_bytes
    }
}

/// Write-ahead logs in an object store, beneath a prefix.
///
/// Every operation of the store's contract is an object operation, with no lock, rename or
/// operation over several objects: a log is opened by creating its mark where none of the name
/// exists; a chunk is staged in memory, uploaded in parts as it passes one, and published by
/// creating its head where its name is free, then asking whether the log's mark is still there,
/// deleting the head where it is not; a removal deletes the mark first, then lists and deletes
/// the log's objects. A store that does not refuse a second create of one name, list an object
/// once written, take uploads in parts or find a deleted object missing is refused as the log is
/// opened, so no log runs unfenced.
///
/// Every request is tried a bounded number of times, each attempt within a deadline, on the
/// [`Clock`] given; what the store's answers mean is in [`ObjectStoreWal::open`].
#[derive(Debug)]
pub struct ObjectStoreWal {
    shared: Arc<Shared>,
}

/// What an object-store log and the chunks it stages share.
#[derive(Debug)]
struct Shared {
    calls: Calls,
    keys: Keys,
    heads: Heads,
    /// Each staging not yet published, discarded or deleted: its log.
    stagings: Mutex<BTreeMap<u64, (PipelineId, LoadId)>>,
    /// Uploads that stagings dropped mid-way began, which the store's next request gives up.
    abandoned: Mutex<Vec<(object_store::path::Path, object_store::MultipartId)>>,
    next: AtomicU64,
    /// The store's identity, once it was asked for.
    identity: Mutex<Option<LoadId>>,
}

impl Shared {
    /// A random token, telling one object of a name apart from another.
    fn token(&self) -> u128 {
        let clock = &self.calls.clock;
        (u128::from(clock.random()) << 64) | u128::from(clock.random())
    }

    /// Bytes: the longest chunk, every part of it at most a part long.
    fn chunk_most(&self) -> u64 {
        let part = u64::try_from(self.calls.options.part_bytes.get()).unwrap_or(u64::MAX);
        part.saturating_mul(OBJECT_PARTS)
    }

    /// Registers a staging of `load`'s log of `pipeline`; its number.
    fn staging(&self, pipeline: &PipelineId, load: LoadId) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.stagings.lock().insert(id, (pipeline.clone(), load));
        id
    }

    /// Gives up every upload a staging dropped mid-way began, as far as each request's attempts
    /// get: a store keeps their parts, unseen, until something ends them.
    async fn reclaim(&self) {
        let abandoned = std::mem::take(&mut *self.abandoned.lock());
        for (key, id) in abandoned {
            self.calls.abort(&key, &id).await;
        }
    }

    /// Ends staging `id`: whether it was still staged, its log's stagings not deleted since.
    fn unstage(&self, id: u64) -> bool {
        self.stagings.lock().remove(&id).is_some()
    }
}

impl ObjectStoreWal {
    /// Logs in `objects` beneath `prefix`, each request tried as `options` say on `clock`, once a
    /// probe found the store does what the log needs.
    ///
    /// An answer that an object is missing or a name taken means what the store's contract
    /// says; a refusal of the credentials is `wal_storage_denied`; a store that does not do what
    /// a log needs is `wal_storage_unsupported`; a request every attempt of which failed or ran
    /// past its deadline is `wal_storage_unavailable`, retryable.
    ///
    /// # Errors
    ///
    /// `wal_prefix_invalid` for a prefix that is empty, longer than 512 bytes, or holds an
    /// empty, `.` or `..` segment or a character beyond `[A-Za-z0-9._-]` and `/`; and as the
    /// probe fails, as above.
    pub async fn open(
        objects: Arc<dyn WalObjects>,
        prefix: &str,
        clock: Arc<dyn Clock>,
        options: ObjectStoreOptions,
    ) -> Result<Self, Error> {
        let shared = Shared {
            calls: Calls {
                objects,
                clock,
                options,
            },
            keys: Keys::parse(prefix)?,
            heads: Heads::default(),
            stagings: Mutex::default(),
            abandoned: Mutex::default(),
            next: AtomicU64::new(0),
            identity: Mutex::default(),
        };
        probe::probe(&shared).await.map_err(Error::from_wal)?;
        Ok(Self {
            shared: Arc::new(shared),
        })
    }
}

impl WalStore for ObjectStoreWal {
    /// Half of what its parts hold: the engine's bound counts a load's batches, and the half left
    /// holds what it does not count, the header, seals, commit and end, each within the budget.
    fn chunk_bytes(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.shared.chunk_most() / 2)
    }

    /// A part: a staging uploads each part as it fills.
    fn staging_bytes(&self) -> u64 {
        u64::try_from(self.shared.calls.options.part_bytes.get()).unwrap_or(u64::MAX)
    }

    fn identity(&self, proposed: LoadId) -> BoxFuture<'_, io::Result<LoadId>> {
        Box::pin(self.shared.identity(proposed))
    }

    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(self.shared.open_log(pipeline, load))
    }

    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        Box::pin(ops::stage(Arc::clone(&self.shared), pipeline, chunk))
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        Box::pin(self.shared.loads(pipeline))
    }

    fn leftovers<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        Box::pin(self.shared.leftovers(pipeline))
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        Box::pin(self.shared.chunks(pipeline, load))
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        Box::pin(self.shared.read(pipeline, chunk, offset, len))
    }

    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let log = (pipeline.clone(), load);
        self.shared
            .stagings
            .lock()
            .retain(|_, staged| *staged != log);
        Box::pin(async { Ok(()) })
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(self.shared.remove(pipeline, chunk))
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(self.shared.remove_log(pipeline, load))
    }
}
