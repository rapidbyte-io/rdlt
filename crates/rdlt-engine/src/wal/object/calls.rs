//! An object-store log's requests: each tried a bounded number of times, each attempt within a
//! deadline scaled by what it moves, with a random wait growing between attempts, all on the
//! store's clock.

use std::future::Future;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt as _, TryStreamExt as _};
use object_store::multipart::PartId;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, MultipartId, ObjectMeta, ObjectStoreExt as _, PutMode, PutPayload,
};

use super::fault::{self, Answer, ObjectFault};
use super::{ObjectStoreOptions, WalObjects};
use crate::env::Clock;
use crate::limits::OBJECT_PARTS;

/// Bytes in a MiB, which a request's deadline grows by.
const MIB: u64 = 1 << 20;

/// What an object-store log's requests go to, and how they are tried.
#[derive(Debug)]
pub(super) struct Calls {
    pub(super) objects: Arc<dyn WalObjects>,
    pub(super) clock: Arc<dyn Clock>,
    pub(super) options: ObjectStoreOptions,
}

impl Calls {
    /// What `attempt` answers at `key`, moving `bytes`: tried again after a failure another
    /// attempt may not meet, and after running past its deadline, up to the attempts the options
    /// allow.
    ///
    /// # Errors
    ///
    /// The store's answer where it is final: [`io::ErrorKind::NotFound`],
    /// [`io::ErrorKind::AlreadyExists`], or an [`ObjectFault`]; [`ObjectFault::Unavailable`]
    /// once every attempt failed.
    pub(super) async fn call<T, F, Fut>(
        &self,
        key: &Path,
        bytes: u64,
        mut attempt: F,
    ) -> io::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = object_store::Result<T>>,
    {
        let mut failed = 0_u32;
        loop {
            let deadline = self.deadline(bytes);
            let answered = tokio::select! {
                biased;
                // An answer ready as the deadline passes is taken.
                answered = attempt() => Some(answered),
                () = self.clock.sleep(deadline) => None,
            };
            let last = match answered {
                Some(Ok(value)) => return Ok(value),
                Some(Err(error)) => match fault::answer(key.as_ref(), error) {
                    Answer::Final(error) => return Err(error),
                    Answer::Transient(error) => Some(error),
                },
                None => None,
            };
            failed += 1;
            if failed >= self.options.attempts.get() {
                let key = key.to_string();
                return Err(ObjectFault::Unavailable { key, source: last }.into());
            }
            self.clock.sleep(self.backoff(failed)).await;
        }
    }

    /// How long an attempt moving `bytes` may take, no object holding more than a chunk does.
    pub(super) fn deadline(&self, bytes: u64) -> Duration {
        let part = u64::try_from(self.options.part_bytes.get()).unwrap_or(u64::MAX);
        let moved = bytes.min(part.saturating_mul(OBJECT_PARTS));
        let mibs = u32::try_from(moved.div_ceil(MIB)).unwrap_or(u32::MAX);
        let options = &self.options;
        options
            .request
            .saturating_add(options.request_per_mib.saturating_mul(mibs))
    }

    /// The wait before the attempt after `failed` failed ones: a random time up to the first
    /// backoff doubled for each failure after the first, and never more than the longest.
    pub(super) fn backoff(&self, failed: u32) -> Duration {
        let doubled = 1_u32
            .checked_shl(failed.saturating_sub(1))
            .unwrap_or(u32::MAX);
        let most = self
            .options
            .backoff
            .saturating_mul(doubled)
            .min(self.options.backoff_most);
        let nanos = u64::try_from(most.as_nanos()).unwrap_or(u64::MAX);
        Duration::from_nanos(self.clock.random() % nanos.saturating_add(1))
    }

    /// Creates `key` holding `payload` where no object of the name exists.
    ///
    /// A name found taken is read back: an object holding `payload` is this create's own, made by
    /// an attempt whose answer was lost, or by another that wrote the same bytes; one holding
    /// anything else is another's. A name answered taken and found empty, as a store answers two
    /// creates racing, is created again.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::AlreadyExists`] where another's object holds the name, and as
    /// [`Calls::call`].
    pub(super) async fn create(&self, key: &Path, payload: PutPayload) -> io::Result<()> {
        let bytes = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
        let mut raced = 0_u32;
        loop {
            let created = self
                .call(key, bytes, || {
                    let mode = PutMode::Create.into();
                    self.objects.put_opts(key, payload.clone(), mode)
                })
                .await;
            let taken = match created {
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => error,
                created => return created.map(drop),
            };
            match self.holds(key, &payload).await {
                Ok(true) => return Ok(()),
                Ok(false) => return Err(taken),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            raced += 1;
            if raced >= self.options.attempts.get() {
                let key = key.to_string();
                return Err(ObjectFault::Unavailable { key, source: None }.into());
            }
            self.clock.sleep(self.backoff(raced)).await;
        }
    }

    /// Puts `payload` at `key`, replacing what is there: for a mark whose every put is alike.
    pub(super) async fn put(&self, key: &Path, payload: PutPayload) -> io::Result<()> {
        let bytes = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
        self.call(key, bytes, || self.objects.put(key, payload.clone()))
            .await
            .map(drop)
    }

    /// Whether `key` holds `payload`, read no further than the first byte that differs.
    async fn holds(&self, key: &Path, payload: &PutPayload) -> io::Result<bool> {
        let bytes = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
        self.call(key, bytes, || async {
            let got = self.objects.get_opts(key, GetOptions::default()).await?;
            if got.meta.size != bytes {
                return Ok(false);
            }
            let mut stream = got.into_stream();
            let mut expected = payload.iter().flat_map(|part| part.iter().copied());
            while let Some(read) = stream.next().await {
                for byte in read? {
                    if expected.next() != Some(byte) {
                        return Ok(false);
                    }
                }
            }
            Ok(expected.next().is_none())
        })
        .await
    }

    /// The bytes of `key` in `range`, fewer where the object ends first: none where `range`
    /// starts at or past its end.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::NotFound`] where there is no such object; [`io::ErrorKind::InvalidData`]
    /// where the store answers more than was asked; and as [`Calls::call`].
    pub(super) async fn read(&self, key: &Path, range: Range<u64>) -> io::Result<Bytes> {
        if range.is_empty() {
            return self
                .head(key)
                .await?
                .map(|_| Bytes::new())
                .ok_or_else(missing);
        }
        let most = range.end - range.start;
        self.call(key, most, || async {
            let options = GetOptions {
                range: Some(asked(&range)),
                ..GetOptions::default()
            };
            match self.objects.get_opts(key, options).await {
                Ok(got) => gathered(got, most).await,
                // A store refuses a range that starts at or past the object's end as a failure
                // of its own kind, which a look at the object tells apart.
                Err(error) => match self.objects.head(key).await {
                    Ok(meta) if meta.size <= range.start => Ok(Ok(Bytes::new())),
                    _ => Err(error),
                },
            }
        })
        .await?
    }

    /// What the store says of `key`; none where there is no such object.
    pub(super) async fn head(&self, key: &Path) -> io::Result<Option<ObjectMeta>> {
        match self.call(key, 0, || self.objects.head(key)).await {
            Ok(meta) => Ok(Some(meta)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Every object beneath `dir`.
    pub(super) async fn list(&self, dir: &Path) -> io::Result<Vec<ObjectMeta>> {
        self.call(dir, 0, || self.objects.list(Some(dir)).try_collect())
            .await
    }

    /// The directories directly beneath `dir`.
    pub(super) async fn dirs(&self, dir: &Path) -> io::Result<Vec<Path>> {
        let listed = self
            .call(dir, 0, || self.objects.list_with_delimiter(Some(dir)))
            .await?;
        Ok(listed.common_prefixes)
    }

    /// Deletes `key`; one that is gone is no error.
    pub(super) async fn delete(&self, key: &Path) -> io::Result<()> {
        match self.call(key, 0, || self.objects.delete(key)).await {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }

    /// Begins an upload of `key` in parts.
    pub(super) async fn begin(&self, key: &Path) -> io::Result<MultipartId> {
        self.call(key, 0, || self.objects.create_multipart(key))
            .await
    }

    /// Uploads `payload` as part `index` of upload `id` of `key`.
    pub(super) async fn part(
        &self,
        key: &Path,
        id: &MultipartId,
        index: usize,
        payload: PutPayload,
    ) -> io::Result<PartId> {
        let bytes = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
        self.call(key, bytes, || {
            self.objects.put_part(key, id, index, payload.clone())
        })
        .await
    }

    /// Completes upload `id` of `key` from `parts`, `len` bytes in all.
    ///
    /// A completion that fails, as one whose answer was lost does when it is tried again, is
    /// done where the object is there, `len` bytes long: its key is this upload's alone.
    pub(super) async fn complete(
        &self,
        key: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
        len: u64,
    ) -> io::Result<()> {
        self.call(key, 0, || async {
            match self
                .objects
                .complete_multipart(key, id, parts.clone())
                .await
            {
                Ok(_) => Ok(()),
                Err(error) => match self.objects.head(key).await {
                    Ok(meta) if meta.size == len => Ok(()),
                    _ => Err(error),
                },
            }
        })
        .await
    }

    /// Gives up upload `id` of `key`, as far as its attempts get: a store keeps the parts of one
    /// it never hears of only until its operator's rule for unfinished uploads ends them.
    pub(super) async fn abort(&self, key: &Path, id: &MultipartId) {
        let aborted = self.call(key, 0, || self.objects.abort_multipart(key, id));
        drop(aborted.await);
    }
}

/// The range a read of `range` asks for: to the object's end where `range` ends past what a
/// signed 64-bit length holds, which some S3 servers refuse to read as a number.
fn asked(range: &Range<u64>) -> GetRange {
    if i64::try_from(range.end).is_ok() {
        GetRange::Bounded(range.clone())
    } else {
        GetRange::Offset(range.start)
    }
}

/// The error for an object found missing.
pub(super) fn missing() -> io::Error {
    io::Error::from(io::ErrorKind::NotFound)
}

/// What `got` holds, read as it streams in, and refused where it is more than `most` bytes.
pub(super) async fn gathered(
    got: object_store::GetResult,
    most: u64,
) -> object_store::Result<io::Result<Bytes>> {
    let mut stream = got.into_stream();
    let mut gathered = BytesMut::new();
    while let Some(read) = stream.next().await {
        let read = read?;
        let held = u64::try_from(gathered.len() + read.len()).unwrap_or(u64::MAX);
        if held > most {
            return Ok(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the object store answered more than the {most} bytes asked"),
            )));
        }
        gathered.extend_from_slice(&read);
    }
    Ok(Ok(gathered.freeze()))
}
