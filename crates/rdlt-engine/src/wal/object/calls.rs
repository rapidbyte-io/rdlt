//! An object-store log's requests: each tried a bounded number of times, each attempt within a
//! deadline scaled by what it moves, with a random wait growing between attempts, all on the
//! store's clock.

use std::future::Future;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use object_store::multipart::PartId;
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, GetOptions, GetRange, MultipartId, ObjectMeta, ObjectStoreExt as _,
    PutMode, PutOptions, PutPayload,
};

use super::fault::{self, Answer, Ask, ObjectFault};
use super::{ObjectStoreOptions, WalObjects};
use crate::env::Clock;
use crate::limits::{OBJECT_LISTED, OBJECT_PARTS};

/// How a request's tries go: at most `attempts`, each within `deadline`, read as `ask`, and
/// whether one ended with its outcome unknown.
pub(super) struct Tries {
    attempts: u32,
    deadline: Duration,
    ask: Ask,
    unsure: bool,
}

impl Tries {
    fn new(attempts: u32, deadline: Duration, ask: Ask) -> Self {
        Self {
            attempts,
            deadline,
            ask,
            unsure: false,
        }
    }
}

/// Objects S3 answers a page of a listing with at most.
const LISTED_PAGE: usize = 1_000;

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
    /// The store's answer where it is final: [`io::ErrorKind::NotFound`], or an
    /// [`ObjectFault`]; [`ObjectFault::Unavailable`] once every attempt failed.
    pub(super) async fn call<T, F, Fut>(&self, key: &Path, bytes: u64, attempt: F) -> io::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = object_store::Result<T>>,
    {
        let attempts = self.options.attempts.get();
        let mut tries = Tries::new(attempts, self.deadline(bytes), Ask::Other);
        self.tried(key, &mut tries, attempt).await
    }

    /// What `attempt` answers at `key`, tried as `tries` say; `tries` notes whether an attempt
    /// ended with its outcome unknown.
    ///
    /// # Errors
    ///
    /// As [`Calls::call`], [`ObjectFault::Unavailable`] once the attempts `tries` allows failed.
    pub(super) async fn tried<T, F, Fut>(
        &self,
        key: &Path,
        tries: &mut Tries,
        mut attempt: F,
    ) -> io::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = object_store::Result<T>>,
    {
        let mut failed = 0_u32;
        loop {
            let answered = tokio::select! {
                biased;
                // An answer ready as the deadline passes is taken.
                answered = attempt() => Some(answered),
                () = self.clock.sleep(tries.deadline) => None,
            };
            let last = match answered {
                Some(Ok(value)) => return Ok(value),
                Some(Err(error)) => match fault::answer(key.as_ref(), error, tries.ask) {
                    Answer::Final(error) => return Err(error),
                    Answer::Transient(error) => Some(error),
                },
                None => None,
            };
            tries.unsure = true;
            failed += 1;
            if failed >= tries.attempts {
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

    /// Creates `key` holding `payload` where no object of the name exists, marked with a token
    /// of its own.
    ///
    /// A name answered taken is another's, unless an attempt before ended with its outcome
    /// unknown: then the object's token is read, and one bearing this create's is its own, made
    /// by that attempt; a name found empty, as a store answers creates racing, is created again.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::AlreadyExists`] where another's object holds the name, and as
    /// [`Calls::call`].
    pub(super) async fn create(&self, key: &Path, payload: PutPayload) -> io::Result<()> {
        self.created(key, payload, self.options.attempts.get())
            .await
    }

    /// Creates `key` as [`Calls::create`] does, by one attempt only.
    ///
    /// An attempt whose outcome is unknown is never made again: it may still land, after the
    /// object it makes was deleted. The object's token then says whether it landed.
    ///
    /// # Errors
    ///
    /// [`ObjectFault::Unavailable`] where the attempt's outcome is unknown and the name holds no
    /// object of its making, and as [`Calls::create`].
    pub(super) async fn create_once(&self, key: &Path, payload: PutPayload) -> io::Result<()> {
        self.created(key, payload, 1).await
    }

    /// Creates `key` as [`Calls::create`] does, in at most `attempts` attempts; where that is one,
    /// a name an attempt of unknown outcome left empty is not created again.
    async fn created(&self, key: &Path, payload: PutPayload, attempts: u32) -> io::Result<()> {
        let bytes = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
        let token = format!("{:016x}{:016x}", self.clock.random(), self.clock.random());
        let mut tries = Tries::new(attempts, self.deadline(bytes), Ask::Create);
        let once = attempts == 1;
        let mut raced = 0_u32;
        loop {
            let created = self
                .tried(key, &mut tries, || {
                    let options = PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::from_iter([(token_name(), token.clone())]),
                        ..PutOptions::default()
                    };
                    self.objects.put_opts(key, payload.clone(), options)
                })
                .await;
            let refusal = match created {
                Err(error)
                    if tries.unsure && (once || error.kind() == io::ErrorKind::AlreadyExists) =>
                {
                    error
                }
                created => return created.map(drop),
            };
            match self.token(key).await {
                Ok(held) if held.as_deref() == Some(token.as_str()) => return Ok(()),
                Ok(_) => return Err(refusal),
                Err(error) if error.kind() == io::ErrorKind::NotFound && !once => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(refusal),
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

    /// The token the create that made `key` marked it with; none where it bears none.
    pub(super) async fn token(&self, key: &Path) -> io::Result<Option<String>> {
        let options = || GetOptions {
            head: true,
            ..GetOptions::default()
        };
        let got = self
            .call(key, 0, || self.objects.get_opts(key, options()))
            .await?;
        Ok(got
            .attributes
            .get(&token_name())
            .map(|value| value.to_string()))
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

    /// Every object beneath `dir`, at most [`OBJECT_LISTED`], each page within a request's
    /// deadline, the listing within as many as its pages may be.
    ///
    /// # Errors
    ///
    /// [`ObjectFault::Crowded`] beyond [`OBJECT_LISTED`], and as [`Calls::call`].
    pub(super) async fn list(&self, dir: &Path) -> io::Result<Vec<ObjectMeta>> {
        let page = self.deadline(0);
        let pages = u32::try_from(OBJECT_LISTED / LISTED_PAGE + 1).unwrap_or(u32::MAX);
        let attempts = self.options.attempts.get();
        let mut tries = Tries::new(attempts, page.saturating_mul(pages), Ask::Other);
        self.tried(dir, &mut tries, || async {
            let mut stream = self.objects.list(Some(dir));
            let mut listed = Vec::new();
            loop {
                let next = tokio::select! {
                    biased;
                    // A page ready as its deadline passes is taken.
                    next = stream.next() => next,
                    () = self.clock.sleep(page) => return Err(late(dir)),
                };
                let Some(meta) = next else {
                    return Ok(Ok(listed));
                };
                if listed.len() >= OBJECT_LISTED {
                    return Ok(Err(crowded(dir)));
                }
                listed.push(meta?);
            }
        })
        .await?
    }

    /// The directories directly beneath `dir`, at most [`OBJECT_LISTED`].
    ///
    /// # Errors
    ///
    /// [`ObjectFault::Crowded`] beyond [`OBJECT_LISTED`], and as [`Calls::call`].
    pub(super) async fn dirs(&self, dir: &Path) -> io::Result<Vec<Path>> {
        let listed = self
            .call(dir, 0, || self.objects.list_with_delimiter(Some(dir)))
            .await?;
        if listed.common_prefixes.len() > OBJECT_LISTED {
            return Err(crowded(dir));
        }
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

/// The metadata naming the token of the create that made an object.
fn token_name() -> Attribute {
    Attribute::Metadata("rdlt-token".into())
}

/// The failure of a page of a listing of `dir` that did not come within its deadline.
fn late(dir: &Path) -> object_store::Error {
    object_store::Error::Generic {
        store: "log",
        source: format!("a page of the listing of {dir} did not come within its deadline").into(),
    }
}

/// The refusal of a listing of `dir` past [`OBJECT_LISTED`].
fn crowded(dir: &Path) -> io::Error {
    ObjectFault::Crowded {
        dir: dir.to_string(),
        most: OBJECT_LISTED,
    }
    .into()
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
