//! An object store that fails on cue, for the write-ahead log's tests and the simulation: each
//! request to an inner store done as asked, failed, slowed, never answered, raced, answered as
//! failed once done, or listed without its newest object, as a plan says.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt as _, TryStreamExt as _};
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetRange, GetResult, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result,
};
use parking_lot::Mutex;

/// What a request asks of the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// A put, `create` where it is refused for a name taken.
    Put {
        /// Whether a name taken refuses it.
        create: bool,
    },
    /// A read, or a look at what the store says of an object.
    Get,
    /// A listing.
    List,
    /// A deletion.
    Delete,
    /// The beginning of an upload in parts.
    Begin,
    /// A part of an upload.
    Part,
    /// The completion of an upload.
    Complete,
    /// The end of an upload given up.
    Abort,
}

/// A request: what it asks, and of which object or directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Call {
    /// What it asks.
    pub op: Op,
    /// The object, or for a listing the directory.
    pub key: String,
}

/// What becomes of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Done as asked.
    None,
    /// Failed before anything is done, as a dropped connection.
    Fail,
    /// Done after this many turns of the scheduler.
    Slow(u32),
    /// Never answered, nor done.
    Hang,
    /// A create answered as taken by a racing create that did not land, nothing done; any other
    /// request fails.
    Raced,
    /// Done, and answered as failed: the answer was lost.
    Answerless,
    /// A listing that misses the newest object it would show; any other request is done.
    Stale,
    /// Refused as the credentials may not do it.
    Denied,
    /// Refused as a request the store does not implement.
    Unsupported,
    /// A create done as a put that replaces what holds the name, as a store that ignores the
    /// condition does; any other request is done.
    Overwrite,
    /// A deletion answered as done, and not done; any other request is done.
    Ignored,
    /// A put done without the metadata it was given, as a store that keeps none; any other
    /// request is done.
    Bare,
    /// Never answered, and done once this long has passed, when the store is next asked
    /// anything, as a request a client gave up on lands late: a put's or a deletion's; any other
    /// request is never answered nor done.
    Late(std::time::Duration),
}

/// A request that lands late: a put, or a deletion.
enum Landing {
    Put(Path, PutPayload, PutOptions),
    Delete(Path),
}

/// What decides each request's fault, in the order the requests come.
pub type Plan = Box<dyn FnMut(&Call) -> Fault + Send>;

/// An object store passing each request to `S` as its plan says.
pub struct Faulty<S> {
    inner: Arc<S>,
    state: Arc<Mutex<State>>,
}

struct State {
    plan: Plan,
    /// Requests that land late: when, and what they do.
    late: Vec<(tokio::time::Instant, Landing)>,
    calls: Vec<Call>,
    /// The range of each read, in order: none where the whole object was asked for.
    ranges: Vec<Option<GetRange>>,
    /// Every object written, in order, the newest last.
    written: Vec<String>,
}

impl<S> Clone for Faulty<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            state: Arc::clone(&self.state),
        }
    }
}

impl<S> fmt::Debug for Faulty<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Faulty")
    }
}

impl<S> fmt::Display for Faulty<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Faulty")
    }
}

/// A plan that does every request as asked.
pub fn faultless() -> Plan {
    Box::new(|_| Fault::None)
}

impl<S: ObjectStore> Faulty<S> {
    /// `inner`, its requests faulted as `plan` says.
    pub fn new(inner: S, plan: Plan) -> Self {
        let state = State {
            plan,
            late: Vec::new(),
            calls: Vec::new(),
            ranges: Vec::new(),
            written: Vec::new(),
        };
        Self {
            inner: Arc::new(inner),
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// The plan from now on.
    pub fn plan(&self, plan: Plan) {
        self.state.lock().plan = plan;
    }

    /// Every request made, in order.
    pub fn calls(&self) -> Vec<Call> {
        self.state.lock().calls.clone()
    }

    /// The range of each read made, in order: none where the whole object was asked for.
    pub fn ranges(&self) -> Vec<Option<GetRange>> {
        self.state.lock().ranges.clone()
    }

    /// The store the requests go to.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn decide(&self, op: Op, key: &Path) -> Fault {
        let call = Call {
            op,
            key: key.to_string(),
        };
        let mut state = self.state.lock();
        let fault = (state.plan)(&call);
        state.calls.push(call);
        fault
    }

    /// Keeps `landing` to land once `after` has passed, and never answers.
    async fn later<T>(&self, after: std::time::Duration, landing: Landing) -> Result<T> {
        let at = tokio::time::Instant::now() + after;
        self.state.lock().late.push((at, landing));
        std::future::pending().await
    }

    /// Lands every late request whose time has come, in the order they were made.
    async fn land(&self) {
        let now = tokio::time::Instant::now();
        let due: Vec<Landing> = {
            let mut state = self.state.lock();
            let due = state.late.extract_if(.., |(at, _)| *at <= now);
            due.map(|(_, landing)| landing).collect()
        };
        for landing in due {
            match landing {
                Landing::Put(key, payload, opts) => {
                    if self.inner.put_opts(&key, payload, opts).await.is_ok() {
                        self.wrote(&key);
                    }
                }
                Landing::Delete(key) => drop(self.inner.delete(&key).await),
            }
        }
    }

    fn wrote(&self, key: &Path) {
        self.state.lock().written.push(key.to_string());
    }

    /// The newest object among `listed`, as the store wrote them.
    fn newest(&self, listed: &[ObjectMeta]) -> Option<Path> {
        let state = self.state.lock();
        state
            .written
            .iter()
            .rev()
            .find(|written| listed.iter().any(|meta| meta.location.as_ref() == *written))
            .map(|written| Path::from(written.as_str()))
    }
}

/// The error of a request failed on cue.
fn failed(what: &'static str) -> Error {
    Error::Generic {
        store: "Faulty",
        source: what.into(),
    }
}

/// Does `action` as `fault` says; `create` where it is a create, which a race refuses.
async fn faulted<T>(
    fault: Fault,
    key: &Path,
    create: bool,
    action: impl Future<Output = Result<T>>,
) -> Result<T> {
    match fault {
        Fault::None | Fault::Stale | Fault::Overwrite | Fault::Ignored | Fault::Bare => {
            action.await
        }
        Fault::Denied => Err(Error::PermissionDenied {
            path: key.to_string(),
            source: "the credentials may not".into(),
        }),
        Fault::Unsupported => Err(Error::NotImplemented {
            operation: "the request".into(),
            implementer: "Faulty".into(),
        }),
        Fault::Fail => Err(failed("the connection dropped")),
        Fault::Slow(turns) => {
            for _ in 0..turns {
                tokio::task::yield_now().await;
            }
            action.await
        }
        Fault::Hang | Fault::Late(_) => std::future::pending().await,
        Fault::Raced if create => Err(Error::AlreadyExists {
            path: key.to_string(),
            source: "a racing create".into(),
        }),
        Fault::Raced => Err(failed("a request raced another")),
        Fault::Answerless => {
            action.await?;
            Err(failed("the answer was lost"))
        }
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for Faulty<S> {
    async fn put_opts(
        &self,
        key: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.land().await;
        let create = matches!(opts.mode, PutMode::Create);
        let fault = self.decide(Op::Put { create }, key);
        if let Fault::Late(after) = fault {
            return self
                .later(after, Landing::Put(key.clone(), payload, opts))
                .await;
        }
        let opts = match fault {
            Fault::Overwrite => PutOptions {
                mode: PutMode::Overwrite,
                ..opts
            },
            Fault::Bare => PutOptions {
                attributes: object_store::Attributes::new(),
                ..opts
            },
            _ => opts,
        };
        let put = faulted(fault, key, create, async {
            let put = self.inner.put_opts(key, payload, opts).await?;
            self.wrote(key);
            Ok(put)
        });
        put.await
    }

    async fn put_multipart_opts(
        &self,
        key: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }

    async fn get_opts(&self, key: &Path, options: GetOptions) -> Result<GetResult> {
        self.land().await;
        let fault = self.decide(Op::Get, key);
        self.state.lock().ranges.push(options.range.clone());
        faulted(fault, key, false, self.inner.get_opts(key, options)).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let this = self.clone();
        locations
            .then(move |location| {
                let this = this.clone();
                async move {
                    let location = location?;
                    this.land().await;
                    let fault = this.decide(Op::Delete, &location);
                    if let Fault::Late(after) = fault {
                        return this.later(after, Landing::Delete(location)).await;
                    }
                    if fault != Fault::Ignored {
                        let deleted = this.inner.delete(&location);
                        faulted(fault, &location, false, deleted).await?;
                    }
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let dir = prefix.cloned().unwrap_or_default();
        let fault = self.decide(Op::List, &dir);
        let this = self.clone();
        let listed = async move {
            this.land().await;
            let listing = this.inner.list(Some(&dir)).try_collect::<Vec<_>>();
            let mut listed = faulted(fault, &dir, false, listing).await?;
            if fault == Fault::Stale
                && let Some(newest) = this.newest(&listed)
            {
                listed.retain(|meta| meta.location != newest);
            }
            Ok::<_, Error>(stream::iter(listed.into_iter().map(Ok)))
        };
        stream::once(listed).try_flatten().boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.land().await;
        let dir = prefix.cloned().unwrap_or_default();
        let fault = self.decide(Op::List, &dir);
        faulted(fault, &dir, false, self.inner.list_with_delimiter(prefix)).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl<S: ObjectStore + MultipartStore> MultipartStore for Faulty<S> {
    async fn create_multipart(&self, key: &Path) -> Result<MultipartId> {
        self.land().await;
        let fault = self.decide(Op::Begin, key);
        faulted(fault, key, false, self.inner.create_multipart(key)).await
    }

    async fn put_part(
        &self,
        key: &Path,
        id: &MultipartId,
        index: usize,
        payload: PutPayload,
    ) -> Result<PartId> {
        self.land().await;
        let fault = self.decide(Op::Part, key);
        let part = self.inner.put_part(key, id, index, payload);
        faulted(fault, key, false, part).await
    }

    async fn complete_multipart(
        &self,
        key: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        self.land().await;
        let fault = self.decide(Op::Complete, key);
        let completed = faulted(fault, key, false, async {
            let completed = self.inner.complete_multipart(key, id, parts).await?;
            self.wrote(key);
            Ok(completed)
        });
        completed.await
    }

    async fn abort_multipart(&self, key: &Path, id: &MultipartId) -> Result<()> {
        self.land().await;
        let fault = self.decide(Op::Abort, key);
        faulted(fault, key, false, self.inner.abort_multipart(key, id)).await
    }
}
