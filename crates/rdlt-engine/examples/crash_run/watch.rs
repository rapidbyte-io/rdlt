//! Watching a run as it loads: the source's reads as they begin and the destination's commits as
//! they land, told on standard output, and a spawned connector killed before a chosen commit or
//! before its answer, or a source before a chosen write.

use std::io::Write as _;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, CommitMeta, Cursor, Destination, DestinationSession,
    DestinationWriter, OpenContext, OpenedSession, PartitionId, PartitionPlan, PartitionSink,
    ReadRequest, Receipt, Result, SegmentId, Source, StreamName, StreamState, TableChange,
    TableRef, WriteStats,
};
use rdlt_host::Kills;

use crate::config::{Answer, Before, Kill, Named, Victim};

/// How long a killed connector may take to be gone before the run goes on regardless, and
/// the test that watches it fails.
const DYING: Duration = Duration::from_secs(60);

/// What a run's watch knows: the reads begun and in flight, the commits asked for and landed, the
/// kill to make, and where the run waits to be killed.
#[derive(Default)]
pub(crate) struct Watch {
    told: AtomicU64,
    pause: Option<u64>,
    reads: AtomicU64,
    reading: AtomicU64,
    asked: AtomicU64,
    commits: AtomicU64,
    /// The writes the destination was asked for.
    writes: AtomicU64,
    kill: Option<(Kills, Kill)>,
    killed: AtomicBool,
    /// Held by the write that kills a source until the source is gone, and taken by every
    /// write after it: none goes on while the source dies.
    dying: tokio::sync::Mutex<()>,
}

impl Watch {
    /// A watch making `kill` through `kills` where given, and holding the run after the `pause`th
    /// read or commit it tells, where given, until the run is killed.
    pub(crate) fn new(kill: Option<(Kills, Kill)>, pause: Option<u64>) -> Self {
        Self {
            pause,
            kill,
            ..Self::default()
        }
    }

    /// Tells `line`, a read begun or a commit landed, and holds what told it for good where the run
    /// pauses after it, so a kill finds the run there.
    async fn tell(&self, line: String) {
        writeln!(std::io::stdout(), "{line}").ok();
        let told = self.told.fetch_add(1, Ordering::SeqCst) + 1;
        if self.pause == Some(told) {
            std::future::pending::<()>().await;
        }
    }

    /// As a read begins: counts it in flight, and tells its number.
    async fn reads(&self) {
        let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        self.reading.fetch_add(1, Ordering::SeqCst);
        self.tell(format!("read {read}")).await;
    }

    /// Before the commit `meta`: kills the destination where it is the chosen one, and waits
    /// for it to be gone, so the commit goes to a connector already dead.
    async fn committing(&self, meta: &CommitMeta) {
        let asked = self.asked.fetch_add(1, Ordering::SeqCst) + 1;
        let Some((kills, kill)) = &self.kill else {
            return;
        };
        let chosen = match (kill.victim, kill.before) {
            (Victim::Source, _) | (Victim::Destination, Before::Answer(_)) => false,
            (Victim::Destination, Before::Commit(commit)) => asked == commit,
            (Victim::Destination, Before::Named(Named::Publish)) => {
                !meta.finish_generations.is_empty()
            }
        };
        if chosen {
            self.kills(kills).await;
        }
    }

    /// Before a write: kills the source where the write is the chosen one, and holds this
    /// write and every later one until the source is gone.
    ///
    /// The engine takes no more of a source than its budget holds and its writes carry away,
    /// and the source reads within a credit of less than a batch: with its writes held, what
    /// the engine took is bounded, so a source with more to send is killed as it reads,
    /// however the host's tasks and threads are scheduled.
    async fn writing(&self) {
        let written = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        let Some((kills, kill)) = &self.kill else {
            return;
        };
        let (Victim::Source, Before::Commit(write)) = (kill.victim, kill.before) else {
            return;
        };
        if written < write {
            return;
        }
        let _held = self.dying.lock().await;
        self.kills(kills).await;
    }

    /// Kills through `kills`, once, telling the reads then in flight, and waits for the
    /// connector killed to be gone: the thread that owns it has reaped it.
    #[expect(
        clippy::disallowed_methods,
        reason = "the harness stands for the CLI, outside the engine, on the real clock"
    )]
    async fn kills(&self, kills: &Kills) {
        if self.killed.swap(true, Ordering::SeqCst) {
            return;
        }
        let reading = self.reading.load(Ordering::SeqCst);
        let alive = rdlt_host::spawned().len();
        kills.kill();
        writeln!(std::io::stdout(), "killed reading {reading}").ok();
        let until = tokio::time::Instant::now() + DYING;
        while rdlt_host::spawned().len() >= alive && tokio::time::Instant::now() < until {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// After the commit the destination was last asked for landed, before its answer is heard:
    /// kills the destination where the answer is the chosen one, and tells the commit landed;
    /// whether it killed it.
    async fn answering(&self) -> bool {
        let Some((kills, kill)) = &self.kill else {
            return false;
        };
        let Before::Answer(Answer { answer }) = kill.before else {
            return false;
        };
        if kill.victim != Victim::Destination || self.asked.load(Ordering::SeqCst) != answer {
            return false;
        }
        self.kills(kills).await;
        writeln!(std::io::stdout(), "landed unanswered {answer}").ok();
        true
    }

    /// After a commit landed: tells its number.
    async fn committed(&self) {
        let commit = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        self.tell(format!("commit {commit}")).await;
    }
}

/// `inner`, its reads counted by `watch`.
pub(crate) fn source(inner: Arc<dyn Source>, watch: Arc<Watch>) -> Arc<dyn Source> {
    Arc::new(Watched { inner, watch })
}

/// `inner`, its commits watched by `watch`.
pub(crate) fn destination(inner: Arc<dyn Destination>, watch: Arc<Watch>) -> Arc<dyn Destination> {
    Arc::new(Watched { inner, watch })
}

struct Watched<C: ?Sized> {
    inner: Arc<C>,
    watch: Arc<Watch>,
}

impl Source for Watched<dyn Source> {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        self.inner.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        self.inner.plan(stream, state)
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.watch.reads().await;
            let read = self.inner.read(request, sink).await;
            self.watch.reading.fetch_sub(1, Ordering::SeqCst);
            read
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        self.inner.committed(stream, cursors)
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.inner.max_calls()
    }
}

impl Destination for Watched<dyn Destination> {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.inner.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(Session {
                    inner: opened.session,
                    watch: Arc::clone(&self.watch),
                }),
                ..opened
            })
        })
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.inner.max_calls()
    }
}

struct Session {
    inner: Box<dyn DestinationSession>,
    watch: Arc<Watch>,
}

impl DestinationSession for Session {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let inner = self.inner.writer(table).await?;
            let watch = Arc::clone(&self.watch);
            Ok(Box::new(Writer { inner, watch }) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            self.watch.committing(meta).await;
            let receipt = self.inner.commit(meta).await?;
            if self.watch.answering().await {
                // The destination is gone with the answer: what the engine hears is what its
                // connection says now.
                return self.inner.commit(meta).await;
            }
            self.watch.committed().await;
            Ok(receipt)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}

struct Writer {
    inner: Box<dyn DestinationWriter>,
    watch: Arc<Watch>,
}

impl DestinationWriter for Writer {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.watch.writing().await;
            self.inner.write(segment, batch).await
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        self.inner.flush()
    }
}
