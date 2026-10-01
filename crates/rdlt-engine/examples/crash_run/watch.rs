//! Watching a run as it loads: the source's reads as they begin and the destination's commits as
//! they land, told on standard output, and a spawned connector killed before a chosen commit.

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, CommitMeta, Cursor, Destination, DestinationSession,
    DestinationWriter, OpenContext, OpenedSession, PartitionId, PartitionPlan, PartitionSink,
    ReadRequest, Receipt, Result, Source, StreamName, StreamState, TableChange, TableRef,
};
use rdlt_host::Kills;

use crate::config::{Before, Kill, Named, Victim};

/// How long a killed connector takes to die, at most: the kill's reaper sends `SIGKILL` as it
/// runs next, so a commit after the wait goes to a connector already dead.
const DYING: Duration = Duration::from_millis(200);

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
    kill: Option<(Kills, Kill)>,
    killed: AtomicBool,
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

    /// Before the commit `meta`: kills where it is the chosen one, telling the reads then in
    /// flight, and waits for the kill to land.
    #[expect(
        clippy::disallowed_methods,
        reason = "the harness stands for the CLI, outside the engine, on the real clock"
    )]
    async fn committing(&self, meta: &CommitMeta) {
        let asked = self.asked.fetch_add(1, Ordering::SeqCst) + 1;
        let Some((kills, kill)) = &self.kill else {
            return;
        };
        let reading = self.reading.load(Ordering::SeqCst);
        let chosen = match (kill.victim, kill.before) {
            (Victim::Source, Before::Commit(commit)) => asked >= commit && reading > 0,
            (Victim::Destination, Before::Commit(commit)) => asked == commit,
            (_, Before::Named(Named::Publish)) => !meta.finish_generations.is_empty(),
        };
        if chosen && !self.killed.swap(true, Ordering::SeqCst) {
            kills.kill();
            writeln!(std::io::stdout(), "killed reading {reading}").ok();
            tokio::time::sleep(DYING).await;
        }
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
        self.inner.writer(table)
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            self.watch.committing(meta).await;
            let receipt = self.inner.commit(meta).await?;
            self.watch.committed().await;
            Ok(receipt)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}
