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

/// What a run's watch knows: the reads begun and in flight, the commits asked for and landed, and
/// the kill to make.
#[derive(Default)]
pub(crate) struct Watch {
    reads: AtomicU64,
    reading: AtomicU64,
    asked: AtomicU64,
    commits: AtomicU64,
    kill: Option<(Kills, Kill)>,
    killed: AtomicBool,
}

impl Watch {
    /// A watch making `kill` through `kills`.
    pub(crate) fn killing(kills: Kills, kill: Kill) -> Self {
        Self {
            kill: Some((kills, kill)),
            ..Self::default()
        }
    }

    /// As a read begins: tells its number, and counts it in flight.
    fn reads(&self) {
        let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        self.reading.fetch_add(1, Ordering::SeqCst);
        writeln!(std::io::stdout(), "read {read}").ok();
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
    fn committed(&self) {
        let commit = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        writeln!(std::io::stdout(), "commit {commit}").ok();
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
            self.watch.reads();
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
            self.watch.committed();
            Ok(receipt)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}
