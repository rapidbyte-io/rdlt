//! The key each commit carries and the key it expects landed: a run's commits follow one another
//! in its session, and a later run's first commit expects the last that landed before it.

use std::sync::Arc;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitKey, CommitMeta, Destination, DestinationSession,
    DestinationWriter, OpenContext, OpenedSession, ReadMode, Receipt, Result, TableChange,
    TableRef,
};
use rdlt_engine::RunStatus;

use crate::support::script::{Script, ScriptStream};
use crate::support::{commit_every, engine, memory, pipeline, stream};

/// Every commit a [`Recording`] destination was sent, in order.
type Sent = Arc<Mutex<Vec<CommitMeta>>>;

/// `inner`, noting every commit it is sent.
struct Recording {
    inner: Arc<dyn Destination>,
    sent: Sent,
}

struct RecordingSession {
    inner: Box<dyn DestinationSession>,
    sent: Sent,
}

impl Destination for Recording {
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
                session: Box::new(RecordingSession {
                    inner: opened.session,
                    sent: Arc::clone(&self.sent),
                }),
                ..opened
            })
        })
    }
}

impl DestinationSession for RecordingSession {
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
        self.sent.lock().push(meta.clone());
        self.inner.commit(meta)
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}

fn recording(inner: Arc<dyn Destination>) -> (Arc<dyn Destination>, Sent) {
    let sent = Sent::default();
    let destination = Recording {
        inner,
        sent: Arc::clone(&sent),
    };
    (Arc::new(destination), sent)
}

/// A stream of two partitions of `rows` rows each, as `name`.
async fn events(name: &str, rows: u64) -> Arc<dyn rdlt_connector::Source> {
    let events = ScriptStream::new("events", 2, rows, 7);
    Script::new(vec![events]).connect(name).await.1
}

#[tokio::test(start_paused = true)]
async fn every_commit_expects_the_one_before_it_landed_across_runs() {
    let (destination, sent) = recording(memory("landed_keys").await);
    for rows in [60, 120] {
        let plan = pipeline(
            "landed-keys",
            [stream("events").read(ReadMode::Incremental)],
        );
        let source = events(&format!("landed_keys_{rows}"), rows).await;
        let outcome = engine(commit_every(10))
            .run(plan, source, Arc::clone(&destination))
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{rows}: {:?}",
            outcome.error
        );
    }
    let sent = sent.lock();
    let keys: Vec<CommitKey> = sent.iter().map(|meta| meta.key).collect();
    let epochs: Vec<_> = keys.iter().map(|key| key.epoch).collect();
    assert!(
        epochs.first() < epochs.last() && sent.len() > 4,
        "two runs' commits: {keys:?}"
    );
    let mut after = None;
    for (meta, previous) in sent
        .iter()
        .zip(std::iter::once(None).chain(keys.iter().map(Some)))
    {
        assert_eq!(meta.after, after, "{:?}", meta.key);
        let seq = previous
            .filter(|previous| previous.epoch == meta.key.epoch)
            .map_or(1, |previous| previous.seq + 1);
        assert_eq!(meta.key.seq, seq, "{:?} after {previous:?}", meta.key);
        after = Some(meta.key);
    }
}
