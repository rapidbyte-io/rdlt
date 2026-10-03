//! The horizon each commit declares: never past a commit a replay may still repeat, and moving
//! on as the log lets its commits go, so a destination keeps no receipt for ever.

use std::sync::Arc;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, Destination, DestinationSession, DestinationWriter,
    Horizon, OpenContext, OpenedSession, ReadMode, Receipt, Result, TableChange, TableRef,
};
use rdlt_engine::{LocalWal, RunStatus, WalStore};

use crate::support::destinations::{Step, failing};
use crate::support::script::{Script, ScriptStream};
use crate::support::{commit_every, engine, logging_engine, memory, pipeline, retrying, stream};

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

/// Where commit `meta` itself stands, as a horizon.
fn itself(meta: &CommitMeta) -> Horizon {
    Horizon {
        load_id: meta.load_id,
        commit_seq: meta.commit_seq,
    }
}

/// A stream of two partitions of 60 rows each, which `readable` says whether its source reads
/// again, as `name`.
async fn events(name: &str, readable: bool) -> Arc<dyn rdlt_connector::Source> {
    let mut events = ScriptStream::new("events", 2, 60, 7);
    events.replayable = readable;
    Script::new(vec![events]).connect(name).await.1
}

#[tokio::test(start_paused = true)]
async fn a_load_without_a_log_declares_each_commit_its_own_horizon() {
    let (destination, sent) = recording(memory("horizon_unlogged").await);
    let plan = pipeline(
        "horizon-unlogged",
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = engine(commit_every(10))
        .run(plan, events("horizon_unlogged", true).await, destination)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let sent = sent.lock();
    assert!(sent.len() > 2, "{} commits", sent.len());
    for meta in sent.iter() {
        assert_eq!(meta.horizon, Some(itself(meta)));
    }
}

#[tokio::test(start_paused = true)]
async fn a_logged_load_s_horizon_never_passes_a_commit_its_log_holds_and_moves_on() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let (destination, sent) = recording(memory("horizon_logged").await);
    let plan = pipeline(
        "horizon-logged",
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = logging_engine(commit_every(10), store)
        .run(plan, events("horizon_logged", false).await, destination)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let sent = sent.lock();
    assert!(sent.len() > 2, "{} commits", sent.len());
    let mut last = None;
    for meta in sent.iter() {
        let horizon = meta.horizon.expect("every commit declares one");
        assert_eq!(horizon.load_id, meta.load_id);
        assert!(horizon <= itself(meta), "{horizon:?} past {meta:?}");
        assert!(last <= Some(horizon), "the horizon went back");
        last = Some(horizon);
    }
    // The log lets received commits go, and the horizon follows.
    let first = itself(&sent[0]);
    assert!(last > Some(first), "{last:?}");
}

#[tokio::test(start_paused = true)]
async fn a_replayed_commit_declares_no_horizon() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let lost = failing(memory("horizon_replayed").await, Step::LoseResponse);
    let (destination, sent) = recording(lost);
    let plan = pipeline(
        "horizon-replayed",
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = logging_engine(retrying(3), store)
        .run(plan, events("horizon_replayed", false).await, destination)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let sent = sent.lock();
    // The commit whose answer was lost is sent again by the replay, which repeats it as the
    // log holds it and lets no receipt go.
    let lost = &sent[0];
    let again: Vec<&CommitMeta> = sent[1..]
        .iter()
        .filter(|meta| itself(meta) == itself(lost))
        .collect();
    assert_eq!(again.len(), 1, "{sent:?}");
    assert_eq!(lost.horizon, Some(itself(lost)));
    assert_eq!(again[0].horizon, None);
}
