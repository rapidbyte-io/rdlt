//! The horizon each commit declares: itself, since nothing commits it again once it lands, so a
//! destination keeps no receipt of an earlier commit.

use std::sync::Arc;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, Destination, DestinationSession, DestinationWriter,
    Horizon, OpenContext, OpenedSession, ReadMode, Receipt, Result, TableChange, TableRef,
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
async fn every_commit_declares_itself_its_horizon_whatever_its_source() {
    for readable in [true, false] {
        let name = format!("horizon_{readable}");
        let (destination, sent) = recording(memory(&name).await);
        let plan = pipeline(
            &name.replace('_', "-"),
            [stream("events").read(ReadMode::Incremental)],
        );
        let outcome = engine(commit_every(10))
            .run(plan, events(&name, readable).await, destination)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{name}: {:?}",
            outcome.error
        );
        let sent = sent.lock();
        assert!(sent.len() > 2, "{name}: {} commits", sent.len());
        for meta in sent.iter() {
            assert_eq!(meta.horizon, Some(itself(meta)), "{name}");
        }
    }
}
