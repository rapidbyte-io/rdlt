//! Change streams in runs that follow their source: the snapshot, then the changes, each change
//! merged once, and a new phase begun only once the last one's end is committed.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Catalog, PartitionId, PartitionPlan, PartitionSink, ReadMode, ReadRequest, Source,
    SourceEvent, StreamName, StreamState,
};
use rdlt_connector_reference::changes::expected;
use rdlt_engine::{CommitPolicy, EngineConfig, RunStatus, Until, WriteMode};

use crate::changes::{changes, orders, rows};
use crate::support::{engine, memory, pipeline, stream};

/// A configuration that commits every second and plans again every 200 ms.
fn following() -> rdlt_engine::EngineConfigBuilder {
    let every =
        CommitPolicy::new(Some(Duration::from_secs(1)), None, None).expect("an interval is valid");
    EngineConfig::builder()
        .lanes(2)
        .barrier_wait(Duration::from_millis(100))
        .replan(Duration::from_millis(200))
        .commit(every)
}

fn merged() -> rdlt_engine::StreamPlan {
    stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge)
}

#[tokio::test(start_paused = true)]
async fn a_following_run_merges_every_change_once() {
    let source = changes(8, &orders(&[90])).await;
    let plan = pipeline("followed", [merged()]).with_until(Until::For(Duration::from_secs(5)));
    let outcome = engine(following())
        .run(plan, source, memory("followed_changes").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(
        rows("followed_changes", "orders"),
        expected(8, &orders(&[90]))
    );
}

/// The change source, whose last snapshot partition starts late, and whose finished snapshot
/// partitions, read again, checkpoint where they ended a second later, as a source that
/// heartbeats does; it records the state each plan is given.
struct Late {
    inner: Arc<dyn Source>,
    started: AtomicBool,
    planned: Mutex<Vec<StreamState>>,
}

impl Source for Late {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.inner.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.inner.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        self.planned.lock().push(state.clone());
        self.inner.plan(stream, state)
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, rdlt_connector::Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.inner.committed(stream, cursors)
    }

    fn read(
        &self,
        request: ReadRequest,
        mut sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let id = request.partition.id().as_str().to_owned();
            if id.starts_with("snapshot-")
                && let Some(cursor) = request.cursor.clone()
            {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let event = SourceEvent::Checkpoint {
                    cursor,
                    answers: None,
                };
                return sink.send(event).await;
            }
            if id == "snapshot-2" && !self.started.swap(true, Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1500)).await;
            }
            self.inner.read(request, sink).await
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_following_run_begins_the_changes_only_once_the_snapshot_s_end_is_committed() {
    let late = Arc::new(Late {
        inner: changes(8, &orders(&[90])).await,
        started: AtomicBool::new(false),
        planned: Mutex::default(),
    });
    for run in 0..2 {
        let plan = pipeline("late", [merged()]).with_until(Until::For(Duration::from_secs(6)));
        let source = Arc::clone(&late) as Arc<dyn Source>;
        let outcome = engine(following())
            .run(plan, source, memory("late_changes").await)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "run {run}: {:?}",
            outcome.error
        );
    }
    // The changes phase's state names its own partitions alone: no snapshot position was
    // recorded after it began.
    let planned = late.planned.lock();
    let changing: Vec<&StreamState> = planned.iter().filter(|state| state.phase == 1).collect();
    assert!(!changing.is_empty(), "the second run planned the changes");
    for state in changing {
        let stale = state
            .partitions
            .keys()
            .any(|id| id.as_str().starts_with("snapshot-"));
        assert!(!stale, "{state:?}");
    }
    assert_eq!(rows("late_changes", "orders"), expected(8, &orders(&[90])));
}
