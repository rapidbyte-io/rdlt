//! Phases: a stream read in phases is planned again once each phase has ended and its end is
//! committed, from what it committed, until a plan names no new phase.

use std::sync::Arc;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Catalog, ConnectorError, Cursor, PartitionId, PartitionPlan, PartitionSink,
    PartitionState, ReadMode, ReadRequest, Result, Source, StreamName, StreamState,
};
use rdlt_connector_reference::changes::{CHANGES, ChangedStream, SNAPSHOT, expected};
use rdlt_engine::{RunControl, RunOutcome, RunStatus, StopMode, WriteMode};

use crate::changes::{changes_of, orders};
use crate::support::{commit_every, engine, memory, pipeline, stream};

/// How many times a stream may be planned before planning fails, so a stream planned without
/// end fails its run rather than hanging it.
const PLANS: usize = 6;

/// The phase a stream was planned in, and each committed partition: whether it is done.
type Planned = (u16, Vec<(String, bool)>);

/// A source that records the state each stream is planned with, and stops the run it is given
/// when a [`Stop`] says.
///
/// It names the phase of every plan, as a source may, and refuses a stream's plans past
/// [`PLANS`].
struct Recorded {
    inner: Arc<dyn Source>,
    plans: Mutex<Vec<(String, Planned)>>,
    stop: Mutex<Option<(Stop, RunControl)>>,
    /// The plan of a stream that fails, if one does.
    fails: Option<usize>,
}

/// When [`Recorded`] stops its run.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// At the first acknowledgment.
    Acknowledged,
    /// As a stream is planned for the `n`th time.
    Planned(usize),
}

impl Recorded {
    fn new(inner: Arc<dyn Source>) -> Arc<Self> {
        Self::failing(inner, None)
    }

    /// A source whose streams' `fails`th plan fails, if any does.
    fn failing(inner: Arc<dyn Source>, fails: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            plans: Mutex::new(Vec::new()),
            stop: Mutex::new(None),
            fails,
        })
    }

    /// Stops the run, if `now` is when it should stop.
    fn stop_at(&self, now: Stop) {
        let mut stop = self.stop.lock();
        if stop.as_ref().is_some_and(|(when, _)| *when == now)
            && let Some((_, control)) = stop.take()
        {
            control.stop(StopMode::AfterCommit);
        }
    }

    /// Runs the pipeline of `orders`, merged, from `source` into `store`.
    async fn run(self: &Arc<Self>, store: &str, stop: Option<Stop>) -> RunOutcome {
        self.run_as(WriteMode::Merge, store, stop).await
    }

    /// Runs the pipeline of `orders`, written as `write` says, from `source` into `store`.
    async fn run_as(
        self: &Arc<Self>,
        write: WriteMode,
        store: &str,
        stop: Option<Stop>,
    ) -> RunOutcome {
        let plan = pipeline(
            "phases",
            [stream("orders").read(ReadMode::Cdc).write(write)],
        );
        let engine = engine(commit_every(10_000));
        let run = engine.run(
            plan,
            Arc::clone(self) as Arc<dyn Source>,
            memory(store).await,
        );
        if let Some(stop) = stop {
            *self.stop.lock() = Some((stop, run.control()));
        }
        run.await
    }

    /// The states `stream` was planned with, in order.
    fn plans(&self, stream: &str) -> Vec<Planned> {
        self.plans
            .lock()
            .iter()
            .filter(|(name, _)| name == stream)
            .map(|(_, planned)| planned.clone())
            .collect()
    }
}

impl Source for Recorded {
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
        Box::pin(async move {
            let partitions = state
                .partitions
                .iter()
                .map(|(id, state)| (id.as_str().to_owned(), *state == PartitionState::Done))
                .collect();
            let name = stream.to_string();
            let planned = {
                let mut plans = self.plans.lock();
                plans.push((name.clone(), (state.phase, partitions)));
                plans.iter().filter(|(planned, _)| *planned == name).count()
            };
            self.stop_at(Stop::Planned(planned));
            if planned > PLANS || self.fails == Some(planned) {
                return Err(ConnectorError::internal(format!(
                    "{stream} planned {planned} times"
                )));
            }
            let mut plan = self.inner.plan(stream, state).await?;
            plan.phase = plan.phase.or(Some(state.phase));
            Ok(plan)
        })
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        self.inner.read(request, sink)
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        self.stop_at(Stop::Acknowledged);
        self.inner.committed(stream, cursors)
    }
}

fn small(name: &str) -> ChangedStream {
    ChangedStream {
        name: name.into(),
        keys: 6,
        snapshot_partitions: 2,
        changes: 5,
        batch_rows: 3,
        truncates: Vec::new(),
        captured: 0,
    }
}

fn snapshot_read(partitions: usize) -> Planned {
    let read = (0..partitions)
        .map(|index| (format!("snapshot-{index}"), false))
        .collect();
    (SNAPSHOT, read)
}

fn changes_read() -> Planned {
    (CHANGES, vec![("changes".to_owned(), false)])
}

#[tokio::test]
async fn each_phase_is_planned_from_what_the_last_committed_and_a_settled_stream_rests() {
    // `orders` reads its snapshot over many commits, and changes long after `small` settles;
    // `small` is planned no more meanwhile.
    let mut large = orders(&[]);
    large.keys = 400;
    large.batch_rows = 2;
    let streams = [large, small("small")];
    let store = "phases_planned";
    let plan = || {
        let merged = |name: &str| stream(name).read(ReadMode::Cdc).write(WriteMode::Merge);
        pipeline("phases", [merged("orders"), merged("small")])
    };
    let source = Recorded::new(changes_of(7, &streams).await);
    let outcome = engine(commit_every(8))
        .run(
            plan(),
            Arc::clone(&source) as Arc<dyn Source>,
            memory(store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    for (name, partitions) in [("orders", 3), ("small", 2)] {
        let expected = vec![
            (SNAPSHOT, Vec::new()),
            snapshot_read(partitions),
            changes_read(),
        ];
        assert_eq!(source.plans(name), expected, "{name}");
    }
    // The next run starts in the phase the first recorded, without the snapshot's entries, and
    // reads nothing: the changes resume where they ended.
    let again = Recorded::new(changes_of(7, &streams).await);
    let outcome = engine(commit_every(8))
        .run(
            plan(),
            Arc::clone(&again) as Arc<dyn Source>,
            memory(store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 0);
    assert_eq!(again.plans("orders"), vec![changes_read(), changes_read()]);
}

#[tokio::test]
async fn a_stopped_run_starts_no_further_phase() {
    // The run stops at its first commit, long before the snapshot ends.
    let mut large = orders(&[]);
    large.keys = 400;
    large.batch_rows = 2;
    let source = Recorded::new(changes_of(8, &[large]).await);
    let plan = pipeline(
        "phases",
        [stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge)],
    );
    let engine = engine(commit_every(4));
    let run = engine.run(
        plan,
        Arc::clone(&source) as Arc<dyn Source>,
        memory("phases_stopped").await,
    );
    *source.stop.lock() = Some((Stop::Acknowledged, run.control()));
    let outcome = run.await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Stopped,
        "{:?}",
        outcome.error
    );
    assert_eq!(source.plans("orders"), vec![(SNAPSHOT, Vec::new())]);
}

#[tokio::test]
async fn a_run_stopped_as_its_phase_ends_is_stopped_rather_than_done() {
    // The snapshot is read before the first commit, whose acknowledgment stops the run before
    // the changes are planned.
    let stream_spec = orders(&[]);
    let source = Recorded::new(changes_of(10, std::slice::from_ref(&stream_spec)).await);
    let outcome = source
        .run("phases_stopped_late", Some(Stop::Acknowledged))
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Stopped,
        "{:?}",
        outcome.error
    );
    let again = Recorded::new(changes_of(10, std::slice::from_ref(&stream_spec)).await);
    let outcome = again.run("phases_stopped_late", None).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(
        crate::changes::rows("phases_stopped_late", "orders"),
        expected(10, &stream_spec)
    );
}

#[tokio::test]
async fn a_phase_begun_as_its_run_stops_starts_where_its_plan_said() {
    // The run stops as the changes are planned, before they are read, yet state records where
    // they start: after the changes the snapshot holds, which a log would otherwise repeat.
    let mut stream_spec = orders(&[]);
    stream_spec.captured = 20;
    let streams = std::slice::from_ref(&stream_spec);
    let source = Recorded::new(changes_of(11, streams).await);
    let outcome = source
        .run_as(WriteMode::Append, "phases_begun", Some(Stop::Planned(2)))
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Stopped,
        "{:?}",
        outcome.error
    );
    let again = Recorded::new(changes_of(11, streams).await);
    let outcome = again.run_as(WriteMode::Append, "phases_begun", None).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(again.plans("orders")[0], changes_read());
    assert_eq!(
        crate::changes::logged("phases_begun", "orders"),
        crate::changes::log(11, &stream_spec)
    );
}

#[tokio::test]
async fn a_phase_a_new_run_begins_starts_where_its_plan_said() {
    // The first run fails as it plans the changes, so the next begins them as it starts.
    let mut stream_spec = orders(&[]);
    stream_spec.captured = 20;
    let streams = std::slice::from_ref(&stream_spec);
    let source = Recorded::failing(changes_of(12, streams).await, Some(2));
    let outcome = source
        .run_as(WriteMode::Append, "phases_new_run", None)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let again = Recorded::new(changes_of(12, streams).await);
    let outcome = again
        .run_as(WriteMode::Append, "phases_new_run", None)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(
        crate::changes::logged("phases_new_run", "orders"),
        crate::changes::log(12, &stream_spec)
    );
}
