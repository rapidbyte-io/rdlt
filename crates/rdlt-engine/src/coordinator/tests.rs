#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Catalog, CommitMeta, ConnectorError, Cursor, DestinationSession, DestinationWriter,
    Epoch, GenerationId, LoadId, Partition, PartitionId, PartitionSink, PartitionState,
    ReadRequest, Receipt, Result, SchemaVersion, SegmentId, Source, StateChange, StateEntry,
    StateKey, StreamName, StreamState, TableChange, TablePath, TableRef, TableSchema,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::{Coordinator, CoordinatorParts, Cycle, PartitionRun, StreamRun};
use crate::compute::RayonPool;
use crate::config::CommitPolicy;
use crate::env::SystemEnv;
use crate::error::{Error, ErrorKind};
use crate::lane::Lanes;
use crate::partition::{Progress, Seal};
use crate::plan::WriteMode;
use crate::report::{AttemptEnd, AttemptLog};

type Commits = Arc<Mutex<Vec<CommitMeta>>>;
type Acks = Arc<Mutex<Vec<(StreamName, Vec<(PartitionId, Cursor)>)>>>;

struct Recorder {
    commits: Commits,
    closed: Arc<AtomicBool>,
    fail: bool,
}

impl DestinationSession for Recorder {
    fn apply_schema<'a>(&'a mut self, _change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn writer<'a>(
        &'a mut self,
        _table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async {
            Err(ConnectorError::internal(
                "the coordinator creates no writers",
            ))
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            if self.fail {
                return Err(ConnectorError::data("commit refused"));
            }
            self.commits.lock().push(meta.clone());
            Ok(Receipt {
                load_id: meta.load_id,
                commit_seq: meta.commit_seq,
                committed_at: UNIX_EPOCH,
                rows: meta.segments.len(),
                bytes: 0,
            })
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.closed.store(true, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

struct Listener {
    acks: Acks,
    commits: Commits,
}

impl Source for Listener {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        Box::pin(async { Err(ConnectorError::internal("unused")) })
    }

    fn plan<'a>(
        &'a self,
        _stream: &'a StreamName,
        _state: &'a StreamState,
    ) -> BoxFuture<'a, Result<Vec<Partition>>> {
        Box::pin(async { Err(ConnectorError::internal("unused")) })
    }

    fn read(&self, _request: ReadRequest, _sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(ConnectorError::internal("unused")) })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            assert!(
                !self.commits.lock().is_empty(),
                "acknowledged before any commit landed"
            );
            self.acks.lock().push((stream.clone(), cursors.to_vec()));
            Ok(())
        })
    }
}

/// A coordinator and the handles a test drives it through.
struct Harness {
    progress: mpsc::UnboundedSender<Progress>,
    barrier: watch::Receiver<u64>,
    stop_reads: CancellationToken,
    stop: CancellationToken,
    cancel: CancellationToken,
    commits: Commits,
    acks: Acks,
    log: Arc<Mutex<AttemptLog>>,
    closed: Arc<AtomicBool>,
}

struct Setup {
    streams: Vec<StreamRun>,
    partitions: Vec<PartitionRun>,
    policy: CommitPolicy,
    barrier_wait: Duration,
    fail_commit: bool,
}

impl Setup {
    fn new(streams: Vec<StreamRun>, partitions: Vec<PartitionRun>) -> Self {
        Self {
            streams,
            partitions,
            policy: CommitPolicy::new(None, Some(1_000), None).unwrap(),
            barrier_wait: Duration::from_secs(60),
            fail_commit: false,
        }
    }

    fn start(self) -> (tokio::task::JoinHandle<Result<(), Error>>, Harness) {
        let commits = Commits::default();
        let acks = Acks::default();
        let closed = Arc::new(AtomicBool::new(false));
        let (progress, progress_feed) = mpsc::unbounded_channel();
        let (barrier_sender, barrier) = watch::channel(0);
        let (lanes, lane_tasks) = Lanes::new(vec![Vec::new()], NonZeroUsize::MIN);
        for lane in lane_tasks {
            tokio::spawn(lane.run(CancellationToken::new()));
        }
        let harness = Harness {
            progress,
            barrier,
            stop_reads: CancellationToken::new(),
            stop: CancellationToken::new(),
            cancel: CancellationToken::new(),
            commits: Arc::clone(&commits),
            acks: Arc::clone(&acks),
            log: Arc::default(),
            closed: Arc::clone(&closed),
        };
        let pool = RayonPool::new(NonZeroUsize::MIN).unwrap();
        let coordinator = Coordinator::new(CoordinatorParts {
            env: Arc::new(SystemEnv::new(pool)),
            policy: self.policy,
            barrier_wait: self.barrier_wait,
            session: Box::new(Recorder {
                commits: Arc::clone(&commits),
                closed,
                fail: self.fail_commit,
            }),
            source: Arc::new(Listener { acks, commits }),
            lanes,
            load_id: LoadId::from_parts(UNIX_EPOCH, 1),
            epoch: Epoch(3),
            streams: self.streams,
            partitions: self.partitions,
            progress: progress_feed,
            barrier: barrier_sender,
            stop_reads: harness.stop_reads.clone(),
            stop: harness.stop.clone(),
            cancel: harness.cancel.clone(),
            log: Arc::clone(&harness.log),
        });
        (tokio::spawn(coordinator.run()), harness)
    }
}

impl Harness {
    fn send(&self, progress: Progress) {
        self.progress.send(progress).unwrap();
    }

    fn seal(
        &self,
        partition: usize,
        segment: u64,
        rows: u64,
        state: PartitionState,
        answers: Option<u64>,
    ) {
        self.send(Progress::Sealed(Seal {
            partition,
            segment: SegmentId(segment),
            rows,
            bytes: rows * 8,
            state,
            answers,
        }));
    }

    fn end(&self, partition: usize, stopped: bool) {
        self.send(Progress::Ended { partition, stopped });
    }

    fn commit_count(&self) -> usize {
        self.commits.lock().len()
    }
}

fn name() -> StreamName {
    StreamName::new("orders").unwrap()
}

fn table(generation: Option<GenerationId>) -> TableRef {
    TableRef {
        path: TablePath::new(["orders"]).unwrap(),
        name: "orders".into(),
        version: SchemaVersion(1),
        generation,
    }
}

fn schema() -> TableSchema {
    TableSchema::new(vec![rdlt_connector::Field::new(
        "id",
        rdlt_connector::LogicalType::Int64,
        false,
    )])
    .unwrap()
}

fn stream(write: WriteMode, cycle: Option<Cycle>, partitions: usize) -> StreamRun {
    let generation = match write {
        WriteMode::Replace => cycle.as_ref().map(|cycle| cycle.generation),
        WriteMode::Append => None,
    };
    StreamRun {
        name: name(),
        write,
        table: table(generation),
        schema: schema(),
        record_schema: false,
        cycle,
        remaining: partitions,
        stopped: false,
    }
}

fn partition(id: &str, on_demand: bool) -> PartitionRun {
    PartitionRun::new(0, PartitionId::parse(id).unwrap(), on_demand)
}

fn cursor(next: u64) -> Cursor {
    Cursor::encode(1, &next).unwrap()
}

fn position(id: &str, state: PartitionState) -> StateChange {
    let entry = StateEntry::Partition {
        stream: name(),
        partition: PartitionId::parse(id).unwrap(),
        state,
    };
    StateChange::Put(entry.to_record())
}

/// Waits until `condition` holds, failing the test after ten minutes of paused time.
async fn until(condition: impl Fn() -> bool) {
    let waiting = async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(600), waiting)
        .await
        .expect("the condition holds within ten minutes");
}

#[tokio::test(start_paused = true)]
async fn sealed_segments_commit_with_their_positions_and_the_source_hears_afterwards() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start();
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Written { rows: 5, bytes: 40 });
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.end(0, false);
    task.await.unwrap().unwrap();
    let commits = harness.commits.lock();
    assert_eq!(commits.len(), 1);
    assert_eq!(
        commits[0].segments.iter().collect::<Vec<_>>(),
        [SegmentId(1)]
    );
    assert_eq!(
        commits[0].state_delta,
        [position("p0", PartitionState::Cursor(cursor(5)))]
    );
    assert_eq!(commits[0].epoch, Epoch(3));
    assert!(commits[0].finish_generations.is_empty());
    let acks = harness.acks.lock();
    assert_eq!(
        *acks,
        [(name(), vec![(PartitionId::parse("p0").unwrap(), cursor(5))])]
    );
    let log = harness.log.lock();
    assert_eq!(log.end, Some(AttemptEnd::Exhausted));
    assert_eq!(log.commits.len(), 1);
    assert_eq!(log.commits[0].streams[&name()].rows, 5);
    assert!(harness.closed.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn commits_follow_the_row_threshold_and_count_their_sequence() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    let (task, harness) = setup.start();
    for segment in 1..=3 {
        harness.send(Progress::Written {
            rows: 10,
            bytes: 80,
        });
        harness.seal(
            0,
            segment,
            10,
            PartitionState::Cursor(cursor(segment * 10)),
            None,
        );
        let expected = usize::try_from(segment).unwrap();
        until(|| harness.commit_count() == expected).await;
    }
    harness.end(0, false);
    task.await.unwrap().unwrap();
    let commits = harness.commits.lock();
    let sequence: Vec<u64> = commits
        .iter()
        .map(|commit| commit.commit_seq.get())
        .collect();
    assert_eq!(sequence[..3], [1, 2, 3]);
    assert!(
        commits
            .iter()
            .all(|commit| commit.load_id == LoadId::from_parts(UNIX_EPOCH, 1))
    );
}

#[tokio::test(start_paused = true)]
async fn rows_that_miss_a_commit_stay_due_until_they_seal() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    setup.barrier_wait = Duration::from_secs(1);
    let (task, harness) = setup.start();
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Written {
        rows: 10,
        bytes: 80,
    });
    // The barrier goes unanswered, so the commit it triggers has nothing to publish.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(harness.commit_count(), 0);
    harness.seal(0, 1, 10, PartitionState::Cursor(cursor(10)), None);
    until(|| harness.commit_count() == 1).await;
    harness.end(0, false);
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_interval_commits_a_quiet_partition() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(Some(Duration::from_secs(10)), None, None).unwrap();
    let (task, harness) = setup.start();
    let started = tokio::time::Instant::now();
    harness.seal(0, 1, 1, PartitionState::Cursor(cursor(1)), None);
    until(|| harness.commit_count() == 1).await;
    assert!(started.elapsed() >= Duration::from_secs(10));
    harness.end(0, false);
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_commit_waits_for_on_demand_partitions_to_answer_its_barrier() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 2)],
        vec![partition("p0", true), partition("p1", false)],
    );
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    setup.barrier_wait = Duration::from_secs(3600);
    let (task, mut harness) = setup.start();
    let started = tokio::time::Instant::now();
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Started { partition: 1 });
    harness.send(Progress::Written { rows: 1, bytes: 8 });
    harness.barrier.changed().await.unwrap();
    assert_eq!(*harness.barrier.borrow_and_update(), 1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        harness.commit_count(),
        0,
        "the on-demand partition has not answered"
    );
    harness.seal(0, 1, 1, PartitionState::Cursor(cursor(1)), Some(1));
    until(|| harness.commit_count() == 1).await;
    assert!(
        started.elapsed() < Duration::from_secs(3600),
        "natural partitions are not waited for"
    );
    harness.end(0, false);
    harness.end(1, false);
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_unanswered_barrier_gives_up_after_its_wait() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 2)],
        vec![partition("p0", true), partition("p1", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    setup.barrier_wait = Duration::from_secs(30);
    let (task, harness) = setup.start();
    let started = tokio::time::Instant::now();
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Started { partition: 1 });
    harness.seal(1, 1, 1, PartitionState::Cursor(cursor(1)), None);
    harness.send(Progress::Written { rows: 1, bytes: 8 });
    until(|| harness.commit_count() == 1).await;
    assert!(started.elapsed() >= Duration::from_secs(30));
    harness.end(0, false);
    harness.end(1, false);
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn empty_segments_record_their_position_without_publishing() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start();
    harness.seal(0, 1, 0, PartitionState::Done, None);
    harness.end(0, false);
    task.await.unwrap().unwrap();
    let commits = harness.commits.lock();
    assert!(commits[0].segments.is_empty());
    assert_eq!(
        commits[0].state_delta,
        [position("p0", PartitionState::Done)]
    );
    assert!(
        harness.acks.lock().is_empty(),
        "done partitions have no cursor to acknowledge"
    );
}

#[tokio::test(start_paused = true)]
async fn nothing_to_publish_or_record_commits_nothing() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start();
    harness.end(0, true);
    task.await.unwrap().unwrap();
    assert_eq!(harness.commit_count(), 0);
    assert_eq!(harness.log.lock().end, Some(AttemptEnd::Exhausted));
}

#[tokio::test(start_paused = true)]
async fn a_new_table_schema_is_recorded_by_the_first_commit_only() {
    let mut orders = stream(WriteMode::Append, None, 1);
    orders.record_schema = true;
    let mut setup = Setup::new(vec![orders], vec![partition("p0", false)]);
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    let (task, harness) = setup.start();
    harness.seal(0, 1, 1, PartitionState::Cursor(cursor(1)), None);
    harness.send(Progress::Written { rows: 1, bytes: 8 });
    until(|| harness.commit_count() == 1).await;
    harness.seal(0, 2, 1, PartitionState::Cursor(cursor(2)), None);
    harness.end(0, false);
    task.await.unwrap().unwrap();
    let commits = harness.commits.lock();
    let schema_entry = StateEntry::Schema {
        table: TablePath::new(["orders"]).unwrap(),
        version: SchemaVersion(1),
        schema: schema(),
    };
    assert!(
        commits[0]
            .state_delta
            .contains(&StateChange::Put(schema_entry.to_record()))
    );
    assert_eq!(
        commits[1].state_delta,
        [position("p0", PartitionState::Cursor(cursor(2)))]
    );
}

fn new_cycle(generation: u64, stale: &[&str]) -> Cycle {
    Cycle {
        generation: GenerationId(generation),
        recorded: false,
        stale: stale
            .iter()
            .map(|id| PartitionId::parse(*id).unwrap())
            .collect(),
        finished: false,
        completed: Vec::new(),
    }
}

#[tokio::test(start_paused = true)]
async fn a_full_read_is_recorded_when_it_starts_and_completed_when_every_partition_ends() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Replace, Some(new_cycle(7, &["old"])), 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    let (task, harness) = setup.start();
    harness.seal(0, 1, 1, PartitionState::Cursor(cursor(1)), None);
    harness.send(Progress::Written { rows: 1, bytes: 8 });
    until(|| harness.commit_count() == 1).await;
    harness.seal(0, 2, 1, PartitionState::Done, None);
    harness.end(0, false);
    task.await.unwrap().unwrap();
    let commits = harness.commits.lock();
    let stale = StateKey::Partition(name(), PartitionId::parse("old").unwrap()).encode();
    let generation = StateEntry::Generation {
        stream: name(),
        generation: GenerationId(7),
    };
    assert_eq!(
        commits[0].state_delta,
        [
            StateChange::Delete(stale),
            StateChange::Put(generation.to_record()),
            position("p0", PartitionState::Cursor(cursor(1))),
        ]
    );
    assert!(commits[0].finish_generations.is_empty());
    let completed = StateEntry::Completed {
        stream: name(),
        generations: vec![GenerationId(7)],
    };
    assert_eq!(
        commits[1].state_delta,
        [
            position("p0", PartitionState::Done),
            StateChange::Delete(StateKey::Generation(name()).encode()),
            StateChange::Put(completed.to_record()),
        ]
    );
    assert_eq!(
        commits[1].finish_generations,
        [(TablePath::new(["orders"]).unwrap(), GenerationId(7))]
    );
    let log = harness.log.lock();
    assert_eq!(log.commits[1].streams[&name()].generations_swapped, 1);
}

#[tokio::test(start_paused = true)]
async fn a_full_append_completes_without_swapping_a_generation() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, Some(new_cycle(4, &[])), 1)],
        vec![partition("p0", false)],
    )
    .start();
    harness.seal(0, 1, 2, PartitionState::Done, None);
    harness.end(0, false);
    task.await.unwrap().unwrap();
    let commits = harness.commits.lock();
    assert!(commits[0].finish_generations.is_empty());
    let completed = StateEntry::Completed {
        stream: name(),
        generations: vec![GenerationId(4)],
    };
    assert!(
        commits[0]
            .state_delta
            .contains(&StateChange::Put(completed.to_record()))
    );
    assert_eq!(
        harness.log.lock().commits[0].streams[&name()].generations_swapped,
        0
    );
}

#[tokio::test(start_paused = true)]
async fn a_stopped_partition_leaves_its_full_read_unfinished() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Replace, Some(new_cycle(4, &[])), 2)],
        vec![partition("p0", false), partition("p1", false)],
    )
    .start();
    harness.seal(0, 1, 1, PartitionState::Done, None);
    harness.end(0, false);
    harness.end(1, true);
    task.await.unwrap().unwrap();
    let commits = harness.commits.lock();
    assert!(commits[0].finish_generations.is_empty());
    let completed = StateEntry::Completed {
        stream: name(),
        generations: vec![GenerationId(4)],
    };
    assert!(
        !commits[0]
            .state_delta
            .contains(&StateChange::Put(completed.to_record()))
    );
}

#[tokio::test(start_paused = true)]
async fn stopping_raises_a_barrier_stops_reads_and_commits_what_is_sealed() {
    let (task, mut harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", true)],
    )
    .start();
    harness.send(Progress::Started { partition: 0 });
    harness.stop.cancel();
    harness.barrier.changed().await.unwrap();
    harness.seal(0, 1, 3, PartitionState::Cursor(cursor(3)), Some(1));
    harness.stop_reads.cancelled().await;
    harness.end(0, true);
    task.await.unwrap().unwrap();
    assert_eq!(harness.commit_count(), 1);
    assert_eq!(harness.log.lock().end, Some(AttemptEnd::Stopped));
}

#[tokio::test(start_paused = true)]
async fn cancelling_ends_the_coordinator_without_committing() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start();
    harness.seal(0, 1, 3, PartitionState::Cursor(cursor(3)), None);
    harness.cancel.cancel();
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(harness.commit_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn partitions_that_vanish_without_ending_cancel_the_coordinator() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start();
    drop(harness.progress);
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_commit_ends_the_coordinator_and_acknowledges_nothing() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.fail_commit = true;
    let (task, harness) = setup.start();
    harness.seal(0, 1, 3, PartitionState::Cursor(cursor(3)), None);
    harness.end(0, false);
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        ErrorKind::Destination
    );
    assert!(harness.acks.lock().is_empty());
    assert!(harness.log.lock().commits.is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_byte_threshold_commits_as_bytes_arrive() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(None, None, Some(100)).unwrap();
    let (task, harness) = setup.start();
    harness.seal(0, 1, 1, PartitionState::Cursor(cursor(1)), None);
    harness.send(Progress::Written { rows: 1, bytes: 60 });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.commit_count(), 0, "60 of 100 bytes");
    harness.send(Progress::Written { rows: 1, bytes: 40 });
    until(|| harness.commit_count() == 1).await;
    harness.end(0, false);
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_completed_read_joins_the_most_recent_sixteen() {
    let mut cycle = new_cycle(99, &[]);
    cycle.completed = (0..16).map(GenerationId).collect();
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, Some(cycle), 1)],
        vec![partition("p0", false)],
    )
    .start();
    harness.seal(0, 1, 1, PartitionState::Done, None);
    harness.end(0, false);
    task.await.unwrap().unwrap();
    let kept: Vec<GenerationId> = (1..16).chain([99]).map(GenerationId).collect();
    let completed = StateEntry::Completed {
        stream: name(),
        generations: kept,
    };
    assert!(
        harness.commits.lock()[0]
            .state_delta
            .contains(&StateChange::Put(completed.to_record()))
    );
}
