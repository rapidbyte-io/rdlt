#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

mod acks;
mod lag;
mod retried;
// Linear time is measured as a thread's CPU time, which only Unix offers.
#[cfg(unix)]
mod scale;
mod seals;
mod state;
mod told;

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Catalog, CommitKey, CommitMeta, ConnectorError, ConnectorErrorKind, Cursor,
    DestinationSession, DestinationWriter, Epoch, GenerationId, PartitionId, PartitionSink,
    PartitionState, PipelineId, ReadRequest, Receipt, Result, SchemaVersion, SegmentId, Source,
    StateChange, StateEntry, StateKey, StreamName, StreamState, TableChange, TablePath, TableRef,
    TableSchema,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Coordinator, CoordinatorParts, Cycle, PartitionRun, StreamRun};
use crate::config::{CommitPolicy, RetryPolicy};
use crate::env::SystemEnv;
use crate::error::{Error, ErrorKind, Origin, codes};
use crate::event::Announcer;
use crate::lane::Lanes;
use crate::naming::Naming;
use crate::partition::{CursorHold, Latest, Progress, Seal};
use crate::plan::StreamPlan;
use crate::plan::WriteMode;
use crate::report::{AttemptEnd, AttemptLog, CommitPhases, Tally};
use crate::status::Live;
use crate::table::{Incoming, MetaNames, Model, Resolver, Settings, SharedSession, Tables};
use crate::watch;

/// How long a commit in flight has to answer once the attempt is cancelled: the run's default
/// close wait.
const CLOSE_WAIT: Duration = Duration::from_secs(60);

type Commits = Arc<Mutex<Vec<CommitMeta>>>;
/// Every try of a commit sent, with the instant it was, in order.
type Sent = Arc<Mutex<Vec<(CommitMeta, tokio::time::Instant)>>>;
/// How the destination answers its next tries of commits, in order; it lands every try past them.
type Tries = Arc<Mutex<VecDeque<Try>>>;
type Acks = Arc<Mutex<Vec<(StreamName, Vec<(PartitionId, Cursor)>)>>>;
/// Every report made, accepted or not, with how many commits had landed when it was, in order.
type Asked = Arc<Mutex<Vec<(Vec<(PartitionId, Cursor)>, usize)>>>;
/// How the source answers its next reports, in order; it accepts every report past them.
type Answers = Arc<Mutex<VecDeque<Answer>>>;

/// How the source answers a report of committed cursors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    Accept,
    /// Refused as a transient failure, as a lost report is.
    Refuse,
    /// Refused as holding a position the source never sent, as a source started again does.
    Unsent,
    /// Never answered.
    Hang,
}

/// How the destination answers a try of a commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Try {
    /// Fails with an error of the kind, coded as given, and lands nothing.
    Fail(ConnectorErrorKind, Option<&'static str>),
    /// Lands the commit, then fails with an error of the kind.
    LandThenFail(ConnectorErrorKind),
    /// Fails rate-limited, asking for the wait given, and lands nothing.
    Limited(Duration),
    /// Lands the commit, then answers once the wait given has passed.
    Late(Duration),
    /// Never answers.
    Hang,
}

impl Try {
    /// The error a try fails with, of `kind` coded `code`.
    fn error(kind: ConnectorErrorKind, code: Option<&'static str>) -> ConnectorError {
        let error = ConnectorError::new(kind, "the commit failed");
        match code {
            Some(code) => error.with_code(code),
            None => error,
        }
    }
}

struct Recorder {
    commits: Commits,
    sent: Sent,
    tries: Tries,
    /// The receipt of the last commit that landed, with which a try of its key is answered.
    landed: Option<Receipt>,
    closed: Arc<AtomicBool>,
    /// Whether each receipt names the commit after the commit it answers.
    skewed: bool,
    /// How long a commit takes.
    pace: Duration,
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
            self.sent
                .lock()
                .push((meta.clone(), tokio::time::Instant::now()));
            tokio::time::sleep(self.pace).await;
            let answer = self.tries.lock().pop_front();
            match answer {
                Some(Try::Fail(kind, code)) => return Err(Try::error(kind, code)),
                Some(Try::Limited(after)) => {
                    return Err(ConnectorError::rate_limited("slow down", Some(after)));
                }
                Some(Try::Hang) => std::future::pending::<()>().await,
                Some(Try::LandThenFail(_) | Try::Late(_)) | None => {}
            }
            if let Some(landed) = self.landed.as_ref().filter(|landed| landed.key == meta.key) {
                return Ok(landed.clone());
            }
            self.commits.lock().push(meta.clone());
            let skew = u64::from(self.skewed);
            let receipt = Receipt {
                key: CommitKey {
                    seq: meta.key.seq + skew,
                    ..meta.key
                },
                committed_at: UNIX_EPOCH,
                rows: meta.segments.len(),
                bytes: 0,
            };
            self.landed = Some(receipt.clone());
            match answer {
                Some(Try::LandThenFail(kind)) => Err(Try::error(kind, None)),
                Some(Try::Late(wait)) => {
                    tokio::time::sleep(wait).await;
                    Ok(receipt)
                }
                _ => Ok(receipt),
            }
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
    answers: Answers,
    asked: Asked,
    /// How long hearing of a commit takes.
    pace: Duration,
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
    ) -> BoxFuture<'a, Result<rdlt_connector::PartitionPlan>> {
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
            let landed = self.commits.lock().len();
            self.asked.lock().push((cursors.to_vec(), landed));
            tokio::time::sleep(self.pace).await;
            assert!(
                !self.commits.lock().is_empty(),
                "acknowledged before any commit landed"
            );
            let answer = self.answers.lock().pop_front().unwrap_or(Answer::Accept);
            match answer {
                Answer::Accept => {}
                Answer::Refuse => return Err(transient("the report was lost")),
                Answer::Unsent => {
                    let error = transient("a position this host was never sent");
                    return Err(error.with_code(rdlt_connector::POSITION_UNSENT));
                }
                Answer::Hang => std::future::pending::<()>().await,
            }
            self.acks.lock().push((stream.clone(), cursors.to_vec()));
            Ok(())
        })
    }
}

fn transient(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Transient, message)
}

/// A coordinator and the handles a test drives it through.
struct Harness {
    progress: mpsc::UnboundedSender<Progress>,
    barrier: watch::Receiver<u64>,
    stop_reads: CancellationToken,
    stop: CancellationToken,
    cancel: CancellationToken,
    commits: Commits,
    /// Every try of a commit sent.
    sent: Sent,
    acks: Acks,
    answers: Answers,
    asked: Asked,
    log: Arc<Mutex<AttemptLog>>,
    closed: Arc<AtomicBool>,
    latest: Arc<Latest>,
    budget: crate::budget::MemoryBudget,
    tally: Arc<Tally>,
    /// The destination session the coordinator commits through.
    session: Arc<SharedSession>,
    /// What the coordinator told of, in order.
    told: Arc<Mutex<Vec<crate::event::Event>>>,
}

struct Setup {
    streams: Vec<StreamRun>,
    partitions: Vec<PartitionRun>,
    policy: CommitPolicy,
    barrier_wait: Duration,
    /// How the destination answers its first tries of commits, in order.
    tries: Vec<Try>,
    /// Whether the destination answers each commit with the receipt of the next.
    skew_receipts: bool,
    /// The key the session found landed, which its first commit expects.
    after: Option<CommitKey>,
    /// A schema the first stream's table is created with before the coordinator starts.
    schema: Option<TableSchema>,
    /// The memory budget's bytes, a quarter of which waiting cursors may hold before a commit
    /// is due.
    budget: u64,
    /// How long the destination takes to commit; the source takes twice as long to hear of it.
    pace: Duration,
    /// The run's retry policy.
    retry: RetryPolicy,
}

impl Setup {
    fn new(mut streams: Vec<StreamRun>, partitions: Vec<PartitionRun>) -> Self {
        // Each stream's table is added in the stream's order.
        for (table, stream) in streams.iter_mut().enumerate() {
            stream.table = table;
        }
        Self {
            streams,
            partitions,
            policy: CommitPolicy::new(None, Some(1_000), None).unwrap(),
            barrier_wait: Duration::from_secs(60),
            tries: Vec::new(),
            skew_receipts: false,
            after: None,
            schema: None,
            budget: u64::MAX,
            pace: Duration::ZERO,
            retry: RetryPolicy::default(),
        }
    }

    /// The tables of the setup's streams, over `session`.
    async fn tables(&self, session: Arc<SharedSession>) -> Arc<Tables> {
        let tables = Tables::new(session);
        let capabilities = rdlt_connector::Capabilities::minimal();
        for stream in &self.streams {
            let resolver = Resolver {
                stream: stream.name.clone(),
                settings: Settings {
                    pipeline: crate::policy::SchemaSettings::default(),
                    stream: StreamPlan::new(stream.name.clone()),
                    key: Vec::new(),
                    owner: None,
                },
                naming: Naming::new(capabilities.identifiers.clone()),
                capabilities: Arc::new(capabilities.clone()),
                meta: MetaNames {
                    load_epoch: "_rdlt_load_epoch".into(),
                    loaded_at: "_rdlt_loaded_at".into(),
                    seq: None,
                    id: None,
                    parent: None,
                    changes: None,
                    history: None,
                },
                root: None,
                columns: u64::MAX,
                unwidened: std::collections::BTreeSet::new(),
            };
            let index = tables
                .add(resolver, &table(None), Model::default())
                .unwrap();
            if let (0, Some(schema)) = (index, &self.schema) {
                tables
                    .fit(index, &Incoming::declared(schema.clone()))
                    .await
                    .unwrap();
            }
        }
        Arc::new(tables)
    }

    async fn start(self) -> (tokio::task::JoinHandle<Result<(), Error>>, Harness) {
        let (coordinator, harness) = self.coordinator().await;
        (tokio::spawn(coordinator.run()), harness)
    }

    /// The coordinator, not yet running, and the handles a test drives it through.
    async fn coordinator(self) -> (Coordinator, Harness) {
        let (commits, acks) = (Commits::default(), Acks::default());
        let (progress, progress_feed) = mpsc::unbounded_channel();
        let (barrier_sender, barrier) = watch::channel(0);
        let (sent, closed) = (Sent::default(), Arc::<AtomicBool>::default());
        let session = SharedSession::new(Box::new(self.recorder(&commits, &sent, &closed)));
        let harness = Harness {
            progress,
            barrier,
            stop_reads: CancellationToken::new(),
            stop: CancellationToken::new(),
            cancel: CancellationToken::new(),
            commits: Arc::clone(&commits),
            sent,
            acks: Arc::clone(&acks),
            answers: Answers::default(),
            asked: Asked::default(),
            log: Arc::default(),
            closed,
            latest: Arc::default(),
            budget: crate::budget::budget(self.budget),
            tally: Arc::default(),
            session: Arc::clone(&session),
            told: Arc::default(),
        };
        let tables = self.tables(session).await;
        let source = Arc::new(self.listener(acks, commits, &harness));
        let (env, live, announcer) = told(&harness);
        let coordinator = Coordinator::new(CoordinatorParts {
            env: Arc::clone(&env),
            policy: self.policy,
            barrier_wait: self.barrier_wait,
            lanes: lanes(&tables),
            tables,
            source,
            epoch: Epoch(3),
            after: self.after,
            streams: self.streams,
            partitions: self.partitions,
            progress: progress_feed,
            latest: Arc::clone(&harness.latest),
            budget: harness.budget.clone(),
            barrier: barrier_sender,
            stop_reads: harness.stop_reads.clone(),
            stop: harness.stop.clone(),
            cancel: harness.cancel.clone(),
            log: Arc::clone(&harness.log),
            launcher: Box::new(|_| Err(Error::internal("the tests start no phases"))),
            done: super::DoneMarkers::default(),
            stored: stored(harness.budget.limits().state_bytes),
            follow: false,
            replan: Duration::from_secs(60),
            tally: Arc::clone(&harness.tally),
            retry: self.retry,
            close_wait: CLOSE_WAIT,
            live,
            announcer,
        });
        (coordinator, harness)
    }
}

/// An environment that keeps what it is told in `harness`, and the live state and announcer of
/// a run's first attempt on it.
fn told(harness: &Harness) -> (Arc<dyn crate::env::Env>, Arc<Live>, Announcer) {
    let told = Arc::clone(&harness.told);
    let observer = move |event: &crate::event::Event| told.lock().push(event.clone());
    let env: Arc<dyn crate::env::Env> =
        Arc::new(SystemEnv::one_core().with_observer(Arc::new(observer)));
    let pipeline = PipelineId::parse("coordinated").unwrap();
    let live = Live::new(
        Arc::clone(&env),
        pipeline.clone(),
        harness.budget.clone(),
        Arc::clone(&harness.tally),
    );
    let announcer = Announcer::new(Arc::clone(&env), pipeline, 1);
    (env, Arc::new(live), announcer)
}

impl Setup {
    /// The destination session recording what it lands into `commits` and every try into
    /// `sent`, answering tries as the setup says.
    fn recorder(&self, commits: &Commits, sent: &Sent, closed: &Arc<AtomicBool>) -> Recorder {
        Recorder {
            commits: Arc::clone(commits),
            sent: Arc::clone(sent),
            tries: Arc::new(Mutex::new(self.tries.iter().copied().collect())),
            landed: None,
            closed: Arc::clone(closed),
            skewed: self.skew_receipts,
            pace: self.pace,
        }
    }

    /// The source hearing of commits, into `acks`, which checks they follow `commits` and
    /// answers as `harness` says.
    fn listener(&self, acks: Acks, commits: Commits, harness: &Harness) -> Listener {
        Listener {
            acks,
            commits,
            answers: Arc::clone(&harness.answers),
            asked: Arc::clone(&harness.asked),
            pace: self.pace * 2,
        }
    }
}

/// One lane over `tables`, running.
fn lanes(tables: &Arc<Tables>) -> Lanes {
    let budget = crate::budget::budget(1 << 30);
    let writers = crate::config::GrowthLimits::default().writers();
    let env: Arc<dyn crate::env::Env> = Arc::new(crate::env::InlineEnv);
    let (lanes, tasks) = Lanes::new(
        (NonZeroUsize::MIN, writers),
        tables,
        NonZeroUsize::MIN,
        &budget,
        (&env, &Arc::default()),
    );
    for lane in tasks {
        tokio::spawn(lane.run(CancellationToken::new()));
    }
    lanes
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
        self.send(Progress::Sealed(seal(
            partition, segment, rows, state, answers,
        )));
    }

    fn end(&self, partition: usize, stopped: bool) {
        self.send(Progress::Ended { partition, stopped });
    }

    fn commit_count(&self) -> usize {
        self.commits.lock().len()
    }
}

/// The seal of `rows` rows of `partition` in `segment`, standing at `state`, which discarded
/// nothing.
fn seal(
    partition: usize,
    segment: u64,
    rows: u64,
    state: PartitionState,
    answers: Option<u64>,
) -> Seal {
    Seal {
        partition,
        segment: SegmentId(segment),
        rows,
        bytes: rows * 8,
        state,
        answers,
        discarded_rows: 0,
        discarded_values: 0,
        discards: crate::report::ColumnDiscards::default(),
        deletes_ignored: 0,
        truncates_ignored: 0,
        shred: crate::report::ShredCounts::default(),
        held: CursorHold::default(),
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
        merge: None,
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

/// No state stored yet, held to `limit` bytes as an open's answer holds it.
fn stored(limit: u64) -> crate::stored::Stored {
    let limits = crate::stored::StateLimits {
        stored: limit,
        request: limit,
    };
    crate::stored::Stored::of(&[], limits)
}

fn stream(write: WriteMode, cycle: Option<Cycle>, partitions: usize) -> StreamRun {
    StreamRun {
        name: name(),
        write,
        table: 0,
        cycle,
        remaining: partitions,
        stopped: false,
        phases: None,
        sequences: None,
        named: std::collections::BTreeSet::new(),
    }
}

fn partition(id: &str, on_demand: bool) -> PartitionRun {
    let stop = CancellationToken::new();
    PartitionRun::new(0, PartitionId::parse(id).unwrap(), on_demand, stop)
}

fn cursor(next: u64) -> Cursor {
    Cursor::encode(1, &next).unwrap()
}

fn position(id: &str, state: PartitionState) -> StateChange {
    let entry = StateEntry::Partition {
        stream: name(),
        partition: PartitionId::parse(id).unwrap(),
        state,
        epoch: Epoch(3),
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

/// Waits for the coordinator's next barrier, failing the test, as `what` says, after a minute of
/// paused time: the barrier raised.
async fn raised(harness: &mut Harness, what: &str) -> u64 {
    tokio::time::timeout(Duration::from_secs(60), harness.barrier.changed())
        .await
        .unwrap_or_else(|_| panic!("{what} within a minute"))
        .expect("the coordinator keeps its barrier's sender");
    harness.barrier.borrow_and_update()
}

/// Waits for the coordinator to end, failing the test after ten minutes of paused time.
async fn ended(task: tokio::task::JoinHandle<Result<(), Error>>) -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(600), task)
        .await
        .expect("the coordinator ends within ten minutes")
        .expect("the coordinator does not panic")
}

#[tokio::test(start_paused = true)]
async fn sealed_segments_commit_with_their_positions_and_the_source_hears_afterwards() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.send(Progress::Started { partition: 0 });
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.end(0, false);
    ended(task).await.unwrap();
    let commits = harness.commits.lock();
    assert_eq!(commits.len(), 1);
    assert_eq!(
        commits[0].segments.iter().collect::<Vec<_>>(),
        [SegmentId(1)]
    );
    assert_eq!(
        commits[0].state_delta.clone(),
        [position("p0", PartitionState::Cursor(cursor(5)))]
    );
    assert_eq!(commits[0].key.epoch, Epoch(3));
    assert!(commits[0].finish_generations.is_empty());
    let acks = harness.acks.lock();
    assert_eq!(
        *acks,
        [(name(), vec![(PartitionId::parse("p0").unwrap(), cursor(5))])]
    );
    let log = harness.log.lock();
    assert_eq!(log.end, Some(AttemptEnd::Exhausted));
    assert_eq!(log.committed.commits, 1);
    assert_eq!(log.committed.streams[&name()].rows, 5);
    assert!(harness.closed.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn what_shredding_a_segment_took_is_reported_with_its_stream_once_committed() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    let shred = crate::report::ShredCounts {
        parsed: 3,
        tripped: 1,
        observed: 1,
        rebuilt: 2,
        exact: 1,
    };
    harness.send(Progress::Sealed(Seal {
        partition: 0,
        segment: SegmentId(1),
        rows: 5,
        bytes: 40,
        state: PartitionState::Cursor(cursor(5)),
        answers: None,
        discarded_rows: 0,
        discarded_values: 0,
        discards: crate::report::ColumnDiscards::default(),
        deletes_ignored: 0,
        truncates_ignored: 0,
        shred,
        held: CursorHold::default(),
    }));
    harness.end(0, false);
    ended(task).await.unwrap();
    assert_eq!(harness.log.lock().committed.streams[&name()].shred, shred);
}

#[tokio::test(start_paused = true)]
async fn a_commit_of_what_was_left_at_the_end_counts_as_such_with_the_time_of_each_phase() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.pace = Duration::from_secs(1);
    let (task, harness) = setup.start().await;
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.end(0, false);
    ended(task).await.unwrap();
    let counters = harness.tally.counters();
    let end = crate::report::Commits {
        end: 1,
        ..crate::report::Commits::default()
    };
    assert_eq!(counters.commits, end);
    // The lanes hold nothing to flush and no log lists anything, at once.
    let phases = CommitPhases {
        commit: Duration::from_secs(1),
        ack: Duration::from_secs(2),
        ..CommitPhases::default()
    };
    assert_eq!(counters.phases, phases);
}

#[tokio::test(start_paused = true)]
async fn commits_follow_the_row_threshold_and_count_their_sequence() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    let (task, harness) = setup.start().await;
    for segment in 1..=3 {
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
    ended(task).await.unwrap();
    let commits = harness.commits.lock();
    let sequence: Vec<u64> = commits.iter().map(|commit| commit.key.seq).collect();
    assert_eq!(sequence[..3], [1, 2, 3]);
    let size = crate::report::Commits {
        size: 3,
        ..crate::report::Commits::default()
    };
    assert_eq!(harness.tally.counters().commits, size);
    assert!(commits.iter().all(|commit| commit.key.epoch == Epoch(3)));
}

#[tokio::test(start_paused = true)]
async fn a_partition_that_seals_on_its_own_commits_once_its_seals_reach_the_threshold() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    let (task, harness) = setup.start().await;
    // Batches of four, each sealed: 8 sealed rows make no commit due.
    for segment in 1..=2 {
        harness.seal(
            0,
            segment,
            4,
            PartitionState::Cursor(cursor(segment * 4)),
            None,
        );
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.commit_count(), 0, "8 sealed rows are not 10");
    harness.seal(0, 3, 4, PartitionState::Cursor(cursor(12)), None);
    until(|| harness.commit_count() == 1).await;
    assert_eq!(
        harness.commits.lock()[0].segments.len(),
        3,
        "the commit took 12 rows"
    );
    harness.end(0, false);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn rows_a_partition_abandoned_make_no_commit_due() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    let (task, mut harness) = setup.start().await;
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Written {
        partition: 0,
        rows: 6,
        bytes: 48,
    });
    harness.send(Progress::Abandoned {
        partition: 0,
        segment: SegmentId(1),
    });
    harness.send(Progress::Written {
        partition: 0,
        rows: 6,
        bytes: 48,
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.barrier.borrow_and_update(), 0, "no commit was due");
    harness.send(Progress::Written {
        partition: 0,
        rows: 4,
        bytes: 32,
    });
    assert_eq!(
        raised(&mut harness, "a barrier is raised").await,
        1,
        "the rows kept make it due"
    );
    harness.end(0, true);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn rows_that_miss_a_commit_stay_due_until_they_seal() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    setup.barrier_wait = Duration::from_secs(1);
    let (task, harness) = setup.start().await;
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Written {
        partition: 0,
        rows: 10,
        bytes: 80,
    });
    // The barrier goes unanswered, so the commit it triggers has nothing to publish.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(harness.commit_count(), 0);
    harness.seal(0, 1, 10, PartitionState::Cursor(cursor(10)), None);
    until(|| harness.commit_count() == 1).await;
    harness.end(0, false);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn rows_of_a_partition_that_seals_on_its_own_make_no_commit_due_until_sealed() {
    // p0 checkpoints only at its end; p1 answers every barrier.
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 2)],
        vec![partition("p0", false), partition("p1", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    setup.barrier_wait = Duration::from_secs(1);
    let (task, mut harness) = setup.start().await;
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Started { partition: 1 });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        harness.barrier.borrow_and_update(),
        0,
        "p0's unsealed rows raise no barrier"
    );
    // p0's rows make no commit due. p1's rows after them each seal at once: they take a
    // commit only once they reach the threshold.
    for segment in 1..=9 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        harness.seal(1, segment, 1, PartitionState::Cursor(cursor(segment)), None);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        harness.commit_count(),
        0,
        "nine rows a commit can take are not ten"
    );
    assert_eq!(harness.barrier.borrow_and_update(), 0, "no barrier");
    harness.seal(1, 10, 1, PartitionState::Cursor(cursor(10)), None);
    until(|| harness.commit_count() == 1).await;
    // p0's rows, once sealed, make the next commit due.
    harness.seal(0, 11, 10, PartitionState::Cursor(cursor(10)), None);
    until(|| harness.commit_count() == 2).await;
    harness.end(0, false);
    harness.end(1, false);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_interval_commits_a_quiet_partition() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(Some(Duration::from_secs(10)), None, None).unwrap();
    let (task, harness) = setup.start().await;
    let started = tokio::time::Instant::now();
    harness.seal(0, 1, 1, PartitionState::Cursor(cursor(1)), None);
    until(|| harness.commit_count() == 1).await;
    assert!(started.elapsed() >= Duration::from_secs(10));
    harness.end(0, false);
    ended(task).await.unwrap();
    let interval = crate::report::Commits {
        interval: 1,
        ..crate::report::Commits::default()
    };
    assert_eq!(harness.tally.counters().commits, interval);
}

#[tokio::test(start_paused = true)]
async fn rows_of_partitions_that_seal_when_asked_keep_commits_at_the_threshold() {
    // Both partitions seal only when a barrier asks; there is no interval.
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 2)],
        vec![partition("p0", true), partition("p1", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    setup.barrier_wait = Duration::from_secs(3600);
    let (task, mut harness) = setup.start().await;
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Started { partition: 1 });
    let written = |harness: &Harness, rows| {
        harness.send(Progress::Written {
            partition: 0,
            rows,
            bytes: rows * 8,
        });
    };
    let mut segment = 0;
    let mut pending = 10;
    written(&harness, 10);
    for barrier in 1..=5 {
        assert_eq!(
            raised(&mut harness, "each ten rows raise a barrier").await,
            barrier
        );
        // p0 answers, then writes a row while the commit still waits for p1.
        segment += 1;
        let state = PartitionState::Cursor(cursor(segment));
        harness.seal(0, segment, pending, state, Some(barrier));
        written(&harness, 1);
        tokio::time::sleep(Duration::from_secs(1)).await;
        segment += 1;
        let state = PartitionState::Cursor(cursor(segment));
        harness.seal(1, segment, 0, state, Some(barrier));
        until(|| harness.commit_count() == usize::try_from(barrier).unwrap()).await;
        // That row and nine more make the next commit due.
        written(&harness, 9);
        pending = 10;
    }
    assert_eq!(
        raised(&mut harness, "the last ten rows raise a barrier").await,
        6
    );
    harness.end(0, false);
    harness.end(1, false);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_partition_still_owing_when_a_cursor_waits_for_room_is_asked_again() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 2)],
        vec![partition("p0", true), partition("p1", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    setup.barrier_wait = Duration::from_secs(60);
    setup.budget = 1 << 20;
    let (task, mut harness) = setup.start().await;
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Started { partition: 1 });
    harness.send(Progress::Written {
        partition: 1,
        rows: 10,
        bytes: 80,
    });
    assert_eq!(raised(&mut harness, "a barrier is raised").await, 1);
    harness.seal(0, 1, 0, PartitionState::Cursor(cursor(1)), Some(1));
    // A cursor finds the cursors' share full while p1 still owes the barrier: the barrier stops
    // waiting long before its wait runs out, and the commit frees the room.
    let started = tokio::time::Instant::now();
    let share = harness.budget.shares().cursors;
    let held = harness.budget.acquire_cursor(share).await.unwrap();
    let budget = harness.budget.clone();
    let waiting = tokio::spawn(async move { budget.acquire_cursor(1).await.map(drop) });
    until(|| !harness.commits.lock().is_empty()).await;
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "the barrier waited its wait out"
    );
    drop(held);
    tokio::time::timeout(Duration::from_secs(60), waiting)
        .await
        .expect("the waiting cursor is admitted within a minute")
        .unwrap()
        .unwrap();
    // p1 may still be answering: its rows count, so p0's next row raises the next barrier.
    harness.send(Progress::Written {
        partition: 0,
        rows: 1,
        bytes: 8,
    });
    let prompt = tokio::time::timeout(Duration::from_secs(1), harness.barrier.changed());
    prompt
        .await
        .expect("p1's rows still make a commit due")
        .unwrap();
    assert_eq!(harness.barrier.borrow_and_update(), 2);
    harness.end(0, false);
    harness.end(1, false);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_seal_heard_in_a_stop_s_barrier_lets_a_waiting_cursor_make_a_commit_due() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    setup.budget = 1 << 20;
    let (task, mut harness) = setup.start().await;
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Written {
        partition: 0,
        rows: 10,
        bytes: 80,
    });
    assert_eq!(raised(&mut harness, "a barrier is raised").await, 1);
    harness.seal(0, 1, 10, PartitionState::Cursor(cursor(10)), Some(1));
    until(|| harness.commit_count() == 1).await;
    // Nothing arrives after that commit but the answer to the stop's barrier.
    harness.stop.cancel();
    assert_eq!(raised(&mut harness, "stopping raises a barrier").await, 2);
    harness.seal(0, 2, 1, PartitionState::Cursor(cursor(11)), Some(2));
    tokio::time::timeout(Duration::from_secs(60), harness.stop_reads.cancelled())
        .await
        .expect("the barrier is answered and the reads stop within a minute");
    let share = harness.budget.shares().cursors;
    let held = harness.budget.acquire_cursor(share).await.unwrap();
    let budget = harness.budget.clone();
    let waiting = tokio::spawn(async move { budget.acquire_cursor(1).await.map(drop) });
    until(|| harness.commit_count() == 2).await;
    assert_eq!(harness.tally.counters().commits.cursors, 1);
    drop(held);
    tokio::time::timeout(Duration::from_secs(60), waiting)
        .await
        .expect("the waiting cursor is admitted within a minute")
        .unwrap()
        .unwrap();
    harness.end(0, true);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_partition_that_did_not_answer_is_not_asked_again_until_it_writes_more() {
    // p1 seals when asked but never answers; p0 answers every barrier.
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 2)],
        vec![partition("p0", true), partition("p1", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(10), None).unwrap();
    setup.barrier_wait = Duration::from_secs(1);
    let (task, mut harness) = setup.start().await;
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Started { partition: 1 });
    let written = |harness: &Harness, partition, rows| {
        harness.send(Progress::Written {
            partition,
            rows,
            bytes: rows * 8,
        });
    };
    written(&harness, 1, 10);
    assert_eq!(raised(&mut harness, "a barrier is raised").await, 1);
    harness.seal(0, 1, 0, PartitionState::Cursor(cursor(1)), Some(1));
    // p1's wait runs out. p0's rows after it raise no barrier of their own, nine short of the
    // threshold: p1's rows, unanswered, no longer count.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let started = tokio::time::Instant::now();
    for _ in 0..9 {
        written(&harness, 0, 1);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        harness.barrier.borrow_and_update(),
        1,
        "no barrier for each event"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "no event waited a barrier out"
    );
    // p1 writing more is asked again.
    written(&harness, 1, 10);
    assert_eq!(
        raised(&mut harness, "p1's new rows raise a barrier").await,
        2
    );
    harness.end(0, false);
    harness.end(1, false);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_commit_waits_for_on_demand_partitions_to_answer_its_barrier() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 2)],
        vec![partition("p0", true), partition("p1", false)],
    );
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    setup.barrier_wait = Duration::from_secs(3600);
    let (task, mut harness) = setup.start().await;
    let started = tokio::time::Instant::now();
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Started { partition: 1 });
    harness.send(Progress::Written {
        partition: 0,
        rows: 1,
        bytes: 8,
    });
    assert_eq!(raised(&mut harness, "a barrier is raised").await, 1);
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
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_unanswered_barrier_gives_up_after_its_wait() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 2)],
        vec![partition("p0", true), partition("p1", true)],
    );
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    setup.barrier_wait = Duration::from_secs(30);
    let (task, harness) = setup.start().await;
    let started = tokio::time::Instant::now();
    harness.send(Progress::Started { partition: 0 });
    harness.send(Progress::Started { partition: 1 });
    harness.seal(1, 1, 1, PartitionState::Cursor(cursor(1)), None);
    harness.send(Progress::Written {
        partition: 0,
        rows: 1,
        bytes: 8,
    });
    until(|| harness.commit_count() == 1).await;
    assert!(started.elapsed() >= Duration::from_secs(30));
    harness.end(0, false);
    harness.end(1, false);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn empty_segments_record_their_position_without_publishing() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.seal(0, 1, 0, PartitionState::Done, None);
    harness.end(0, false);
    ended(task).await.unwrap();
    let commits = harness.commits.lock();
    assert!(commits[0].segments.is_empty());
    assert_eq!(
        commits[0].state_delta.clone(),
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
    .start()
    .await;
    harness.end(0, true);
    ended(task).await.unwrap();
    assert_eq!(harness.commit_count(), 0);
    assert_eq!(harness.log.lock().end, Some(AttemptEnd::Exhausted));
}

#[tokio::test(start_paused = true)]
async fn a_new_table_schema_is_recorded_by_the_first_commit_only() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    setup.schema = Some(schema());
    let (task, harness) = setup.start().await;
    harness.seal(0, 1, 1, PartitionState::Cursor(cursor(1)), None);
    until(|| harness.commit_count() == 1).await;
    harness.seal(0, 2, 1, PartitionState::Cursor(cursor(2)), None);
    harness.end(0, false);
    ended(task).await.unwrap();
    let commits = harness.commits.lock();
    let path = TablePath::new(["orders"]).unwrap();
    let keys = |index: usize| -> Vec<String> {
        commits[index]
            .state_delta
            .iter()
            .filter_map(|change| match change {
                StateChange::Put(record) => Some(record.key.clone()),
                StateChange::Delete(_) => None,
            })
            .collect()
    };
    assert!(keys(0).contains(&StateKey::Schema(path.clone()).encode()));
    assert!(keys(0).contains(&StateKey::Names(path).encode()));
    assert_eq!(
        commits[1].state_delta.clone(),
        [position("p0", PartitionState::Cursor(cursor(2)))]
    );
}

#[tokio::test(start_paused = true)]
async fn discards_are_reported_with_the_commit_that_publishes_their_segment() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.send(Progress::Sealed(Seal {
        partition: 0,
        segment: SegmentId(1),
        rows: 2,
        bytes: 16,
        state: PartitionState::Cursor(cursor(4)),
        answers: None,
        discarded_rows: 2,
        discarded_values: 3,
        discards: crate::report::ColumnDiscards::default(),
        deletes_ignored: 0,
        truncates_ignored: 0,
        shred: crate::report::ShredCounts::default(),
        held: CursorHold::default(),
    }));
    harness.end(0, false);
    ended(task).await.unwrap();
    let log = harness.log.lock();
    let report = &log.committed.streams[&name()];
    assert_eq!(
        (report.rows, report.discarded_rows, report.discarded_values),
        (2, 2, 3)
    );
}

#[tokio::test(start_paused = true)]
async fn a_segment_left_without_rows_publishes_nothing_and_counts_only_what_it_dropped() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.send(Progress::Sealed(Seal {
        partition: 0,
        segment: SegmentId(1),
        rows: 0,
        bytes: 0,
        state: PartitionState::Cursor(cursor(6)),
        answers: None,
        discarded_rows: 2,
        discarded_values: 0,
        discards: crate::report::ColumnDiscards::default(),
        deletes_ignored: 3,
        truncates_ignored: 1,
        shred: crate::report::ShredCounts::default(),
        held: CursorHold::default(),
    }));
    harness.end(0, false);
    ended(task).await.unwrap();
    let commits = harness.commits.lock();
    assert_eq!(commits.len(), 1);
    assert!(commits[0].segments.is_empty());
    assert_eq!(
        commits[0].state_delta,
        [position("p0", PartitionState::Cursor(cursor(6)))]
    );
    let log = harness.log.lock();
    let report = &log.committed.streams[&name()];
    assert_eq!(
        (
            report.commits,
            report.rows,
            report.discarded_rows,
            report.deletes_ignored,
            report.truncates_ignored
        ),
        (0, 0, 2, 3, 1)
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
    // The interval commits what the partition sealed by then, the read's start with it.
    setup.policy = CommitPolicy::new(Some(Duration::from_secs(10)), None, None).unwrap();
    let (task, harness) = setup.start().await;
    harness.seal(0, 1, 1, PartitionState::Cursor(cursor(1)), None);
    until(|| harness.commit_count() == 1).await;
    harness.seal(0, 2, 1, PartitionState::Done, None);
    harness.end(0, false);
    ended(task).await.unwrap();
    let commits = harness.commits.lock();
    let stale = StateKey::Partition(name(), PartitionId::parse("old").unwrap()).encode();
    let generation = StateEntry::Generation {
        stream: name(),
        generation: GenerationId(7),
    };
    assert_eq!(
        commits[0].state_delta.clone(),
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
        commits[1].state_delta.clone(),
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
    assert_eq!(log.committed.streams[&name()].generations_swapped, 1);
}

#[tokio::test(start_paused = true)]
async fn a_full_append_completes_without_swapping_a_generation() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, Some(new_cycle(4, &[])), 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.seal(0, 1, 2, PartitionState::Done, None);
    harness.end(0, false);
    ended(task).await.unwrap();
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
        harness.log.lock().committed.streams[&name()].generations_swapped,
        0
    );
}

#[tokio::test(start_paused = true)]
async fn a_stopped_partition_leaves_its_full_read_unfinished() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Replace, Some(new_cycle(4, &[])), 2)],
        vec![partition("p0", false), partition("p1", false)],
    )
    .start()
    .await;
    harness.seal(0, 1, 1, PartitionState::Done, None);
    harness.end(0, false);
    harness.end(1, true);
    ended(task).await.unwrap();
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
    .start()
    .await;
    harness.send(Progress::Started { partition: 0 });
    harness.stop.cancel();
    assert_eq!(raised(&mut harness, "a barrier is raised").await, 1);
    harness.seal(0, 1, 3, PartitionState::Cursor(cursor(3)), Some(1));
    tokio::time::timeout(Duration::from_secs(60), harness.stop_reads.cancelled())
        .await
        .expect("stopping stops the reads within a minute");
    harness.end(0, true);
    ended(task).await.unwrap();
    assert_eq!(harness.commit_count(), 1);
    assert_eq!(harness.log.lock().end, Some(AttemptEnd::Stopped));
}

#[tokio::test(start_paused = true)]
async fn cancelling_ends_the_coordinator_without_committing() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.seal(0, 1, 3, PartitionState::Cursor(cursor(3)), None);
    harness.cancel.cancel();
    assert_eq!(ended(task).await.unwrap_err().kind(), ErrorKind::Cancelled);
    assert_eq!(harness.commit_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn partitions_that_vanish_without_ending_cancel_the_coordinator() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    drop(harness.progress);
    assert_eq!(ended(task).await.unwrap_err().kind(), ErrorKind::Cancelled);
}

#[tokio::test(start_paused = true)]
async fn a_receipt_for_another_commit_fails_the_commit_and_tells_the_source_nothing() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.skew_receipts = true;
    let (task, harness) = setup.start().await;
    harness.seal(0, 1, 3, PartitionState::Cursor(cursor(3)), None);
    harness.end(0, false);
    let error = ended(task).await.unwrap_err();
    // The engine found the mismatch in what the destination answered.
    assert_eq!(
        (
            error.kind(),
            error.origin(),
            error.code(),
            error.is_retryable()
        ),
        (
            ErrorKind::Destination,
            Origin::Engine,
            Some(codes::RECEIPT_MISMATCH),
            false
        )
    );
    assert!(harness.acks.lock().is_empty());
    assert_eq!(harness.log.lock().committed.commits, 0);
}

#[tokio::test(start_paused = true)]
async fn the_byte_threshold_commits_as_sealed_bytes_reach_it() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(None, None, Some(100)).unwrap();
    let (task, harness) = setup.start().await;
    // The harness's seals weigh 8 bytes a row.
    harness.seal(0, 1, 7, PartitionState::Cursor(cursor(7)), None);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(harness.commit_count(), 0, "56 of 100 bytes");
    harness.seal(0, 2, 6, PartitionState::Cursor(cursor(13)), None);
    until(|| harness.commit_count() == 1).await;
    harness.end(0, false);
    ended(task).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_completed_read_joins_the_most_recent_sixteen() {
    let mut cycle = new_cycle(99, &[]);
    cycle.completed = (0..16).map(GenerationId).collect();
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, Some(cycle), 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.seal(0, 1, 1, PartitionState::Done, None);
    harness.end(0, false);
    ended(task).await.unwrap();
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

#[tokio::test(start_paused = true)]
async fn each_commit_expects_the_one_before_it_landed_the_first_the_one_the_open_found() {
    let found = CommitKey {
        epoch: Epoch(2),
        seq: 7,
    };
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.after = Some(found);
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    let (task, harness) = setup.start().await;
    for segment in 1..=2 {
        let next = segment * 5;
        harness.seal(0, segment, 5, PartitionState::Cursor(cursor(next)), None);
        until(|| harness.commit_count() == usize::try_from(segment).unwrap()).await;
    }
    harness.end(0, false);
    ended(task).await.unwrap();
    let keyed: Vec<(CommitKey, Option<CommitKey>)> = harness
        .commits
        .lock()
        .iter()
        .map(|commit| (commit.key, commit.after))
        .collect();
    let key = |seq| CommitKey {
        epoch: Epoch(3),
        seq,
    };
    assert_eq!(keyed[..2], [(key(1), Some(found)), (key(2), Some(key(1)))]);
    assert!(
        harness.log.lock().pending.is_none(),
        "the landed commit is no longer pending"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_commit_stays_pending_in_the_log() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.tries = vec![Try::Fail(ConnectorErrorKind::Data, None)];
    let (task, harness) = setup.start().await;
    harness.seal(0, 1, 3, PartitionState::Cursor(cursor(3)), None);
    harness.end(0, false);
    ended(task).await.unwrap_err();
    let log = harness.log.lock();
    let pending = log
        .pending
        .as_ref()
        .expect("the commit in flight is pending");
    assert_eq!(
        (pending.key, pending.streams[&name()].rows),
        (
            CommitKey {
                epoch: Epoch(3),
                seq: 1
            },
            3
        )
    );
}

#[tokio::test(start_paused = true)]
async fn a_seal_without_rows_or_discards_reports_no_stream() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.seal(0, 1, 0, PartitionState::Cursor(cursor(3)), None);
    harness.end(0, false);
    ended(task).await.unwrap();
    let log = harness.log.lock();
    assert_eq!(log.committed.commits, 1, "the position still commits");
    assert!(
        log.committed.streams.is_empty(),
        "{:?}",
        log.committed.streams
    );
}

/// What a commit of one seal of `state`, with `rows` rows, of a partition that started at
/// offset 5 tells the source, and whether it is progress; `schema` records a table's schema too.
async fn committed_from_five(
    state: PartitionState,
    rows: u64,
    schema: Option<TableSchema>,
) -> (Vec<Cursor>, bool) {
    let started = cursor(5);
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false).starting(Some(&started))],
    );
    setup.schema = schema;
    let (task, harness) = setup.start().await;
    harness.seal(0, 1, rows, state, None);
    harness.end(0, false);
    ended(task).await.unwrap();
    assert_eq!(harness.commit_count(), 1);
    let told = harness.acks.lock();
    let told = told.iter().flat_map(|(_, cursors)| cursors);
    let told = told.map(|(_, cursor)| cursor.clone()).collect();
    (told, harness.log.lock().progressed)
}

#[tokio::test(start_paused = true)]
async fn a_commit_is_progress_unless_it_only_records_partitions_where_they_stood() {
    let at = |next| PartitionState::Cursor(cursor(next));
    // Sealed where it started, with no row: recorded again, told again, and no progress.
    assert_eq!(
        committed_from_five(at(5), 0, None).await,
        (vec![cursor(5)], false)
    );
    // A row, a position that moved, a partition done, or a table's schema: each is progress.
    // A partition done with no checkpoint in the commit is told where it stood.
    assert_eq!(
        committed_from_five(at(5), 1, None).await,
        (vec![cursor(5)], true)
    );
    assert_eq!(
        committed_from_five(at(6), 0, None).await,
        (vec![cursor(6)], true)
    );
    assert_eq!(
        committed_from_five(PartitionState::Done, 0, None).await,
        (vec![cursor(5)], true)
    );
    assert_eq!(
        committed_from_five(at(5), 0, Some(schema())).await,
        (vec![cursor(5)], true)
    );
}

#[tokio::test(start_paused = true)]
async fn a_partition_done_after_its_last_checkpoint_is_still_told_that_checkpoint() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    // Both seals fall in one commit: state records the partition done, and the source hears
    // of the last position it sent.
    harness.seal(0, 1, 0, PartitionState::Cursor(cursor(3)), None);
    harness.seal(0, 2, 0, PartitionState::Done, None);
    harness.end(0, false);
    ended(task).await.unwrap();
    let commits = harness.commits.lock();
    assert_eq!(
        commits[0].state_delta.clone(),
        [position("p0", PartitionState::Done)]
    );
    assert_eq!(
        *harness.acks.lock(),
        [(name(), vec![(PartitionId::parse("p0").unwrap(), cursor(3))])]
    );
}
