//! Latency: how long a row takes from its push to landing, and to its acknowledgement, in a
//! following pipeline whose source pushes at half the throughput the cell first measures.
//!
//! The source pushes rows that hold the instant of their push, from a stream it can read again
//! and from one it cannot, each told its cursor once its commit lands, for the three commit
//! intervals a run follows it. The destination reads the clock as the commit holding
//! a row returns, the source as a reported cursor covers it. Each case runs in process and with
//! both connectors spawned, as production places them, committing on the streaming default's
//! thresholds at their interval, 10 s, and at 1 s. It prints, for each stream, p50, p99 and the
//! maximum of both latencies, the most a push was acknowledged after it landed and the pushes
//! never acknowledged, each the median and range over the case's runs. An argument
//! `transient-<n>` adds the cases whose destination fails one commit in n `Transient` before it
//! lands, and `refused-<n>` those whose source refuses one report in n.
//!
//! A run follows its source on past its intervals until a push of each stream has landed, as a
//! loaded host may spend them starting the run. As a test, every case commits every 200 ms, and
//! every push must land and be acknowledged.

#![forbid(unsafe_code)]

mod connector;
#[path = "latency/connectors.rs"]
mod connectors;

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group};
use rdlt_connector::serve::Served;
use rdlt_connector::{ConnectContext, Destination, PipelineId, ReadMode, Source, StreamName};
use rdlt_engine::{
    CommitPolicy, Cores, Engine, EngineConfig, PipelinePlan, Report, RunHandle, RunOutcome,
    StopMode, StreamPlan, SystemEnv, Until,
};
use rdlt_host::{ConnectorRef, Local, Provider as _};
use tokio::runtime::Runtime;

use connectors::{LandingConfig, ROWS, STREAMS, StampedConfig, landing_factory, stamped_factory};

/// How long the connectors the bench spawned have to stop once it is done.
const STOPPING: Duration = Duration::from_secs(20);

/// The runs of a case its readings are of: criterion's samples, its warm-up's left out.
const SAMPLES: usize = 10;

/// The commit interval of every case run as a test.
const TESTED: Duration = Duration::from_millis(200);

/// The most times a run a fault ended is run again, one commit interval each.
const RUNS_AGAIN: usize = 10;

/// The most a run follows its source past its intervals for a push of every stream to land.
const LANDING: Duration = Duration::from_secs(30);

/// The wait between looks for a push of every stream landed, once a run's intervals are past.
const LOOKING: Duration = Duration::from_millis(10);

/// Whether criterion times the benchmarks, rather than running each once as a test.
fn timed() -> bool {
    std::env::args().any(|arg| arg == "--bench")
}

/// Where a case's connectors run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Placement {
    InProcess,
    Process,
}

impl Placement {
    fn name(self) -> &'static str {
        match self {
            Self::InProcess => "in-process",
            Self::Process => "process",
        }
    }
}

/// What a case's connectors fail, one call in so many, drawn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    /// The destination answers a commit `Transient` before it lands.
    Transient(NonZeroU64),
    /// The source refuses a report of committed cursors.
    Refused(NonZeroU64),
}

impl Fault {
    /// The faults `args` name, `transient-<n>` and `refused-<n>`, alone or ending a case's id.
    fn named(args: &[String]) -> Vec<Self> {
        let mut faults = Vec::new();
        for arg in args {
            let named = match arg.rsplit('/').next().and_then(|last| last.split_once('-')) {
                Some(("transient", one_in)) => one_in.parse().ok().map(Self::Transient),
                Some(("refused", one_in)) => one_in.parse().ok().map(Self::Refused),
                _ => None,
            };
            if let Some(fault) = named.filter(|fault| !faults.contains(fault)) {
                faults.push(fault);
            }
        }
        faults
    }
}

/// One benchmark: where its connectors run, the interval it commits at, and what they fail.
#[derive(Clone, Copy, Debug)]
struct Case {
    placement: Placement,
    every: Duration,
    fault: Option<Fault>,
}

impl Case {
    /// Each placement committing every 10 s and every second, then again with each fault `args`
    /// name.
    fn all(args: &[String]) -> Vec<Self> {
        let faults = std::iter::once(None).chain(Fault::named(args).into_iter().map(Some));
        let mut cases = Vec::new();
        for fault in faults {
            for placement in [Placement::InProcess, Placement::Process] {
                cases.extend([10, 1].map(|seconds| Self {
                    placement,
                    every: Duration::from_secs(seconds),
                    fault,
                }));
            }
        }
        cases
    }

    /// Its interval, and its fault where it has one: `10s` or `1s/refused-10`.
    fn parameter(self) -> String {
        let every = self.every.as_secs();
        match self.fault {
            None => format!("{every}s"),
            Some(Fault::Transient(one_in)) => format!("{every}s/transient-{one_in}"),
            Some(Fault::Refused(one_in)) => format!("{every}s/refused-{one_in}"),
        }
    }

    /// The interval it commits at: its own when timed, [`TESTED`] as a test.
    fn interval(self) -> Duration {
        if timed() { self.every } else { TESTED }
    }
}

/// What one run read: its report, and each push's instants by stream and stamp.
struct Run {
    report: Report,
    /// How many times the run was run again before what it read landed.
    again: usize,
    pushes: BTreeMap<(String, i64), Instants>,
}

/// The instants a push landed and was acknowledged, in nanoseconds of the host's monotonic clock.
#[derive(Default)]
struct Instants {
    landed: Option<i64>,
    acknowledged: Option<i64>,
}

/// One stream's readings of a run, in milliseconds: p50, p99 and the maximum from push to landed
/// and from push to acknowledged, and the most a push was acknowledged after it landed, none where
/// no push was; and how many pushes landed that were never acknowledged.
struct Readings {
    landed: Option<[f64; 3]>,
    acknowledged: Option<[f64; 3]>,
    after: Option<f64>,
    unacknowledged: usize,
}

impl Run {
    /// The run whose connectors appended their records under `dir`, its `report`, and how many
    /// times it was run `again`.
    fn read(dir: &Path, report: Report, again: usize) -> Self {
        let mut pushes: BTreeMap<(String, i64), Instants> = BTreeMap::new();
        for file in ["landed", "acknowledged"] {
            let text = std::fs::read_to_string(dir.join(file)).unwrap_or_default();
            for line in text.lines() {
                let fields: Vec<&str> = line.split(' ').collect();
                let [stream, stamp, instant] = fields[..] else {
                    panic!("a record's line is a stream, a stamp and an instant: {line}");
                };
                let nanoseconds = |text: &str| -> i64 { text.parse().expect("a record's times") };
                let push = pushes.entry((stream.to_owned(), nanoseconds(stamp)));
                let push = push.or_default();
                let at = match file {
                    "landed" => &mut push.landed,
                    _ => &mut push.acknowledged,
                };
                at.get_or_insert(nanoseconds(instant));
            }
        }
        Self {
            report,
            again,
            pushes,
        }
    }

    /// The pushes a second the run landed of both streams, between the first and the last.
    #[expect(
        clippy::cast_precision_loss,
        reason = "counts and spans far below 2^52"
    )]
    fn rate(&self) -> f64 {
        let landed = self.pushes.iter().filter(|(_, push)| push.landed.is_some());
        let stamps: Vec<i64> = landed.map(|((_, stamp), _)| *stamp).collect();
        let span = stamps.iter().max().zip(stamps.iter().min());
        let span = span.map_or(0, |(last, first)| last - first);
        assert!(
            span > 0,
            "the run measuring a placement lands two pushes or more"
        );
        (stamps.len() - 1) as f64 / (span as f64 / 1e9)
    }

    /// The readings of `stream`.
    #[expect(clippy::cast_precision_loss, reason = "nanoseconds of a run fit 2^52")]
    fn readings(&self, stream: &str) -> Readings {
        let millis = |from: i64, to: i64| (to - from) as f64 / 1e6;
        let (mut landed, mut acknowledged, mut after) = (Vec::new(), Vec::new(), None);
        let mut unacknowledged = 0;
        let range = (stream.to_owned(), i64::MIN)..=(stream.to_owned(), i64::MAX);
        for ((_, stamp), push) in self.pushes.range(range) {
            landed.extend(push.landed.map(|at| millis(*stamp, at)));
            acknowledged.extend(push.acknowledged.map(|at| millis(*stamp, at)));
            match (push.landed, push.acknowledged) {
                (Some(landed), Some(told)) => {
                    after = Some(after.unwrap_or(f64::MIN).max(millis(landed, told)));
                }
                (Some(_), None) => unacknowledged += 1,
                (None, _) => {}
            }
        }
        Readings {
            landed: quantiles(landed),
            acknowledged: quantiles(acknowledged),
            after,
            unacknowledged,
        }
    }
}

/// Awaits `run`, stopping it after a commit once it has followed its source for `least` and a
/// push of every stream has landed under `dir` since it started, or [`LANDING`] after `least`
/// regardless.
async fn stopped(mut run: RunHandle, least: Duration, dir: &Path) -> RunOutcome {
    let control = run.control();
    // A run made again after a fault waits for pushes it lands, not those the run before it did.
    let before = landed(dir).await;
    let landing = async {
        tokio::time::sleep(least).await;
        let limit = Instant::now() + LANDING;
        while Instant::now() < limit {
            let now = landed(dir).await;
            if now.iter().zip(&before).all(|(now, before)| now > before) {
                break;
            }
            tokio::time::sleep(LOOKING).await;
        }
    };
    tokio::select! {
        biased;
        // A run that ends by itself, as a failing one may, is not stopped.
        outcome = &mut run => return outcome,
        () = landing => {}
    }
    control.stop(StopMode::AfterCommit);
    run.await
}

/// How many landed pushes of each stream the destination has recorded under `dir`.
async fn landed(dir: &Path) -> [usize; STREAMS.len()] {
    let path = dir.join("landed");
    let read = tokio::task::spawn_blocking(move || std::fs::read_to_string(path)).await;
    // The destination creates its record as its first commit lands.
    let text = read.ok().and_then(Result::ok).unwrap_or_default();
    STREAMS.map(|stream| {
        let records = text
            .lines()
            .filter(|line| line.split(' ').next() == Some(stream));
        records.count()
    })
}

/// p50, p99 and the maximum of `values`, by nearest rank; none of no values.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "a rank within the values' count"
)]
fn quantiles(mut values: Vec<f64>) -> Option<[f64; 3]> {
    values.sort_by(f64::total_cmp);
    let at = |quantile: f64| {
        let rank = (quantile * values.len() as f64).ceil() as usize;
        values.get(rank.max(1) - 1).copied()
    };
    Some([at(0.5)?, at(0.99)?, *values.last()?])
}

/// The runtime and the cores every case runs within, how connectors are spawned, and each
/// placement's throughput, once measured.
struct Cell {
    runtime: Runtime,
    cores: Cores,
    local: Local,
    binary: PathBuf,
    measured: [Option<f64>; 2],
}

impl Cell {
    fn new() -> Self {
        let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cores.runtime_workers().get())
            .enable_all()
            .build()
            .expect("a runtime starts");
        Self {
            runtime,
            cores,
            local: Local::trusting_binaries().env_passthrough(connector::CORES),
            binary: std::env::current_exe().expect("the bench's binary has a path"),
            measured: [None; 2],
        }
    }

    /// The pushes a second of both streams `placement` lands unpaced, committing every second
    /// when timed, measured the first time it is asked.
    fn measured(&mut self, placement: Placement) -> f64 {
        if let Some(rate) = self.measured[placement as usize] {
            return rate;
        }
        let every = Duration::from_secs(1);
        let unpaced = Case {
            placement,
            every,
            fault: None,
        };
        let rate = self.run(unpaced, None).rate();
        self.measured[placement as usize] = Some(rate);
        rate
    }

    /// Runs `case`, each stream pushing every `pace`, or as fast as the engine takes its pushes.
    fn run(&self, case: Case, pace: Option<Duration>) -> Run {
        let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a directory");
        let every = case.interval();
        let source = StampedConfig {
            pace,
            record: dir.path().join("acknowledged"),
            refuse: match case.fault {
                Some(Fault::Refused(one_in)) => Some(one_in),
                _ => None,
            },
        };
        let destination = LandingConfig {
            record: dir.path().join("landed"),
            transient: match case.fault {
                Some(Fault::Transient(one_in)) => Some(one_in),
                _ => None,
            },
        };
        let connected = self.connectors(case.placement, &source, &destination);
        let (source, destination) = self.runtime.block_on(connected);
        let bytes = CommitPolicy::streaming().bytes().map(NonZeroU64::get);
        let policy = CommitPolicy::new(Some(every), None, bytes).expect("a valid policy");
        let config = EngineConfig::builder().commit(policy).build();
        let env = Arc::new(SystemEnv::try_new(self.cores).expect("the pool starts"));
        let engine = Engine::new(config.expect("a valid configuration"), env);
        let streams = STREAMS.map(|name| {
            let name = StreamName::new(name).expect("the streams' names are valid");
            StreamPlan::new(name).read(ReadMode::Incremental)
        });
        let pipeline = PipelineId::parse("latency").expect("a valid id");
        let plan = PipelinePlan::new(pipeline, streams).expect("a valid plan");
        let run = |least: Duration| {
            let plan = plan.clone().with_until(Until::Forever);
            let run = engine.run(plan, Arc::clone(&source), Arc::clone(&destination));
            self.runtime.block_on(stopped(run, least, dir.path()))
        };
        let outcome = run(every * 3);
        let (mut error, mut again) = (outcome.error, 0);
        // A run a fault ended is run again until what it read lands, as its next run lands it.
        while error.is_some() && again < RUNS_AGAIN {
            error = run(every).error;
            again += 1;
        }
        assert!(error.is_none(), "the run fails: {error:?}");
        let run = Run::read(dir.path(), outcome.report, again);
        for stream in STREAMS.iter().filter(|_| case.fault.is_none()) {
            let readings = run.readings(stream);
            let read = readings.landed.and(readings.acknowledged);
            let every = readings.unacknowledged == 0;
            assert!(
                read.is_some() && every,
                "every push of {stream} lands and is acknowledged"
            );
        }
        run
    }

    /// The source and the destination `source` and `destination` configure, placed as
    /// `placement` says.
    async fn connectors(
        &self,
        placement: Placement,
        source: &StampedConfig,
        destination: &LandingConfig,
    ) -> (Arc<dyn Source>, Arc<dyn Destination>) {
        let source = serde_json::to_value(source).expect("a configuration serializes");
        let destination = serde_json::to_value(destination).expect("a configuration serializes");
        let (sources, destinations) = (stamped_factory(), landing_factory());
        if placement == Placement::InProcess {
            let source = sources.connect(source, ConnectContext::new()).await;
            let destination = destinations
                .connect(destination, ConnectContext::new())
                .await;
            let source = source.expect("the source connects");
            let destination = destination.expect("the destination connects");
            return (Arc::from(source), Arc::from(destination));
        }
        let reference = |id| ConnectorRef::new(id).path(&self.binary);
        let source_reference = reference(sources.spec().id.clone());
        let destination_reference = reference(destinations.spec().id.clone());
        let source = self.local.source(&source_reference, &source).await;
        let destination = self
            .local
            .destination(&destination_reference, &destination)
            .await;
        let source = source.expect("the source is spawned").connector;
        let destination = destination.expect("the destination is spawned").connector;
        (Arc::from(source), Arc::from(destination))
    }
}

fn latency(c: &mut Criterion) {
    let args: Vec<String> = std::env::args().collect();
    let mut cell = Cell::new();
    let mut used = Vec::new();
    let mut group = c.benchmark_group("latency");
    group
        .sample_size(SAMPLES)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(Duration::from_millis(1));
    for case in Case::all(&args) {
        // A run lasts longer than three intervals, so three a sample make each sample one run.
        group.measurement_time(case.every * 3 * u32::try_from(SAMPLES).unwrap_or(1));
        let (mut runs, mut measured) = (Vec::new(), 0.0);
        let id = BenchmarkId::new(case.placement.name(), case.parameter());
        group.bench_function(id, |b| {
            measured = cell.measured(case.placement);
            // Half of what it measured, an even share a stream.
            let pace = Duration::from_secs_f64(4.0 / measured);
            b.iter_custom(|iterations| {
                let started = Instant::now();
                for _ in 0..iterations {
                    runs.push(cell.run(case, Some(pace)));
                }
                started.elapsed()
            });
        });
        // Only a benchmark that ran has runs, so a listing prints nothing.
        if !runs.is_empty() {
            used.push((case, measured, runs));
        }
    }
    group.finish();
    for (case, measured, runs) in &used {
        print(
            *case,
            *measured,
            &runs[runs.len().saturating_sub(SAMPLES)..],
        );
    }
}

/// Prints a case's readings over `runs`, which pushed at half of `measured` pushes a second, and
/// what its last run committed.
#[expect(
    clippy::print_stdout,
    reason = "criterion reports the runs' times; the latencies are printed beside them"
)]
#[expect(
    clippy::cast_precision_loss,
    reason = "a count of pushes far below 2^52"
)]
fn print(case: Case, measured: f64, runs: &[Run]) {
    let Some(last) = runs.last().map(|run| &run.report) else {
        return;
    };
    let each = |phase: Duration| {
        let commits = u32::try_from(last.commits.max(1)).unwrap_or(u32::MAX);
        (phase / commits).as_secs_f64() * 1e3
    };
    let (commits, phases) = (&last.counters.commits, &last.counters.phases);
    let acks = &last.counters.acks;
    println!(
        "latency/{}/{}: {} runs at {:.0} pushes a second of {ROWS} rows, half what it landed \
         unpaced; the last made {} attempts, was run again {} times, and made {} commits, {} on \
         the interval, {} at the end, {} on size or cursors; a commit's flush {:.1} ms, commit \
         {:.1} ms, ack {:.1} ms; {} reports failed and {} owed cursors were given up",
        case.placement.name(),
        case.parameter(),
        runs.len(),
        measured / 2.0,
        last.attempted,
        runs.last().map_or(0, |run| run.again),
        last.commits,
        commits.interval,
        commits.end,
        commits.size + commits.cursors,
        each(phases.flush),
        each(phases.commit),
        each(phases.ack),
        acks.failed,
        acks.dropped,
    );
    for stream in STREAMS {
        let readings: Vec<Readings> = runs.iter().map(|run| run.readings(stream)).collect();
        let of = |reading: fn(&Readings) -> Option<f64>| spread(readings.iter().map(reading));
        println!(
            "  {stream}, ms: push to landed p50 {}, p99 {}, max {}; push to acknowledged p50 {}, \
             p99 {}, max {}; acknowledged after landing at most {}; never acknowledged {}",
            of(|readings| readings.landed.map(|landed| landed[0])),
            of(|readings| readings.landed.map(|landed| landed[1])),
            of(|readings| readings.landed.map(|landed| landed[2])),
            of(|readings| readings.acknowledged.map(|acknowledged| acknowledged[0])),
            of(|readings| readings.acknowledged.map(|acknowledged| acknowledged[1])),
            of(|readings| readings.acknowledged.map(|acknowledged| acknowledged[2])),
            of(|readings| readings.after),
            of(|readings| Some(readings.unacknowledged as f64)),
        );
    }
}

/// The median of `samples` and their range, or `unmeasured` where any is unknown.
fn spread(samples: impl Iterator<Item = Option<f64>>) -> String {
    let Some(mut samples) = samples.collect::<Option<Vec<f64>>>() else {
        return "unmeasured".to_owned();
    };
    samples.sort_by(f64::total_cmp);
    let (low, high) = (samples[0], samples[samples.len() - 1]);
    let median = samples[samples.len() / 2];
    format!("{median:.1} ({low:.1} to {high:.1})")
}

criterion_group!(benches, latency);

fn main() -> ExitCode {
    if connector::asked() {
        return connector::serve(|| {
            Served::new()
                .with_source(stamped_factory())
                .with_destination(landing_factory())
        });
    }
    benches();
    Criterion::default().configure_from_args().final_summary();
    rdlt_host::stop_spawned(STOPPING).expect("every connector the bench spawned stops");
    ExitCode::SUCCESS
}
