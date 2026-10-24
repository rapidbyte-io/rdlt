//! Many tables: streams followed at a low rate, and a snapshot of many small tables.
//!
//! A followed stream is one unbounded partition pushing a batch of a thousand rows and a
//! checkpoint a pace apart on the bench's clock, committed on a short interval, for a fixed span;
//! the snapshot runs at the engine's defaults. Each writes through as many writers as lanes, and
//! every run lands every row it pushes.

#[cfg(test)]
mod tests;

use std::fmt;
use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rdlt_connector::{Destination, PipelineId, ReadMode, Source};
use rdlt_wire::limits::MAX_CALLS;
use serde_json::Value;

use super::matrix::nonzero;
use super::replay::{connect_replay, following};
use super::runner::{Runner, streams};
use super::{Passthrough, Replayed, Sinking, register, sink, split_replay_config};
use crate::budget::Shares;
use crate::env::{Env, InlineEnv};
use crate::{CommitPolicy, ComputePoolError, Cores, EngineConfig, PipelinePlan, Report, Until};

/// Calls a run holds open on a served source's connection beside its reads: the heartbeat, and
/// one control call at a time, as a commit's acknowledgement.
const BESIDE_READS: usize = 2;

/// One many-table cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TablesCell {
    /// This many streams, each followed as one unbounded partition.
    Followed(NonZeroUsize),
    /// [`TablesCell::SNAPSHOT_STREAMS`] streams of one bounded partition each, read once.
    Snapshot,
}

impl TablesCell {
    /// Every cell: 16, 64, 192 and 256 followed streams, then the snapshot.
    pub const ALL: [Self; 5] = [
        Self::Followed(nonzero(16)),
        Self::Followed(nonzero(64)),
        Self::Followed(nonzero(192)),
        Self::Followed(nonzero(256)),
        Self::Snapshot,
    ];
    /// Streams the snapshot reads, a table each.
    pub const SNAPSHOT_STREAMS: NonZeroUsize = nonzero(256);
    /// Pushes each table of the snapshot holds.
    pub const SNAPSHOT_PUSHES: usize = 4;
    /// Pushes each followed stream makes before it falls quiet.
    pub const FOLLOWED_PUSHES: usize = 5;
    /// Rows a push holds.
    pub const ROWS: u32 = 1_000;

    /// The cells a source served over its one connection can run: each followed stream's read
    /// holds a call of the connection open for the run, beside the run's other calls, and a
    /// connection carries at most [`MAX_CALLS`].
    pub fn served() -> impl Iterator<Item = Self> {
        let calls = usize::try_from(MAX_CALLS).unwrap_or(usize::MAX);
        Self::ALL.into_iter().filter(move |cell| match cell {
            Self::Followed(streams) => streams.get().saturating_add(BESIDE_READS) <= calls,
            Self::Snapshot => true,
        })
    }

    /// The streams the cell reads.
    pub fn streams(self) -> NonZeroUsize {
        match self {
            Self::Followed(streams) => streams,
            Self::Snapshot => Self::SNAPSHOT_STREAMS,
        }
    }

    /// The cell's name in a benchmark's: `followed/<streams>` or `snapshot/<streams>`.
    pub fn name(self) -> String {
        match self {
            Self::Followed(streams) => format!("followed/{streams}"),
            Self::Snapshot => format!("snapshot/{}", Self::SNAPSHOT_STREAMS),
        }
    }

    /// The name of the replay its source pushes.
    pub fn replay(self) -> String {
        self.name().replace('/', "_")
    }

    /// The cell whose replay is `name`.
    pub fn named(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|cell| cell.replay() == name)
    }

    /// The cell's place in [`TablesCell::ALL`].
    ///
    /// # Panics
    ///
    /// Panics where the cell is a followed one of a count of streams [`TablesCell::ALL`] does not
    /// hold.
    pub fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|cell| *cell == self)
            .expect("a cell of the bench")
    }

    /// The pushes a run makes, every stream's together.
    pub fn pushes(self) -> usize {
        let each = match self {
            Self::Followed(_) => Self::FOLLOWED_PUSHES,
            Self::Snapshot => Self::SNAPSHOT_PUSHES,
        };
        self.streams().get() * each
    }

    /// The rows a run lands.
    pub fn rows(self) -> u64 {
        u64::try_from(self.pushes()).expect("pushes fit in 64 bits") * u64::from(Self::ROWS)
    }

    /// The pushes its source replays, an even share a stream, made alike in whichever process
    /// asks for them.
    pub fn replayed(self) -> Replayed {
        let pushes = u32::try_from(self.pushes()).expect("pushes fit in 32 bits");
        Replayed::Batches(Passthrough::run_of(pushes, Self::ROWS))
    }
}

/// When a followed cell's streams push, its commits are due, and its streams are quiet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Schedule {
    /// The wait between a stream's pushes.
    pub pace: Duration,
    /// The commit interval.
    pub interval: Duration,
    /// How long after its start a run's streams are quiet: every push made and committed.
    pub quiet: Duration,
    /// How long a run follows its streams.
    pub span: Duration,
}

impl Schedule {
    /// A push a second a stream, a commit every two seconds, quiet from seven seconds of ten.
    pub const TIMED: Self = Self {
        pace: Duration::from_secs(1),
        interval: Duration::from_secs(2),
        quiet: Duration::from_secs(7),
        span: Duration::from_secs(10),
    };
    /// As a test runs a cell: the same pushes closer together, a run of two seconds.
    pub const TESTED: Self = Self {
        pace: Duration::from_millis(20),
        interval: Duration::from_millis(40),
        quiet: Duration::from_secs(1),
        span: Duration::from_secs(2),
    };
}

/// A run's report, and a meter read at the start and the end of its window: a followed run's
/// quiet seconds, the whole of a snapshot.
#[derive(Debug)]
pub struct Metered<T> {
    /// The run's report.
    pub report: Report,
    /// The meter at the window's start and at its end.
    pub readings: [T; 2],
    /// The window's length.
    pub window: Duration,
}

/// A many-table cell's engine and the pushes it loads.
#[derive(Debug)]
pub struct Tables {
    runner: Runner,
    cell: TablesCell,
    schedule: Schedule,
    config: EngineConfig,
    writers: NonZeroU16,
    replayed: OnceLock<Replayed>,
}

impl Tables {
    /// `cell` on a runtime of `cores`' workers and an engine within `cores`: a followed cell
    /// reading a partition more than its streams, within the default budget where it admits them
    /// and within the least that does otherwise, committing on `schedule`'s interval and following
    /// for its span; the snapshot at the engine's defaults.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start.
    pub fn try_new(
        cores: Cores,
        cell: TablesCell,
        schedule: Schedule,
    ) -> Result<Self, ComputePoolError> {
        let pipeline = PipelineId::parse("tables").expect("a valid id");
        let (config, plan) = match cell {
            TablesCell::Followed(count) => {
                let partitions = count.saturating_add(1);
                let memory = EngineConfig::least_memory(partitions.get())
                    .expect("a few hundred partitions have a least budget")
                    .max(EngineConfig::default().memory().get());
                let bytes = CommitPolicy::streaming().bytes().map(NonZeroU64::get);
                let policy = CommitPolicy::new(Some(schedule.interval), None, bytes);
                let config = EngineConfig::builder()
                    .memory(memory)
                    .partitions(partitions.get())
                    .commit(policy.expect("a valid policy"))
                    .build();
                let plans = streams(count)
                    .into_iter()
                    .map(|stream| stream.read(ReadMode::Incremental));
                let plan = PipelinePlan::new(pipeline, plans).expect("a valid plan");
                let plan = plan.with_until(Until::For(schedule.span));
                (config.expect("a valid configuration"), plan)
            }
            TablesCell::Snapshot => {
                let plans = streams(TablesCell::SNAPSHOT_STREAMS);
                let plan = PipelinePlan::new(pipeline, plans).expect("a valid plan");
                (EngineConfig::default(), plan)
            }
        };
        let writers = u16::try_from(cores.count().get())
            .ok()
            .and_then(NonZeroU16::new)
            .unwrap_or(NonZeroU16::MAX);
        Ok(Self {
            runner: Runner::configured(cores, config.clone(), plan)?,
            cell,
            schedule,
            config,
            writers,
            replayed: OnceLock::new(),
        })
    }

    /// The cell.
    pub fn cell(&self) -> TablesCell {
        self.cell
    }

    /// Bytes: the budget the cell runs within.
    pub fn memory(&self) -> u64 {
        self.config.memory().get()
    }

    /// Bytes: the least budget that admits the partitions the cell reads at once.
    ///
    /// # Panics
    ///
    /// Panics where no budget does, which takes 2^43 partitions.
    pub fn least_memory(&self) -> u64 {
        EngineConfig::least_memory(self.partitions().get())
            .expect("the cell's partitions have a least budget")
    }

    /// Bytes: the share of the cell's budget for what reads keep.
    pub fn kept_share(&self) -> u64 {
        Shares::of(self.memory()).reads
    }

    /// Partitions the cell reads at once.
    pub fn partitions(&self) -> NonZeroUsize {
        self.config.partitions()
    }

    /// Writers the cell's sink declares: one a core, so the engine has as many lanes.
    pub fn writers(&self) -> NonZeroU16 {
        self.writers
    }

    /// The configuration of the cell's source.
    pub fn replay_config(&self) -> Value {
        let config =
            split_replay_config(&self.cell.replay(), NonZeroUsize::MIN, self.cell.streams());
        match self.cell {
            TablesCell::Followed(_) => following(config, self.schedule.pace),
            TablesCell::Snapshot => config,
        }
    }

    /// The configuration of the cell's sink.
    pub fn sink_config(&self) -> Value {
        Sinking::Ipc.writing(self.writers)
    }

    /// The cell's source and sink in this process, its pushes registered.
    pub async fn connectors(&self) -> (Arc<dyn Source>, Arc<dyn Destination>) {
        let replayed = self.replayed.get_or_init(|| self.cell.replayed());
        register(&self.cell.replay(), replayed.clone());
        let source = connect_replay(self.replay_config()).await;
        (source, sink(Sinking::Ipc, self.writers).await)
    }

    /// Runs `future` on the cell's runtime, where connectors served from this process run.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runner.block_on(future)
    }

    /// Runs the engine from `source` into `destination`, reading `meter` at the start and the
    /// end of the run's window: a followed run's quiet seconds, the whole of a snapshot.
    ///
    /// # Panics
    ///
    /// Panics where the run fails, or lands other rows than the cell pushes.
    pub fn run<T>(
        &self,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
        meter: impl Fn() -> T,
    ) -> Metered<T> {
        let clock = InlineEnv;
        let (report, (readings, window)) = match self.cell {
            TablesCell::Followed(_) => {
                let quiet = self.schedule.quiet;
                let metered = async {
                    clock.sleep(quiet).await;
                    let (first, began) = (meter(), clock.instant());
                    clock.sleep(self.schedule.span.saturating_sub(quiet)).await;
                    let (last, ended) = (meter(), clock.instant());
                    ([first, last], ended.saturating_duration_since(began))
                };
                self.runner.load_beside(source, destination, metered)
            }
            TablesCell::Snapshot => {
                let (first, began) = (meter(), clock.instant());
                let report = self.runner.load(source, destination);
                let (last, ended) = (meter(), clock.instant());
                (
                    report,
                    ([first, last], ended.saturating_duration_since(began)),
                )
            }
        };
        assert_eq!(
            report.rows,
            self.cell.rows(),
            "tables/{}: a run lands every row its streams push",
            self.cell.name()
        );
        Metered {
            report,
            readings,
            window,
        }
    }
}

impl fmt::Display for Tables {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (streams, rows) = (self.cell.streams(), TablesCell::ROWS);
        match self.cell {
            TablesCell::Followed(_) => {
                let Schedule {
                    pace,
                    interval,
                    quiet,
                    span,
                } = self.schedule;
                write!(
                    f,
                    "{streams} streams of an unbounded partition each, {} pushes of {rows} rows a \
                     stream {pace:?} apart, committed every {interval:?}, followed for {span:?}, \
                     quiet from {quiet:?}",
                    TablesCell::FOLLOWED_PUSHES,
                )?;
            }
            TablesCell::Snapshot => write!(
                f,
                "{streams} tables of a bounded partition and {} pushes of {rows} rows each",
                TablesCell::SNAPSHOT_PUSHES,
            )?,
        }
        write!(
            f,
            "; {} partitions read at once within {:.1} MiB, the least that admits them {:.1} MiB; \
             {} writers",
            self.partitions(),
            mebibytes(self.memory()),
            mebibytes(self.least_memory()),
            self.writers,
        )
    }
}

/// `bytes` in mebibytes.
#[expect(
    clippy::cast_precision_loss,
    reason = "a budget's bytes stay far below 2^52"
)]
fn mebibytes(bytes: u64) -> f64 {
    bytes as f64 / f64::from(1 << 20)
}

/// Each commit phase's time a commit over `reports`, in milliseconds: the median of the runs and
/// their range, for the flush, the horizon, the log, the commit and the acknowledgement.
pub fn phases(reports: &[&Report]) -> String {
    let phase = |name: &str, of: fn(&crate::CommitPhases) -> Duration| {
        let each = reports.iter().map(|report| {
            let commits = u32::try_from(report.commits)
                .ok()
                .filter(|commits| *commits > 0);
            commits.map(|commits| (of(&report.counters.phases) / commits).as_secs_f64() * 1e3)
        });
        format!("{name} {}", super::spread(each))
    };
    [
        phase("flush", |phases| phases.flush),
        phase("horizon", |phases| phases.horizon),
        phase("log", |phases| phases.log),
        phase("commit", |phases| phases.commit),
        phase("acknowledgement", |phases| phases.ack),
    ]
    .join(", ")
}
