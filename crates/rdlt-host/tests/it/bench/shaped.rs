//! Workloads other than the passthrough's, each served with both connectors in processes of their
//! own over the socket, as production runs them, beside the bench that runs it in process: the
//! wide table of five thousand columns pushed as Arrow, and the merge of hot keys.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput};
use rdlt_connector::{Destination, Source};
use rdlt_engine::bench::{Form, Merged, Readings, Replayed, Sinking, Wide, replay_config};
use rdlt_engine::{Cores, Report};
use rdlt_host::Options;

use crate::processes::{Placed, Processes};
use crate::{Pids, Seconds, Transport, Usage, spread};

/// A workload served with both connectors in processes of their own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shaped {
    /// The wide bench's `arrow/5000`, into a sink that discards each batch.
    Wide,
    /// The normalized bench's `merge/zipf`, into the IPC sink.
    Merge,
}

impl Shaped {
    /// Every workload.
    const ALL: [Self; 2] = [Self::Wide, Self::Merge];
    /// Columns of the wide table.
    const COLUMNS: u16 = 5_000;

    /// The replay its source pushes.
    pub(crate) fn replay(self) -> &'static str {
        match self {
            Self::Wide => "wide_arrow_5000",
            Self::Merge => "merge_zipf",
        }
    }

    /// The workload whose replay is `name`.
    pub(crate) fn named(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|shaped| shaped.replay() == name)
    }

    /// The workload's place in [`Shaped::ALL`].
    pub(crate) fn index(self) -> usize {
        self as usize
    }

    /// The pushes its source replays, made alike in whichever process asks for them.
    pub(crate) fn replayed(self) -> Replayed {
        match self {
            Self::Wide => Wide::replayed(Form::Arrow, Self::COLUMNS, Wide::PUSHES),
            Self::Merge => Replayed::Json(Merged::pushes(Merged::ROWS)),
        }
    }

    /// Its name, as its bench in process names it.
    fn shape(self) -> String {
        match self {
            Self::Wide => format!("wide/arrow/{}", Self::COLUMNS),
            Self::Merge => "normalized/merge/zipf".to_owned(),
        }
    }

    /// What its sink does with each batch: what its sink in process does.
    fn sinking(self) -> Sinking {
        match self {
            Self::Wide => Sinking::Discard,
            Self::Merge => Sinking::Ipc,
        }
    }

    /// The rows a run pushes.
    fn rows(self) -> u64 {
        match self {
            Self::Wide => Wide::rows_of(Form::Arrow, Self::COLUMNS, Wide::PUSHES),
            Self::Merge => Merged::ROWS,
        }
    }

    /// The pushes a run makes.
    fn pushes(self) -> u64 {
        match self {
            Self::Wide => u64::from(Wide::PUSHES),
            Self::Merge => Merged::ROWS.div_ceil(Merged::PUSH_ROWS.get()),
        }
    }
}

/// The engine of a workload and what it loads.
enum Loaded {
    Wide(Wide),
    Merge(Merged),
}

impl Loaded {
    /// The engine of `shaped` within `cores`.
    fn new(shaped: Shaped, cores: Cores) -> Self {
        let started = "the pool starts";
        match shaped {
            Shaped::Wide => Self::Wide(
                Wide::try_new(cores, Form::Arrow, Shaped::COLUMNS, Wide::PUSHES).expect(started),
            ),
            Shaped::Merge => Self::Merge(Merged::try_new(cores, Merged::ROWS).expect(started)),
        }
    }

    /// The bytes a run pushes.
    fn bytes(&self) -> u64 {
        match self {
            Self::Wide(wide) => wide.bytes(),
            Self::Merge(merged) => merged.bytes(),
        }
    }

    /// Runs `future` on the engine's runtime.
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        match self {
            Self::Wide(wide) => wide.block_on(future),
            Self::Merge(merged) => merged.block_on(future),
        }
    }

    /// Runs the engine from `source` into `destination`; its report.
    fn load(&self, source: Arc<dyn Source>, destination: Arc<dyn Destination>) -> Report {
        match self {
            Self::Wide(wide) => wide.load(source, destination),
            Self::Merge(merged) => merged.load(source, destination),
        }
    }
}

/// Runs `shaped` `runs` times on `loaded`, each with connectors of its own spawned by
/// `processes`: how long the runs took, the CPU time they took a gigabyte, and the last report.
fn timed(
    shaped: Shaped,
    (loaded, processes): (&Loaded, &mut Processes),
    runs: u64,
) -> (Duration, Seconds, Option<Report>) {
    let options = Options::default();
    let config = replay_config(shaped.replay());
    let (mut took, mut usage, mut last) = (Duration::ZERO, Usage::default(), None);
    for _ in 0..runs {
        let (source, destination, pids) = loaded.block_on(async {
            let source = processes.source(Transport::Socket, &config, &options).await;
            let sinking = shaped.sinking();
            let destination = processes
                .destination(Transport::Socket, sinking, &options)
                .await;
            let Placed { connector, pid } = source;
            let pids = Pids {
                source: Some(pid),
                destination: Some(destination.pid),
            };
            (connector, destination.connector, pids)
        });
        // Each connector is held until its CPU time is read: dropped, it stops.
        let held = (Arc::clone(&source), Arc::clone(&destination));
        let (started, before) = (Instant::now(), Usage::now(pids));
        let report = loaded.load(source, destination);
        took += started.elapsed();
        Usage::now(pids).add_since(before, &mut usage);
        last = Some(black_box(report));
        drop(held);
    }
    (took, Seconds::of(usage, took, loaded.bytes(), runs), last)
}

/// Times each workload, its connectors spawned over the socket, then prints the CPU time a
/// gigabyte each took and its last run's readings.
#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; the CPU time a run takes is printed beside them"
)]
pub(crate) fn shaped(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut group = c.benchmark_group("served");
    group.sample_size(10);
    for shaped in Shaped::ALL {
        let (mut made, mut seconds, mut last) = (None, Vec::new(), None);
        group.throughput(Throughput::Elements(shaped.rows()));
        let id = BenchmarkId::new("process/socket", format!("both/{}", shaped.shape()));
        group.bench_function(id, |b| {
            let (loaded, processes) =
                made.get_or_insert_with(|| (Loaded::new(shaped, cores), Processes::new()));
            b.iter_custom(|runs| {
                let (took, sample, report) = timed(shaped, (&*loaded, &mut *processes), runs);
                seconds.push(sample);
                last = report.or(last.take());
                took
            });
        });
        // Only a benchmark that ran has timed its runs, so a listing prints nothing.
        if seconds.is_empty() {
            continue;
        }
        let total = spread(seconds.iter().map(|sample| Some(sample.total)));
        let (host, source, destination) = (
            spread(seconds.iter().map(|sample| Some(sample.host))),
            spread(seconds.iter().map(|sample| sample.source)),
            spread(seconds.iter().map(|sample| sample.destination)),
        );
        println!(
            "served/process/socket/both/{}: {total} CPU seconds a GB, user and system, over {} \
             samples; the bench's process {host}; source {source}; destination {destination}",
            shaped.shape(),
            seconds.len(),
        );
        if let Some(report) = &last {
            let pushes = shaped.pushes();
            let readings = Readings::of(report.elapsed, &report.counters, pushes, cores.count());
            println!("  readings: {readings}");
        }
    }
    group.finish();
}
