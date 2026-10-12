//! The write-ahead log: a batch frame encoded, the engine logging every batch ahead on a disk
//! and on tmpfs at a commit each batch, every sixteen and once, and on tmpfs at a commit each
//! batch into a log of little room, and a replay's read of a log.

#![forbid(unsafe_code)]

use std::hint::black_box;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group,
    criterion_main,
};
use rdlt_engine::bench::{Logged, Passthrough, logical_bytes, replay_scan, sample_log};
use rdlt_engine::{Cores, GrowthLimits, LocalWal, Report, WalStore};

/// Commit cadences: a commit each batch, every sixteen, and once.
const CADENCES: [(&str, Option<u64>); 3] = [("each", Some(1)), ("16", Some(16)), ("once", None)];

/// Batch frames the tight case's log holds at most: few enough that, committing each batch, a
/// batch finds the log full and a relief makes room.
const TIGHT: u64 = 4;

/// Whether criterion times the benchmarks, rather than running each once as a test.
fn timed() -> bool {
    std::env::args().any(|arg| arg == "--bench")
}

/// The roots logs are kept under: tmpfs where the host has one, and the disk beside the build.
#[expect(clippy::print_stderr, reason = "a skipped case is said once")]
fn roots() -> Vec<(&'static str, PathBuf)> {
    let mut roots = Vec::new();
    let shm = Path::new("/dev/shm");
    if shm.is_dir() {
        roots.push(("tmpfs", shm.to_path_buf()));
    } else {
        eprintln!("wal: no /dev/shm, so no tmpfs cases");
    }
    roots.push(("disk", PathBuf::from(env!("CARGO_TARGET_TMPDIR"))));
    roots
}

fn encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal/encode");
    for (name, rows) in [("1MiB", Logged::ROWS), ("7MiB", Passthrough::ROWS)] {
        let batch = Passthrough::batch(0, rows);
        group.throughput(Throughput::Bytes(logical_bytes(std::slice::from_ref(
            &batch,
        ))));
        group.bench_function(name, |b| b.iter(|| black_box(sample_log(batch.clone()))));
    }
    group.finish();
}

/// Bytes: the batch frames a run of `batches` batches logs, each the size of the first.
fn frame_bytes(batches: u32) -> u64 {
    Logged::frame_len(Logged::ROWS) * u64::from(batches)
}

/// Every root's case at each cadence, and tmpfs's tight case.
fn logging(c: &mut Criterion) {
    let batches = if timed() { Logged::BATCHES } else { 16 };
    let mut group = c.benchmark_group("wal/engine");
    group.sampling_mode(SamplingMode::Flat).sample_size(10);
    group.throughput(Throughput::Bytes(frame_bytes(batches)));
    let roots = roots();
    let log_bytes = GrowthLimits::default().log_bytes();
    for (root, path) in &roots {
        for (cadence, every) in CADENCES {
            engine(
                &mut group,
                (*root, path.as_path()),
                cadence,
                every,
                log_bytes,
                batches,
            );
        }
    }
    let tight =
        NonZeroU64::new(Logged::frame_len(Logged::ROWS) * TIGHT).expect("a frame has bytes");
    for (root, path) in roots.iter().filter(|(root, _)| *root == "tmpfs") {
        engine(
            &mut group,
            (*root, path.as_path()),
            "tight",
            Some(1),
            tight,
            batches,
        );
    }
    group.finish();
}

/// The case `<root>/<name>`: its load made when it first runs, so a process that runs one case
/// makes that one alone, committing every `every` batches or once into a log holding at most
/// `log_bytes`, and its counters printed after it.
#[expect(
    clippy::print_stdout,
    reason = "the run's counters are printed beside criterion's times"
)]
fn engine(
    group: &mut BenchmarkGroup<'_, WallTime>,
    (root, path): (&str, &Path),
    name: &str,
    every: Option<u64>,
    log_bytes: NonZeroU64,
    batches: u32,
) {
    let mut case = None;
    let mut last = None;
    group.bench_function(BenchmarkId::new(root, name), |b| {
        let (_, logged) = case.get_or_insert_with(|| {
            let cores = Cores::try_from_host().expect("the host says how many cores");
            let dir = tempfile::tempdir_in(path).expect("a directory of the bench's own");
            let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(dir.path()));
            let every = every.and_then(NonZeroU64::new);
            let logged = Logged::limited(cores, (store, log_bytes), batches, Logged::ROWS, every)
                .expect("the pool starts");
            (dir, logged)
        });
        b.iter(|| last = Some(logged.run()));
    });
    if let (Some((_, logged)), Some(report)) = (&case, &last) {
        println!(
            "wal/engine/{root}/{name}: {}",
            counted(report, logged.frame_bytes())
        );
    }
}

/// The report's log counters a commit: what a run of `frames` bytes of batch frames logged.
#[expect(
    clippy::cast_precision_loss,
    reason = "ratios and means printed to three places"
)]
fn counted(report: &Report, frames: u64) -> String {
    let commits = report.commits.max(1);
    let log = &report.counters.log;
    let phase = report.counters.phases.log.as_secs_f64() * 1e3 / commits as f64;
    format!(
        "commits {}, log phase {phase:.3} ms a commit, appended {:.3} of the frames, \
         carried {:.3}, publishes {:.2} a commit, removes {:.2} a commit, reliefs {}",
        report.commits,
        log.appended as f64 / frames as f64,
        log.carried as f64 / frames as f64,
        log.requests.publish as f64 / commits as f64,
        log.requests.remove as f64 / commits as f64,
        log.reliefs,
    )
}

/// Each root's log left when its case first runs, so a process that runs one case leaves that
/// one alone, and its read amplification printed after it.
#[expect(
    clippy::print_stdout,
    reason = "the read amplification is printed beside criterion's times"
)]
#[expect(
    clippy::cast_precision_loss,
    reason = "a ratio printed to three places"
)]
fn replaying(c: &mut Criterion) {
    let batches = if timed() { Logged::BATCHES } else { 16 };
    let mut group = c.benchmark_group("wal/replay");
    group.sampling_mode(SamplingMode::Flat).sample_size(10);
    group.throughput(Throughput::Bytes(frame_bytes(batches)));
    for (root, path) in roots() {
        let mut left = None;
        let mut last = None;
        group.bench_function(root, |b| {
            let dir = left.get_or_insert_with(|| {
                let cores = Cores::try_from_host().expect("the host says how many cores");
                let dir = tempfile::tempdir_in(&path).expect("a directory of the bench's own");
                let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(dir.path()));
                Logged::try_new(cores, store, batches, Logged::ROWS, None)
                    .expect("the pool starts")
                    .leave();
                dir
            });
            b.iter(|| last = Some(replay_scan(dir.path(), "logged").expect("the log reads")));
        });
        if let Some(read) = last {
            println!(
                "wal/replay/{root}: read {:.3} of the log, {} reads",
                read.read as f64 / read.logged as f64,
                read.reads
            );
        }
    }
    group.finish();
}

criterion_group!(benches, encoding, logging, replaying);
criterion_main!(benches);
