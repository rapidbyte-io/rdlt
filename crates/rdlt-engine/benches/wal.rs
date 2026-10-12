//! The write-ahead log: a batch frame encoded, the engine logging every batch ahead on a disk
//! and on tmpfs at a commit each batch, every sixteen and once, and a replay's read of a log.

#![forbid(unsafe_code)]

use std::hint::black_box;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use rdlt_engine::bench::{Logged, Passthrough, logical_bytes, replay_scan, sample_log};
use rdlt_engine::{Cores, LocalWal, Report, WalStore};

/// Commit cadences: a commit each batch, every sixteen, and once.
const CADENCES: [(&str, Option<u64>); 3] = [("each", Some(1)), ("16", Some(16)), ("once", None)];

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

#[expect(
    clippy::print_stdout,
    reason = "the run's counters are printed beside criterion's times"
)]
fn logging(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let batches = if timed() { Logged::BATCHES } else { 16 };
    let mut group = c.benchmark_group("wal/engine");
    group.sampling_mode(SamplingMode::Flat).sample_size(10);
    group.throughput(Throughput::Elements(
        u64::from(batches) * u64::from(Logged::ROWS),
    ));
    for (root, path) in roots() {
        for (cadence, every) in CADENCES {
            let mut run = None;
            group.bench_function(BenchmarkId::new(root, cadence), |b| {
                let (_dir, logged, last) = run.get_or_insert_with(|| {
                    let dir = tempfile::tempdir_in(&path).expect("a directory of the bench's own");
                    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(dir.path()));
                    let every = every.and_then(NonZeroU64::new);
                    let logged = Logged::try_new(cores, store, batches, Logged::ROWS, every)
                        .expect("the pool starts");
                    (dir, logged, None)
                });
                b.iter(|| *last = Some(logged.run()));
            });
            if let Some((_, logged, Some(report))) = run {
                println!(
                    "wal/engine/{root}/{cadence}: {}",
                    counted(&report, logged.frame_bytes())
                );
            }
        }
    }
    group.finish();
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

#[expect(
    clippy::print_stdout,
    reason = "the read amplification is printed beside criterion's times"
)]
#[expect(
    clippy::cast_precision_loss,
    reason = "a ratio printed to three places"
)]
fn replaying(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let batches = if timed() { Logged::BATCHES } else { 16 };
    let mut group = c.benchmark_group("wal/replay");
    group.sampling_mode(SamplingMode::Flat).sample_size(10);
    group.throughput(Throughput::Elements(
        u64::from(batches) * u64::from(Logged::ROWS),
    ));
    for (root, path) in roots() {
        let mut left = None;
        group.bench_function(root, |b| {
            let (dir, _) = left.get_or_insert_with(|| {
                let dir = tempfile::tempdir_in(&path).expect("a directory of the bench's own");
                let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(dir.path()));
                let logged = Logged::try_new(cores, store, batches, Logged::ROWS, None)
                    .expect("the pool starts");
                logged.leave();
                let first = replay_scan(dir.path(), "logged").expect("the log reads");
                (dir, first)
            });
            b.iter(|| black_box(replay_scan(dir.path(), "logged").expect("the log reads")));
        });
        if let Some((_, first)) = left {
            println!(
                "wal/replay/{root}: read {:.3} of the log, {} reads",
                first.read as f64 / first.logged as f64,
                first.reads
            );
        }
    }
    group.finish();
}

criterion_group!(benches, encoding, logging, replaying);
criterion_main!(benches);
