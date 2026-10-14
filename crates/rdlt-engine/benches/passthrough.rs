//! Arrow passthrough: the engine moving the same rows in batches of three sizes, from one to
//! sixteen partitions of one or four streams, committed at the end or every sixteenth of the
//! rows, each cell's readings printed after it; and its paired cell, within 1 GiB and through
//! one writer as its bound is stated, against a bare loop writing the same batches to the same
//! destination, timed in blocks that run each side first as often as last, so drift between runs
//! cancels out of their ratio.
//!
//! The chosen cells run unless an argument names `full`, which runs every cell. As a test, each
//! cell runs once in two small batches for each partition of each stream.
//!
//! The logged cells move a pipeline of two streams, the second of which its source cannot read
//! again, so that each load logs both streams' batches ahead, on tmpfs and on the disk beside the
//! build; beside them, the same pipeline with both streams readable again logs nothing.

#![forbid(unsafe_code)]

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use rdlt_engine::bench::{Cell, Mixed, Paired, Passthrough, Readings};
use rdlt_engine::{Cores, LocalWal, Report, WalStore};

/// Samples the paired group takes, each the ratio of the blocks it ran.
const SAMPLES: usize = 30;

/// One side of a pair.
#[derive(Clone, Copy)]
enum Side {
    Bare,
    Engine,
}

/// The runs of one block: each side runs first once and last once.
const BLOCK: [Side; 4] = [Side::Bare, Side::Engine, Side::Engine, Side::Bare];

/// Whether criterion times the benchmarks, rather than running each once as a test.
fn timed() -> bool {
    std::env::args().any(|arg| arg == "--bench")
}

/// `cell` at the size this run moves it: whole when timed, small as a test.
fn sized(cell: Cell) -> Cell {
    if timed() { cell } else { cell.tested() }
}

#[expect(
    clippy::print_stdout,
    reason = "criterion reports the engine's time; the ratio and readings are printed beside it"
)]
#[expect(
    clippy::disallowed_methods,
    reason = "a benchmark times its runs on the real clock"
)]
fn passthrough(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let (workers, threads) = (cores.workers(), cores.compute_threads());
    let layout = format!("{workers}+{threads}");
    let mut group = c.benchmark_group("passthrough");
    let paired = sized(Cell::PAIRED);
    let mut bench = None;
    let mut ratios = Vec::new();
    group.sample_size(SAMPLES).sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(Passthrough::bytes(paired)));
    group.bench_function(BenchmarkId::new("paired", &layout), |b| {
        let bench = bench
            .get_or_insert_with(|| Passthrough::paired(cores, paired).expect("the pool starts"));
        b.iter_custom(|blocks| {
            if blocks == 0 {
                return Duration::ZERO;
            }
            let mut took = [Duration::ZERO; 2];
            for _ in 0..blocks {
                for side in BLOCK {
                    let started = Instant::now();
                    black_box(match side {
                        Side::Bare => bench.bare_loop(),
                        Side::Engine => bench.engine_run(),
                    });
                    took[side as usize] += started.elapsed();
                }
            }
            let [bare, engine] = took;
            ratios.push(engine.as_secs_f64() / bare.as_secs_f64());
            engine / 2
        });
    });
    drop(bench);
    // Only a run that measured has ratios, so listing the benchmarks prints nothing.
    if let Some(paired) = Paired::of(&ratios[ratios.len().saturating_sub(SAMPLES)..]) {
        println!(
            "passthrough/paired/{layout}: {workers} runtime workers, {threads} compute threads"
        );
        println!("  engine over bare loop: {paired}");
    }
    let args: Vec<String> = std::env::args().collect();
    let (kind, cells) = Cell::asked(&args);
    group.sample_size(10);
    for cell in cells {
        let moved = sized(cell);
        group.throughput(Throughput::Bytes(Passthrough::bytes(moved)));
        let (mut bench, mut last) = (None, None);
        // Named by the whole cell, so a listing and a timed run name it alike.
        group.bench_function(BenchmarkId::new(kind, cell.name()), |b| {
            let bench = bench.get_or_insert_with(|| {
                Passthrough::try_new(cores, moved).expect("the pool starts")
            });
            b.iter(|| last = Some(bench.engine_report()));
        });
        if let Some(report) = &last {
            let batches = u64::from(moved.batches);
            let readings = Readings::of(report.elapsed, &report.counters, batches, threads);
            println!("passthrough/{kind}/{}: {readings}", cell.name());
        }
    }
    group.finish();
}

/// The roots logs are kept under: tmpfs where the host has one, and the disk beside the build.
#[expect(clippy::print_stderr, reason = "a skipped cell is said once")]
fn roots() -> Vec<(&'static str, PathBuf)> {
    let mut roots = Vec::new();
    let shm = Path::new("/dev/shm");
    if shm.is_dir() {
        roots.push(("tmpfs", shm.to_path_buf()));
    } else {
        eprintln!("passthrough: no /dev/shm, so no tmpfs cell");
    }
    roots.push(("disk", PathBuf::from(env!("CARGO_TARGET_TMPDIR"))));
    roots
}

/// The mixed cell logged under each root, then unlogged, each made when it first runs and its
/// log counters printed after it.
#[expect(
    clippy::print_stdout,
    reason = "the log counters are printed beside criterion's times"
)]
fn logged(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let cell = sized(Mixed::CELL);
    let mut group = c.benchmark_group("passthrough/logged");
    group.sample_size(10).sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(Passthrough::bytes(cell)));
    let mut cases: Vec<(&str, Option<PathBuf>)> = roots()
        .into_iter()
        .map(|(root, path)| (root, Some(path)))
        .collect();
    cases.push(("unlogged", None));
    for (name, root) in cases {
        let (mut bench, mut last) = (None, None);
        // Named by the whole cell, so a listing and a timed run name it alike.
        group.bench_function(BenchmarkId::new(name, Mixed::CELL.name()), |b| {
            let (_, mixed) = bench.get_or_insert_with(|| {
                let Some(root) = &root else {
                    return (None, Mixed::unlogged(cores, cell).expect("the pool starts"));
                };
                let dir = tempfile::tempdir_in(root).expect("a directory of the bench's own");
                let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(dir.path()));
                let mixed = Mixed::logged(cores, cell, store).expect("the pool starts");
                (Some(dir), mixed)
            });
            b.iter(|| last = Some(mixed.run()));
        });
        if let (Some((_, mixed)), Some(report)) = (&bench, &last) {
            println!(
                "passthrough/logged/{name}/{}: {}",
                Mixed::CELL.name(),
                counted(report, mixed.frame_bytes())
            );
        }
    }
    group.finish();
}

/// What a run of a mixed cell logged: its bytes a commit, the share of them that the batch
/// frames of the stream its source can read again take, and the log phase a commit; `frames`
/// that stream's batch frames' bytes and every stream's.
#[expect(
    clippy::cast_precision_loss,
    reason = "ratios and means printed to three places"
)]
fn counted(report: &Report, (replayable, frames): (u64, u64)) -> String {
    let commits = report.commits.max(1);
    let log = &report.counters.log;
    let phase = report.counters.phases.log.as_secs_f64() * 1e3 / commits as f64;
    let share = if log.appended == 0 {
        0.0
    } else {
        replayable as f64 / log.appended as f64
    };
    format!(
        "commits {}, logged {:.3} MiB a commit, {:.3} of the frames, of which the stream that \
         reads again {share:.3}, log phase {phase:.3} ms a commit",
        report.commits,
        log.appended as f64 / f64::from(1 << 20) / commits as f64,
        log.appended as f64 / frames as f64,
    )
}

criterion_group!(benches, passthrough, logged);
criterion_main!(benches);
