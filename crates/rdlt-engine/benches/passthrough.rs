//! Arrow passthrough: the engine moving the same rows in batches of three sizes, from one to
//! sixteen partitions of one or four streams, committed at the end or every sixteenth of the
//! rows, each cell's readings printed after it; and its paired cell, within 1 GiB and through
//! one writer as its bound is stated, against a bare loop writing the same batches to the same
//! destination, timed in blocks that run each side first as often as last, so drift between runs
//! cancels out of their ratio.
//!
//! The chosen cells run unless an argument names `full`, which runs every cell. As a test, each
//! cell runs once in two small batches for each partition of each stream.

#![forbid(unsafe_code)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use rdlt_engine::Cores;
use rdlt_engine::bench::{Cell, Paired, Passthrough, Readings};

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

criterion_group!(benches, passthrough);
criterion_main!(benches);
