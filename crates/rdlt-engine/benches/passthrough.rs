//! Arrow passthrough: the engine against a bare loop writing the same batches to the same
//! destination, timed in blocks that run each side first as often as last, so drift between
//! runs cancels out of their ratio.

#![forbid(unsafe_code)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use rdlt_engine::Cores;
use rdlt_engine::bench::{Paired, Passthrough, logical_bytes};

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

#[expect(
    clippy::print_stdout,
    reason = "criterion reports the engine's time; the ratio is printed beside it"
)]
#[expect(
    clippy::disallowed_methods,
    reason = "a benchmark times its runs on the real clock"
)]
fn passthrough(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let bench = Passthrough::try_new(cores, Passthrough::BATCHES, Passthrough::ROWS)
        .expect("the pool starts");
    let (workers, threads) = (cores.workers(), cores.compute_threads());
    let layout = format!("{workers}+{threads}");
    let mut ratios = Vec::new();
    let mut group = c.benchmark_group("passthrough");
    group.sample_size(SAMPLES).sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(logical_bytes(bench.batches())));
    group.bench_function(BenchmarkId::new("paired", &layout), |b| {
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
    group.finish();
    println!("passthrough/paired/{layout}: {workers} runtime workers, {threads} compute threads");
    if let Some(paired) = Paired::of(&ratios[ratios.len().saturating_sub(SAMPLES)..]) {
        println!("  engine over bare loop: {paired}");
    }
}

criterion_group!(benches, passthrough);
criterion_main!(benches);
