//! The normalized write path: keyless nested JSON, pushed a flush at a time, normalized into
//! three tables by the engine into a destination that discards them, in flushes of one unit and
//! of several, whose integers are judged together.
//!
//! Throughput counts the rows of the three tables.

#![forbid(unsafe_code)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rdlt_engine::Cores;
use rdlt_engine::bench::Normalized;

#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; each case's layout and units are printed beside them"
)]
fn normalized(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut group = c.benchmark_group("normalized");
    group.sample_size(10);
    for per_push in Normalized::PUSH_ROWS {
        let mut bench = None;
        group.throughput(Throughput::Elements(Normalized::loaded(Normalized::ROOTS)));
        group.bench_function(BenchmarkId::new("keyless", per_push), |b| {
            let bench = bench.get_or_insert_with(|| {
                let bench = Normalized::try_new(cores, Normalized::ROOTS, per_push)
                    .expect("the pool starts");
                let (fewest, most) = bench.units();
                println!(
                    "normalized/keyless/{per_push}: {} runtime workers, {} compute threads, \
                     {fewest} to {most} units a flush, {} bytes of JSON a run",
                    cores.workers(),
                    cores.compute_threads(),
                    bench.bytes(),
                );
                bench
            });
            b.iter(|| black_box(bench.run()));
        });
    }
    group.finish();
}

criterion_group!(benches, normalized);
criterion_main!(benches);
