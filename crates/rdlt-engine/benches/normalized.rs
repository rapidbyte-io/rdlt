//! The normalized write path: keyless nested JSON, pushed a flush at a time, normalized into
//! three tables by the engine into a destination that discards them, in flushes of one unit and
//! of several, whose integers are judged together; and twice as many rows read from four and
//! from sixteen partitions, within the default budget and within four times it, each printing
//! its last run's readings.
//!
//! Throughput counts the rows of the three tables.

#![forbid(unsafe_code)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rdlt_engine::Cores;
use rdlt_engine::bench::{Normalized, Readings};

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
                    "normalized/keyless/{per_push}: {workers} runtime workers, {threads} compute \
                     threads, {fewest} to {most} units a flush, {} bytes of JSON a run",
                    bench.bytes(),
                    workers = cores.runtime_workers(),
                    threads = cores.count(),
                );
                bench
            });
            b.iter(|| black_box(bench.run()));
        });
    }
    group.finish();
}

/// A budget's name in a benchmark's: its mebibytes, or its gibibytes where whole.
fn budget(bytes: u64) -> String {
    if bytes.is_multiple_of(1 << 30) {
        format!("{}GiB", bytes >> 30)
    } else {
        format!("{}MiB", bytes >> 20)
    }
}

#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; each case's readings are printed beside them"
)]
fn partitioned(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut group = c.benchmark_group("normalized/partitions");
    group.sample_size(10);
    group.throughput(Throughput::Elements(Normalized::loaded(
        Normalized::SPLIT_ROOTS,
    )));
    let (roots, per_push) = (Normalized::SPLIT_ROOTS, Normalized::SPLIT_PUSH_ROWS);
    for partitions in Normalized::PARTITIONS {
        for memory in Normalized::BUDGETS {
            let (mut bench, mut last) = (None, None);
            let name = format!("{partitions}/{}", budget(memory));
            group.bench_function(&name, |b| {
                let bench = bench.get_or_insert_with(|| {
                    Normalized::within(cores, roots, per_push, memory)
                        .expect("the pool starts")
                        .partitioned(partitions)
                });
                b.iter(|| last = Some(bench.report()));
            });
            if let Some(report) = &last {
                let flushes = roots.div_ceil(per_push.get());
                let readings =
                    Readings::of(report.elapsed, &report.counters, flushes, cores.count());
                println!(
                    "normalized/partitions/{name}: {readings}, peak memory {} MiB",
                    report.peak_memory >> 20
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, normalized, partitioned);
criterion_main!(benches);
