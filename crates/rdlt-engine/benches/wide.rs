//! Wide tables: rows of a thousand and of five thousand columns, pushed as Arrow batches and as
//! JSON, loaded through the engine into a destination that discards them.
//!
//! Throughput counts rows; each case prints the bytes a run pushes.

#![forbid(unsafe_code)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rdlt_engine::Cores;
use rdlt_engine::bench::{Form, Wide};

#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; each case's layout and rows are printed beside them"
)]
fn wide(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut group = c.benchmark_group("wide");
    group.sample_size(10);
    for (form, name) in [(Form::Arrow, "arrow"), (Form::Json, "json")] {
        for columns in Wide::COLUMNS {
            let mut bench = None;
            let rows = Wide::rows_of(form, columns, Wide::PUSHES);
            group.throughput(Throughput::Elements(rows));
            group.bench_function(BenchmarkId::new(name, columns), |b| {
                let bench = bench.get_or_insert_with(|| {
                    let bench =
                        Wide::try_new(cores, form, columns, Wide::PUSHES).expect("the pool starts");
                    println!(
                        "wide/{name}/{columns}: {} runtime workers, {} compute threads, \
                         {rows} rows of {} bytes a run",
                        cores.workers(),
                        cores.compute_threads(),
                        bench.bytes(),
                    );
                    bench
                });
                b.iter(|| black_box(bench.run()));
            });
        }
    }
    group.finish();
}

criterion_group!(benches, wide);
criterion_main!(benches);
