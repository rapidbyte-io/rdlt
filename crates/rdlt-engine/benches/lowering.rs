//! Lowering: a batch of each kind a lowering plan prepares, prepared as a partition prepares it,
//! on one core.

#![forbid(unsafe_code)]

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rdlt_engine::bench::Lowering;

fn lowering(c: &mut Criterion) {
    let mut group = c.benchmark_group("lowering");
    for lowering in Lowering::all(Lowering::ROWS) {
        group.throughput(Throughput::Elements(lowering.rows()));
        group.bench_function(lowering.name(), |b| {
            b.iter(|| black_box(lowering.prepare()));
        });
    }
    group.finish();
}

criterion_group!(benches, lowering);
criterion_main!(benches);
