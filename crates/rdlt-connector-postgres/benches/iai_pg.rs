//! Gated instruction-count benches for the binary-COPY hot paths — the
//! decoder (source, feature 005 T027) and the encoder (destination, feature
//! 019 F5). Both join the 003 perf gate: baselines in
//! benches/perf-baselines.json, compare-iai.sh fails on >3% regression.
//!
//! Instruction counts are load-insensitive, which is exactly what the encoder
//! rewrite needs: the wall-clock cells cannot resolve a change this size
//! against machine noise, but callgrind can.

use std::hint::black_box;

use arrow_array::RecordBatch;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};

fn wire_10k() -> Vec<u8> {
    rdlt_connector_postgres::source::testhook::bench_wire(10_000)
}

fn batch_10k() -> RecordBatch {
    rdlt_connector_postgres::dest::testhook::bench_batch(10_000)
}

#[library_benchmark]
#[bench::rows_10k(wire_10k())]
fn pg_copy_decode_10k(wire: Vec<u8>) -> u64 {
    black_box(rdlt_connector_postgres::source::testhook::bench_decode(
        &wire,
    ))
}

#[library_benchmark]
#[bench::rows_10k(batch_10k())]
fn pg_copy_encode_10k(batch: RecordBatch) -> u64 {
    black_box(rdlt_connector_postgres::dest::testhook::bench_encode(
        &batch,
    ))
}

library_benchmark_group!(
    name = hotpath;
    benchmarks = pg_copy_decode_10k, pg_copy_encode_10k
);
main!(library_benchmark_groups = hotpath);
