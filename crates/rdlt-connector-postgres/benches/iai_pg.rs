//! Gated instruction-count bench for the binary-COPY decoder (feature 005
//! T027) — joins the 003 perf gate: baseline in benches/perf-baselines.json,
//! compare-iai.sh fails on >3% regression.

use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};

fn wire_10k() -> Vec<u8> {
    rdlt_connector_postgres::source::testhook::bench_wire(10_000)
}

#[library_benchmark]
#[bench::rows_10k(wire_10k())]
fn pg_copy_decode_10k(wire: Vec<u8>) -> u64 {
    black_box(rdlt_connector_postgres::source::testhook::bench_decode(
        &wire,
    ))
}

library_benchmark_group!(name = hotpath; benchmarks = pg_copy_decode_10k);
main!(library_benchmark_groups = hotpath);
