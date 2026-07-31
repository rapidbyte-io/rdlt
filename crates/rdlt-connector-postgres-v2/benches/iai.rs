//! Gated instruction-count benches for the binary-COPY hot paths — the
//! decoder (source) and the encoder (destination). Instruction counts are
//! load-insensitive: the wall-clock cells cannot resolve changes this size
//! against machine noise, but callgrind can. Compared BY HAND against the
//! first-generation crate's `iai_pg` numbers (parity gate) — not wired into
//! the workspace perf gate while both generations coexist.

use std::hint::black_box;

use arrow_array::RecordBatch;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};

fn wire_10k() -> Vec<u8> {
    rdlt_connector_postgres_v2::testsupport::source::bench_wire(10_000)
}

fn batch_10k() -> RecordBatch {
    rdlt_connector_postgres_v2::testsupport::destination::bench_batch(10_000)
}

#[library_benchmark]
#[bench::rows_10k(wire_10k())]
fn copy_decode_10k(wire: Vec<u8>) -> u64 {
    black_box(rdlt_connector_postgres_v2::testsupport::source::bench_decode(&wire))
}

#[library_benchmark]
#[bench::rows_10k(batch_10k())]
fn copy_encode_10k(batch: RecordBatch) -> u64 {
    black_box(rdlt_connector_postgres_v2::testsupport::destination::bench_encode(&batch))
}

library_benchmark_group!(
    name = hotpath;
    benchmarks = copy_decode_10k, copy_encode_10k
);
main!(library_benchmark_groups = hotpath);
