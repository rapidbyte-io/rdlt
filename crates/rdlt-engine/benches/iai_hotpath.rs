//! Instruction-count benches for the perf-regression gate.
//!
//! iai-callgrind measures deterministic instruction counts — stable on shared CI
//! runners where wall time flaps. Recorded baselines live in
//! `benches/perf-baselines.json` (lockfile discipline); `benches/harness/compare-iai.sh`
//! fails on >3% regression. Needs valgrind + a version-matched
//! `iai-callgrind-runner`.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use rdlt_engine::identity::{FieldValue, RowIdBuilder};

const ROWS: usize = 10_000;

fn nested_ndjson(rows: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows * 160);
    for i in 0..rows {
        out.extend_from_slice(
            format!(
                r#"{{"id":{i},"name":"user-{i}","score":{},"active":{},"profile":{{"city":"NYC","zip":{}}},"tags":[{{"label":"a"}},{{"label":"b"}}]}}"#,
                i as f64 * 0.5,
                i % 2 == 0,
                10001 + i % 100,
            )
            .as_bytes(),
        );
        out.push(b'\n');
    }
    out
}

fn structured_batch(rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(Float64Array::from_iter_values(
                (0..rows).map(|i| i as f64 * 0.5),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("row-{i}")),
            )),
        ],
    )
    .expect("batch")
}

#[library_benchmark]
#[bench::rows_10k(nested_ndjson(ROWS))]
fn shred_nested_10k(bytes: Vec<u8>) -> u64 {
    black_box(rdlt_engine::fuzzing::bench_shred_bytes(&bytes))
}

#[library_benchmark]
#[bench::rows_10k(structured_batch(ROWS))]
fn passthrough_10k(batch: RecordBatch) -> u64 {
    black_box(rdlt_engine::fuzzing::bench_passthrough(&batch))
}

#[library_benchmark]
#[bench::rows_10k(ROWS)]
fn identity_keyed_10k(rows: usize) -> u64 {
    let mut acc = 0u64;
    for i in 0..rows {
        let key = format!("{i}");
        let mut builder = RowIdBuilder::keyed();
        builder.field("id", FieldValue::Bytes(key.as_bytes()));
        acc ^= builder.finish().as_bytes()[0] as u64;
    }
    black_box(acc)
}

#[library_benchmark]
#[bench::rows_10k(ROWS)]
fn identity_keyless_10k(rows: usize) -> u64 {
    let mut acc = 0u64;
    for i in 0..rows {
        let content = format!(r#"{{"id":{i},"name":"user-{i}"}}"#);
        let mut builder = RowIdBuilder::keyless();
        builder.field("", FieldValue::Bytes(content.as_bytes()));
        acc ^= builder.finish().as_bytes()[0] as u64;
    }
    black_box(acc)
}

library_benchmark_group!(
    name = hotpath;
    benchmarks = shred_nested_10k, passthrough_10k, identity_keyed_10k, identity_keyless_10k
);
main!(library_benchmark_groups = hotpath);
