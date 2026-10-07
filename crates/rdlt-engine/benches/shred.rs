//! JSON shredding throughput: each corpus on one core, and the nested corpus as one stream over
//! more cores.
//!
//! `RDLT_SHRED_CORPUS` names a JSON lines file to measure as one more corpus, such as the corpus
//! of the comparison with the old engine (docs/perf/shred.md).

#![forbid(unsafe_code)]

use std::hint::black_box;
use std::num::NonZeroUsize;

use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rdlt_engine::bench::{
    CHUNK_BYTES, CORPUS_BYTES, Corpus, PUSH_BYTES, normalize, pool_cores, shred, shred_on,
};
use rdlt_engine::{Cores, RayonPool};

fn single_core(c: &mut Criterion) {
    let mut corpora: Vec<(String, Vec<Bytes>)> = Corpus::SHREDDED
        .into_iter()
        .map(|corpus| (corpus.name(), corpus.pushes(CORPUS_BYTES)))
        .collect();
    if let Ok(path) = std::env::var("RDLT_SHRED_CORPUS") {
        let file = std::fs::read_to_string(path).expect("RDLT_SHRED_CORPUS names a readable file");
        let lines: Vec<&str> = file.lines().collect();
        let per_push = lines
            .len()
            .div_ceil(file.len().div_ceil(PUSH_BYTES).max(1))
            .max(1);
        let pushes = lines
            .chunks(per_push)
            .map(|lines| Bytes::from(lines.join("\n")))
            .collect();
        corpora.push(("file".to_owned(), pushes));
    }
    let mut group = c.benchmark_group("shred");
    group.sample_size(10);
    for (name, pushes) in &corpora {
        let bytes: usize = pushes.iter().map(Bytes::len).sum();
        group.throughput(Throughput::Bytes(u64::try_from(bytes).unwrap_or(u64::MAX)));
        group.bench_function(name, |b| {
            b.iter(|| black_box(shred(pushes, CHUNK_BYTES).expect("the corpus shreds")));
        });
    }
    group.finish();
}

/// The nested corpus shredded by pools of each count of cores [`pool_cores`] gives for the
/// host's, each beside the bench's one runtime worker, which only waits.
#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; each pool's threads are printed beside them"
)]
fn many_cores(c: &mut Criterion) {
    let pushes = Corpus::Nested.pushes(CORPUS_BYTES);
    let bytes: usize = pushes.iter().map(Bytes::len).sum();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime starts");
    let mut group = c.benchmark_group("shred_cores");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(u64::try_from(bytes).unwrap_or(u64::MAX)));
    let host = Cores::try_from_host().expect("the host says how many cores the bench may use");
    for count in pool_cores(host.count()) {
        let cores = Cores::new(count, NonZeroUsize::MIN);
        let mut pool = None;
        group.bench_function(BenchmarkId::from_parameter(count), |b| {
            let pool = pool.get_or_insert_with(|| {
                println!(
                    "shred_cores/{count}: 1 runtime worker, {} compute threads",
                    cores.compute_threads()
                );
                RayonPool::try_new(cores).expect("the pool starts")
            });
            b.iter(|| {
                let batches = runtime.block_on(shred_on(pool, &pushes, CHUNK_BYTES));
                black_box(batches.expect("the corpus shreds"))
            });
        });
    }
    group.finish();
}

/// The arrow-json decoder against the shredder on flat JSON of a known schema, the fast path
/// ADR 0008 evaluated.
fn arrow_json_fast_path(c: &mut Criterion) {
    let narrow = Schema::new(vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("value", DataType::Int64, true),
        ArrowField::new("flag", DataType::Boolean, true),
    ]);
    let columns: u16 = 200;
    let wide = Schema::new(
        (0..columns)
            .map(|column| {
                let kind = if column % 2 == 0 {
                    DataType::Int64
                } else {
                    DataType::Utf8
                };
                ArrowField::new(format!("c{column}"), kind, true)
            })
            .collect::<Vec<_>>(),
    );
    let mut group = c.benchmark_group("fast_path");
    group.sample_size(10);
    for (corpus, schema) in [(Corpus::FlatNarrow, narrow), (Corpus::Wide(columns), wide)] {
        let pushes = corpus.pushes(CORPUS_BYTES);
        let bytes: usize = pushes.iter().map(Bytes::len).sum();
        group.throughput(Throughput::Bytes(u64::try_from(bytes).unwrap_or(u64::MAX)));
        group.bench_function(BenchmarkId::new("shredder", corpus.name()), |b| {
            b.iter(|| black_box(shred(&pushes, CHUNK_BYTES).expect("the corpus shreds")));
        });
        let schema = Arc::new(schema);
        group.bench_function(BenchmarkId::new("arrow_json", corpus.name()), |b| {
            b.iter(|| black_box(decode(&pushes, &schema)));
        });
    }
    group.finish();
}

/// The rows of `pushes` as arrow-json decodes them against `schema`.
fn decode(pushes: &[Bytes], schema: &SchemaRef) -> usize {
    let mut rows = 0;
    for push in pushes {
        let mut decoder = arrow_json::ReaderBuilder::new(Arc::clone(schema))
            .with_batch_size(1 << 16)
            .build_decoder()
            .expect("the schema decodes");
        let mut offset = 0;
        while offset < push.len() {
            let read = decoder.decode(&push[offset..]).expect("the corpus decodes");
            offset += read;
            rows += decoder
                .flush()
                .expect("rows decode")
                .map_or(0, |batch| batch.num_rows());
            if read == 0 {
                break;
            }
        }
        rows += decoder
            .flush()
            .expect("rows decode")
            .map_or(0, |batch| batch.num_rows());
    }
    rows
}

/// Shredding rows with arrays, alone and normalized into child tables with their lineage:
/// keyed roots hash their key, keyless ones their whole row.
fn normalizing(c: &mut Criterion) {
    let pushes = Corpus::WithArrays.pushes(CORPUS_BYTES);
    let bytes: usize = pushes.iter().map(Bytes::len).sum();
    let mut group = c.benchmark_group("normalize");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(u64::try_from(bytes).unwrap_or(u64::MAX)));
    group.bench_function("shred_only", |b| {
        b.iter(|| black_box(shred(&pushes, CHUNK_BYTES).expect("the corpus shreds")));
    });
    for (name, key) in [("keyed", &["id"][..]), ("keyless", &[][..])] {
        group.bench_function(name, |b| {
            b.iter(|| {
                let batches = shred(&pushes, CHUNK_BYTES).expect("the corpus shreds");
                let parts: usize = batches
                    .iter()
                    .map(|batch| {
                        normalize(batch, 8, key)
                            .expect("the batch normalizes")
                            .len()
                    })
                    .sum();
                black_box(parts)
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    single_core,
    many_cores,
    arrow_json_fast_path,
    normalizing
);
criterion_main!(benches);
