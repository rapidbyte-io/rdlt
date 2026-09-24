//! JSON shredding throughput (spec §21.1, §21.2): each corpus on one core, and the nested corpus
//! as one stream over more cores.
//!
//! `RDLT_SHRED_CORPUS` names a JSON lines file to measure as one more corpus, such as the corpus
//! of the comparison with the old engine (docs/perf/shred.md).

use std::fmt::Write as _;
use std::hint::black_box;
use std::num::NonZeroUsize;

use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rdlt_engine::RayonPool;
use rdlt_engine::bench::{shred, shred_on};

/// Bytes each corpus holds.
const CORPUS_BYTES: usize = 32 << 20;
/// Bytes per push, the default coalescing target.
const PUSH_BYTES: usize = 8 << 20;
/// Bytes per shredding job, the default chunk size.
const CHUNK_BYTES: usize = 1 << 20;

/// A deterministic source of numbers for the corpora.
struct Mix(u64);

impl Mix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Rows from `row` until the corpus holds `CORPUS_BYTES`, cut into pushes of whole lines.
fn corpus(mut row: impl FnMut(u64, &mut Mix) -> String) -> Vec<Bytes> {
    let mut mix = Mix(7);
    let mut pushes = Vec::new();
    let mut push = String::with_capacity(PUSH_BYTES + 4096);
    let (mut total, mut index) = (0, 0);
    while total < CORPUS_BYTES {
        let line = row(index, &mut mix);
        total += line.len() + 1;
        push.push_str(&line);
        push.push('\n');
        if push.len() >= PUSH_BYTES {
            pushes.push(Bytes::from(std::mem::take(&mut push)));
        }
        index += 1;
    }
    if !push.is_empty() {
        pushes.push(Bytes::from(push));
    }
    pushes
}

/// Nested rows of about 170 bytes.
fn nested(index: u64, mix: &mut Mix) -> String {
    let cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"];
    format!(
        r#"{{"id":{index},"name":"user-{index:07}","score":{}.{:02},"active":{},"created_at":"2026-09-{:02}T12:{:02}:00Z","profile":{{"city":"{}","zip":"{}","geo":{{"lat":{}.{:05},"lon":{}.{:05}}}}}}}"#,
        mix.below(1000),
        mix.below(100),
        index.is_multiple_of(3),
        1 + index % 28,
        index % 60,
        cities[usize::try_from(mix.below(5)).unwrap_or(0)],
        10_000 + mix.below(90_000),
        49 + mix.below(6),
        mix.below(100_000),
        14 + mix.below(10),
        mix.below(100_000),
    )
}

/// Nested rows of which about one in ten thousand carries an optional key after its name, so most
/// chunks lack a column the others have, and those that hold it meet it before most columns.
fn sparse(index: u64, mix: &mut Mix) -> String {
    let row = nested(index, mix);
    if mix.below(10_000) == 0 {
        let name = row.find(r#","score""#).unwrap_or(row.len() - 1);
        format!(
            r#"{},"tag":"t{}"{}"#,
            &row[..name],
            mix.below(100),
            &row[name..]
        )
    } else {
        row
    }
}

/// Flat rows of three narrow columns.
fn flat_narrow(index: u64, mix: &mut Mix) -> String {
    format!(
        r#"{{"id":{index},"value":{},"flag":{}}}"#,
        mix.next() >> 12,
        mix.below(2) == 0
    )
}

/// Flat rows of 200 columns, integers and short strings.
fn flat_wide(_: u64, mix: &mut Mix) -> String {
    let columns: Vec<String> = (0..200)
        .map(|column| match column % 2 {
            0 => format!(r#""c{column}":{}"#, mix.below(1 << 20)),
            _ => format!(r#""c{column}":"v{}""#, mix.below(1000)),
        })
        .collect();
    format!("{{{}}}", columns.join(","))
}

/// Rows of long strings with escapes.
fn string_heavy(index: u64, mix: &mut Mix) -> String {
    let mut text = String::new();
    for _ in 0..8 {
        write!(text, "word{} \\\"quoted\\\" é ", mix.below(1000)).expect("writing to a string");
    }
    format!(
        r#"{{"id":{index},"body":"{text}","title":"title {} é"}}"#,
        mix.below(1000)
    )
}

fn single_core(c: &mut Criterion) {
    let mut corpora = vec![
        ("nested", corpus(nested)),
        ("sparse", corpus(sparse)),
        ("flat_narrow", corpus(flat_narrow)),
        ("flat_wide", corpus(flat_wide)),
        ("string_heavy", corpus(string_heavy)),
    ];
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
        corpora.push(("file", pushes));
    }
    let mut group = c.benchmark_group("shred");
    group.sample_size(10);
    for (name, pushes) in &corpora {
        let bytes: usize = pushes.iter().map(Bytes::len).sum();
        group.throughput(Throughput::Bytes(u64::try_from(bytes).unwrap_or(u64::MAX)));
        group.bench_function(*name, |b| {
            b.iter(|| black_box(shred(pushes, CHUNK_BYTES).expect("the corpus shreds")));
        });
    }
    group.finish();
}

fn many_cores(c: &mut Criterion) {
    let pushes = corpus(nested);
    let bytes: usize = pushes.iter().map(Bytes::len).sum();
    let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime starts");
    let mut group = c.benchmark_group("shred_cores");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(u64::try_from(bytes).unwrap_or(u64::MAX)));
    for threads in [1, 2, 4, 8].into_iter().filter(|threads| *threads <= cores) {
        let pool = RayonPool::new(NonZeroUsize::new(threads).expect("at least one thread"))
            .expect("the pool starts");
        group.bench_with_input(BenchmarkId::from_parameter(threads), &pool, |b, pool| {
            b.iter(|| {
                let batches = runtime.block_on(shred_on(pool, &pushes, CHUNK_BYTES));
                black_box(batches.expect("the corpus shreds"))
            });
        });
    }
    group.finish();
}

/// The arrow-json decoder against the shredder on flat JSON of a known schema: the fast path spec
/// §7.4 allows where it is faster (ADR 0008 records the evaluation).
fn arrow_json_fast_path(c: &mut Criterion) {
    let narrow = Schema::new(vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("value", DataType::Int64, true),
        ArrowField::new("flag", DataType::Boolean, true),
    ]);
    let wide = Schema::new(
        (0..200)
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
    for (name, pushes, schema) in [
        ("flat_narrow", corpus(flat_narrow), narrow),
        ("flat_wide", corpus(flat_wide), wide),
    ] {
        let bytes: usize = pushes.iter().map(Bytes::len).sum();
        group.throughput(Throughput::Bytes(u64::try_from(bytes).unwrap_or(u64::MAX)));
        group.bench_function(BenchmarkId::new("shredder", name), |b| {
            b.iter(|| black_box(shred(&pushes, CHUNK_BYTES).expect("the corpus shreds")));
        });
        let schema = Arc::new(schema);
        group.bench_function(BenchmarkId::new("arrow_json", name), |b| {
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

criterion_group!(benches, single_core, many_cores, arrow_json_fast_path);
criterion_main!(benches);
