//! Wide tables: rows of a thousand and of five thousand columns, pushed as Arrow batches and as
//! JSON, loaded through the engine into a destination that discards them; a table of a hundred
//! columns, and one of five thousand kept to its first hundred by discarding the others' values,
//! both in batches of the five thousand columns' rows; and a sparse table of five thousand
//! columns whose rows each hold a hundred, pushed as Arrow and as JSON.
//!
//! Throughput counts rows; each case prints the bytes a run pushes.

#![forbid(unsafe_code)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rdlt_engine::bench::{Form, Wide};
use rdlt_engine::{ComputePoolError, Cores};

/// A case: its group and its name in it, the rows a run loads, and how its workload is made.
struct Case {
    group: &'static str,
    name: String,
    rows: u64,
    made: Box<dyn Fn(Cores) -> Wide>,
}

/// How a case's workload is made within the cores it is given.
type Making = fn(Cores) -> Result<Wide, ComputePoolError>;

/// Every case: each width in each form, then the kept tables and the sparse ones.
fn cases() -> Vec<Case> {
    let made = |made: Making| -> Box<dyn Fn(Cores) -> Wide> {
        Box::new(move |cores| made(cores).expect("the pool starts"))
    };
    let mut cases = Vec::new();
    for (form, group) in [(Form::Arrow, "arrow"), (Form::Json, "json")] {
        for columns in Wide::COLUMNS {
            cases.push(Case {
                group,
                name: columns.to_string(),
                rows: Wide::rows_of(form, columns, Wide::PUSHES),
                made: Box::new(move |cores| {
                    Wide::try_new(cores, form, columns, Wide::PUSHES).expect("the pool starts")
                }),
            });
        }
    }
    let kept_rows = Wide::rows_of(Form::Arrow, Wide::SPARSE, Wide::KEPT_PUSHES);
    let sparse_rows = Wide::rows_of(Form::Arrow, Wide::SPARSE, Wide::SPARSE_PUSHES);
    cases.extend([
        Case {
            group: "arrow",
            name: Wide::KEPT.to_string(),
            rows: kept_rows,
            made: made(|cores| Wide::keeping(cores, Wide::KEPT, Wide::KEPT, Wide::KEPT_PUSHES)),
        },
        Case {
            group: "arrow",
            name: format!("{}_keep_{}", Wide::SPARSE, Wide::KEPT),
            rows: kept_rows,
            made: made(|cores| Wide::keeping(cores, Wide::SPARSE, Wide::KEPT, Wide::KEPT_PUSHES)),
        },
        Case {
            group: "sparse",
            name: "arrow".to_owned(),
            rows: sparse_rows,
            made: made(|cores| Wide::sparse(cores, Form::Arrow, Wide::SPARSE_PUSHES)),
        },
        Case {
            group: "sparse",
            name: "json".to_owned(),
            rows: sparse_rows,
            made: made(|cores| Wide::sparse(cores, Form::Json, Wide::SPARSE_PUSHES)),
        },
    ]);
    cases
}

#[expect(
    clippy::print_stdout,
    reason = "criterion reports times; each case's layout and rows are printed beside them"
)]
fn wide(c: &mut Criterion) {
    let cores = Cores::try_from_host().expect("the host says how many cores the bench may use");
    let mut group = c.benchmark_group("wide");
    group.sample_size(10);
    for case in cases() {
        let mut bench = None;
        group.throughput(Throughput::Elements(case.rows));
        group.bench_function(BenchmarkId::new(case.group, &case.name), |b| {
            let bench = bench.get_or_insert_with(|| {
                let bench = (case.made)(cores);
                println!(
                    "wide/{}/{}: {workers} runtime workers, {threads} compute threads, {} rows \
                     of {} bytes a run, {} values discarded",
                    case.group,
                    case.name,
                    bench.rows(),
                    bench.bytes(),
                    bench.discards(),
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

criterion_group!(benches, wide);
criterion_main!(benches);
