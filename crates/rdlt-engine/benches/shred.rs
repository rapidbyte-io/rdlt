//! Shredder/engine throughput microbench (T063): nested-JSON rows through the full
//! engine (shred → channels → memory destination), reported as rows/s.
//!
//! Run: `cargo bench -p rdlt-engine`
//! The pinned-dlt comparison lives in `benches/` at the repo root (methodology:
//! baseline first, same data, same hardware — research.md R12).

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryDestination, MemorySource};
use serde_json::json;

fn nested_rows(n: usize) -> Vec<serde_json::Value> {
    (0..n)
        .map(|i| {
            json!({
                "id": i,
                "name": format!("user-{i}"),
                "score": (i as f64) * 0.5,
                "active": i % 2 == 0,
                "created_at": "2026-07-19T10:00:00Z",
                "profile": {"city": "NYC", "zip": 10001 + (i % 100), "geo": {"lat": 40.7, "lon": -74.0}},
                "tags": [{"label": "a"}, {"label": "b"}],
                "meta": {"source": "bench", "batch": i / 100}
            })
        })
        .collect()
}

fn bench_shred(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");
    const ROWS: usize = 10_000;
    let rows = nested_rows(ROWS);

    let mut group = c.benchmark_group("engine");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("nested_json_10k_rows", |b| {
        b.iter(|| {
            let source = MemorySource::single_stream(
                rdlt_connector::source::StreamSpec::new("bench"),
                rows.clone(),
            );
            let dest = MemoryDestination::new();
            let report = runtime
                .block_on(Engine::new(EngineConfig::new("bench"), source, dest).run())
                .expect("run");
            assert!(report.total_rows() >= ROWS as u64);
        })
    });
    group.finish();
}

criterion_group!(benches, bench_shred);
criterion_main!(benches);
