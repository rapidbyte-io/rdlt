# Quickstart: File & Arrow-Native Ingestion (feature 002)

## For embedders (target API once implemented)

```rust
use rdlt::prelude::*;

// JSONL files → DuckDB, incremental across runs.
let mut pipeline = Pipeline::builder("file_demo")
    .source(rdlt::file::FileSource::from_yaml(r#"
streams:
  - name: events
    format: jsonl
    path: "data/events-*.jsonl"
"#)?)
    .destination(rdlt::duckdb::DuckDb::open("out.duckdb")?)
    .workdir(".rdlt")
    .build()?;
let report = pipeline.run().await?;   // re-run later: only new/appended data loads

// Parquet passthrough → parquet export (structured; Merge would fail at build()).
let mut pipeline = Pipeline::builder("pq_copy")
    .source(rdlt::file::FileSource::from_yaml(r#"
streams:
  - name: metrics
    format: parquet
    path: "in/*.parquet"
"#)?)
    .destination(rdlt::parquet::ParquetDir::open("export/")?)
    .build()?;
```

CLI equivalents: `[source.file]` and `[destination.parquet]` arms in `pipeline.toml`.

## For contributors

- New crates: `crates/rdlt-source-file`, `crates/rdlt-dest-parquet` — SPI-only.
- Engine passthrough: `crates/rdlt-engine/src/shred/passthrough.rs` + the Arrow arm
  in `runtime/graph.rs`.
- Contract deltas live in `specs/002-file-arrow-ingestion/contracts/` (fold into the
  001 contract docs at merge).
- Certification: both connectors must pass `rdlt-testkit`'s conformance suites; the
  passthrough path gets crash-matrix coverage (WAL segments of structured batches
  replay like any other).
- Benchmarks: three new rows via `benches/` (baseline-first, as ever).
