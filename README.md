# rdlt

A small, embeddable ELT engine in Rust, with a few deeply verified
connectors.

rdlt moves data from sources (Postgres incl. CDC, REST APIs, JSONL/CSV/
Parquet files locally or on S3-compatible storage) into destinations
(Postgres, DuckDB, parquet/jsonl files, Iceberg REST catalogs) with
exactly-once commits, crash recovery via a write-ahead log, schema
inference and evolution, and typed error classification the engine's
retry budget can act on.

The scope is deliberately narrow: a core one person can audit end-to-end,
where every connector is certified against a shared conformance suite,
crash-safety is proven by live fail-point sweeps, and performance claims
are enforced by a benchmark gate. Breadth — orchestration, scheduling,
catalogs of connectors — belongs to products built on top, not here.

## Use it as a library

```rust,ignore
use rdlt::prelude::*;

let report = Pipeline::builder("orders")
    .source(source)          // any rdlt source connector
    .destination(dest)       // any rdlt destination connector
    .write_mode(WriteMode::Append)
    .workdir(".rdlt")        // enables the WAL + crash recovery
    .build()?
    .run()
    .await?;
```

Connectors live behind cargo features on the `rdlt` facade crate
(`rest`, `postgres`, `duckdb`, `file`, `parquet`, `iceberg`).

## Use it from the CLI

One YAML document describes the whole pipeline; the CLI adds zero engine
capability beyond parsing it:

```sh
rdlt run pipeline.yaml
```

## Development

```sh
make lint     # fmt + clippy, warnings are errors
make test     # cargo nextest + doc-tests (container suites skip without a runtime)
make check    # everything a PR must pass
```

Architectural decisions and per-feature contracts live under `specs/`;
benchmarks and their governance under `benches/`.

## License

Apache-2.0
