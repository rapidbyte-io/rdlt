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

### Parquet output is compressed

Parquet destinations write **snappy-compressed** files. Earlier versions wrote
uncompressed ones, so the same data now produces much smaller files — on a
1M-row extract, roughly a quarter of the bytes.

This is a change to the files rdlt writes, not to what they contain: every
parquet reader handles snappy, and nothing about the data or its schema
changes. It is called out because the file sizes will visibly drop.

To restore the previous behaviour, or to choose something else:

```yaml
destination:
  file:
    path: out/
    format: parquet
    parquet:
      compression: uncompressed   # snappy (default) | gzip | zstd | brotli | lz4_raw | uncompressed
```

The other settings are `compression_level` (only for codecs that have one —
gzip, zstd, brotli), `dictionary_enabled`, `dictionary_page_size_limit`,
`data_page_size_limit` and `max_row_group_rows`. Anything omitted takes the
default.

The dictionary limit defaults well below parquet's own, which is what lets
compression *save* encoder time on high-cardinality columns rather than cost
it: without a lower cap, such a column interns nearly every distinct value
before falling back to plain encoding, and then compresses that work too.
Columns with few distinct values are unaffected either way.

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
