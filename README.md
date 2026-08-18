# rdlt

A small, embeddable ELT engine in Rust, with a few deeply verified
connectors.

rdlt moves data from sources (Postgres incl. CDC, Oracle, REST APIs,
JSONL/CSV/Parquet files locally or on S3-compatible storage) into
destinations (Postgres, DuckDB, Snowflake, Iceberg REST catalogs,
parquet/jsonl files) with exactly-once commits, crash recovery via a
write-ahead log, schema inference and evolution, and typed error
classification the engine's retry budget can act on.

The scope is deliberately narrow: a core one person can audit end-to-end,
where every connector is certified against a shared conformance suite,
crash-safety is proven by live fail-point sweeps, and performance claims
are enforced by a benchmark gate. Breadth — orchestration, scheduling,
catalogs of connectors — belongs to products built on top, not here.

## Use it as a library

```rust,ignore
use rdlt::prelude::*;

let report = Pipeline::builder("orders")
    .source(source)          // any SPI Source — e.g. a spawned connector
    .destination(dest)       // any SPI Destination
    .write_mode(WriteMode::Append)
    .workdir(".rdlt")        // enables the WAL + crash recovery
    .build()?
    .run()
    .await?;
```

Connectors are separate binaries, spawned per run and supervised over
a local socket — none are compiled into the engine, which knows none by
name. A pipeline names one by its id (`connector: {id:
io.rapidbyte.postgres, config: …}`); the id's last segment resolves to
a `rdlt-connector-<segment>` binary on PATH, and `path:` names an
out-of-tree binary explicitly. The first-party connectors live in the
sibling [rdlt-connectors](https://github.com/rapidbyte-io/rdlt-connectors)
repository — its `make connector-bins` builds their release binaries;
put them on PATH and every id above resolves. This repo ships the
engine, the CLI, and the reference connector its own gates spawn.

## Use it from the CLI

One YAML document describes the whole pipeline; the CLI adds zero engine
capability beyond parsing it:

```sh
rdlt run pipeline.yaml        # live progress on a terminal, plain lines in CI
rdlt validate pipeline.yaml   # the run's gates, without the run
rdlt schema io.rapidbyte.postgres   # a connector's config JSON Schema — an
                                    # id or a binary path; the connector
                                    # is spawned and asked
```

On a terminal, `run` draws a live display — per-stream rows read and
written, bytes, rows/s, commit recency — and ends with a summary table
of the exactly-once totals (the full JSON report goes to stdout when
redirected, or to `--report`). Off a terminal it logs a line per event;
`-q` silences, `-v` adds detail, `--events` captures the raw feed as
NDJSON. Exit codes are stable and scriptable. The numbers come from
the library's own telemetry seams — events, the `Metrics` fold, and
`tracing` spans — documented in [docs/telemetry.md](docs/telemetry.md)
for anyone embedding rdlt directly.

Runnable pipelines covering every connector live in the
[rdlt-connectors](https://github.com/rapidbyte-io/rdlt-connectors)
repository's `examples/`, each executed as written before being
committed. Every connector has one example showing its COMPLETE
configuration (a property that repo's test suite enforces), and the
containerised ones ship a seeded `compose.yaml` — `docker compose up`
and run. Start with `pokemon-to-jsonl`, which reads a public API and
needs no setup at all.

### Parquet output is compressed

Parquet destinations write **snappy-compressed** files by default — on a
1M-row extract, roughly a quarter of the bytes of uncompressed output,
and every parquet reader handles snappy. To choose another codec, or
none:

```yaml
destination:
  connector:
    id: io.rapidbyte.file
    config:
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

## Throughput and how to scale it

On the reference machine (32 cores, the committed bench fixtures), a single
pipeline sustains roughly **1.19M rows/s** on a 1M-row relational copy.

Aggregate throughput comes from running **concurrent pipelines**, not from
parallelism inside one. Measured across independent processes on the same
destination:

| concurrent pipelines | rows/s | vs one |
|---:|---:|---:|
| 1 | 1.19M | 1.00x |
| 2 | 2.25M | 1.89x |
| 4 | 5.13M | 4.32x |
| 8 | 10.0M | **8.43x** |

Wall-clock stays flat as concurrency rises, so this is close to linear scaling
with no engine configuration to tune.

That split is deliberate, not a gap. A full-refresh load puts the rows and the
clearing of the old ones in **one transaction on one connection**, which is
precisely what makes a reload atomic — a reader never sees a half-replaced
table. Parallelising that single load would trade a correctness property for
throughput, so rdlt does not. If you need more aggregate throughput, run more
pipelines.

## Development

```sh
make lint     # fmt + clippy, warnings are errors
make test     # cargo nextest + doc-tests (container suites skip without a runtime)
make check    # everything a PR must pass
```

Architectural decisions are recorded in the code's own load-bearing
comments and in `docs/`;
benchmarks and their governance under `benches/`.

## License

Apache-2.0
