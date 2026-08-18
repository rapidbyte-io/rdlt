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

## How it fits together

- **`rdlt`** — the crate you depend on. It names the vocabulary, owns the
  pipeline *document* and its construction, and exposes the engine's
  boundary (the `Pipeline` builder). It knows no connector by name.
- **Connectors are separate processes.** A pipeline names one by id
  (`connector: {id: io.rapidbyte.postgres, config: …}`); the id's last
  segment resolves to a `rdlt-connector-<segment>` binary on `PATH`
  (or `path:` names one explicitly). The runtime spawns it per run,
  handshakes over a local socket, and the connector's own gate validates
  its config — refusals arrive in the connector's wording. The
  first-party connectors live in the sibling
  [rdlt-connectors](https://github.com/rapidbyte-io/rdlt-connectors)
  repository (`make connector-bins` builds their release binaries). This
  repository ships the engine, the CLI, and the reference connector its
  own gates spawn.
- **`rdlt-cli`** (`rdlt`) — a thin, scriptable face over the library; it
  adds no engine capability.

## Quickstart as a library

Add the crate (the workspace is not on crates.io yet; depend on the git
repository):

```toml
[dependencies]
rdlt = { git = "https://github.com/rapidbyte-io/rdlt", version = "0.3" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Write a pipeline document. This one needs only this repository's own
reference connector — build it once and put it on `PATH`
(`cargo build --release -p rdlt-connector-reference --features bin-serve`,
then `export PATH="$PWD/target/release:$PATH"`):

```yaml
# pipeline.yaml — one JSONL file in, JSONL parts + commit receipts out
pipeline: events-copy
workdir: .rdlt/events-copy        # the write-ahead log; keep it between runs
write_mode: append                # append | replace | merge: {key: [col, …]}
source:
  connector:
    id: io.rapidbyte.reference
    config: { path: ./events.jsonl }
destination:
  connector:
    id: io.rapidbyte.reference
    config: { path: ./out }        # or `config: ./dest.yaml` — a document by path
```

Read it, parse it, build it, run it — every configuration refusal dies at
`build`, before a row moves:

```rust
use std::path::Path;

use rdlt::document;
use rdlt::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new("pipeline.yaml");
    let text = document::read(path)?;
    let doc = document::parse(&text)?;
    let base = path.parent().unwrap_or(Path::new(""));   // path-form configs resolve here

    let pipeline = document::build(&doc, base).await?;   // spawns + handshakes both connectors

    // Optional: the typed event feed and cooperative cancellation.
    let mut events = pipeline.events();                  // subscribe BEFORE run()
    let cancel = pipeline.cancellation_token();          // cancel.cancel() == a crash: build again to resume
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            eprintln!("{event:?}");                      // RunStarted, StreamStarted, BatchLoaded, Committed, …
        }
    });
    let _ = cancel;

    match pipeline.run().await {                         // consumes the pipeline; resumable across runs
        Ok(report) => println!("{} rows in {} ms", report.total_rows(), report.elapsed_ms),
        Err(Error::Cancelled) => println!("cancelled — build the same document again to resume"),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
```

What the pieces are:

- `document::{read, parse, build, build_with}` — the document path. `build`
  uses the runtime's default local provider (spawn from `PATH`);
  `build_with(&doc, base, &your_provider)` lets an embedder decide how a
  connector requirement becomes a process — a pool, a remote scheduler —
  by implementing `rdlt::runtime::provider::Provider`.
- `pipeline::Pipeline` / `pipeline::Builder` — the engine's boundary. The
  builder takes a *source value* and a *destination value* and the
  pipeline-wide policies (`write_mode`, `write_mode_for`, `schema_policy`,
  `batch_policy`, `commit_policy`, `workdir`, byte and stream budgets). In
  production those values are the runtime's process adapters that
  `document::build` constructs for you; hand-rolled `impl Source` /
  `impl Destination` values are test doubles.
- The vocabulary lives behind its nouns: `rdlt::{commit, cursor, error,
  event, id, metrics, policy, report}`. `use rdlt::prelude::*` glob-imports
  what a pipeline author touches (never `Error` — spell
  `rdlt::error::Error`, so the glob cannot shadow your own).
- Outcome: `run()` returns `report::Run` — the exactly-once totals per
  table (rows, bytes, discards), commits, retries, elapsed — the same
  record the CLI prints as JSON. Live numbers come from `event` and the
  `metrics::Metrics` fold; the `tracing` span contract is in
  [docs/telemetry.md](docs/telemetry.md).
- Errors: `rdlt::error::Error` is a closed taxonomy — `Config`, `Schema`,
  `Source`, `Destination`, `Wal`, `Cancelled`, `Internal` — the CLI's exit
  codes mirror it one-to-one. `document::Error` splits construction into
  `Resolve` (the document) and `Build` (the engine).

Authoring a connector is a separate concern: `rdlt::sdk` (the connector
SDK) and `rdlt::sdk::spi` (the wire-side traits) — see
[docs/connector-authoring.md](docs/connector-authoring.md).

## Use it from the CLI

One YAML document describes the whole pipeline; the CLI adds zero engine
capability beyond parsing it:

```sh
rdlt run pipeline.yaml                # live progress on a terminal, plain lines in CI
rdlt run pipeline.yaml --report r.json --events events.ndjson
rdlt validate pipeline.yaml           # the run's gates (spawn + handshake), without the run
rdlt schema io.rapidbyte.postgres     # a connector's config JSON Schema — the FULL id,
                                      # or a binary path; the connector is spawned and asked
```

On a terminal, `run` draws a live display — per-stream rows read and
written, bytes, rows/s, commit recency — and ends with a summary table
of the exactly-once totals; the full JSON report goes to stdout when
redirected, or to `--report`. Off a terminal it logs a line per event.
`-q` silences the feed, `-v` adds detail, `--no-progress` forces the
line-per-event form, `--output auto|plain|json` picks the mode
explicitly (`json` = no feed, report JSON on stdout even on a terminal),
`--events <path|->` captures the raw feed as NDJSON (`-` needs
`--report`, so the two machine outputs never interleave), `--color`
follows `NO_COLOR` under `auto`. Exit codes are stable and scriptable:
0 success · 2 config · 3 schema · 4 source · 5 destination · 6 WAL/disk ·
7 cancelled · 64 usage · 70 internal defect · 74 file I/O.

Runnable pipelines covering every first-party connector live in the
[rdlt-connectors](https://github.com/rapidbyte-io/rdlt-connectors)
repository's `examples/`, each executed as written before being
committed; the containerised ones ship a seeded `compose.yaml`. Note the
document format is now `connector: {id, config}` only — examples still
written with the older per-connector short spellings (`postgres:`,
`rest:` …) are being ported; rewrite such an arm as
`connector: {id: io.rapidbyte.<name>, config: <the same document>}`.

### Parquet output is compressed

Parquet destinations (the file connector) write **snappy-compressed**
files by default — on a 1M-row extract, roughly a quarter of the bytes
of uncompressed output, and every parquet reader handles snappy. To
choose another codec, or none:

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
default. The dictionary limit defaults well below parquet's own, which is
what lets compression *save* encoder time on high-cardinality columns
rather than cost it.

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

## What it guarantees

- **Exactly-once publication.** Writes stage invisibly and publish atomically
  with pipeline state, idempotent per `(load_id, commit_seq)`; a crash at any
  point resumes without duplicating or losing rows.
- **Schema evolution under a policy you choose** — evolve, freeze, discard
  row, discard value — per pipeline, table, or column; a frozen contract
  refuses the change with a typed error naming the column and both types.
- **Nothing silent.** Discards are counted, refusals are typed, retries are
  bounded and reported; structured (Arrow) input is refused rather than
  rounded, wrapped, or nulled. The trust model and the ceilings that back
  it are in [SECURITY.md](SECURITY.md).

## Development

```sh
make lint     # fmt + clippy, warnings are errors
make test     # cargo nextest + doc-tests (container suites skip without a runtime)
make check    # everything a PR must pass
```

Architectural decisions are recorded in the code's own load-bearing
comments and in `docs/`; benchmarks and their governance under `benches/`.

## License

Apache-2.0
