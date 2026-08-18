# rdlt

A small, embeddable ELT engine in Rust, with a few deeply verified
connectors.

rdlt moves data from sources (Postgres incl. CDC, Oracle, REST APIs,
JSONL/CSV/Parquet files locally or on S3-compatible storage) into
destinations (Postgres, DuckDB, Snowflake, Iceberg REST catalogs,
parquet/jsonl files) with exactly-once commits, crash recovery via a
write-ahead log, schema inference and evolution, and typed errors the
engine's bounded retries can act on. It is a library first: the `rdlt`
crate is what you embed; the `rdlt` binary is a thin, scriptable face
over it.

The scope is deliberately narrow: a core one person can audit end-to-end,
where every connector is certified against a shared conformance suite,
crash-safety is proven by live fail-point sweeps, and performance claims
are enforced by a benchmark gate. Breadth — orchestration, scheduling,
catalogs of connectors — belongs to products built on top, not here.

**Status:** pre-1.0 (`0.3.0`), not yet on crates.io — depend on the git
repository. Connectors are separate binaries from the sibling
[rdlt-connectors](https://github.com/rapidbyte-io/rdlt-connectors)
repository; this repository ships the engine, the CLI, and the reference
connector its own gates spawn.

## The model in three lines

- **One YAML/JSON document describes a pipeline**: settings, a source arm,
  a destination arm. Each arm names a connector by id and gives it a
  config document the engine never interprets.
- **Connectors are processes.** `io.rapidbyte.postgres` resolves to a
  `rdlt-connector-postgres` binary on `PATH` (or `path:` names one); the
  runtime spawns it per run, handshakes over a local socket, and the
  connector's own gate validates its config — refusals arrive in the
  connector's wording. The engine knows no connector by name.
- **`document::build` hands the engine's boundary — the `Pipeline`
  builder — a source value and a destination value.** In production those
  are the runtime's process adapters; hand-rolled `impl Source` /
  `impl Destination` values are test doubles.

## Quickstart — CLI

```sh
# build the CLI and this repo's reference connector, put both on PATH
cargo build --release -p rdlt-cli
cargo build --release -p rdlt-connector-reference --features bin-serve
export PATH="$PWD/target/release:$PATH"

rdlt validate pipeline.yaml     # the run's gates (spawn + handshake), without the run
rdlt run pipeline.yaml          # live progress on a terminal, one line per event in CI
```

Where `pipeline.yaml` is, for example:

```yaml
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

`run` prints the exactly-once totals; the full JSON report goes to stdout
when redirected, or to `--report <path>`. `--events <path|->` captures the
typed event feed as NDJSON (`-` needs `--report`, so the two machine
outputs never interleave); `-q` silences the feed, `-v` adds detail,
`--no-progress` forces the line-per-event form, `--output auto|plain|json`
picks the mode explicitly (`json` = no feed, report JSON on stdout even on
a terminal), `--color` follows `NO_COLOR` under `auto`.
`rdlt schema <full-id-or-path>` prints a connector's config JSON Schema
by spawning it and asking. Exit codes are stable: 0 success · 2 config ·
3 schema · 4 source · 5 destination · 6 WAL/disk · 7 cancelled ·
64 usage · 70 internal defect · 74 file I/O.

## Quickstart — library

```toml
[dependencies]
rdlt = { git = "https://github.com/rapidbyte-io/rdlt", version = "0.3" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Read, parse, build, run. Every configuration refusal dies at `build`,
before a row moves; `run` consumes the pipeline (a run is single-shot —
build the same document again to resume after a crash or cancellation).

```rust
use std::path::Path;

use rdlt::document;
use rdlt::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new("pipeline.yaml");
    let doc = document::parse(&document::read(path)?)?;
    let base = path.parent().unwrap_or(Path::new(""));   // path-form configs resolve here

    let pipeline = document::build(&doc, base).await?;   // spawns + handshakes both connectors

    let mut events = pipeline.events();                  // subscribe BEFORE run()
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {    // RunStarted, StreamStarted, BatchLoaded,
            eprintln!("{event:?}");                      // Committed, Discarded, StreamFinished, …
        }
    });
    let cancel = pipeline.cancellation_token();          // cancel.cancel() at any instant == a crash
    let _ = cancel;

    match pipeline.run().await {
        Ok(report) => println!("{} rows in {} ms", report.total_rows(), report.elapsed_ms),
        Err(Error::Cancelled) => println!("cancelled — build again to resume"),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
```

**The document can come from anywhere.** `document::parse` takes YAML or
JSON text (JSON is valid YAML). If your host already holds the pipeline
as a value, deserialize it directly; if it composes pipelines in code,
build the `Document` itself — no text involved:

```rust
use rdlt::document::{Config, Document, WriteMode, connector::Connector};

// (a) JSON text
let doc = rdlt::document::parse(r#"{"pipeline":"p","source":{"connector":{"id":"io.rapidbyte.reference","config":{"path":"a.jsonl"}}},"destination":{"connector":{"id":"io.rapidbyte.reference","config":"./dest.yaml"}}}"#)?;

// (b) a serde_json::Value you already have
let doc: Document = serde_json::from_value(value)?;

// (c) constructed
let doc = Document {
    pipeline: "p".into(),
    workdir: None,
    write_mode: Some(WriteMode::Append),
    batch_policy: None,
    commit_policy: None,
    source: Connector { id: "io.rapidbyte.reference".into(), version: None, path: None,
                        config: Config::Inline(serde_json::json!({ "path": "a.jsonl" })) },
    destination: Connector { id: "io.rapidbyte.reference".into(), version: None, path: None,
                             config: Config::Path("./dest.yaml".into()) },
};
```

The pieces, and where to look:

| You want to… | Use |
|---|---|
| decide how a connector id becomes a process (a pool, a remote scheduler, per-tenant sandboxing) | `document::build_with(&doc, base, &provider)` with your `impl rdlt::runtime::provider::Provider`; `build` uses the runtime's default local provider (spawn from `PATH`) |
| set policies in code instead of the document | `pipeline::Pipeline::builder(name)` — `write_mode`, `write_mode_for`, `schema_policy`, `batch_policy`, `commit_policy`, `workdir`, byte/stream budgets — fed with the source/destination values `build_with` would construct |
| name the vocabulary | `rdlt::{commit, cursor, error, event, id, metrics, policy, report}`; `use rdlt::prelude::*` glob-imports what a pipeline author touches (never `Error` — spell `rdlt::error::Error`) |
| read the outcome | `run()` → `report::Run`: per-table rows/bytes/discards, commits, retries, elapsed — the same record the CLI prints as JSON |
| watch it live | `pipeline.events()` (typed `event::PipelineEvent`, lossy under a slow consumer — the report is the complete record) and the `metrics::Metrics` fold; the `tracing` span contract is in [docs/telemetry.md](docs/telemetry.md) |
| handle failure | `rdlt::error::Error` is a closed taxonomy — `Config`, `Schema`, `Source`, `Destination`, `Wal`, `Cancelled`, `Internal` — the CLI's exit codes mirror it; `document::Error` splits construction into `Resolve` (the document) and `Build` (the engine) |
| write a connector | `rdlt::sdk` (the connector SDK) and `rdlt::sdk::spi` (the wire-side traits) — [docs/connector-authoring.md](docs/connector-authoring.md) |

## The pipeline document

| Key | Meaning |
|---|---|
| `pipeline` | stable name; state, cursors, WAL and receipts are keyed on it — renaming starts a fresh pipeline |
| `workdir` | where the write-ahead log lives (default `.rdlt/<pipeline>` beside the document); one workdir per pipeline; unset means no WAL — recovery still works by re-extracting from the last committed cursor |
| `write_mode` | `append` (default), `replace`, or `merge: {key: [col, …]}`; per-stream overrides via the builder |
| `batch_policy` | how many rows/bytes the engine accumulates before each destination write (`{every_rows: N}` / `{every_bytes: N}`) — memory/throughput, destination-agnostic |
| `commit_policy` | when accumulated rows are committed — durability, i.e. what a crash costs; a batch never spans a commit |
| `source` / `destination` | `connector: {id, config, version?, path?}` — the ONLY arm form; `config` is the connector's own document, inline or a path (YAML/JSON) resolved relative to the pipeline file |

The config inside an arm is opaque to rdlt: it crosses the wire in the
handshake and the connector validates it. Runnable documents for every
first-party connector live in the connectors repository's `examples/`;
any still written with the older per-connector short spellings
(`postgres:`, `rest:` …) rewrite as `connector: {id: io.rapidbyte.<name>,
config: <the same document>}`.

## What it guarantees

- **Exactly-once publication.** Writes stage invisibly and publish
  atomically with pipeline state, idempotent per `(load_id, commit_seq)`;
  a crash at any point resumes without duplicating or losing rows.
  Commits land at source checkpoints, so a resumed run never re-extracts
  rows it already published.
- **Schema evolution under a policy you choose** — evolve, freeze, discard
  row, discard value — per pipeline, table, or column; a frozen contract
  refuses the change with a typed error naming the column and both types.
- **Nothing silent.** Discards are counted, refusals are typed, retries are
  bounded and reported; structured (Arrow) input is refused rather than
  rounded, wrapped, or nulled. Backpressure is by bytes, so peak memory is
  capped whatever the rows look like. The trust model and the ceilings
  that back it are in [SECURITY.md](SECURITY.md).

## Performance

Every figure is a recorded, competitor-paired session under
[`benches/`](benches/RESULTS.md) — nothing is quoted alone. On the
2026-08-14/16 recordings (spawned connectors, byte-bounded), rdlt vs dlt:
`pg-to-pg-1m` 9.9×, `s3jsonl-to-pg-200k` 79×, `s3jsonl-to-s3parquet-200k`
57×, `pg-to-pg-dedup-1m` 2.6× — all four bars held (`TARGET=gate make
bench`). A 1M-row Postgres→Postgres copy runs in about 1.1 s on the
reference machine.

Aggregate throughput comes from running **concurrent pipelines**, not from
parallelism inside one: a full-refresh load puts the rows and the clearing
of the old ones in one transaction on one connection — that is what makes
a reload atomic — so rdlt does not parallelise a single load. Need more?
Run more pipelines.

Parquet destinations write snappy-compressed files by default
(`config: {format: parquet, parquet: {compression: uncompressed | gzip |
zstd | brotli | lz4_raw}}` to change it; `compression_level`,
`dictionary_enabled`, `dictionary_page_size_limit`, `data_page_size_limit`,
`max_row_group_rows` are the other knobs).

## Repository layout

| Crate | Role |
|---|---|
| `rdlt` | the facade — the crate you depend on |
| `rdlt-cli` | the `rdlt` binary |
| `rdlt-engine` | the deep module: shred → schema → WAL → load |
| `rdlt-core` | the vocabulary that crosses boundaries (ids, errors, events, report, commit policies) |
| `rdlt-connector` | the SPI (wire-side traits and gates) |
| `rdlt-connector-sdk` | the connector-authoring framework (`serve` runs a connector as a process) |
| `rdlt-connector-protocol` / `-client` / `rdlt-runtime` | the wire protocol, the engine-side adapter, process lifecycle |
| `rdlt-connector-reference` | the reference connector — the exemplar third-party connectors copy |
| `rdlt-testkit` / `rdlt-certify` | conformance kits and the wire-side certifier |
| `rdlt-bench` | the benchmark harness (dev-only) |

```sh
make lint     # fmt + clippy, warnings are errors
make test     # cargo nextest + doc-tests (container suites skip without a runtime)
make check    # everything a PR must pass
```

Design records live in `docs/` (ADRs, telemetry, connector authoring);
benchmark governance under `benches/`.

## License

Apache-2.0
