# rdlt

A library-first ELT engine: extract → shred (normalize) → load, with schema
inference and evolution, incremental cursors, and crash-safe resumable runs.

This is the facade — the crate to depend on. It re-exports the engine, the
vocabulary, and the bundled connectors behind features.

```rust,no_run
use rdlt::prelude::*;

# async fn demo() -> Result<(), RdltError> {
let pipeline = Pipeline::builder("demo")
    .source(my_source)
    .destination(my_destination)
    .write_mode(WriteMode::Append)
    .build()?; // configuration errors die here, before any I/O
let report: RunReport = pipeline.run().await?;
# Ok(())
# }
```

Configuration errors surface at `build()`, not halfway through a load.

## Connectors

Each is a feature, so you compile only what you use:

| Feature | Connector |
|---|---|
| `postgres` / `postgres-dest` | PostgreSQL source (binary COPY → Arrow, plus logical-replication CDC) and destination |
| `duckdb` | DuckDB destination |
| `file` | JSONL / Parquet / CSV source and destination, local or S3 |
| `rest` | declarative REST source — one YAML document describes an API |
| `iceberg` | Apache Iceberg destination over a REST catalog |

## What it guarantees

- **Exactly-once publication.** Writes stage invisibly and publish atomically
  with pipeline state, idempotent per `(load_id, commit_seq)`. A crash at any
  point resumes without duplicating or losing rows.
- **Schema evolution under a policy you choose.** Evolve, Freeze,
  DiscardRow, or DiscardValue — per pipeline, per table, or per column. A
  frozen contract refuses the change with a typed error naming the column and
  both types.
- **Nothing silent.** Discards are counted, refusals are typed, retries are
  bounded and reported.

## Scope

rdlt stays small on purpose: one engine and a few well-verified connectors,
each with a real test suite behind it. Breadth belongs to products built on
top, not to the engine.
