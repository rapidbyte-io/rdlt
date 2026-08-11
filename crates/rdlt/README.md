# rdlt

A library-first ELT engine: extract → shred (normalize) → load, with schema
inference and evolution, incremental cursors, and crash-safe resumable runs.

This is the facade — the crate to depend on. It re-exports the engine and
the vocabulary; connectors run out of process, spawned per run.

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

Connectors are separate binaries, spawned per run and supervised over a
local socket — none are compiled into this crate. A pipeline document
names one by its rich spelling (`postgres:`, `duckdb:`, `file:`,
`rest:`, `oracle:`, `iceberg:`, `snowflake:`):

```yaml
source:
  postgres:
    conn: "host=127.0.0.1 dbname=shop"
    tables: [{ name: orders }]
```

or explicitly, which is what the rich form desugars to:

```yaml
source:
  connector:
    id: io.rapidbyte.postgres
    config: { conn: "host=..." }
```

Both forms resolve identically: the id's last segment names the binary
(`rdlt-connector-postgres` on PATH, or `path:` overrides), the config
document crosses the wire opaquely, and the connector's own gate
validates it — refusals arrive in the connector's own wording. The
config is given inline (as above) or as a path string
(`config: ./creds.yaml`) pointing at a YAML/JSON document of the same
shape — never both at once.

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
