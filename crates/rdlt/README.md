# rdlt

A library-first ELT engine: extract → shred (normalize) → load, with schema
inference and evolution, incremental cursors, and crash-safe resumable runs.

This is the facade — the one crate to depend on. It names the vocabulary,
owns the pipeline document, and constructs pipelines over connectors that
run out of process, spawned per run. It knows no connector by name.

## The document path

A pipeline is ONE YAML document: pipeline-wide settings, a source arm and
a destination arm, each arm naming a connector by id:

```yaml
pipeline: shop-orders
write_mode: replace
source:
  connector:
    id: io.rapidbyte.postgres
    config:
      conn: "host=127.0.0.1 dbname=shop"
      tables: [{ name: orders }]
destination:
  connector:
    id: io.rapidbyte.duckdb
    config: { path: out/shop.db }
```

Construct it from the file, run it — construction spawns and handshakes
both connectors, and every configuration refusal dies there, before a row
moves:

```rust,no_run
use rdlt::error::Error;
use rdlt::pipeline::Pipeline;

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let pipeline = Pipeline::from_file("pipeline.yaml").await?;
match pipeline.run().await {
    Ok(report) => println!("{} rows", report.total_rows()),
    Err(Error::Cancelled) => println!("cancelled — build again to resume"),
    Err(error) => return Err(error.into()),
}
# Ok(())
# }
```

The id's last segment names the binary (`rdlt-connector-postgres` on
PATH; `path:` overrides), the config document crosses the wire opaquely,
and the connector's own gate validates it — refusals arrive in the
connector's own wording. `config` is given inline (as above) or as a path
string (`config: ./creds.yaml`) to a YAML/JSON document of the same
shape, resolved beside the pipeline document. The first-party connector
binaries are built and installed from the sibling
[rdlt-connectors](https://github.com/rapidbyte-io/rdlt-connectors)
repository.

## The builder is the boundary

`Pipeline::from_file` — and its siblings `from_text` (YAML or JSON text)
and `from_document` (a parsed or constructed `document::Document`) —
hand the engine's boundary — the `Pipeline` builder — a source value and
a destination value. In production those values are the runtime's
process adapters over the spawned connectors; an embedder with its own
provider (a pool, a remote scheduler) supplies it through
`Pipeline::from_document_with`. Hand-rolled `impl Source` /
`impl Destination` values are test doubles. Missing halves are a compile
error; every configuration refusal is a typed error at construction, not
halfway through a load.

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
