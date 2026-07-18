# rdlt — Rust Data Ingestion Engine: Design

**Date:** 2026-07-18
**Status:** Approved design, pre-implementation
**Reference implementation studied:** Python `dlt` (checked out at `../dlt`)

## 1. Purpose & positioning

rdlt is a **library-first ELT engine in Rust**: extract → shred (normalize) → load, with
schema inference/evolution, incremental cursors, and crash-safe resumable runs. It is the
foundation crate for **rapidbyte**, a future Airbyte/Fivetran-like platform, and must remain
embeddable in any product (CLI, Lambda, server).

It is inspired by Python dlt's architecture but is a **clean-room design**: rdlt owns its
data semantics (lineage columns, nesting model, naming) and diverges where Arrow-first
design allows something strictly better.

**In scope (engine):** correct, fast, resumable data movement; schema evolution; incremental
state; connector SPI; typed observability; connector conformance testing.
**Out of scope (platform concerns, deliberately):** scheduling, multi-tenancy, isolation
(run one process per pipeline), secrets management, catalog UI, user auth.

## 2. Decisions log

| Decision | Choice | Rejected alternatives |
|---|---|---|
| Product shape | Core Rust library + thin dev CLI | Python bindings first; dlt drop-in accelerator |
| Connector model | In-process Rust traits (SPI crate) | WASM sandbox; Airbyte-style process protocol (possible later: SPI is object-safe & serde-friendly) |
| Data plane | Streaming Arrow `RecordBatch` through bounded channels, checkpointed WAL spill | Disk-staged packages (dlt model); pure in-memory |
| Engine core | Arrow-first with micro-batch JSON shredder | Row-centric engine; DataFusion substrate |
| Semantics | Clean-room (`_rdlt_*` lineage, struct preservation, widening lattice) | dlt-compatible semantics |
| v1 vertical slice | Declarative REST source → DuckDB + Postgres | Files-first performance showcase; SQL replication first |

## 3. Architecture

```
Source (REST/SQL/files)
   │  push: rows(JSON) | arrow(RecordBatch) | checkpoint(cursor)
   ▼
Shredder ── schema inference + evolution ──► SchemaRegistry
   │  RecordBatch keyed (load_id, table, seq) + SchemaDelta stream
   ▼
WAL writer (parquet segments + manifest)          ◄── replay on resume
   │  bounded async channel (backpressure)
   ▼
Loader ──► Destination LoadSession
              ensure_table(delta) → write(batch)* → commit(meta)  [atomic: data + state]
```

All stages run concurrently as tokio tasks over bounded channels: wall-clock approaches
`max(stage times)` rather than dlt's `sum(stage times)`; backpressure is intrinsic (a slow
destination slows extraction — no config).

### 3.1 Workspace layout

```
crates/
├── rdlt-connector      # SPI + vocabulary: Source/Destination/LoadSession traits, StreamSpec,
│                       #   Cursor, TableSchema, DestCapabilities, error taxonomy. SEMVER-SACRED.
├── rdlt-engine         # Deep module: shred/ schema/ wal/ state/ runtime/ load/ — all pub(crate).
│                       #   Public surface ≈ Engine::run + RunReport + PipelineEvent + RdltError.
├── rdlt                # Facade: Pipeline::builder (typestate), prelude, re-exports;
│                       #   features = ["rest", "duckdb", "postgres"].
├── rdlt-testkit        # MemorySource/MemoryDestination, conformance suites, run harness.
├── rdlt-source-rest    # Declarative (YAML/JSON-configured) REST source.  → rdlt-connector only
├── rdlt-dest-duckdb    # DuckDB destination (Arrow ingestion).            → rdlt-connector only
├── rdlt-dest-postgres  # Postgres destination (binary COPY).              → rdlt-connector only
└── rdlt-cli            # Thin dev CLI over the facade (TOML spec → run → report).
```

Dependency arrows only point one way: `connectors → rdlt-connector ← rdlt-engine ← rdlt ← rdlt-cli`.
Connectors physically cannot reach engine internals. `cargo semver-checks` gates
`rdlt-connector` in CI from day one.

Design rationale (deep-module vocabulary): the SPI crate is the **external seam** serving two
audiences (embedders, connector authors); the engine is one **deep module** whose internal
seams (`shred`, `wal`, `state`, `schema`) are `pub(crate)` — unit-tested privately, free to
churn without semver cost. A god object is an interface problem, not a line-count problem.
A separate `rdlt-model` crate was considered and rejected (fails the deletion test — same
audience and release cadence as the SPI crate).

### 3.2 Engine module tree (`rdlt-engine`, all `pub(crate)` unless noted)

```
src/
├── lib.rs        # pub: Engine entry, RunReport, PipelineEvent, RdltError
├── runtime/      # task graph, bounded channels, retry driver, cancel token
├── shred/
│   ├── infer.rs  # type inference + widening lattice
│   ├── nest.rs   # struct preservation, child-table splitting, lineage ids
│   └── build.rs  # Arrow columnar builders
├── schema/       # registry, diffing → SchemaDelta, contracts, version hashing
├── wal/          # parquet segments + manifest, resume scan, GC
├── state/        # cursor/state docs (serde), round-trip through destination
└── load/         # commit protocol, migration planning vs DestCapabilities
```

## 4. Public interfaces

### 4.1 Connector SPI (`rdlt-connector`)

```rust
#[async_trait]
pub trait Source: Send + Sync + 'static {
    fn spec(&self) -> ConnectorSpec;                       // name, version, config JSON-schema
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError>;
    /// Read ONE stream, pushing through `req.out`. Engine schedules streams concurrently.
    async fn read(&self, req: ReadRequest<'_>) -> Result<(), SourceError>;
}

pub struct ReadRequest<'a> {
    pub stream: &'a StreamSpec,
    pub since:  Option<&'a Cursor>,   // last committed cursor; source MUST resume from it
    pub out:    RecordsOut,           // push handle
}

impl RecordsOut {
    pub async fn rows(&mut self, rows: impl IntoIterator<Item = serde_json::Value>) -> Result<(), ChannelClosed>;
    pub async fn arrow(&mut self, batch: RecordBatch) -> Result<(), ChannelClosed>;
    /// "All rows pushed so far are complete up to `cursor`."
    pub async fn checkpoint(&mut self, cursor: Cursor) -> Result<(), ChannelClosed>;
}

#[non_exhaustive]
pub enum SourceError {
    Transient(BoxError),                       // engine retries with backoff + jitter
    RateLimited { retry_after: Option<Duration>, source: BoxError },
    Fatal(BoxError),                           // run aborts
}

#[async_trait]
pub trait Destination: Send + Sync + 'static {
    fn spec(&self) -> ConnectorSpec;
    fn capabilities(&self) -> DestCapabilities; // merge support, type matrix, ident rules, nesting support
    async fn open(&self, ctx: OpenCtx<'_>) -> Result<Box<dyn LoadSession>, DestError>;
}

#[async_trait]
pub trait LoadSession: Send {
    /// Create/migrate physical table. Idempotent; called before first write at each schema version.
    async fn ensure_table(&mut self, schema: &TableSchema) -> Result<(), DestError>;
    /// Engine guarantees `batch` conforms to the last ensured schema — sessions never see drift.
    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError>;
    /// Atomically publish all writes since last commit AND persist `meta` (cursors, schema
    /// versions). Idempotent per (load_id, commit_seq): re-commit returns the prior receipt.
    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError>;
    /// Recover engine state persisted by a previous run's commit.
    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError>;
}
```

Contract highlights (part of the interface, enforced by conformance suite):
- Ordering: rows between two `checkpoint` calls form one recovery unit; per-table writes
  arrive in `seq` order; `ensure_table` precedes writes at a new version.
- Backpressure: awaiting `rows()/arrow()` **is** the flow control.
- Retries are engine-owned; connectors never write retry loops.
- Capabilities are **data, not traits** — the engine plans around them (fail-fast at build
  time, e.g. merge requested but unsupported).
- All exchange types are serde-serializable and traits are object-safe → a future
  process/WASM host adapter can implement the SPI over a wire without engine changes.

### 4.2 Embedder API (`rdlt` facade)

```rust
let mut pipeline = Pipeline::builder("github_issues")
    .source(RestSource::from_yaml(include_str!("github.yaml"))?)
    .destination(Postgres::connect(&url)?.dataset("raw"))
    .write_mode(WriteMode::Merge { key: &["id"] })     // default: Append
    .schema_policy(SchemaPolicy::evolve())             // default
    .workdir(".rdlt")                                  // default; holds WAL
    .build()?;                                         // typestate: missing source/dest = compile
                                                       // error; config errors die here, pre-I/O
let mut events = pipeline.events();                    // typed stream: StreamStarted, BatchLoaded,
                                                       //   SchemaEvolved{delta}, Committed{cursor}
let report: RunReport = pipeline.run().await?;         // resumable; cancel-safe (drop or token)
```

- `RunReport`: per-table rows/bytes, schema migrations applied, discarded-row counts,
  cursor positions, `resumed_from`, elapsed. Serde-stable (platforms persist it).
- Observability: typed `events()` stream for product UIs **plus** `tracing` spans
  (`rdlt.extract`, `rdlt.shred`, `rdlt.load`) for ops. Returned stream, not injected
  observer trait.
- `PipelineEvent`, `RunReport`, error enums: `#[non_exhaustive]` + stable serde — they are
  part of the product-facing contract.

### 4.3 Errors (embedder layer)

`RdltError::{Config, Schema(ContractViolation), Source{stream, source}, Destination{..}, Wal(..)}` —
each variant maps to an operator action (fix config / unfreeze contract / check API / check
warehouse / check disk). `thiserror` throughout; `anyhow` never crosses a public seam.
**No silent failures:** discards, widenings, and retries are counted in `RunReport` and
emitted as events.

## 5. Shredder & schema semantics (clean-room rules)

### 5.1 Logical types
`Bool, Int64, Float64, Decimal(p,s), Utf8, Binary, TimestampTz, TimestampNaive, Date, Time,
Uuid, Json` — mapped to Arrow physically, to destination types via `DestCapabilities`.
`Json` is the typed escape hatch (JSONB/JSON in destinations): undecomposable values are
preserved, never dropped, never exploded into variant columns.

### 5.2 Inference & widening lattice
Per column, types move only upward (lossless):
`Null → T`; `Int64 → Float64 → Decimal → Utf8`; `Bool → Utf8`; temporal → `Utf8`;
any irreconcilable conflict → `Json`. **Divergence from dlt:** type conflicts widen one
column; they never multiply columns (no `col__v_text` variants). Widening is a
`SchemaDelta` flowing through normal evolution. String→timestamp detection: unambiguous
ISO-8601 with timezone only; per-column `StreamSpec` hints override inference (exposed in
REST source YAML).

### 5.3 Nesting
- Nested **objects** stay Arrow `Struct` columns inside the engine; **lowering happens at the
  destination seam** driven by capabilities (DuckDB: real STRUCTs; Postgres v1: flatten at
  load boundary). Flattening is collision-safe: distinct source keys can never silently
  merge (deterministic hash suffix on collision).
- **Lists of objects** → child tables with lineage keys. **Lists of scalars** → Arrow `List`
  where supported, child table otherwise.

### 5.4 Lineage & identity
System columns: `_rdlt_load_id` (every row), `_rdlt_id` (deterministic row hash: content
hash without primary key, key hash with; propagates to children so nested changes produce
new child ids for merge), `_rdlt_parent_id` + `_rdlt_pos` on child tables.
Schemas are content-hashed; every `SchemaDelta` carries `from_hash → to_hash` (auditable,
replayable). Contracts per table/column/type: `Evolve | Freeze | DiscardRow | DiscardValue`;
`Freeze` turns a would-be delta into a typed error before any row is written; discards are
counted, never silent.

## 6. WAL, checkpoints & recovery

**Principle: the destination is the sole source of truth; the WAL is a replayable buffer.**
State commits atomically with data (`commit(meta)`), so correctness survives total loss of
the work directory; the WAL only makes recovery cheap.

- WAL segment = parquet file keyed `(load_id, table, seq)`; append-only manifest records
  segment → schema hash → covering source checkpoint. Schema deltas are manifested before
  any batch at the new version.
- `CommitPolicy` (N checkpoints | bytes | seconds) groups checkpoint spans into commit
  units; after a commit receipt, covered segments are GC'd. fsync at commit boundaries only.

Crash matrix:

| Died during | On restart |
|---|---|
| Extraction, pre-WAL | Read state from destination; resume source from last committed cursor |
| WAL written, pre-commit | Replay WAL into destination (no re-extraction), commit, resume from latest WAL'd checkpoint |
| Mid-commit | Idempotent per `(load_id, commit_seq)` — destination returns prior receipt |
| WAL lost/corrupt | Re-extract from last committed cursor — slower, never wrong |

Invariants: (1) delivery to `write` is at-least-once, visibility is exactly-once;
(2) a cursor is never committed unless every row it covers is in the same commit unit;
(3) replay preserves delta-before-batch order, so mid-run evolution survives crashes;
(4) **cancellation is a crash we chose** — one recovery path, no separate graceful-shutdown
protocol. Non-seekable sources (queues/webhooks): frequent checkpoints shrink the
redelivery window; `Merge` mode dedups on `_rdlt_id`, restoring effective exactly-once.

## 7. Testing strategy

1. **Shredder property tests** (proptest): lattice monotonicity, lossless
   shred→lower→reassemble (guaranteed by `Json` fallback), deterministic `_rdlt_id`,
   collision-safe naming. Clean-room semantics as executable laws.
2. **Crash-injection recovery suite** (highest value): fault points at every crash-matrix
   row; restart; assert exactly-once visibility and no cursor skips. MemoryDestination +
   tempdir WAL; deterministic, runs in normal CI.
3. **Connector conformance suites** (`rdlt-testkit`, public): resume-from-cursor,
   checkpoint ordering, `ensure_table`/`commit` idempotency. Our three connectors pass them
   in CI; rapidbyte's catalog inherits the gate ("certified = passes conformance").
4. **Integration:** DuckDB in-process; Postgres via testcontainers; REST via wiremock.
5. **End-to-end + benchmarks:** see §8; criterion microbenches on the shredder per-PR.

CI gates: `cargo semver-checks` on `rdlt-connector`, clippy `-D warnings`, rustfmt,
doc-tests on all public examples.

## 8. Benchmarks & performance targets

Method: measure the pinned-dlt baseline **first**, same hardware, same datasets, one-command
reproducible harness (dlt in a container). Benchmark suite is v1 scope, not an afterthought.

| Benchmark | Target vs Python dlt |
|---|---|
| Nested-JSON shred microbench (rows/s/core) | ≥ 20x |
| End-to-end: jsonl files → DuckDB | ≥ 10x |
| End-to-end: local mock REST API → Postgres (engine-bound) | ≥ 5x |
| Arrow passthrough: parquet → parquet | ≥ 2x |
| Peak RSS (file→DuckDB run) | ≤ 1/5th |
| Cold start to first row loaded | ≤ 1/20th |

Rationale: dlt's JSON-normalize hot path is per-row interpreted Python with per-value
allocation — the Rust shredder writes into contiguous Arrow buffers (an allocation-count
argument, not a language-benchmark argument). Arrow-path and destination-bound cases are
published honestly (2–5x): real API syncs are bounded by the API; the mock-REST benchmark
shows the engine ceiling with methodology stated.

## 9. v1 scope summary

- `rdlt-connector`, `rdlt-engine`, `rdlt` facade, `rdlt-testkit`, `rdlt-cli`
- `rdlt-source-rest` (declarative: base URL, auth, pagination strategies, cursor field,
  per-column type hints)
- `rdlt-dest-duckdb` (Arrow ingestion, STRUCT support), `rdlt-dest-postgres` (binary COPY,
  flatten lowering, staging+merge)
- Write modes: `Append`, `Replace`, `Merge { key }`
- Schema policies: `Evolve` (default), `Freeze`, `DiscardRow`, `DiscardValue`
- Benchmark harness + CI as above

**Not in v1:** transformers/derived streams, CDC, WASM/process connector hosts, object-store
WAL spill, Python bindings, SQL sources (fast follow), vector destinations.

## 10. Key dependencies

`arrow` + `parquet` (arrow-rs), `tokio`, `serde`/`serde_json`, `async-trait`, `thiserror`,
`tracing`, `reqwest` (REST source), `duckdb` (bundled), `tokio-postgres`, `proptest`,
`criterion`, `wiremock`, `testcontainers` (dev). No DataFusion in v1 (query-engine
impedance mismatch with evolve-mid-stream ingestion; possible later for post-load SQL).
