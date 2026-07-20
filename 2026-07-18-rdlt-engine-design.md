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
| Vocabulary seam | Separate `rdlt-core`: pure data contracts + semantic laws | Types folded into the SPI crate (earlier draft); engine-private types |
| Semantics | Clean-room (`_rdlt_*` lineage, struct preservation, widening lattice) | dlt-compatible semantics |
| v1 vertical slice | Declarative REST source → DuckDB + Postgres | Files-first performance showcase; SQL replication first |

## 3. Architecture

```
Source (REST/SQL/files)
   │  push: raw_json(bytes) | rows(JSON) | arrow(RecordBatch) | checkpoint(cursor)
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

Two runtime rules that the RSS and throughput targets (§8) depend on:
- Channels are bounded by **bytes, not batch count** — peak memory is capped regardless of
  row width; a batch-count bound silently scales RSS with schema size.
- The shredder is CPU-bound and runs on a dedicated thread pool; tokio hosts the I/O stages
  only. Parse-heavy work on the async runtime would starve source/destination I/O and
  serialize the pipeline it exists to parallelize. Arrow batches pushed by sources bypass
  the shredder entirely (schema-check only) — the parquet→parquet passthrough path is
  zero-copy.

### 3.1 Workspace layout

```
crates/
├── rdlt-core           # Vocabulary: ids (PipelineId, LoadId, StreamName, TableName), LogicalType
│                       #   + widening lattice `widen(a,b)`, TableSchema, SchemaDelta + content
│                       #   hashing, Cursor, StateDoc, CommitMeta/CommitReceipt, WriteMode,
│                       #   contracts, naming/normalization, RunReport, PipelineEvent.
│                       #   Pure data + pure functions; deps ≈ serde + arrow-schema. SEMVER-SACRED.
├── rdlt-connector      # In-process SPI: Source/Destination/LoadSession traits, RecordsOut,
│                       #   StreamSpec, DestCapabilities, SourceError/DestError taxonomy.
│                       #   Re-exports rdlt-core.                                  SEMVER-SACRED.
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

Dependency arrows only point one way:
`rdlt-core ← rdlt-connector ← {connectors, rdlt-testkit, rdlt-engine}; rdlt-engine ← rdlt ← rdlt-cli`.
Connectors physically cannot reach engine internals. `cargo semver-checks` gates both
`rdlt-core` and `rdlt-connector` in CI from day one.

Design rationale (two sacred seams, one deep module):
- `rdlt-core` is the **vocabulary seam** — every type that is persisted, reported, or will
  cross a wire (WAL manifest entries, destination-persisted `StateDoc`, `RunReport`,
  `SchemaDelta`), plus the product's semantic laws as pure functions (lattice join,
  `_rdlt_id`/schema content hashing, identifier normalization). These outlive any trait:
  rapidbyte platform code that only reads reports/state links core, not the SPI or engine,
  and a future process/WASM connector host speaks core types over a wire without ever
  seeing a Rust trait. **Charter test: if it needs tokio, I/O, or arrow compute, it does
  not go in core** — that keeps core from becoming a dumping ground.
- `rdlt-connector` is the **in-process SPI seam**: the traits and their adjuncts
  (capabilities, connector error taxonomy). Traits churn faster than vocabulary (new
  methods, new capability fields); splitting the crates means trait evolution never
  version-bumps the on-disk formats. An earlier draft folded the vocabulary into the SPI
  crate — rejected once persisted-format semver and platform (non-engine) consumers were
  weighed.
- The engine is one **deep module** whose internal seams (`shred`, `wal`, `state`,
  `schema`) are `pub(crate)` — unit-tested privately, free to churn without semver cost.
  A god object is an interface problem, not a line-count problem.

### 3.2 Engine module tree (`rdlt-engine`, all `pub(crate)` unless noted)

```
src/
├── lib.rs        # pub: Engine entry, RunReport, PipelineEvent, RdltError
├── runtime/      # task graph, bounded channels, retry driver, cancel token
├── shred/
│   ├── infer.rs  # type inference; drives rdlt-core's widen() lattice, value-checked
│   ├── nest.rs   # struct preservation, child-table splitting, lineage ids
│   └── build.rs  # Arrow columnar builders (raw-JSON bytes → buffers, no Value tree)
├── schema/       # registry, diffing → SchemaDelta, contracts (types live in rdlt-core)
├── wal/          # parquet segments + manifest, resume scan, GC
├── state/        # cursor/state docs (serde), round-trip through destination
└── load/         # commit protocol, migration planning vs DestCapabilities
```

## 4. Public interfaces

### 4.1 Connector SPI (`rdlt-connector`)

Vocabulary types below (`Cursor`, `TableSchema`, `CommitMeta`, ids, …) are defined in
`rdlt-core` and re-exported here; this crate adds the traits.

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
    /// Perf path: raw JSON bytes (one document, an array, or NDJSON). The shredder parses
    /// straight into Arrow builders — no serde_json::Value tree is ever materialized.
    /// Sources that already hold bytes (HTTP bodies, files) should always use this.
    pub async fn raw_json(&mut self, bytes: Bytes) -> Result<(), ChannelClosed>;
    /// Convenience path for sources that construct rows programmatically.
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
    /// Create/migrate physical table and record its write disposition (`mode` is the
    /// root stream's mode for child tables — merge applies it at commit time).
    /// Idempotent; called before first write at each schema version.
    async fn ensure_table(&mut self, schema: &TableSchema, mode: &WriteMode) -> Result<(), DestError>;
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
- Table ownership: exactly one stream writes a given table (and its child tables) per run;
  the engine rejects colliding stream→table mappings at build time, so destinations never
  need cross-stream ordering logic.
- Staging teardown: writes are staged and invisible until `commit`; `open` MUST make any
  uncommitted staged data from a previous (crashed) session invisible and reclaimable.
  This is what makes at-least-once delivery to `write` safe — replay after a crash may
  re-send batches that were written but never committed.
- Backpressure: awaiting `raw_json()/rows()/arrow()` **is** the flow control.
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
  part of the product-facing contract. `PipelineEvent`/`RunReport` live in `rdlt-core`, so
  rapidbyte platform code can persist and render them without linking the engine.

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
Per column, types move only upward. The join `widen(a, b)` is a pure function in
`rdlt-core`, property-tested for monotonicity, commutativity, and idempotence:
`Null → T` for any `T`; `Int64 → Float64 → Utf8`; `Int64 → Decimal(p,0) → Decimal(p,s) → Utf8`;
`Bool → Utf8`; temporal → `Utf8`; `Float64 ⊔ Decimal → Utf8` (Decimal is **not** a superset
of Float64 — NaN/±Inf and the exponent range don't fit, so a `Float64 → Decimal` edge would
be a silent-corruption bug); any irreconcilable conflict → `Json`. JSON inference never
produces `Decimal` (JSON numbers arrive as Int64/Float64); `Decimal` enters only via
per-column hints or Arrow-native sources.

Widening is **value-checked, not just type-checked**: `Int64 → Float64` is exact only
within ±2^53, so the shredder verifies every converted value and escalates the column to
`Utf8` at the first inexact one — losslessness is enforced at runtime, never assumed.
Widenings to `Utf8` use canonical renderings (RFC 3339 for temporal, `true`/`false` for
bool, shortest-round-trip for floats), so they are lossless as text and byte-deterministic
across runs.

**Divergence from dlt:** type conflicts widen one column; they never multiply columns (no
`col__v_text` variants). Widening is a `SchemaDelta` flowing through normal evolution.
String→timestamp detection: unambiguous ISO-8601 with timezone only; per-column
`StreamSpec` hints override inference (exposed in REST source YAML).

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
new child ids for merge), `_rdlt_parent_id` + `_rdlt_pos` on child tables, and
`_rdlt_root_id` (the root row's `_rdlt_id`) on child tables at **every** nesting depth.

`_rdlt_root_id` is what makes `Merge` correct for nested data: merging a root row replaces
its entire subtree — delete descendants by root id, insert the new ones — with no
parent-chain walk at any depth (grandchildren are unreachable through `_rdlt_parent_id`
alone without a join per level). Note the keyless case: content-hash `_rdlt_id` means
byte-identical rows collapse to one under `Merge` — that is the documented dedup
semantics, not a bug.

Schemas are content-hashed; every `SchemaDelta` carries `from_hash → to_hash` (auditable,
replayable). Contracts per table/column/type: `Evolve | Freeze | DiscardRow | DiscardValue`;
`Freeze` turns a would-be delta into a typed error before any row is written; discards are
counted, never silent.

**Row-id hash decision (feature 003, FR-008 — recorded 2026-07-20):** the
incumbent blake3 stays. Measured on the iai instruction benches: keyed identity
20.5 M instr / 10k rows, keyless 29.3 M / 10k; total blake3 work is ~16% of the
shred stage (531 M instr / 10k rows post-optimization), which bounds any hash
swap's flagship end-to-end effect at well under the >30% switch threshold the
feature clarified. xxh3-128 would be a real but small win at the cost of
changing every persisted `_rdlt_id` before release; incumbent stability wins.
The algorithm is FROZEN at the first release tag.

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
protocol; (5) uncommitted staged data from a dead session is torn down by the next `open`
(§4.1) — at-least-once delivery is safe *because* nothing is visible until `commit`.

Durability honesty: fsync-at-commit-boundaries means crash-matrix row 2 covers process
crashes; on power loss, un-fsynced WAL segments may vanish and recovery degrades to row 4
(re-extract from the last committed cursor) — slower, never wrong. Non-seekable sources (queues/webhooks): frequent checkpoints shrink the
redelivery window; `Merge` mode dedups on `_rdlt_id`, restoring effective exactly-once.

## 7. Testing strategy

1. **Semantic-law property tests** (proptest): lattice laws (monotone, commutative,
   idempotent joins), deterministic `_rdlt_id`, collision-safe naming — these live in
   `rdlt-core` beside the pure functions they verify. Shredder round-trip tests
   (lossless shred→lower→reassemble, guaranteed by value-checked widening + `Json`
   fallback) live in `rdlt-engine`. Clean-room semantics as executable laws.
2. **Crash-injection recovery suite** (highest value): fault points at every crash-matrix
   row; restart; assert exactly-once visibility and no cursor skips. MemoryDestination +
   tempdir WAL; deterministic, runs in normal CI.
3. **Connector conformance suites** (`rdlt-testkit`, public): resume-from-cursor,
   checkpoint ordering, `ensure_table`/`commit` idempotency, staging teardown on `open`
   after a simulated crash. Our three connectors pass them in CI; rapidbyte's catalog
   inherits the gate ("certified = passes conformance").
4. **Integration:** DuckDB in-process; Postgres via testcontainers; REST via wiremock.
5. **End-to-end + benchmarks:** see §8; criterion microbenches on the shredder per-PR.

CI gates: `cargo semver-checks` on `rdlt-core` and `rdlt-connector`, clippy `-D warnings`,
rustfmt, doc-tests on all public examples.

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
allocation — the Rust shredder parses raw JSON bytes (`RecordsOut::raw_json`) straight
into contiguous Arrow buffers, materializing no intermediate value tree (an
allocation-count argument, not a language-benchmark argument). Arrow-path and destination-bound cases are
published honestly (2–5x): real API syncs are bounded by the API; the mock-REST benchmark
shows the engine ceiling with methodology stated.

## 9. v1 scope summary

- `rdlt-core`, `rdlt-connector`, `rdlt-engine`, `rdlt` facade, `rdlt-testkit`, `rdlt-cli`
- `rdlt-source-rest` (declarative: base URL, auth, pagination strategies, cursor field,
  per-column type hints)
- `rdlt-dest-duckdb` (Arrow ingestion, STRUCT support), `rdlt-dest-postgres` (binary COPY,
  flatten lowering, staging+merge)
- Write modes: `Append`, `Replace`, `Merge { key }`
- Schema policies: `Evolve` (default), `Freeze`, `DiscardRow`, `DiscardValue`
- Benchmark harness + CI as above

**Not in v1:** transformers/derived streams, CDC, WASM/process connector hosts, object-store
WAL spill, Python bindings, SQL sources (fast follow), vector destinations.

**Delivered post-v1 (feature 002, 2026-07-19):** bundled file source (JSONL + Parquet,
glob + per-file incremental cursors), minimal parquet-file destination, and engine
Arrow passthrough (structured streams: schema mapping + policy + `_rdlt_load_id` only;
Merge rejected — clauses S7/E7/B4). The §8 "Arrow passthrough: parquet → parquet"
benchmark cell is realized via the parquet destination, with a parquet→DuckDB bonus
row for context.

## 10. Key dependencies

`arrow` + `parquet` (arrow-rs; `rdlt-core` depends only on `arrow-schema`), `tokio`,
`serde`/`serde_json`, `async-trait`, `thiserror`, `tracing`, `reqwest` (REST source),
`duckdb` (bundled), `tokio-postgres`, `proptest`, `criterion`, `wiremock`,
`testcontainers` (dev). The raw-JSON shred path parses with `serde_json`'s streaming
deserializer first; a `simd-json` feature is a measured follow-up, not a v1 dependency. No DataFusion in v1 (query-engine
impedance mismatch with evolve-mid-stream ingestion; possible later for post-load SQL).
