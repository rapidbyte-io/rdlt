# Phase 0 Research: rdlt — Data Ingestion Engine Library

**Date**: 2026-07-19
**Status**: Complete — no NEEDS CLARIFICATION markers remained in the Technical Context;
all decisions below were settled in the approved design
([`2026-07-18-rdlt-engine-design.md`](../../2026-07-18-rdlt-engine-design.md)), produced
after studying the Python `dlt` reference implementation (`../dlt`). This document
consolidates them in decision/rationale/alternatives form so downstream phases can cite
them as `R#`.

## R1 — Product shape: core Rust library + thin dev CLI

- **Decision**: Ship a library (facade crate `rdlt`) with a thin development CLI; no
  daemon, scheduler, or UI.
- **Rationale**: rdlt is the foundation for the rapidbyte platform and must embed in any
  product (CLI, Lambda, server). Platform concerns (scheduling, multi-tenancy, secrets,
  auth) stay out so the engine's correctness surface stays small.
- **Alternatives considered**: Python bindings first (defers the Rust-native audience,
  doubles the API surface to stabilize); dlt drop-in accelerator (chains us to dlt's
  semantics — rejected with the clean-room decision R6).

## R2 — Workspace: two sacred seams + one deep module (9 crates)

- **Decision**: `rdlt-core` (vocabulary: pure data + pure semantic functions),
  `rdlt-connector` (SPI traits), `rdlt-engine` (deep module, internals `pub(crate)`),
  `rdlt` facade, `rdlt-testkit`, three connectors, `rdlt-cli`. Both seam crates gated by
  `cargo semver-checks` in CI.
- **Rationale**: The vocabulary and the trait contract have different consumers and
  cadences. Persisted/wire formats (StateDoc, WAL manifest, RunReport) must not
  major-bump because a trait gained a method; rapidbyte platform code that only reads
  reports/state links `rdlt-core` alone; a future process/WASM connector host speaks core
  types over a wire without seeing Rust traits. Engine internals stay `pub(crate)` so they
  can churn without semver cost. Charter test keeping core honest: needs tokio, I/O, or
  arrow compute → not core.
- **Alternatives considered**: Types folded into the SPI crate (earlier draft — couples
  on-disk format semver to trait churn); engine-private types (platform consumers would
  have to link the engine); single mega-crate with modules (connectors could reach engine
  internals; no compiler-enforced seam).

## R3 — Connector model: in-process Rust traits

- **Decision**: `Source`/`Destination`/`LoadSession` async traits in `rdlt-connector`;
  object-safe, all exchange types serde-serializable.
- **Rationale**: In-process traits are the fastest and simplest host for v1's bundled
  connectors, and object-safety + serde-friendliness keep the door open: a future
  process or WASM host can adapt the same SPI over a wire without engine changes.
- **Alternatives considered**: WASM sandbox now (heavy build/runtime cost before any
  connector exists); Airbyte-style process protocol (serialization tax on the hot path;
  isolation is a platform concern, not an engine one).

## R4 — Data plane: streaming Arrow batches + WAL spill

- **Decision**: Push-based flow — sources push raw JSON bytes / rows / Arrow batches;
  shredder produces `RecordBatch`es keyed `(load_id, table, seq)`; byte-bounded channels
  connect concurrent stages; parquet-segment WAL buffers between shred and load.
- **Rationale**: Wall-clock approaches `max(stage times)` vs dlt's `sum(stage times)`;
  backpressure is intrinsic (awaiting the push is the flow control — no tuning knobs);
  byte-bounded channels cap RSS independent of row width. The WAL makes recovery cheap
  without being a correctness dependency (see R8).
- **Alternatives considered**: Disk-staged packages between stages (dlt model — serializes
  the stages, costs a full materialization per stage); pure in-memory (loses cheap
  recovery; RSS unbounded on destination stalls).

## R5 — Shredder: Arrow-first, raw-bytes parse path

- **Decision**: Micro-batch JSON shredder writing directly into Arrow columnar builders;
  primary ingest path is `raw_json(bytes)` (no `serde_json::Value` tree materialized);
  CPU-bound shredding on a dedicated thread pool, tokio reserved for I/O stages;
  source-pushed Arrow batches bypass the shredder (schema-check only).
- **Rationale**: The ≥20× shred target is an allocation-count argument — dlt's hot path
  allocates per value; contiguous builder writes don't. A `Value`-tree intermediate would
  reinstate per-value allocation and forfeit the target. Parsing on the async runtime
  would starve I/O and re-serialize the pipeline.
- **Alternatives considered**: Row-centric engine with terminal Arrow conversion (pays
  row→column pivot twice); DataFusion substrate (query-engine impedance mismatch with
  evolve-mid-stream ingestion; revisit post-load); `simd-json` in v1 (deferred — adopt
  behind a feature only if criterion shows serde_json's streaming deserializer short of
  target).

## R6 — Semantics: clean-room, not dlt-compatible

- **Decision**: Own lineage columns (`_rdlt_load_id`, `_rdlt_id`, `_rdlt_parent_id`,
  `_rdlt_pos`, `_rdlt_root_id`), own nesting model (structs preserved in-engine, lowered
  at the destination seam by capability), own naming rules.
- **Rationale**: dlt-compatibility would import decisions made for Python-era constraints
  (variant columns, flatten-always). Arrow-first allows strictly better semantics:
  struct preservation, single-column widening, deterministic collision-safe naming.
- **Alternatives considered**: dlt-compatible semantics (eases migration but freezes their
  design residue into our sacred seams permanently).

## R7 — Widening lattice: value-checked, honest about lossiness

- **Decision**: Pure `widen(a, b)` join in `rdlt-core`: `Null → T`;
  `Int64 → Float64 → Utf8`; `Int64 → Decimal(p,0) → Decimal(p,s) → Utf8`; `Bool → Utf8`;
  temporal → `Utf8`; `Float64 ⊔ Decimal → Utf8`; irreconcilable → `Json`. Conversions are
  value-checked (`Int64 → Float64` exact only within ±2^53 — first inexact value escalates
  the column to `Utf8`). `Utf8` renderings are canonical (RFC 3339, `true`/`false`,
  shortest-round-trip floats). Type conflicts widen one column, never multiply columns.
- **Rationale**: "Lossless" must be enforced, not asserted: a `Float64 → Decimal` edge
  would silently corrupt (NaN/±Inf, exponent range), and unchecked `Int64 → Float64`
  silently rounds above 2^53. Value-checking makes losslessness a runtime invariant that
  property tests can state as law. Single-column widening keeps destination schemas stable
  and queryable (no `col__v_text` variants).
- **Alternatives considered**: dlt's variant columns (schema churn, query breakage);
  type-only lattice (silent precision loss — a correctness bug given the project's
  correctness-first stance); always-Utf8 on any conflict (loses typed columns
  unnecessarily for the common Int64/Float64 case).

## R8 — Recovery: destination is sole source of truth; WAL is a cache

- **Decision**: Cursors/state commit atomically with data via `commit(meta)`, persisted in
  the destination; commits idempotent per `(load_id, commit_seq)`; WAL replay is an
  optimization; staged-but-uncommitted data is torn down on next `open`; cancellation is
  handled as a crash (single recovery path). fsync at commit boundaries only.
- **Rationale**: Correctness must survive total loss of the workdir — so the WAL can be
  fast-and-loose (fsync rarely) while the destination transaction carries the guarantee.
  At-least-once delivery to `write` + staging invisibility + idempotent commit compose
  into exactly-once visibility. One recovery path means the crash-injection suite tests
  the only path that exists.
- **Alternatives considered**: State in a local/sidecar store (splits the atomic
  data+state commit — the classic double-write problem); graceful-shutdown protocol
  distinct from crash recovery (two paths, one rarely tested); fsync-per-segment
  (throughput cost for a guarantee the destination already provides).

## R9 — v1 vertical slice: declarative REST → DuckDB + Postgres

- **Decision**: One YAML/JSON-configured REST source (auth, pagination strategies, cursor
  field, per-column type hints); DuckDB destination (Arrow ingestion, real STRUCTs);
  Postgres destination (binary COPY, collision-safe flatten lowering, staging+merge).
- **Rationale**: REST→warehouse is the highest-frequency ingestion job in the wild and
  exercises every engine feature (inference, nesting, cursors, evolution, merge). Two
  destinations with different capability profiles (STRUCT-native vs flatten, embedded vs
  server) prove the capability-driven lowering seam is real, not hypothetical.
- **Alternatives considered**: Files-first performance showcase (impressive numbers,
  doesn't exercise cursors/auth/pagination); SQL replication first (largest surface,
  CDC adjacency — deferred as fast follow).

## R10 — Dependencies

- **Decision**: `arrow` + `parquet` (arrow-rs) pinned at one workspace version
  (`rdlt-core` restricted to `arrow-schema`); `tokio`, `serde`/`serde_json`,
  `async-trait`, `thiserror`, `tracing`, `bytes`; `reqwest` (REST), `duckdb` bundled,
  `tokio-postgres`; dev/test: `proptest`, `criterion`, `wiremock`, `testcontainers`.
  Exact versions chosen at implementation start via `cargo add` (latest stable) and
  committed in `Cargo.lock`; arrow-rs major pinned workspace-wide.
- **Rationale**: All are the de-facto standard crates in their niche with active
  maintenance; keeping arrow-rs to a single workspace-wide major avoids the ecosystem's
  most common version-skew failure. `rdlt-core`'s `arrow-schema`-only diet keeps the
  vocabulary crate light for platform consumers.
- **Alternatives considered**: DataFusion (R5); `simd-json` in v1 (R5); `sqlx` for
  Postgres (runtime-agnostic abstraction unneeded; `tokio-postgres` gives binary COPY
  control); `polars` (DataFrame layer, not an ingestion substrate).

## R11 — Testing & CI strategy

- **Decision**: Four tiers — (1) semantic-law proptests beside the pure functions in
  `rdlt-core`, shredder round-trip tests in `rdlt-engine`; (2) crash-injection suite over
  every crash-matrix row (deterministic, in normal CI); (3) public connector conformance
  suites in `rdlt-testkit` (bundled connectors pass in CI; "certified = passes
  conformance"); (4) integration (DuckDB in-process, Postgres testcontainers, REST
  wiremock) + criterion microbenches per PR. CI gates: `cargo semver-checks` on both seam
  crates, clippy `-D warnings`, rustfmt, doc-tests. Test runner: `cargo nextest run`
  (doc-tests via `cargo test --doc`).
- **Rationale**: The engine's value proposition is a correctness claim; the crash suite is
  the only way to make "exactly-once visibility" a tested property instead of a slogan.
  Public conformance converts the SPI contract from prose into an executable gate that
  rapidbyte's catalog inherits.
- **Alternatives considered**: Integration-first testing (slow feedback, can't enumerate
  crash points); private conformance (forfeits the certification story).

## R12 — Benchmark methodology

- **Decision**: Pinned dlt version in a container; baseline measured first on the same
  hardware/datasets; one-command harness; targets: shred ≥20×, jsonl→DuckDB ≥10×,
  mock-REST→Postgres ≥5×, parquet passthrough ≥2×, RSS ≤1/5th, cold start ≤1/20th.
  Benchmark suite is v1 scope.
- **Rationale**: Performance is a headline claim (spec SC-003..005) and must be
  reproducible and honest — engine-bound cases labeled as such, API-bound reality
  acknowledged. Building the harness after the engine invites unmeasurable regressions;
  per-PR shredder microbenches catch them at the source.
- **Alternatives considered**: Synthetic-only microbenches (not credible against an
  incumbent); benchmarking as post-v1 work (targets become unfalsifiable during the
  period that matters most).
