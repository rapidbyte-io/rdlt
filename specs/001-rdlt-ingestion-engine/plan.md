# Implementation Plan: rdlt — Data Ingestion Engine Library

**Branch**: `001-rdlt-ingestion-engine` | **Date**: 2026-07-19 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/001-rdlt-ingestion-engine/spec.md`, technical
design from [`2026-07-18-rdlt-engine-design.md`](../../2026-07-18-rdlt-engine-design.md)
(approved, pre-implementation).

## Summary

Build a library-first ELT engine in Rust — extract → shred (normalize) → load — with
schema inference/evolution along a value-checked widening lattice, incremental cursors,
and crash-safe resumable runs with exactly-once destination visibility. Architecture:
streaming Arrow `RecordBatch`es through byte-bounded channels across concurrent tokio
stages, a parquet-segment WAL as a replayable buffer, and the destination as sole source
of truth (state commits atomically with data). Delivered as a 9-crate workspace with two
semver-sacred seams (`rdlt-core` vocabulary, `rdlt-connector` SPI) and one deep engine
module; v1 vertical slice is a declarative REST source loading into DuckDB and Postgres.

## Technical Context

**Language/Version**: Rust, stable toolchain (edition 2024; MSRV = latest stable at first
release, pinned in `rust-toolchain.toml`)

**Primary Dependencies**: `arrow` + `parquet` (arrow-rs; `rdlt-core` limited to
`arrow-schema`), `tokio`, `serde`/`serde_json`, `async-trait`, `thiserror`, `tracing`,
`bytes`; connectors: `reqwest`, `duckdb` (bundled), `tokio-postgres`. See research.md R10.

**Storage**: Local workdir WAL (parquet segments + append-only JSON manifest); pipeline
state persisted *in the destination* (`_rdlt_state`), committed atomically with data.
Destinations v1: DuckDB (embedded), PostgreSQL.

**Testing**: `cargo nextest run` (workspace policy; doc-tests via `cargo test --doc`),
`proptest` for semantic laws, crash-injection harness in `rdlt-testkit`, `criterion`
microbenches, `wiremock` (REST), `testcontainers` (Postgres), DuckDB in-process.

**Target Platform**: Linux + macOS, x86_64/aarch64; library embeddable in CLI, server,
Lambda (no daemon, no background threads outside `run()`).

**Project Type**: Multi-crate Rust workspace library + thin dev CLI.

**Performance Goals**: vs pinned Python dlt baseline, same hardware/data: shred ≥20×
rows/s/core; jsonl→DuckDB e2e ≥10×; mock-REST→Postgres ≥5×; parquet→parquet ≥2×; cold
start ≤1/20th. Baseline measured first; one-command reproducible harness.

**Constraints**: Peak RSS ≤ 1/5th of dlt baseline and flat as volume grows 100× (channels
bounded by bytes, not batch count); exactly-once destination visibility under crash at any
stage; no silent failures (all discards/widenings/retries in `RunReport`); `rdlt-core` and
`rdlt-connector` gated by `cargo semver-checks`; shredder runs on dedicated CPU pool
(tokio hosts I/O only).

**Scale/Scope**: v1 = 9 crates, 3 bundled connectors (REST source, DuckDB + Postgres
destinations), 3 write modes, 4 schema policies, conformance + crash-injection + benchmark
suites. Out of scope: transformers, CDC, WASM/process hosts, object-store WAL, Python
bindings, SQL sources (fast follow).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` is an unratified template — no project constitution
exists yet, so no constitutional gates can fail. **Gate: PASS (vacuous).** In its absence,
the approved design doc supplies the working principles this plan is checked against, and
they are restated here so `/speckit-tasks` and reviews can enforce them:

| Working principle (from design doc) | Plan compliance |
|---|---|
| Library-first: engine embeddable anywhere; platform concerns out of scope | Facade crate + thin CLI; no scheduler/daemon/UI anywhere in the plan |
| Two sacred seams: `rdlt-core`, `rdlt-connector` semver-gated from day one | CI gate in Phase 1 contracts; connectors depend on SPI only |
| Correctness before speed: exactly-once visibility, no silent failures | Crash-injection suite is the highest-value test tier; `RunReport` accounting is an FR |
| Tests as executable laws | proptest laws live beside the pure functions in `rdlt-core`; conformance suite is public |
| Honest benchmarks | Baseline measured first, methodology published, engine-bound cases labeled |

*Post-Phase-1 re-check (2026-07-19): design artifacts (data-model, contracts) introduce no
new crates, no new dependencies, and no platform-scope creep beyond the design doc. PASS.*

Recommendation (non-blocking): ratify a real constitution via `/speckit-constitution`
seeded from the table above.

## Project Structure

### Documentation (this feature)

```text
specs/001-rdlt-ingestion-engine/
├── spec.md              # Feature specification (/speckit-specify output)
├── plan.md              # This file
├── research.md          # Phase 0: consolidated decisions + rationale
├── data-model.md        # Phase 1: vocabulary types, relationships, invariants
├── quickstart.md        # Phase 1: embedder quickstart + dev workflow
├── contracts/
│   ├── connector-spi.md     # Source/Destination/LoadSession traits + behavioral contract
│   ├── embedder-api.md      # Pipeline builder, events, RunReport, error taxonomy
│   └── persisted-formats.md # StateDoc, WAL manifest, commit receipt — stability rules
├── checklists/
│   └── requirements.md  # Spec quality checklist (complete)
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
Cargo.toml               # workspace root: members, shared lints, shared deps
rust-toolchain.toml
crates/
├── rdlt-core/           # Vocabulary seam: ids, LogicalType + widen(), TableSchema,
│   └── src/             #   SchemaDelta + hashing, Cursor, StateDoc, CommitMeta/Receipt,
│                        #   WriteMode, contracts, naming, RunReport, PipelineEvent.
│                        #   Pure data + pure functions. deps ≈ serde + arrow-schema.
├── rdlt-connector/      # SPI seam: Source/Destination/LoadSession traits, RecordsOut,
│   └── src/             #   StreamSpec, DestCapabilities, SourceError/DestError.
├── rdlt-engine/         # Deep module (all pub(crate) except lib.rs surface):
│   └── src/
│       ├── lib.rs       #   pub: Engine, RdltError (re-exports RunReport/PipelineEvent)
│       ├── runtime/     #   task graph, byte-bounded channels, retry driver, cancel token
│       ├── shred/       #   infer.rs, nest.rs, build.rs (raw-JSON → Arrow buffers)
│       ├── schema/      #   registry, diffing → SchemaDelta, contract enforcement
│       ├── wal/         #   parquet segments + manifest, resume scan, GC
│       ├── state/       #   StateDoc round-trip through destination
│       └── load/        #   commit protocol, migration planning vs DestCapabilities
├── rdlt/                # Facade: Pipeline::builder (typestate), prelude;
│   └── src/             #   features = ["rest", "duckdb", "postgres"]
├── rdlt-testkit/        # MemorySource/MemoryDestination, conformance suites,
│   └── src/             #   crash-injection harness, run harness
├── rdlt-source-rest/    # Declarative YAML/JSON REST source → rdlt-connector only
├── rdlt-dest-duckdb/    # DuckDB destination (Arrow ingestion, STRUCT lowering)
├── rdlt-dest-postgres/  # Postgres destination (binary COPY, flatten, staging+merge)
└── rdlt-cli/            # Thin dev CLI over facade (TOML spec → run → report)
benches/                 # Cross-crate benchmark harness + pinned-dlt baseline (container)
```

**Structure Decision**: Cargo workspace, dependency arrows one-way only:
`rdlt-core ← rdlt-connector ← {connectors, rdlt-testkit, rdlt-engine}; rdlt-engine ← rdlt ← rdlt-cli`.
Nine crates is deliberate, not accidental complexity: two are semver-sacred seams with
different consumers and release cadences (vocabulary vs traits), one is the deep engine
module free to churn privately, three are connectors that must prove the SPI is sufficient
(no privileged access), plus facade, testkit, and CLI. Collapsing any pair couples a seam
to a consumer it doesn't serve (rationale: design doc §3.1, research.md R2/R3).

## Complexity Tracking

No constitutional gates exist to violate (constitution unratified); no deviations from the
approved design were introduced by this plan. Table intentionally empty.
