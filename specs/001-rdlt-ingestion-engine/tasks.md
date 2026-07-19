# Tasks: rdlt — Data Ingestion Engine Library

**Input**: Design documents from `/specs/001-rdlt-ingestion-engine/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md,
design doc `2026-07-18-rdlt-engine-design.md` (repo root)

**Tests**: INCLUDED — the spec makes test suites first-class deliverables (FR-016 public
conformance suite, SC-002 crash-injection coverage, SC-006 property-tested semantic laws).
Story test tasks are written first and must fail before implementation.

**Organization**: Phases 3–7 map to spec user stories US1–US5. Phases 8–9 are
cross-cutting deliverables (bundled connectors/CLI per FR-017/FR-018, benchmarks per
SC-003..005) — no story label per template rules.

**Test runner**: `cargo nextest run` (doc-tests via `cargo test --doc`) — repo policy.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: US1–US5 from spec.md (user story phases only)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Workspace skeleton, toolchain, CI — everything else hangs off this.

- [X] T001 Create Cargo workspace: root `Cargo.toml` (9 members; `[workspace.dependencies]` pinning arrow/parquet, tokio, serde/serde_json, async-trait, thiserror, tracing, bytes per research.md R10; shared `[workspace.lints]`), `rust-toolchain.toml`, `.gitignore`
- [X] T002 Scaffold all member crates with minimal `Cargo.toml` + empty `lib.rs`/`main.rs`: `crates/rdlt-core`, `crates/rdlt-connector`, `crates/rdlt-engine`, `crates/rdlt`, `crates/rdlt-testkit`, `crates/rdlt-source-rest`, `crates/rdlt-dest-duckdb`, `crates/rdlt-dest-postgres`, `crates/rdlt-cli` — dependency arrows one-way only (plan.md Structure Decision); `rdlt-core` deps limited to serde + arrow-schema
- [X] T003 [P] Configure `rustfmt.toml` and workspace clippy lints (deny warnings) at repo root
- [X] T004 [P] CI workflow in `.github/workflows/ci.yml`: fmt check, clippy `-D warnings`, `cargo nextest run`, `cargo test --doc`, `cargo semver-checks` on `rdlt-core` + `rdlt-connector` (document the expected one-time red on bootstrap PR — baseline predates crates)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The two seam crates (`rdlt-core` vocabulary, `rdlt-connector` SPI), memory
connectors, and engine runtime skeleton. **No user story can start before this phase
completes** — every story consumes these types and traits.

- [X] T005 [P] Identifier newtypes (`PipelineId`, `LoadId`, `StreamName`, `TableName`, `SchemaHash`, `RowId`) with serde + `Display` in `crates/rdlt-core/src/ids.rs` (data-model.md §1 — no bare String/u64 across seams)
- [X] T006 [P] `LogicalType` enum + pure `widen(a, b)` lattice join in `crates/rdlt-core/src/types.rs` per research.md R7 (two numeric chains; NO `Float64 → Decimal` edge; `Float64 ⊔ Decimal → Utf8`; irreconcilable → `Json`)
- [X] T007 [P] proptest lattice laws (commutativity, idempotence, monotonicity; `Utf8`/`Json` absorption) in `crates/rdlt-core/tests/lattice_laws.rs` (SC-006 — laws live beside the pure functions)
- [X] T008 [P] Identifier normalization + collision-safe naming (deterministic hash suffix; distinct source names never merge) in `crates/rdlt-core/src/naming.rs` with proptest in `crates/rdlt-core/tests/naming_props.rs`
- [X] T009 `ColumnDef`/`TableSchema`/`SchemaDelta` + canonical serialization + content hashing in `crates/rdlt-core/src/schema.rs` (depends T005, T006; hashing rules per contracts/persisted-formats.md §5 — serde-layout changes are a state-migration event)
- [X] T010 [P] `Cursor`, `StateDoc`, `CommitMeta`, `CommitReceipt`, `WriteMode`, `SchemaPolicy` contract enums (all with `format_version` where persisted) in `crates/rdlt-core/src/{cursor.rs,state.rs,commit.rs,policy.rs}` per data-model.md §§4,6
- [X] T011 [P] `RunReport` + `PipelineEvent` + `RdltError` taxonomy (`#[non_exhaustive]`, serde-stable, thiserror) in `crates/rdlt-core/src/{report.rs,event.rs,error.rs}` per data-model.md §§7–8
- [X] T012 Deterministic row identity: `_rdlt_id` content-hash (keyless) / key-hash (keyed) + child/root propagation rules in `crates/rdlt-core/src/identity.rs` with determinism proptest in `crates/rdlt-core/tests/identity_props.rs` (depends T005)
- [X] T013 [P] `SourceError`/`DestError` taxonomy (`Transient`/`RateLimited`/`Fatal`) + `ConnectorSpec` in `crates/rdlt-connector/src/{error.rs,spec.rs}`
- [X] T014 [P] `StreamSpec` (cursor field, primary key, type hints) + `DestCapabilities` (merge, type matrix, ident rules, nesting) in `crates/rdlt-connector/src/{stream.rs,capabilities.rs}`
- [X] T015 `Source`/`Destination`/`LoadSession` traits + `ReadRequest`/`OpenCtx` (exhaustive pub-field structs + `new()` hedge) + `RecordsOut` handle (`raw_json`/`rows`/`arrow`/`checkpoint`) + `pub use arrow` re-export in `crates/rdlt-connector/src/lib.rs`, with object-safety compile test in `crates/rdlt-connector/tests/object_safe.rs` (depends T010, T013, T014)
- [X] T016 Engine skeleton: public surface (`Engine`, re-exported `RunReport`/`PipelineEvent`/`RdltError`) in `crates/rdlt-engine/src/lib.rs` + byte-bounded channel (capacity in BYTES, not batches) + cancel token in `crates/rdlt-engine/src/runtime/channel.rs` with capacity unit tests (depends T011, T015)
- [X] T017 [P] `MemorySource` (MUST honor `ReadRequest.since` — carried-over lesson) + `MemoryDestination` (staging invisibility, idempotent commit by `(load_id, commit_seq)`, `StateDoc` storage, teardown-on-open) in `crates/rdlt-testkit/src/memory/{source.rs,dest.rs}` (depends T015)

**Checkpoint**: Both seam crates compile with their law tests green; memory connectors
exist; user stories can begin.

---

## Phase 3: User Story 1 — First full sync from source to destination (Priority: P1) 🎯 MVP

**Goal**: Nested, untyped records flow memory→memory into typed tables with inferred
schemas, child-table splitting, and `_rdlt_*` lineage — no schema declared, Append mode.

**Independent Test**: `crates/rdlt-engine/tests/us1_full_sync.rs` — in-memory source with
nested/heterogeneous records through a full run; assert types, widening, child linkage,
lineage columns, `Json` fallback (spec US1 acceptance scenarios 1–3).

- [X] T018 [P] [US1] Write failing integration test covering all three US1 acceptance scenarios (typed root+child tables; single-column widening; `Json` preservation of undecomposable values) in `crates/rdlt-engine/tests/us1_full_sync.rs`
- [X] T019 [US1] Type inference with per-column observed-state tracking + value-checked widening escalation (`Int64` beyond ±2^53 under `Float64` → `Utf8`; canonical `Utf8` renderings; unambiguous ISO-8601-with-tz timestamp detection; `StreamSpec` hints override) in `crates/rdlt-engine/src/shred/infer.rs`
- [X] T020 [US1] Nesting: struct preservation, list-of-objects → child-table split, list-of-scalars handling, `_rdlt_load_id`/`_rdlt_id`/`_rdlt_parent_id`/`_rdlt_pos`/`_rdlt_root_id` stamping at every depth in `crates/rdlt-engine/src/shred/nest.rs` (uses core identity from T012)
- [X] T021 [US1] Arrow columnar builders: `raw_json` bytes parsed via serde_json streaming deserializer directly into buffers (NO intermediate `Value` tree) + `rows()` fallback path in `crates/rdlt-engine/src/shred/build.rs`
- [X] T022 [US1] Schema registry: per-table current version, diff → `SchemaDelta`, delta-emitted-before-first-batch-at-new-version ordering in `crates/rdlt-engine/src/schema/registry.rs`
- [X] T023 [US1] Loader (Append): drive `ensure_table` → `write` in `seq` order per contract E1/E4; capability-driven lowering (struct-native passthrough vs collision-safe flatten) in `crates/rdlt-engine/src/load/mod.rs`
- [X] T024 [US1] Runtime task graph: source task → shredder on dedicated CPU pool → loader, wired by byte-bounded channels; Arrow-push bypass (schema-check only); `Engine::run()` happy path returning minimal `RunReport` in `crates/rdlt-engine/src/runtime/graph.rs`
- [X] T025 [US1] Facade: `Pipeline::builder` typestate (missing source/dest = compile error), build-time capability validation, no I/O before `run()` (embedder-api.md B1–B3) in `crates/rdlt/src/builder.rs` + compile-fail test in `crates/rdlt/tests/typestate.rs`
- [X] T026 [P] [US1] Shredder round-trip proptests: shred → lower → reassemble losslessly (value-checked widening + `Json` fallback guarantee) in `crates/rdlt-engine/tests/shred_roundtrip.rs`
- [X] T027 [US1] Wire everything through the facade until T018 passes end-to-end (fix-up task across `crates/rdlt-engine/src/` and `crates/rdlt/src/`)

**Checkpoint**: MVP — a full nested sync works memory→memory via the public facade.

---

## Phase 4: User Story 2 — Incremental sync (Priority: P2)

**Goal**: Second run moves only new data (cursor resume via destination-persisted state);
Append/Replace/Merge write modes, with merge replacing whole subtrees.

**Independent Test**: `crates/rdlt-engine/tests/us2_incremental.rs` — run twice; assert
the source was asked to resume from the committed cursor and merge replaced an updated
record's children exactly (spec US2 acceptance scenarios).

- [X] T028 [P] [US2] Write failing tests: resume-from-cursor (second run sees `since`) + merge subtree replacement (old children gone, new present) in `crates/rdlt-engine/tests/us2_incremental.rs`
- [X] T029 [US2] Checkpoint flow: `RecordsOut::checkpoint` → recovery-unit bookkeeping; cursor-coverage invariant (cursor committed iff all covered rows in same commit unit) in `crates/rdlt-engine/src/state/checkpoints.rs`
- [X] T030 [US2] State round-trip: assemble `CommitMeta` (per-stream cursors, schema hashes, counters), call `commit()`, recover via `read_state()` at run start in `crates/rdlt-engine/src/state/statedoc.rs`
- [X] T031 [US2] Pass recovered cursor as `ReadRequest.since`; add `MemorySource` assertion helper that covered ranges are never re-requested in `crates/rdlt-testkit/src/memory/source.rs`
- [X] T032 [US2] Merge mode: delete-by-`_rdlt_root_id` + insert subtree; keyed (key-hash) vs keyless (content-hash dedup) semantics; `MemoryDestination` merge support in `crates/rdlt-engine/src/load/merge.rs` + `crates/rdlt-testkit/src/memory/dest.rs`
- [X] T033 [US2] Replace mode in `crates/rdlt-engine/src/load/mod.rs` + build-time fail-fast when `Merge` requested without destination merge capability (test in `crates/rdlt/tests/build_validation.rs`)
- [X] T034 [US2] Make T028 pass end-to-end

**Checkpoint**: Recurring pipelines are practical; all three write modes work.

---

## Phase 5: User Story 3 — Crash-safe, resumable runs (Priority: P3)

**Goal**: Kill the run anywhere; restart converges to the uninterrupted result.
Exactly-once visibility becomes a tested property (SC-002).

**Independent Test**: `crates/rdlt-engine/tests/us3_crash_matrix.rs` — fault injection at
every crash-matrix row (design doc §6); restarted run's destination state byte-identical
to uninterrupted run.

- [X] T035 [P] [US3] Crash-injection harness: named fault points, kill-at-point runner, restart-and-compare helper in `crates/rdlt-testkit/src/crash.rs`
- [X] T036 [US3] WAL writer: parquet segments keyed `(load_id, table, seq)` under `<workdir>/wal/`, append-only JSONL manifest (`segment`/`delta`/`checkpoint`/`committed` records, delta-before-segment ordering) in `crates/rdlt-engine/src/wal/writer.rs` per contracts/persisted-formats.md §§2–3
- [X] T037 [US3] WAL resume: single forward-pass manifest scan (torn final line truncated), replay into destination preserving delta-before-batch order, corrupt-segment quarantine → degrade to re-extract, segment GC after receipt in `crates/rdlt-engine/src/wal/resume.rs`
- [X] T038 [US3] Commit protocol driver: commit-unit state machine (accumulating → committing → committed), `CommitPolicy` grouping (N checkpoints | bytes | seconds), idempotent re-commit consuming prior receipt, fsync at commit boundaries only in `crates/rdlt-engine/src/load/commit.rs`
- [X] T039 [US3] Staging teardown on `open` (D4) exercised in `MemoryDestination` + engine workdir lock refusing concurrent `run()` (embedder-api.md R5) in `crates/rdlt-testkit/src/memory/dest.rs` + `crates/rdlt-engine/src/runtime/lock.rs`
- [X] T040 [US3] Crash-matrix tests: all four rows (pre-WAL / WAL-pre-commit / mid-commit / WAL-lost) × restart, asserting exactly-once visibility and no cursor skips in `crates/rdlt-engine/tests/us3_crash_matrix.rs`
- [X] T041 [US3] Cancellation-as-crash tests (cancel token + drop mid-run → next run recovers identically) appended to `crates/rdlt-engine/tests/us3_crash_matrix.rs`

**Checkpoint**: The correctness headline is now enforced by CI, not asserted by prose.

---

## Phase 6: User Story 4 — Schema evolution under policy (Priority: P4)

**Goal**: Evolve/Freeze/DiscardRow/DiscardValue enforced per table/column; freeze fails
typed and early; discards counted, never silent.

**Independent Test**: `crates/rdlt-engine/tests/us4_policies.rs` — mid-run shape change
under each policy; assert evolved schema / pre-write typed failure / exact discard counts
(spec US4 acceptance scenarios).

- [X] T042 [P] [US4] Write failing tests for all three US4 acceptance scenarios (evolve adds nullable column; freeze aborts before any violating row is written; discard loads conforming data with exact counts) in `crates/rdlt-engine/tests/us4_policies.rs`
- [X] T043 [US4] Contract enforcement at the registry seam: `Freeze` converts would-be `SchemaDelta` into `RdltError::Schema(ContractViolation)` naming table/column/change, pre-write; `DiscardRow`/`DiscardValue` filtering with counters in `crates/rdlt-engine/src/schema/contracts.rs`
- [X] T044 [US4] Facade policy surface: per-table/per-column `SchemaPolicy` configuration on the builder in `crates/rdlt/src/builder.rs`
- [X] T045 [US4] Route discard/widening counters into `RunReport` + emit as events; make T042 pass (`crates/rdlt-engine/src/runtime/graph.rs`, `crates/rdlt-engine/src/schema/contracts.rs`)

**Checkpoint**: Drift handling is a controllable, observable policy.

---

## Phase 7: User Story 5 — Observable runs and verifiable connectors (Priority: P5)

**Goal**: Typed event stream + full-accounting `RunReport`; public conformance suites
that certify connectors with clause-naming diagnostics.

**Independent Test**: event-order and report-accounting tests in
`crates/rdlt-engine/tests/us5_observability.rs`; conformance suites fail a deliberately
non-compliant connector naming the violated clause (spec US5 acceptance scenarios).

- [X] T046 [P] [US5] Typed event stream: causal-order `PipelineEvent` emission (SchemaEvolved before first BatchLoaded at new version; Committed after covered events) in `crates/rdlt-engine/src/runtime/events.rs` + `pipeline.events()` on facade in `crates/rdlt/src/lib.rs`
- [X] T047 [P] [US5] `tracing` spans `rdlt.extract`/`rdlt.shred`/`rdlt.load` with per-stream fields across `crates/rdlt-engine/src/runtime/graph.rs`
- [X] T048 [US5] Full `RunReport` accounting + invariant test (report totals == `MemoryDestination` visible reality; retries/widenings/discards all present — FR-012/SC-008) in `crates/rdlt-engine/tests/us5_observability.rs`
- [X] T049 [US5] Public Source conformance suite (clauses S1–S6 of contracts/connector-spi.md, each clause = named test) in `crates/rdlt-testkit/src/conformance/source.rs`
- [X] T050 [US5] Public Destination conformance suite (clauses D1–D8; includes staging-teardown-after-simulated-crash) in `crates/rdlt-testkit/src/conformance/dest.rs`
- [X] T051 [US5] Deliberately non-compliant fixture connectors (ignores `since`; non-idempotent commit) + tests asserting suites fail with clause-naming diagnostics in `crates/rdlt-testkit/tests/conformance_negative.rs`
- [X] T052 [US5] Run both conformance suites against the memory connectors in CI ("certified = passes conformance" gate) in `crates/rdlt-testkit/tests/conformance_memory.rs`

**Checkpoint**: All five user stories independently functional; SPI contract is executable.

---

## Phase 8: Bundled Connectors & CLI (cross-cutting — FR-017/FR-018)

**Purpose**: Prove the SPI on real systems (three capability profiles) and ship the dev
CLI. Connectors depend on `rdlt-connector` ONLY — needing engine internals means the SPI
is wrong; stop and raise it.

- [X] T053 [P] REST source config model (YAML: base URL, auth, pagination strategies, cursor field, per-column type hints) with serde + JSON-schema for `ConnectorSpec` in `crates/rdlt-source-rest/src/config.rs`
- [X] T054 REST source implementation: pagination drivers, `raw_json` body push (perf path), checkpoint from cursor field, `Transient`/`RateLimited`/`Fatal` classification from HTTP status in `crates/rdlt-source-rest/src/lib.rs`
- [X] T055 REST source tests: wiremock integration (pagination, rate-limit honoring, resume) + source conformance suite in `crates/rdlt-source-rest/tests/conformance.rs`
- [X] T056 [P] DuckDB destination: Arrow-native ingestion, STRUCT-preserving lowering, staged writes + atomic commit with `_rdlt_state` + idempotent receipts in `crates/rdlt-dest-duckdb/src/lib.rs`
- [X] T057 DuckDB tests: in-process integration + destination conformance suite in `crates/rdlt-dest-duckdb/tests/conformance.rs`
- [X] T058 [P] Postgres destination: binary COPY writes, collision-safe flatten lowering, staging tables + transactional commit + merge (delete-by-root-id) in `crates/rdlt-dest-postgres/src/lib.rs`
- [X] T059 Postgres tests: testcontainers integration + destination conformance suite in `crates/rdlt-dest-postgres/tests/conformance.rs`
- [X] T060 Facade features (`rest`/`duckdb`/`postgres`), prelude, quickstart doc-example as doc-test in `crates/rdlt/src/lib.rs`
- [X] T061 CLI: TOML pipeline spec → run; human events on stderr, `RunReport` JSON on stdout/`--report`; exit codes mirror `RdltError` variants (documented) in `crates/rdlt-cli/src/main.rs`

**Checkpoint**: The spec's v1 vertical slice (REST → DuckDB/Postgres) works for real.

---

## Phase 9: Benchmarks & Polish (SC-003..005 + release hygiene)

**Purpose**: Make the performance claims reproducible and honest; final quality pass.

- [X] T062 [P] Pinned-dlt baseline: container image, reference datasets, one-command harness measuring the BASELINE FIRST in `benches/baseline/` + `benches/run-e2e.sh` (research.md R12)
- [X] T063 [P] Criterion shredder microbench (rows/s/core on nested JSON) wired per-PR in `crates/rdlt-engine/benches/shred.rs`
- [ ] T064 End-to-end benches: jsonl→DuckDB, mock-REST→Postgres (engine-bound, labeled), parquet→parquet passthrough, peak-RSS + cold-start capture in `benches/e2e/`
- [ ] T065 Run full matrix vs targets (≥20× shred, ≥10× files, ≥5× REST, ≥2× passthrough, ≤1/5 RSS, ≤1/20 cold start); record results + methodology in `benches/RESULTS.md` — misses become tracked issues, not silent
- [X] T066 [P] rustdoc + doc-tests pass on all public items of `rdlt-core`/`rdlt-connector`/`rdlt` (CI-gated) — examples in crate-level docs
- [X] T067 Validate quickstart end-to-end: embedder example from `specs/001-rdlt-ingestion-engine/quickstart.md` compiles and runs against DuckDB; fix drift
- [ ] T068 Establish `cargo semver-checks` baseline (first release tag), final clippy/fmt sweep, commit `Cargo.lock` (workspace policy) across repo

---

## Dependencies & Execution Order

### Phase Dependencies

```
Phase 1 (Setup)
  └─► Phase 2 (Foundational — BLOCKS all stories)
        └─► Phase 3 (US1, MVP) ─► Phase 4 (US2) ─► Phase 5 (US3)
              │                                        │
              ├─► Phase 6 (US4) — needs US1 only       │
              ├─► Phase 7 (US5) — accounting parts benefit from US2–US4 counters
              └────────────────────────────────────────┴─► Phase 8 (connectors: need US1–US3 semantics + US5 conformance suites)
                                                              └─► Phase 9 (benchmarks need Phase 8 connectors)
```

Engine stories are sequential in practice (US2 builds on US1's pipeline, US3 on US2's
commit path) — the independence guarantee is at the *test* level (each story's suite runs
standalone against memory connectors), not team-parallelism across engine internals.

### Key task-level dependencies

- T009 ← T005, T006 · T012 ← T005 · T015 ← T010, T013, T014 · T016 ← T011, T015 · T017 ← T015
- Within each story phase: the failing-test task (T018/T028/T035+T040/T042) precedes implementation; the final "make it pass" task closes the phase.
- T049/T050 (conformance suites) ← contracts finalized by US1–US3 behavior; T055/T057/T059 ← T049/T050.

### Parallel Opportunities

- Phase 2: T005–T008 all parallel; then T010/T011/T013/T014 parallel; T017 parallel with T016.
- Phase 3: T018 ∥ T026 (test files) while T019→T021 proceed; T025 parallel with loader work.
- Phase 7: T046 ∥ T047; T049 ∥ T050 after contracts settle.
- Phase 8: the three connectors (T053-55 ∥ T056-57 ∥ T058-59) are fully independent crates — the widest parallel window in the project.
- Phase 9: T062 ∥ T063 ∥ T066.

## Parallel Example: Phase 8 (widest window)

```bash
# Three independent connector tracks, one per developer/agent:
Task: "REST source config+impl+conformance in crates/rdlt-source-rest/"      # T053–T055
Task: "DuckDB destination + conformance in crates/rdlt-dest-duckdb/"         # T056–T057
Task: "Postgres destination + conformance in crates/rdlt-dest-postgres/"     # T058–T059
```

## Implementation Strategy

- **MVP** = Phases 1–3 (T001–T027): full nested sync, memory→memory, via the public
  facade. Demo-able and property-tested. Stop and validate here.
- **Correctness core** = Phases 4–5: after US3, the crash matrix is CI-enforced — this is
  the earliest point at which "rdlt is correct under failure" is a defensible claim.
- **Certification** = Phases 6–7 complete the engine contract surface; **product** =
  Phase 8 makes it real; **credibility** = Phase 9 makes the performance claims public
  and reproducible.
- Commit after each task or logical group (checkpoints are safe stopping points).

## Notes

- Format validated: every task has checkbox + sequential ID + exact file path; [P] only
  where files and dependencies allow; story labels only in Phases 3–7.
- 68 tasks total: Setup 4, Foundational 13, US1 10, US2 7, US3 7, US4 4, US5 7,
  Connectors/CLI 9, Benchmarks/Polish 7.

---

## Implementation notes (2026-07-19, /speckit-implement session)

**Complete**: Phases 1–8 fully (T001–T061); Phase 9 partially (T062, T063, T066, T067).
83 workspace tests green (`cargo nextest run`), clippy `-D warnings` clean, doc-tests
pass. CLI smoke-tested end-to-end (live HTTP API → REST source → engine → DuckDB).

**Open (honest status)**:
- T064/T065: criterion microbench runs (~48K nested rows/s end-to-end, unoptimized,
  includes per-iteration setup); the pinned-dlt baseline container + full comparison
  matrix are scaffolded in `benches/` but NOT executed — `benches/RESULTS.md` refuses
  comparison multiples until the baseline column exists.
- T068: `cargo semver-checks` baseline needs a first release tag (CI job documented as
  red-once on bootstrap); `Cargo.lock` exists but committing is deferred to the first
  repo commit.

**Deviations from task text (documented, deliberate)**:
- T021: the shredder parses via `serde_json` into per-row `Value`s rather than a fully
  streaming no-tree path; the module seam (`shred/build.rs` takes parsed rows) is
  shaped so the streaming parser can replace it when benchmarks demand (design doc
  §10 already defers `simd-json` to a measured follow-up).
- Arrow pinned to 58.3 (not latest): duckdb-rs links arrow 58; single workspace-wide
  arrow version is a correctness requirement (RecordBatch identity through the SPI).
- `LoadSession::ensure_table` gained a `mode: &WriteMode` parameter (destinations need
  the disposition at commit time for merge); contracts/connector-spi.md and the design
  doc were updated in step.
- Facade default workdir: builder defaults to NO Wal (`None`) unless `.workdir()` is
  called; the CLI defaults to `.rdlt`. Correctness holds either way (re-extraction);
  quickstart shows `.workdir()` for cheap recovery.
- Engine-level empty-key `Merge` is allowed (content-hash dedup semantics); the facade
  requires a non-empty key.

**Bugs caught by the test suites during implementation** (the suites paid rent):
- WAL replay double-apply: segments beyond the last checkpoint were replayed AND
  re-extracted (crash suite caught; replay now truncates at the last checkpoint).
- Stage truncation ordering: both SQL destinations truncated stages per-table, breaking
  child-subtree merge deletes that read the root's stage (Postgres e2e caught; both
  now truncate after all tables publish).
- DuckDB multi-instance visibility: separate `Connection::open`s on one file can't see
  each other's catalogs; destination now holds one shared instance + `try_clone`.
