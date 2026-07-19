# Tasks: File & Arrow-Native Ingestion

**Input**: Design documents from `/specs/002-file-arrow-ingestion/` (delta on feature
001 — its contracts and architecture remain authoritative).

**Tests**: INCLUDED — conformance certification and benchmark cells are spec
deliverables (FR-004/FR-011/SC-001..004); story test tasks are written first and must
fail before implementation. Runner: `cargo nextest run`.

**Organization**: Phases 3–5 map to spec user stories US1–US3; Phase 6 is
cross-cutting (benchmarks, contract fold-in).

## Format: `[ID] [P?] [Story] Description`

---

## Phase 1: Setup

- [X] T001 Scaffold `crates/rdlt-source-file` and `crates/rdlt-dest-parquet` (Cargo.tomls depending on rdlt-connector only + empty lib.rs), add both to workspace members and `[workspace.dependencies]`, add `glob` to workspace deps in root `Cargo.toml`
- [X] T002 [P] Facade features: `file = ["dep:rdlt-source-file"]`, `parquet = ["dep:rdlt-dest-parquet"]` + re-exports in `crates/rdlt/Cargo.toml` and `crates/rdlt/src/lib.rs`

---

## Phase 2: Foundational (blocks US2/US3; US1 needs only T003's field present)

- [X] T003 `StreamSpec.structured: bool` (serde default false, additive — the PR's semver-checks job must stay green) + rustdoc pointing at clause S7 in `crates/rdlt-connector/src/stream.rs`

**Checkpoint**: workspace builds; SPI amendment in place.

---

## Phase 3: User Story 1 — Load local files into a destination (Priority: P1) 🎯 MVP

**Goal**: JSONL files/globs → any destination with full record-stream semantics and
per-file incremental resume; shrunk files fail loudly.

**Independent Test**: `crates/rdlt-source-file/tests/jsonl.rs` — temp dir of JSONL
files: full first load; append + new file → second run loads exactly the delta;
shrunk file → error naming it; conformance suite passes.

- [X] T004 [P] [US1] Write failing tests: full glob load, append/new-file resume delta, shrunk-file error naming the file, empty glob = empty success, missing named file = error, malformed line names file+offset in `crates/rdlt-source-file/tests/jsonl.rs`
- [X] T005 [US1] Config model (`format: jsonl|parquet`, path/glob, per-stream record options; parquet streams may not declare primary_key) with validation in `crates/rdlt-source-file/src/config.rs`
- [X] T006 [US1] `FileCursor` (per-file `{done, size}` map, format_version 1): encode/decode via `Cursor`, resume rules incl. shrunk-detection per data-model §1 in `crates/rdlt-source-file/src/cursor.rs`
- [X] T007 [US1] JSONL read loop: lexicographic snapshot listing, slab reads pushed via `raw_json`, per-slab + per-file-boundary checkpoints, byte-offset resume, malformed-input classification in `crates/rdlt-source-file/src/jsonl.rs` + `src/lib.rs` (Source impl, streams() from config)
- [X] T008 [US1] Certification: source conformance suite green in `crates/rdlt-source-file/tests/conformance.rs`; make T004 pass
- [X] T009 [US1] Wire-up: CLI `[source.file]` arm in `crates/rdlt-cli/src/main.rs`; end-to-end jsonl→DuckDB test (bundled connector, incremental across two runs) in `crates/rdlt-source-file/tests/e2e_duckdb.rs`

**Checkpoint**: MVP — file ingestion is a supported, certified connector.

---

## Phase 4: User Story 2 — Structured pass-through (Priority: P2)

**Goal**: Parquet files flow as Arrow batches through the engine without re-shredding;
policies and evolution behave identically; parquet destination publishes them.

**Independent Test**: `crates/rdlt-engine/tests/passthrough.rs` — arrow batches from a
memory source: contents/types preserved, `_rdlt_load_id` stamped, evolve adds column,
freeze rejects pre-publication; crash-matrix row for structured segments.

- [X] T010 [P] [US2] Write failing engine tests: passthrough content/type preservation + load-id stamping, schema evolution under evolve, freeze rejection naming table/column, undeclared-stream Arrow push rejected (clause S7), WAL replay of structured segments converges in `crates/rdlt-engine/tests/passthrough.rs`
- [X] T011 [US2] Passthrough module: arrow schema → `TableSchema` (inverse physical mapping; unmappable type = typed error naming column), registry diff + `SchemaPolicy` enforcement, constant `_rdlt_load_id` column append (zero data copy) in `crates/rdlt-engine/src/shred/passthrough.rs`
- [X] T012 [US2] Graph Arrow arm: route `PushPayload::Arrow` through passthrough for `structured` streams (runtime rejection for undeclared streams), emit standard Delta/Batch items in `crates/rdlt-engine/src/runtime/graph.rs`
- [X] T013 [US2] Parquet reading in the file source: row-group batches via the parquet arrow reader, row-group cursor resume, `structured: true` stream specs in `crates/rdlt-source-file/src/parquet.rs`
- [X] T014 [P] [US2] Parquet destination: temp-dir staging, atomic renames, `_rdlt_state.json`/`_rdlt_commits.json` (format_version), deterministic staged names per `(load_id, commit_seq, table, n)`, teardown-on-open, Append/Replace per data-model §4 in `crates/rdlt-dest-parquet/src/lib.rs`
- [X] T015 [US2] Certification: destination conformance suite green (probe = parquet row count) in `crates/rdlt-dest-parquet/tests/conformance.rs`; make T010 pass
- [X] T016 [US2] Wire-up: CLI `[destination.parquet]` arm in `crates/rdlt-cli/src/main.rs`; end-to-end parquet→parquet copy test in `crates/rdlt-dest-parquet/tests/e2e_copy.rs`

**Checkpoint**: structured data moves end-to-end without re-processing.

---

## Phase 5: User Story 3 — Fail fast without per-row identity (Priority: P3)

**Goal**: Merge on a structured stream is a build-time error naming the stream.

**Independent Test**: `crates/rdlt/tests/facade.rs` additions — merge+structured fails
at build() naming the stream; append/replace succeed.

- [X] T017 [P] [US3] Write failing tests: build() rejects Merge on a structured stream with the stream named (clause B4); Append/Replace build and run; engine-level defense rejects too when facade is bypassed in `crates/rdlt/tests/facade.rs` and `crates/rdlt-engine/tests/passthrough.rs`
- [X] T018 [US3] Enforce B4 in `crates/rdlt/src/builder.rs` (needs stream specs at build: validate via `Source::streams()`… if async unavailable pre-run, enforce at run-start planning in `crates/rdlt-engine/src/runtime/graph.rs` and document the seam chosen) + make T017 pass

**Checkpoint**: all three stories independently green.

---

## Phase 6: Benchmarks & fold-in (cross-cutting)

- [X] T019 [P] Benchmark row 1: jsonl→DuckDB via bundled file source (same dataset/methodology as the existing RESULTS.md row; retire the example-binary measurement) in `benches/run-e2e.sh` + `benches/RESULTS.md`
- [X] T020 [P] Benchmark rows 2+3: parquet→parquet (≥2× cell, engine-bound) and parquet→DuckDB bonus, with a pinned-dlt parquet-reading baseline in `benches/baseline/pipeline_parquet.py` + `benches/RESULTS.md`
- [X] T021 Fold contract amendments into the base docs: S7/E7/B4 into `specs/001-rdlt-ingestion-engine/contracts/connector-spi.md` + embedder B4 into `contracts/embedder-api.md`; amend the design doc's benchmark table (parquet→parquet cell realized; bonus row noted) in `2026-07-18-rdlt-engine-design.md`
- [X] T022 Full sweep: `cargo fmt`, clippy `-D warnings`, `cargo nextest run --workspace`, doc-tests; update `specs/002-file-arrow-ingestion/tasks.md` implementation notes; PR to main (first PR through the armed CI gates incl. blocking semver-checks)

---

## Dependencies & Execution Order

```
Phase 1 ─► Phase 2 (T003)
             ├─► Phase 3 (US1: T004–T009)          — only needs T001+T003
             └─► Phase 4 (US2: T010–T016)          — T013 depends on US1's T005–T007 (file listing/cursor shared)
                        └─► Phase 5 (US3: T017–T018)
Phase 6: T019 after US1; T020 after US2; T021–T022 last.
```

### Parallel Opportunities

- T002 ∥ T003 after T001.
- Phase 4: T014 (parquet destination) is fully independent of T011–T013 (engine + source) — the widest window.
- T019 ∥ T020 once their stories land.

## Implementation Strategy

- **MVP** = Phases 1–3: certified file ingestion (the flagship benchmark's product
  path). Stop and validate.
- Phase 4 delivers the engine capability + both new connectors; Phase 5 is a small
  guardrail; Phase 6 publishes the numbers and folds contracts.
- This feature merges via PR — the first real exercise of the blocking semver gate
  (T003's "additive" claim gets machine-checked).

## Implementation notes (post-completion)

- **Bug caught by T020's benchmark**: the parquet→DuckDB run failed with
  `Catalog Error: Column with name _rdlt_load_id already exists!`. Root cause was in
  `rdlt-core`'s `UniqueNamer` — it dedupes by *source name*, so seeding system columns
  via `name_for("_rdlt_load_id")` made an input column literally named
  `_rdlt_load_id` (which rdlt-generated parquet has) *alias* the system column instead
  of getting suffixed → duplicate column in the CREATE TABLE DDL. The feature-001
  shredder had the same latent bug for `_rdlt_id`. Fixed with a new
  `UniqueNamer::reserve()` (claims a name under an un-matchable owner) used by both
  the shredder and passthrough; regression tests in `tests/passthrough.rs`
  (`input_column_named_like_system_column_is_suffixed`) and `tests/us1_full_sync.rs`
  (`json_field_named_like_system_column_is_suffixed`).
- Measured (2026-07-19, same machine/methodology as feature 001): jsonl→DuckDB via
  the product CLI **1.73 s / 642 MB RSS** vs dlt 19.60 s → **11.3×** (SC-002 met,
  product claim). parquet→parquet rdlt **77 ms / 47 MB** vs dlt arrow-native
  **185 ms / 218 MB** → **2.4×** (SC-004 ≥2× met). Bonus parquet→DuckDB: rdlt 307 ms
  vs dlt 352 ms (near-parity as expected — both reduce to DuckDB's C++ appender).
- The dlt parquet baseline needed `dlt[duckdb,filesystem]` + feeding pre-read
  `pyarrow.Table`s (its fastest route) to keep the comparison honest.
- Postgres conformance in the full sweep requires the podman user socket
  (`systemctl --user start podman.socket` + `DOCKER_HOST=unix:///run/user/1000/podman/podman.sock`).
- Final sweep: fmt clean, clippy 0 warnings, 109/109 nextest, doc-tests green.

## Notes

- Format validated: checkbox + ID + exact path on every task; [P] only where files
  and dependencies allow; story labels only in Phases 3–5.
- 22 tasks: Setup 2, Foundational 1, US1 6, US2 7, US3 2, Benchmarks/fold-in 4.
