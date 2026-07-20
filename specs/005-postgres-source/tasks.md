# Tasks: Postgres SQL Source Connector

**Input**: Design documents from `/specs/005-postgres-source/`

**Prerequisites**: plan.md, spec.md, research.md (R1–R9), data-model.md,
contracts/source-config.md, contracts/type-mapping.md, quickstart.md

**Tests**: INCLUDED — the spec explicitly demands thorough test +
crash-test coverage ("100% robustness", US4, SC-003/SC-005). Correctness
nets land with or before the code they protect; benchmark numbers (US3)
are quoted only on hardened code (plan phase ordering). Safe Rust only:
`unsafe_code = "deny"`, no new exceptions.

**Organization**: grouped by user story; US1 (snapshot) is the MVP; US2
(incremental) builds on it; US4 (robustness) is deliberately ordered
BEFORE US3 (benchmarks) per plan.md.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

- [X] T001 Create crate `crates/rdlt-source-postgres` (Cargo.toml:
      tokio-postgres + rustls connector, arrow, serde/serde_yaml,
      thiserror, tracing, rdlt-core/rdlt-connector; dev: testcontainers
      postgres, proptest) with module stubs per plan structure
      (`lib.rs`, `config.rs`, `reflect.rs`, `types.rs`,
      `copy_decode.rs`, `sqlgen.rs`, `cursor.rs`); add to workspace
      members + `rdlt` facade dependency; `cargo clippy` clean.
- [X] T002 [P] Shared test fixture in
      `crates/rdlt-source-postgres/tests/common/mod.rs`: testcontainers
      postgres:16 helper (start, psql-exec seed, conn string) mirroring
      the rdlt-dest-postgres test conventions, so every suite below
      reuses one fixture shape.

## Phase 2: Foundational (blocking all stories)

**⚠️ CRITICAL**: no user-story work before this phase completes.

- [X] T003 Config model in `crates/rdlt-source-postgres/src/config.rs`
      per contracts/source-config.md: full document (conn, schema,
      include_views, tables[], cursor{column, initial_value, boundary,
      direction, end_value, nulls}, primary_key, included/excluded
      columns, batch knobs), `deny_unknown_fields`, `from_yaml`,
      validation rules 4–6 (local checks) with typed errors; unit tests
      for every rejection rule in the contract.
- [X] T004 Connection establishment in
      `crates/rdlt-source-postgres/src/lib.rs`: tokio-postgres client
      with rustls TLS honoring `sslmode` (R1); error CLASSIFICATION per
      corrected R6 (connect io -> Transient for engine-owned retry,
      config/auth -> Fatal), typed `SourceError` naming table+phase;
      classification unit tests + one testcontainers smoke test.
- [X] T005 Type mapping in `crates/rdlt-source-postgres/src/types.rs`
      per contracts/type-mapping.md: OID(+typmod) → `LogicalType` for
      every contract row (lossless + policy tables), cursor-capable
      predicate, textual-fallback rule; exhaustive unit tests keyed to
      the contract tables (each row cited).
- [X] T006 Catalog reflection in
      `crates/rdlt-source-postgres/src/reflect.rs` (R3): one-round-trip
      pg_catalog query → `ReflectedTable` (column order/name/OID/typmod,
      NOT NULL, PK), schema + relkind filtering incl. `include_views`,
      contract validation rules 2–3 (table/cursor-column existence);
      testcontainers test: reflect a seeded schema with quoted
      identifiers, PKs, views, non-default schema.

**Checkpoint**: config/connect/types/reflect stand alone and tested —
US1 may begin.

## Phase 3: User Story 1 — Snapshot a Postgres database (Priority: P1) 🎯 MVP

**Goal**: connection string + table list → faithful typed replication
to any destination, streamed with bounded memory, per-table
statement-level snapshot.

**Independent Test**: seed the type-matrix schema; run a snapshot
pipeline to DuckDB; row counts, types, and values match exactly
(spec US1 independent test).

- [X] T007 [US1] Binary COPY decoder core in
      `crates/rdlt-source-postgres/src/copy_decode.rs` (R1/R4): COPY
      BINARY header/trailer + length-prefixed tuple parsing (NULL = -1)
      into Arrow builders for the LOSSLESS contract rows (bool, ints,
      floats, numeric→Decimal, text family, bytea, timestamps with
      epoch rebase, date, time, uuid, json/jsonb); batch cut at
      `batch_target_bytes`/`batch_max_rows`; typed decode errors (never
      panics); unit tests on hand-crafted wire bytes incl. truncation
      and NOT-NULL-violation drift.
- [X] T008 [US1] Decoder policy rows in `copy_decode.rs` +
      `types.rs`: unconstrained/oversized numeric → Utf8 canonical
      text, enums → label text, arrays/composites/ranges → canonical
      Json, interval/inet/money/timetz → Utf8, ±infinity timestamp/date
      saturation (contract "Special values"); unit tests per rule.
- [X] T009 [US1] SQL generation (snapshot half) in
      `crates/rdlt-source-postgres/src/sqlgen.rs`: strict identifier
      quoting, column projection from included/excluded config,
      `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` statement assembly;
      unit tests incl. hostile identifiers (quote-injection attempts).
- [X] T010 [US1] `Source` impl (snapshot path) in
      `crates/rdlt-source-postgres/src/lib.rs`: `spec()`, `streams()`
      (reflection → `StreamSpec{structured: true, cursor_field}`),
      `read()` driving copy_out → decoder → `PushPayload::Arrow`
      awaited pushes (S5 backpressure) → final `Checkpoint`;
      `ChannelClosed` = cancel (S4), connection dropped; typed errors
      naming table + phase (FR-008 surface).
- [X] T011 [P] [US1] Facade + CLI wiring: export
      `rdlt::postgres_source::PostgresSource` in
      `crates/rdlt/src/lib.rs`; add `SourceSpec::Postgres { config }`
      arm to `crates/rdlt-cli/src/main.rs` (mirrors rest/file arms);
      doc-comment examples compile (`cargo test --doc`).
- [X] T012 [US1] Conformance suite in
      `crates/rdlt-source-postgres/tests/conformance.rs`
      (testcontainers): full type-matrix round-trip into DuckDB
      (values + arrow types asserted per contract row), selection
      modes (explicit list / whole schema / include_views), quoted
      + mixed-case identifiers, non-default schema, empty table,
      zero-data-column rejection, table dropped between reflect and
      read → typed error (US1 acceptance scenarios 1–3 + edge cases).
- [X] T013 [P] [US1] Differential property test in
      `crates/rdlt-source-postgres/tests/differential.rs` (R8):
      proptest typed row sets seeded into pg; assert `copy_decode`
      batches ≡ driver-`FromSql`-built reference batches,
      byte-identical, across the generator's full type coverage.
- [X] T014 [P] [US1] Decoder fuzz target in
      `fuzz/fuzz_targets/pg_copy_decode.rs`: arbitrary bytes → decoder
      must return typed errors, never panic/OOM; register in
      FUZZ_TARGETS (Makefile) alongside existing targets.

**Checkpoint**: snapshot MVP — independently shippable and demoable
(quickstart steps 1–3 work end-to-end).

## Phase 4: User Story 2 — Incremental sync on a cursor column (Priority: P2)

**Goal**: dlt-parity cursor semantics; watermark + boundary dedup in
engine state; exactly-once across restarts.

**Independent Test**: run, mutate around the boundary (including
watermark-equal duplicates), re-run — exactly the new rows land and
state holds the max cursor (spec US2 independent test).

- [X] T015 [US2] Cursor state in
      `crates/rdlt-source-postgres/src/cursor.rs` (R5, data-model §3):
      `{watermark, boundary_keys}` ↔ engine `Cursor` JSON encode/decode
      for every cursor-capable type (contract rendering rules),
      monotonicity guard; property test `decode(encode(v)) == v`
      (data-model validation rule).
- [X] T016 [US2] SQL generation (incremental half) in `sqlgen.rs`:
      typed-literal-with-cast rendering (COPY takes no binds —
      injection-safe by construction, R5), boundary matrix (closed ≥ /
      open >, max/min direction, optional end_value < / ≤), NULL
      include (`IS NULL` union) / exclude; unit tests over the full
      matrix incl. literal-escaping of string/uuid/timestamp cursors.
- [X] T017 [US2] Incremental `read()` in `lib.rs`: `since` → resume
      predicate; per-batch watermark + boundary-key tracking; closed-
      boundary re-fetch dedup source-side; `Checkpoint(Cursor)` cadence
      after covered pushes; never emit a regressed watermark (FR-007).
- [X] T018 [US2] Incremental suite in
      `crates/rdlt-source-postgres/tests/incremental.rs`
      (testcontainers): first-run-full/second-run-delta,
      watermark-equal duplicates deduped (closed) vs open-boundary
      opt-out, NULL cursor include/exclude, regressing clock never
      moves state backward, initial_value/end_value windows, PK-less
      table (row-hash keys), Merge write-mode upsert end-to-end
      (US2 acceptance scenarios 1–5).

**Checkpoint**: incremental complete — snapshot + incremental both
independently green.

## Phase 5: User Story 4 — Robustness, proven by crash testing (Priority: P4 — ordered before benchmarks by design)

**Goal**: kill/drop anywhere → typed errors + convergent re-run; memory
provably bounded. Numbers in US3 are only quoted on code that has
passed this phase.

**Independent Test**: crash sweep over all registered pg fail points
(both passes) + forced connection drops → every case converges
exactly-once on re-run (spec US4 independent test).

- [X] T019 [US4] Register fail points (`pg_after_reflect`,
      `pg_mid_copy_stream`, `pg_before_checkpoint`,
      `pg_after_batch_push`) in the 003 fail-point registry behind the
      `failpoints` feature, wired through `lib.rs`/`copy_decode.rs`
      read/checkpoint paths (FR-009).
- [X] T020 [US4] Crash-sweep suite in
      `crates/rdlt-source-postgres/tests/crash_sweep.rs`: sweep every
      registered point, FIRST- and SECOND-occurrence passes (the 003
      lesson), assert typed error + exactly-once convergence on re-run
      against real Postgres + DuckDB; wire into `make test
      TARGET=sweep` (Makefile).
- [X] T021 [P] [US4] Connection-loss tests in `crash_sweep.rs`:
      (a) failpoint-injected io error mid-COPY → typed `SourceError`
      naming table+phase, committed work preserved, re-run resumes;
      (b) container kill mid-table → same convergence (US4 acceptance
      scenarios 2–3; Transient classification asserted — engine owns
      the retry loop per corrected R6).
- [X] T022 [P] [US4] Memory-ceiling test (SC-002) in
      `crates/rdlt-source-postgres/tests/memory_bound.rs`: seed a
      table ≥ 10× the ceiling, run the release CLI as a subprocess
      under `prlimit --as` with small batch knobs, assert success +
      row-count equality; self-skips with a visible note when prlimit
      is absent (R8).
- [X] T023 [US4] Schema-drift matrix in `conformance.rs`: column
      added / dropped / retyped between reflect and read → schema
      policy applied or typed error, never misaligned data (US4
      acceptance scenario 4); document the outcome per policy in the
      test names.

**Checkpoint**: hardened — the sweep, drift, and memory nets are green;
benchmark measurement may begin.

## Phase 6: User Story 3 — Benchmark cells, measured baseline-first (Priority: P3)

**Goal**: postgres→DuckDB and postgres→Postgres rows with
measurement-first bars per the 004 protocol; no existing gated cell
regresses.

**Independent Test**: harness produces both rows with same-session
baseline-first pairs, dataset identity, and policy-linked bar
derivations (spec US3 independent test).

- [ ] T024 [US3] Deterministic seed in `benches/baseline/seed_pg.py`
      (+ SQL): pg-wide (1 M × 12 typed cols) and pg-jsonb (200 k nested
      docs, reusing the harness generator) datasets into postgres:16;
      prints row counts + content hash (identity, R7); idempotent
      re-seed.
- [ ] T025 [US3] dlt baseline scripts
      `benches/baseline/pipeline_pg_duckdb.py` and
      `benches/baseline/pipeline_pg_pg.py` (backend parameter:
      pyarrow | sqlalchemy | connectorx), in-process self-timing +
      peak-RSS like the existing baselines; extend
      `benches/baseline/Dockerfile` with sql_database extras
      (sqlalchemy/psycopg2/pyarrow/connectorx) — dlt pin unchanged
      (1.29.0) and recorded.
- [ ] T026 [US3] rdlt cells recipe in `benches/run-e2e.sh` (or sibling
      `benches/run-pg.sh` if run-e2e length demands): baseline FIRST
      (pyarrow gated, sqlalchemy + connectorx scoreboard), then rdlt
      pg→DuckDB and pg→Postgres via the CLI, 5-run medians,
      `/usr/bin/time -v` RSS; quiet-machine discipline per 004.
- [ ] T027 [US3] Gated decoder bench `pg_copy_decode_10k` (canned COPY
      bytes → Arrow, no network) in
      `crates/rdlt-engine/benches/iai_hotpath.rs` pattern — placed as
      `crates/rdlt-source-postgres/benches/iai_pg.rs` wired into
      `make bench TARGET=iai` + `benches/compare-iai.sh`; record its
      NEW baseline in `benches/perf-baselines.json` in a commit naming
      this feature (P5-compliant new entry, not a drift re-record).
- [ ] T028 [US3] Measure + record: same-session baseline-first pairs
      for both cells; set gated bars measurement-first with explicit
      headroom; add matrix rows (Gated? status), version-policy
      entries, History note in `benches/RESULTS.md`; evidence artifacts
      (environment header per 004 rule) in
      `specs/005-postgres-source/evidence/`; verify existing gated
      cells within ±3% via `make bench TARGET=iai` (FR-011).

**Checkpoint**: performance claims recorded with the house discipline.

## Phase 7: Polish & cross-cutting

- [ ] T029 [P] Docs: crate README + rustdoc examples for
      `rdlt-source-postgres` (config walkthrough from the contract),
      design-doc `2026-07-18-rdlt-engine-design.md` §9 "Delivered
      post-v1" entry (SQL source fast-follow delivered, CDC still out),
      quickstart.md kept truthful against the shipped CLI surface.
- [ ] T030 Full verification sweep on the final tree: `make check`
      (lint, nextest incl. new suites, crash sweeps, iai gate) +
      `cargo test --doc`; `cargo semver-checks check-release
      --baseline-rev origin/main -p rdlt-core -p rdlt-connector`
      (FR-012: no SPI breakage); record the sweep + SC-006-style
      traceability walk (spec claims → evidence) in
      `specs/005-postgres-source/evidence/README.md`.
- [ ] T031 Implementation-notes block at the top of this tasks.md
      (house convention): outcomes, measured cells, deviations,
      backlog surfaced (e.g. cross-table snapshot, custom SQL streams,
      other dialects).

## Dependencies & Execution Order

```text
Phase 1 (T001 → T002∥) ─► Phase 2 (T003∥T005 → T004 → T006)
   └─► Phase 3 / US1: T007 → T008 → T009 → T010 → (T011∥T012∥T013∥T014)
          └─► Phase 4 / US2: T015 → T016 → T017 → T018
                 └─► Phase 5 / US4: T019 → T020 → (T021∥T022∥T023)
                        └─► Phase 6 / US3: T024 → T025 → (T026, T027) → T028
                               └─► Phase 7: (T029∥) T030 → T031
```

- T003/T005 are independent files ([P] against each other); T004 needs
  T003 (retry config), T006 needs T004+T005.
- US2 depends on US1's `read()` skeleton and sqlgen; US4 depends on
  US1+US2 paths existing (fail points live in them); US3 is hard-gated
  on US4 (plan ordering: numbers only on hardened code).
- T027 can start after T007 (decoder exists) but its baseline is
  recorded only at T028 time — listed in US3 to keep the P5 gate rule
  in one place.
- Measurement tasks (T026/T028) are quiet-machine serialized; nothing
  heavy runs beside them (003/004 lesson).

### Parallel Opportunities

- After T010: T011, T012, T013, T014 touch disjoint files.
- After T020: T021, T022, T023 are disjoint suites.
- T029 (docs) any time after US3; T002 alongside late Phase-1 work.

## Implementation Strategy

- **MVP** = Phases 1–3: a demoable, conformance-tested snapshot
  connector (quickstart works). Stop and validate before incremental.
- US2 then US4 build on it in strict order; US3 last so every quoted
  number stands on hardened, swept code.
- Every task that produces a measurement commits its evidence artifact
  in the same change (004 rule); every suite lands in the same change
  as the code it nets (correctness before speed).

## Notes

- 31 tasks: Setup 2, Foundational 4, US1 8, US2 4, US4 5, US3 5,
  Polish 3.
- Format validated: checkbox + sequential ID on every task; [P] only on
  genuinely disjoint files; story labels only in Phases 3–6.
