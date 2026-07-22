# Tasks: DuckDB Destination Completeness

**Input**: Design documents from `/specs/013-duckdb-completeness/`

**Prerequisites**: plan.md, research.md (R1–R8), data-model.md,
contracts/shared-merge-core.md (SM1–SM8), quickstart.md

**Tests**: included — this feature's discipline is weld-based: golden-SQL
pins land BEFORE the extraction, probes land BEFORE their dialect arms,
behavior cells land WITH each arm, and the matrix commits WITH the cells
that close its gaps (011 rule).

**Organization**: tasks grouped by user story; US order is build order.
The extraction (Phase 2) and the DuckDB work (Phase 3+) are SEPARATE
commits — SM4 reviewability.

## Phase 1: Setup

- [ ] T001 The weld: measure the duckdb-crate coverage BASELINE
  (`cargo llvm-cov nextest -p rdlt-connector-duckdb`, recorded before
  any new cell — 011 R2 rule) and write the golden-SQL pin suite
  `crates/rdlt-connector-postgres/tests/golden_sql.rs` against TODAY'S
  code: capture the exact SQL strings the postgres dest generates for a
  representative plan matrix (each strategy × dedup_sort × merge_key ×
  hard_delete × scd2 absent modes × explicit-vs-default strategy);
  suite green against unmodified code, strings asserted verbatim.

## Phase 2: Foundational — the extraction (blocking; pure moves, no DuckDB code)

- [ ] T002 Scaffold `crates/rdlt-connector-sqlcore` (member in
  `Cargo.toml`, deps serde + thiserror only — SM8) and MOVE the shared
  vocabulary from `crates/rdlt-connector-postgres/src/dest/config.rs`:
  `MergeStrategy`, `Scd2Options`, `TableOptions`, `DestOptions`
  (neutral names, serde spellings frozen) plus the parse-layer
  validation; postgres re-exports at the existing paths
  (`PgDestOptions`/`PgTableOptions` aliases) — CLI, facade, and schema
  round-trip tests untouched-green.
- [ ] T003 Move the plan shapes: `MergePlan` (dedup/survivor ordering,
  scope replacement, strategy arms, hard-delete-on-survivor decision,
  per-table single-commit-unit state) from
  `crates/rdlt-connector-postgres/src/dest/commit.rs` into
  `crates/rdlt-connector-sqlcore/src/plan.rs`, and define
  `MergeDialect` in `crates/rdlt-connector-sqlcore/src/dialect.rs`
  (hooks per data-model §3: quote_ident, dedup_select, delete_in/
  scope_delete, upsert, scd2_close/scd2_open, tx_boundary_expr,
  ensure_merge_index) — SQL TEXT only (SM2); unit cells for shape
  logic move with the code.
- [ ] T004 Extract the postgres dialect into
  `crates/rdlt-connector-postgres/src/dest/dialect.rs` and rewire
  `dest/commit.rs` through sqlcore. PROOF (SM4): golden_sql pins
  byte-identical, full postgres suite + refined-merge/scd2/upsert
  crash sweeps green with zero behavioral edits, semver-checks "no
  update required", `TARGET='pg-wide-*' make bench` in-band. This
  phase's work is ONE reviewable commit of moves.

**Checkpoint**: sqlcore exists, postgres runs on it, nothing observable
changed anywhere. DuckDB work starts only now.

## Phase 3: User Story 1 — One merge vocabulary, two destinations (P1) 🎯 MVP

**Goal**: the full 008/010 options vocabulary behaves identically on
DuckDB via the shared core + a DuckDB dialect.

**Independent test**: swap `postgres:` for `duckdb:` in the 008/010
option docs — every option behaves as documented or fails with the same
typed error.

- [ ] T005 [US1] Probe cells FIRST (R4/R5, one test per assumption) in
  `crates/rdlt-connector-duckdb/tests/probes.rs`: DISTINCT ON ordering
  semantics, ON CONFLICT DO UPDATE against a CREATE UNIQUE INDEX
  target, tx-stable now() inside one transaction, IS DISTINCT FROM —
  each probe documents pass/fail; a fail converts the dependent arm to
  a typed capability gap (SM3) and is recorded for the matrix.
- [ ] T006 [US1] Options hook + validation on the DuckDB dest: split
  `crates/rdlt-connector-duckdb/src/lib.rs` into
  `src/{config,commit}.rs` (the 008 split-when-code-arrives rule),
  add `.options(DestOptions)` consuming sqlcore's shared parse layer,
  and the open-time checks (existence/collisions/capability gaps)
  with the SAME typed-error posture — incl. explicit merge_strategy
  under append/replace rejection (011 R5) and NULL-in-key; identical
  typed-error cells beside the postgres wording.
- [ ] T007 [US1] The DuckDB dialect + strategy arms in
  `crates/rdlt-connector-duckdb/src/dialect.rs` + `src/commit.rs`:
  delete_insert (default — today's keyed merge routed through the
  shared plan), upsert (ON CONFLICT + auto-ensured unique
  merge-identity index), scd2 (validity columns, boundary via
  tx-stable now(), absent keep/retire under the per-table single-unit
  rule), each welded to behavior cells in
  `crates/rdlt-connector-duckdb/tests/strategies.rs` proving
  destination-visible outcomes (totals, in-place update, history
  close/open) per the 008 contract.
- [ ] T008 [US1] The 010 refinements on DuckDB: dedup_sort (ordered
  survivor via the shared dedup shape; values beat NULL, deterministic
  ties) and merge_key (scope delete before the strategy arm,
  single-unit rule shared with scd2 absent-retire) + hard_delete
  composition — behavior cells in
  `crates/rdlt-connector-duckdb/tests/refinements.rs` mirroring the
  010 postgres cells' claims.

**Checkpoint**: US1 delivers dlt-parity merge on DuckDB; docs vocabulary
is destination-neutral in substance.

## Phase 4: User Story 2 — Honest capabilities closed (P2)

**Goal**: `Json` lands as native DuckDB JSON; every capability row is
true-and-proven or false-and-documented.

**Independent test**: a Json-typed column round-trips through DuckDB's
JSON type and `json_extract` works on it.

- [ ] T009 [US2] JSON probe + flip: probe the bundled JSON extension
  (R6) in `crates/rdlt-connector-duckdb/tests/probes.rs`; on pass,
  create Json columns as JSON with the stage→target CAST in
  `src/commit.rs`, flip `json_type: true` in capabilities, and prove
  round-trip (incl. the postgres jsonb escape-hatch feed) +
  `json_extract` in `crates/rdlt-connector-duckdb/tests/json.rs`;
  on fail, record the finding and keep the capability false —
  either way the matrix row cites proof.
- [ ] T010 [P] [US2] Capability audit: every `DestCapabilities` field
  for DuckDB verified true-and-proven (cells cited) or
  false-and-documented, recorded as matrix rows; README capability
  matrix row updated.

## Phase 5: User Story 3 — Verified to the postgres standard (P3)

**Goal**: matrix, differential oracle, sweeps, coverage floor, dlt
parity record.

**Independent test**: matrix zero uncited rows; differential green;
sweeps armed; coverage ≥80% recorded.

- [ ] T011 [US3] Cross-destination differential oracle in
  `crates/rdlt/tests/dest_differential.rs` (R7): shared feed scripts
  (append/replace/keyed merge × 3 strategies, duplicates + dedup_sort,
  hard_delete flags, scoped loads, rejection cases) through postgres
  (testcontainers) and DuckDB (temp file); equivalence = canonical
  per-table rows + typed-error-class parity, modulo the documented
  type-affinity table written alongside the suite.
- [ ] T012 [US3] Crash sweeps for the new arms:
  extend the duckdb failpoints and
  `crates/rdlt-connector-duckdb/tests/recovery.rs` (or a new
  `tests/sweep.rs`) — armed-fire pins per strategy, crash/rerun
  convergence, single-unit rules under crash-resume (the 010 lesson:
  sweep the scoped-stream crash window explicitly).
- [ ] T013 [US3] The traceability matrix
  `specs/013-duckdb-completeness/matrix.md`: every duckdb destination
  option/value/validation row cited (011 rules — citations to
  006/008/010/013 suites first, gap cells written WITH this task where
  the audit finds holes), zero uncited rows; probe outcomes and
  capability gaps recorded as rows.
- [ ] T014 [US3] Coverage close-out: re-measure
  `cargo llvm-cov nextest -p rdlt-connector-duckdb`, reach the ≥80%
  floor, classify exclusions, record baseline→final in
  `benches/RESULTS.md` beside the 011 record.
- [ ] T015 [US3] dlt parity record in
  `specs/013-duckdb-completeness/dlt-parity.md`: per-option behavior
  vs pinned dlt 1.29.0's duckdb destination (010 format), deviations
  documented individually; out-of-scope-everywhere features listed as
  such.

## Phase 6: Polish & close-out

- [ ] T016 Scoreboard bench cells: add `duckdb-strategy-delete-insert-1m`
  and `duckdb-strategy-upsert-1m` to `benches/cells/merge.toml`
  (pg-src fixture → DuckDB file dest, load-2 timed, `{{run}}`-unique
  50% updates, verify 1M rows) + pipeline templates in
  `benches/cells/pipelines/`; run them, commit artifacts,
  `TARGET=report make bench` regenerates the merge table.
- [ ] T017 README destination-options section goes destination-neutral
  (one options reference, per-destination capability notes) in
  `README.md`; close-out sweep — `make check`, `cargo test --doc`,
  semver-checks "no update required", every gated bar within
  tolerance, quickstart.md commands walked verbatim; tasks/matrix
  cross-check (zero unresolved rows).

## Dependencies

- T001 → T002 → T003 → T004 (strictly sequential; the extraction chain)
- US1: T005 → T006 → T007 → T008 (probes gate the arms)
- US2: T009 after T007 (needs the commit seam); T010 [P] anytime after T006
- US3: T011 after T008 (needs all arms); T012 after T008; T013 after
  T008–T012 (cites them); T014 after T013 (coverage measured after the
  last cell); T015 after T008
- T016 after T007; T017 last
- Parallel: T009/T010; T011/T012/T015 (different files); T016 with US3
  tasks (different files, but its RUN needs a quiet machine — schedule
  the measurement after compile-heavy work)

## Implementation strategy

MVP = Phases 1–3: after T004 the codebase is refactored but externally
identical (a safe merge point on its own); after T008 DuckDB has full
merge parity. US2/US3 make the parity claim honest. The golden-SQL pins
(T001) are the contract for everything in Phase 2 — if a pin must
change, STOP: that is a behavioral edit, not an extraction. Probes
(T005/T009) decide arms before they're written — a red probe is a
recorded typed capability gap, not a blocker.
