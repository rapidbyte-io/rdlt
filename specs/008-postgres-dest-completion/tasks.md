# Tasks: Postgres Destination Completion

**Input**: Design documents from `/specs/008-postgres-dest-completion/`

**Prerequisites**: plan.md, spec.md, research.md (R1–R11, incl. the R2
zero-config owner clarification), data-model.md,
contracts/{dest-types, merge-strategies, scd2}.md, quickstart.md

**Tests**: INCLUDED — every success criterion is test-defined (type
fidelity SC-001, strategy conformance + sweep SC-002/003, SCD2 SC-004,
measured index claim SC-005, unchanged gate SC-006, relocation + F6
SC-007, schema round-trips SC-008). Safe Rust only; zero new
dependencies; zero SPI changes; WriteMode frozen.

**Organization**: by user story after one BLOCKING foundational task
(the moves-only relocation). US1 fidelity is the MVP. US2 strategies
next; US3 SCD2 builds on US2's config/strategy machinery; US4 is the
F6 error-chain closure (its relocation half lives in T001).

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

No setup tasks — no new dependencies, no workspace changes (plan
Technical Context).

## Phase 2: Foundational (BLOCKING)

- [X] T001 Pure relocation of `crates/rdlt-postgres/src/dest/mod.rs`
      (613 lines) into `dest/{mod.rs, config.rs, ddl.rs, encode.rs,
      commit.rs}` per the plan's layout — MOVES ONLY, zero behavior
      change, own commit stating the rule; `config.rs` starts as the
      builder relocation (options types come in T005); gate: the FULL
      existing suite (unit, dest_conformance, dest_recovery, both
      crash sweeps armed) passes unchanged (FR-011, SC-007 first half).

**Checkpoint**: the destination has the source's shape; every later
task edits a focused module.

## Phase 3: User Story 1 — Native type fidelity (Priority: P1) 🎯 MVP

**Goal**: decimals/JSON/UUID land as NUMERIC(p,s)/JSONB/UUID with NOT
NULL honored — ZERO user configuration (R2 owner clarification: the
capability flags are code-level connector declarations; no config key
exists). Pre-008 text columns stay untouched but visible.

**Independent Test**: spec US1 — pg→pg round trip yields native
catalog types; SUM over decimals equals the source exactly; JSON-path
and uuid-literal queries work; NOT NULL created; old-table behavior
documented and visible.

- [X] T002 [US1] Wire encoders in
      `crates/rdlt-postgres/src/dest/encode.rs`: `NumericWire(i128,
      scale)` (base-10000 groups — mirror of the source decoder),
      `JsonbWire` (version byte 1 + UTF-8), `UuidWire` (16 bytes from
      canonical text; parse failure = typed error naming the column)
      as `ToSql` impls (R1); property tests round-tripping encode →
      `source/copy_decode.rs` decode over the full range incl. NULLs,
      precision edges, negative/zero/fractional decimals (T5).
- [X] T003 [US1] Native DDL + capability flip + conformance (WELDED —
      plan rule: a flipped capability without its round-trip proof is
      a silent-corruption risk): `dest/ddl.rs` `sql_type` gains
      `NUMERIC(p,s)`/`JSONB`/`UUID` + NOT NULL from
      `ColumnDef.nullable` (CREATE only, T4); `dest/encode.rs`
      `copy_type`/`cell_value` decide per TABLE-SCHEMA logical type,
      never raw arrow (T6); `dest/mod.rs` capabilities flip
      `decimal: true, json_type: true`; conformance cells in
      `crates/rdlt-postgres/tests/dest_conformance.rs`: catalog type
      assertions, decimal SUM equality, JSON-path query, uuid-literal
      join, NULL rows, boundary values, JSONB-rejected document =
      typed error naming the column (SC-001, T1–T3, T8).
- [X] T004 [US1] ~~Pre-008 text-column visibility~~ RESOLVED BY OWNER
      DECISION (greenfield): the fallback was implemented, then removed —
      no installed base exists to protect. Additive-only rule unchanged;
      mismatched hand-created tables fail loudly at publish (server
      message + SQLSTATE via describe()). Contract dest-types.md T7,
      spec FR-003/US1-AS5, and research R3 all amended in the same
      change.

**Checkpoint**: warehouse consumers get real types — shippable MVP.

## Phase 4: User Story 2 — Merge strategies + speed (Priority: P2)

**Goal**: upsert (auto unique index, 23505 typed), hard-delete column,
supporting indexes with a measured before/after; no-config behavior
byte-identical.

**Independent Test**: spec US2 — update-heavy upsert converges with
exact totals; flagged rows disappear; duplicate-key upsert fails
naming the key; sweep green; index measurement recorded.

- [ ] T005 [US2] Options surface in
      `crates/rdlt-postgres/src/dest/config.rs`: `PgDestOptions`
      {merge_strategy (delete_insert default), tables:
      map<table, PgTableOptions {merge_strategy, hard_delete,
      scd2: Scd2Options {valid_from, valid_to, absent}}>} per
      data-model; serde + schemars + `from_value`; builder
      `Postgres::options(...)`; validation errors NAME the field
      (scd2+hard_delete contradiction S8, unknown strategy, empty
      column names); unit tests for the validation matrix (FR-004,
      FR-012 config half).
- [ ] T006 [US2] Index ensure in `crates/rdlt-postgres/src/dest/ddl.rs`:
      deterministic names (`rdlt_ix_/rdlt_ux_<table>_<hash>`),
      `IF NOT EXISTS`, identity-appropriate columns (`_rdlt_id`/
      `_rdlt_root_id`/key columns; upsert = UNIQUE; scd2 = (key…,
      valid_to)) per data-model table; 23505 during unique-create =
      typed error naming the key columns (M3); unit tests for name
      determinism + conformance cell for the duplicate-key rejection
      in `tests/dest_conformance.rs` (M5, M6, M7).
- [ ] T007 [US2] Strategy SQL in
      `crates/rdlt-postgres/src/dest/commit.rs`: upsert arm (DISTINCT
      ON dedup + ON CONFLICT DO UPDATE, shredded identity AND keyed
      structured — R4) + hard-delete composition (flagged-key DELETE +
      exclusion from insert/upsert, bool `= TRUE` / other
      `IS NOT NULL` — R5); conformance in `tests/dest_conformance.rs`:
      upsert update-heavy convergence + three stable re-runs,
      hard-delete exact totals + never-loaded no-op + redelivery
      no-op, default-config byte-identical pin (M1) (SC-002 first
      half, SC-003).
- [ ] T008 [US2] Upsert under fire in
      `crates/rdlt-postgres/tests/dest_crash_sweep.rs`: plumb strategy
      into the keyed sweep harness; upsert cells across every
      registered fail point × three actions with armed-fire pins
      (M2, SC-002 second half).
- [ ] T009 [US2] Index measurement (FR-009, SC-005): recipe in
      `benches/run-pg.sh` (or a sibling script) — large keyed merge,
      drop-index baseline vs indexed, same session, 5-run medians;
      record BOTH numbers as a scoreboard entry in
      `benches/RESULTS.md` (no new gate; version-policy untouched).

**Checkpoint**: production merge power, measured.

## Phase 5: User Story 3 — SCD2 history (Priority: P3)

**Goal**: full version history per key with stable boundaries and an
absence policy; exactly-once under redelivery.

**Independent Test**: spec US3 — three load rounds produce correct
versions, ranges, single-active invariant, point-in-time answers;
redelivery adds zero versions.

- [ ] T010 [US3] SCD2 implementation:
      `crates/rdlt-postgres/src/dest/ddl.rs` creates validity columns
      (configured names; collision + keyless typed errors at ensure —
      S1) and the scd2 index; `dest/commit.rs` scd2 arm inside the
      publish transaction — in-batch last-wins dedup (S4), NULL-safe
      `IS DISTINCT FROM` change detection over non-key columns,
      retire-then-insert at one boundary per commit unit (minted at
      first execution; D3 receipts make redelivery a no-op — S5),
      skip-unchanged (S3), absence policy keep/retire (S6);
      scd2+hard_delete rejected (S8) (R6).
- [ ] T011 [US3] SCD2 conformance in
      `crates/rdlt-postgres/tests/scd2.rs` (new): three-round history
      — version counts, non-overlapping contiguous ranges, exactly one
      active per key, point-in-time queries (S7); both absence
      policies; engine-driven crash/redelivery cell proving zero
      duplicate versions (S5); collision + keyless + scd2-on-keyless
      rejection cells (SC-004).

## Phase 6: User Story 4 — Error chains (Priority: P4)

**Goal**: review-F6 closed — every destination db error carries the
server's message + SQLSTATE. (The story's relocation half shipped in
T001.)

**Independent Test**: spec US4 — a forced database failure surfaces
the server message and SQLSTATE in the pipeline error.

- [X] T012 [US4] `describe()` in
      `crates/rdlt-postgres/src/dest/commit.rs`: prefer
      `as_db_error()` (message + SQLSTATE), else source-chain walk;
      `transient()`/`fatal()` route through it, SQLSTATE transient
      heuristic (08/53/57/40) unchanged (R8); regression test in
      `tests/dest_conformance.rs`: forced constraint violation
      surfaces message + SQLSTATE, never bare "db error" (FR-010,
      SC-007 second half).

## Phase 7: Polish & close-out

- [ ] T013 [P] CLI + schemas: `crates/rdlt-cli/src/main.rs`
      `[destination.postgres]` gains merge_strategy/tables/hard_delete/
      scd2 (same serde types); `crates/rdlt-postgres/tests/config_schema.rs`
      grows PgDestOptions round-trips — examples validate, unknown
      fields fail both, contradiction cells (SC-008, FR-012).
- [ ] T014 [P] Strategy scoreboard: merge-heavy cells (1M rows, ~50%
      updates: delete-insert vs upsert) in `benches/run-pg.sh`;
      medians recorded as scoreboard entries in `benches/RESULTS.md`
      with a history line (FR-013; measurement-first, no gate moves).
- [ ] T015 Close-out: `make check` + `cargo test --doc` +
      `cargo semver-checks check-release --baseline-rev origin/main
      -p rdlt-core -p rdlt-connector` ("no update required" — zero SPI
      promise); gated bars within tolerance (SC-006); README +
      quickstart truthful against shipped surfaces (incl. the
      zero-config native-types statement); implementation-notes block
      at the top of this file; dlt-parity statement updated (upsert/
      scd2/hard-delete closed; dedup-sort + geometry recorded OUT);
      all tasks [X].

## Dependencies & Execution Order

```text
Phase 2: T001 (BLOCKS everything)
   ├► US1: T002 → T003 → T004        (MVP)
   ├► US2: T005 → T006 → T007 → T008 → T009
   │        └► US3: T010 → T011      (needs T005 config + T006 indexes)
   ├► US4: T012                      (independent after T001)
   └► Polish: T013 ∥ T014 (T013 needs T005; T014 needs T007) → T015
```

- T002 is unit-only (no container) — parallel-safe with T005/T012 by
  file, but single-session order follows the phases.
- T003's capability flip is WELDED to its conformance cells (plan
  rule); do not split.
- T009 and T014 are measurements — quiet machine, protocol P2.

## Implementation Strategy

T001 first, always. MVP = Phase 3 (native types, zero config) — the
highest-value shippable slice. Strategies and SCD2 layer on without
touching defaults (M1 pins byte-identical no-config behavior). Any
stop-point after a phase leaves main-mergeable state: nothing outside
`crates/rdlt-postgres` + CLI + benches changes, and the perf gate
arbitrates every step (SC-006).
