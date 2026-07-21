# Tasks: Merge Refinements — Ordered Dedup + Scope Keys

## Implementation notes (close-out, 2026-07-21)

- **All 9 tasks complete.** `make check` green (322 workspace tests incl.
  9 refinement conformance cells + all sweeps, iai gate within
  tolerance); `cargo test --doc` green; semver-checks: rdlt-core AND
  rdlt-connector "no semver update required"; zero new dependencies.
- **The headline finding — MR5 corrected by this feature's own sweep**:
  the planned receipts design (multi-commit-unit scope replacement) was
  WRONG. The refined-merge crash sweep proved it before it shipped: a
  crash-RESUMED load is a NEW load delivering a PARTIAL feed, and its
  scope delete destroyed the previous attempt's committed rows (75/100
  survived the recovery cell). Resume-vs-fresh is undecidable
  destination-side, so the spec's "complete truth for its scope" premise
  must arrive atomically: scope replacement runs in the load's FIRST
  commit unit only; a later unit with staged scoped rows is a typed
  error advising the commit thresholds (scd2 S6 rule + remedy); later
  EMPTY units tolerated (conformance-pinned). Receipts table deleted.
  Research R2 amendment, contract MR5, and data-model record it.
- **dedup_sort** landed as planned: one ORDER BY rewrite in the shared
  dedup shape; survivor drives hard-delete/upsert/SCD2 by construction;
  matrix proves desc/asc under wrong arrival, FR-002 unchanged-absent,
  survivor's-flag hard-delete both directions, NULL/tie/replay policy.
- **Scoreboard**: scope-replace of a 100k-row scope on 10M rows =
  1,559.9 ms median — 19% FASTER than the identity-only delete of the
  same keys (one scope-index probe vs 100k key lookups); ordered dedup
  278.4 ms vs 334.6 ms last-wins on a 2×-duplicated 1M-row stage. No
  new gates; existing bars untouched by construction.
- **Review round (workflow-backed, 23 agents, 2026-07-21)**: 10 findings,
  all fixed. The big three were in MY single-commit-unit guard: (1) it
  keyed on `load_committed_before` so a crash-resume bypassed it (new
  load_id) and silently destroyed the previous attempt's scope rows;
  (2) load-global keying made any multi-stream pipeline fatal spuriously;
  (3) the split-feed error left a half-replaced scope with no recovery
  prescription. Fix: ONE per-table discipline shared with scd2
  absent-retire — first-STAGED-unit execution, typed error on a split
  feed (recovery prescribed), empty units skipped (also closing scd2
  retire's mass-retirement-on-empty-stage edge AND its own multi-stream
  false positive), marks applied post-commit + on the D3 replay branch.
  The irreducible crash-resume residual (mid-feed-checkpointing scoped
  stream + crash in the window) is RECORDED in MR5/quickstart — scoped
  streams must checkpoint only at feed end. Also fixed: dedup_sort on a
  merge-key column typed (silent no-op); both options typed-rejected
  under Append/Replace (008 F6 class); merge_key columns join the auto
  index plan (pinned — the scoreboard number is now honest);
  CLAUDE.md/plan.md/tasks.md purged of the killed receipts design;
  scd2+dedup_sort composition cell added (MR2 for scd2 pinned); the two
  divergent single-unit guards unified.
- **Deviations**: the open-time validation site is
  `dest/commit.rs::ensure_table` (tasks text said ddl.rs — followed the
  code, where M7/S1 live). Also hardened the 009 container-kill cell
  (pre-existing race: a fast catch-up could FINISH before the container
  teardown landed under parallel load — backlog widened 400k×120B →
  1M×300B).

**Input**: Design documents from `/specs/010-merge-refinements/`

**Prerequisites**: plan.md, spec.md, research.md (R1–R5 + recorded dlt
deviations), data-model.md, contracts/merge-refinements.md (MR1–MR8),
quickstart.md

**Tests**: INCLUDED — every success criterion is test-defined (US1
matrix SC-001, US2 matrix SC-002, sweep SC-003, validation SC-004,
schemas/CLI SC-005, governance SC-006, scoreboard SC-007). Safe Rust
only; ZERO new dependencies; ZERO SPI changes; behavior unchanged when
the options are absent (FR-002).

**Organization**: config surface is foundational (everything reads it);
US1 is the MVP (one ORDER BY rewrite, welded to its matrix); US2 builds
the receipts machinery; US3 hardens the surface; polish sweeps,
measures, closes out.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

No setup tasks — no new dependencies, no workspace changes (plan
Technical Context).

## Phase 2: Foundational (BLOCKING)

- [X] T001 Config surface in `crates/rdlt-postgres/src/dest/config.rs`:
      `SortOrder { Asc, Desc }` + `DedupSort { column, order }` (order
      REQUIRED, R1) and `PgTableOptions.{dedup_sort: Option<DedupSort>,
      merge_key: Option<Vec<String>>}`; parse-time shape validation in
      `PgDestOptions::validate` (empty column, empty/duplicate merge_key
      list — typed, names the table) with a unit matrix; schemars
      derives (MR7 schema cells land in T006).

**Checkpoint**: both options parse, validate, and reach the commit path.

## Phase 3: User Story 1 — Ordered survivor selection (Priority: P1) 🎯 MVP

**Goal**: `dedup_sort` picks the surviving in-load version by column +
direction through the ONE shared dedup shape; absent = unchanged.

**Independent Test**: spec US1 — one load, two versions of one key in
wrong arrival order: `desc` keeps the newest, `asc` the oldest, absent
keeps last-arriving; the survivor's flag drives hard-delete.

- [X] T002 [US1] `MergePlan::deduped()` ordering rewrite in
      `crates/rdlt-postgres/src/dest/commit.rs`: `ORDER BY {key},
      {column} {DESC|ASC} NULLS LAST, __rdlt_arrival DESC` when
      dedup_sort is declared (R1/MR1) — `flagged_roots()` untouched
      (shredded streams reject the option in T005); WELDED conformance
      (plan rule — an unproven survivor rule is silent data corruption):
      the MR-US1 matrix in `crates/rdlt-postgres/tests/dest_conformance.rs`
      — desc and asc survivors under wrong arrival order; the FR-002
      absence cell (last-wins byte-for-byte unchanged); survivor's-flag
      hard-delete (US1-AS3, MR2); NULL-loses-to-value and all-NULL
      last-wins (US1-AS4); tie determinism; replay stability across a
      re-run (US1-AS5); upsert AND delete-insert arms both covered
      (SC-001).

**Checkpoint**: ordered survivors proven — shippable MVP.

## Phase 4: User Story 2 — Scope-key replacement (Priority: P2)

**Goal**: `merge_key` replaces every delivered scope wholesale, sound
across multi-commit-unit loads via durable per-load receipts.

**Independent Test**: spec US2 — seed two scopes, re-deliver one with a
row missing: the row is gone, the untouched scope intact, identity
merging still applies inside the delivered scope.

- [X] T003 [US2] [SUPERSEDED by the R2 amendment — receipts removed; see close-out notes] Receipts substrate in
      `crates/rdlt-postgres/src/dest/ddl.rs` +
      `crates/rdlt-postgres/src/dest/commit.rs`: create
      `_rdlt_scope_receipts (load_id, table_name, scope, PRIMARY KEY
      (load_id, table_name, scope))` alongside `_rdlt_commits`; prune
      other loads' receipts for a table at the load's FIRST unit
      (`load_committed_before == false`, mirroring replace-truncate-once,
      R2/MR5).
- [X] T004 [US2] [AS AMENDED — first-staged-unit rule, no receipts; see close-out notes] Scope delete in
      `crates/rdlt-postgres/src/dest/commit.rs`: before the strategy
      arm, inside the publish tx — delete target rows whose scope
      matches an UNRECEIPTED stage scope (NULL-in-any-column excluded
      both sides, MR4), insert receipts `ON CONFLICT DO NOTHING`, then
      the existing arm unchanged (delete-insert AND upsert, R2/MR3);
      WELDED conformance (plan rule — the multi-unit cell is
      NON-OPTIONAL, the 008 S6/F2 lesson): the MR-US2 matrix in
      `crates/rdlt-postgres/tests/dest_conformance.rs` — delivered-scope
      replacement with undelivered rows gone (US2-AS1), untouched
      scopes intact, unseen-scope insert (US2-AS2), scope-moving update
      never duplicates (US2-AS3), NULL scope matches nothing (US2-AS4),
      a MULTI-COMMIT-UNIT load (engine commit thresholds forced small)
      replaces each scope exactly once, replay idempotence (US2-AS5),
      composition with hard_delete and with dedup_sort together
      (SC-002).

**Checkpoint**: window/partition refreshes converge, any load shape.

## Phase 5: User Story 3 — Loud, typed configuration surface (Priority: P3)

**Goal**: every invalid shape is its own typed error before data moves;
both options ride schemas and the CLI.

**Independent Test**: spec US3 — the validation matrix plus schema/CLI
round-trips.

- [X] T005 [US3] Open-time validation in
      `crates/rdlt-postgres/src/dest/ddl.rs` (`ensure_table`, next to
      M7/S1): nonexistent dedup_sort/merge_key columns (typed, names
      table + column); collisions with the hard_delete flag and scd2
      validity columns; both options rejected on shredded (identity)
      streams; `merge_key` + scd2 rejected (R3/MR6); conformance error
      cells for EVERY row of the data-model error taxonomy in
      `crates/rdlt-postgres/tests/dest_conformance.rs` (SC-004).
- [X] T006 [P] [US3] Schemas + CLI in
      `crates/rdlt-postgres/tests/config_schema.rs` (dest-options cells:
      documented example with both fields validates AND parses; unknown
      fields and bad `order` tokens fail both layers) and a
      `[destination.postgres.tables.<n>]` toml passthrough cell for both
      options in `crates/rdlt-cli/src/main.rs` tests (zero CLI code
      change expected — serde carries them, MR7/SC-005).

## Phase 6: Polish & close-out

- [X] T007 Crash-sweep arms in
      `crates/rdlt-postgres/tests/dest_crash_sweep.rs`: the existing
      registered dest fail points swept with dedup_sort AND merge_key
      armed (both occurrence passes, armed-fire pins), post-recovery
      equality incl. a receipts-consistency check — a replayed unit
      never double-deletes a scope (SC-003, MR5).
- [X] T008 [P] Scoreboard in `benches/run-merge-refinements.sh` (new):
      (a) scope-replace of one 100k-row scope in the 10M-row harness vs
      identity-only delete-insert; (b) ordered dedup on a 2×-duplicated
      1M-row stage vs last-wins; 5-run medians recorded in
      `benches/RESULTS.md` (scoreboard, no gate; existing gated bars
      within tolerance — R4/SC-007).
- [X] T009 Close-out: `make check` + `cargo test --doc` +
      `cargo semver-checks check-release --baseline-rev origin/main
      -p rdlt-core -p rdlt-connector` ("no update required");
      README + 008 `merge-strategies.md` cross-reference +
      quickstart truthful; implementation-notes block at the top of
      this file; memory update; all tasks [X] (SC-005/SC-006).

## Dependencies & Execution Order

```text
Phase 2: T001
   └► US1: T002              (MVP; needs T001)
   └► US2: T003 → T004       (needs T001; independent of US1's cells,
                              but T004's composition cell needs T002)
   └► US3: T005 ∥ T006       (T005 needs T001; T006 needs T001)
   Polish: T007 (needs T002+T004) ∥ T008 (needs T002+T004) → T009 (last)
```

- T006 and T008 are [P]-parallel with their phase peers (different
  files, no shared state).
- T002's US1 matrix and T004's US2 matrix (incl. the multi-commit-unit
  cell) are WELDED to their implementation tasks — do not split (plan
  Phase 2 note).
- The FR-002 absence cell lands WITH T002, not later.

## Implementation Strategy

MVP = Phases 2+3 (ordered survivors through the shared dedup shape,
proven against a real server). US2 adds the receipts machinery; US3
hardens the surface; polish proves crash discipline and records the
numbers. Nothing outside `crates/rdlt-postgres` (+ CLI test, benches)
changes; semver-checks arbitrates the zero-SPI promise and the perf
gate arbitrates every step.
