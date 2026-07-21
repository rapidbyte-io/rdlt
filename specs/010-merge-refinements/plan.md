# Implementation Plan: Merge Refinements — Ordered Dedup + Scope Keys

**Branch**: `010-merge-refinements` | **Date**: 2026-07-21 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/010-merge-refinements/spec.md`

## Summary

Two per-table destination options on the 008 surface, both landing in
machinery that already exists. `dedup_sort` is ONE ORDER BY rewrite:
every in-load survivor decision flows through `MergePlan::deduped()`
(`DISTINCT ON (key) … ORDER BY key, __rdlt_arrival DESC`), so inserting
`<column> <dir> NULLS LAST` ahead of the arrival tie-breaker gives
ordered survivors to delete-insert, upsert, hard-delete, and SCD2 change
detection simultaneously (R1). `merge_key` is a scope delete BEFORE the
strategy arm, made multi-commit-unit-sound the same way replace-truncate
and D3 idempotence already are: a durable per-load guard in the publish
transaction — `_rdlt_scope_receipts (load_id, table, scope)` ensures
each scope is replaced at most once per load, and committed-unit
redelivery never reaches merge SQL at all (R2). NULL policies are
defined, not inherited (values beat NULL; NULL is not a scope). Both
options are keyed-structured-only with the full two-layer validation
matrix (R3), ride the generated schemas + CLI passthrough with zero CLI
changes (R5), and get measurement-first scoreboard cells (R4). dlt
1.29.0 is the audited parity reference; three deliberate deviations are
recorded (no append-fallback, no arbitrary survivor, defined NULLs).

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies**: none new

**Storage**: one new destination auxiliary table
(`_rdlt_scope_receipts`), managed like `_rdlt_commits`; no engine state
changes

**Testing**: dest_conformance MR matrices (US1/US2/US3 cells against a
real server); dest_crash_sweep coverage of the scope-delete and ordered-
dedup paths under the existing registered fail points (both passes,
armed-fire pins); config_schema round-trips; CLI toml passthrough cells

**Target Platform**: Linux; reference machine unchanged

**Project Type**: Rust library workspace + dev CLI; no new crates

**Performance Goals**: scoreboard cells (R4, 5-run medians): scope-
replace vs identity-only delete-insert on the 10M-row harness; ordered
vs last-wins dedup on a duplicated stage. No new gates; existing bars
within tolerance

**Constraints**: safe Rust only; ZERO rdlt-core/rdlt-connector changes
(semver-checks "no update required"); no new dependencies; behavior
unchanged when the options are absent (FR-002); additive-only drift
rules untouched

**Scale/Scope**: two config fields + one enum, one ORDER BY rewrite, one
scope-delete step + receipts table, validation matrix, ~2 conformance
matrices, 2 scoreboard cells, 1 contract

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from features 001–009. **Seams sacred**: PASS — zero SPI
changes; WriteMode vocabulary frozen; both controls are destination
config exactly like the 008 strategies. **No silent failures**: PASS —
every invalid shape is a distinct typed error naming table + column;
absent options change nothing; NULL semantics documented (R1/R2).
**Correctness before speed**: PASS — the multi-unit hazard is designed
out with durable receipts (the S6/F2 lesson applied up front, not found
in review); every new path crash-swept before any number is recorded.
**Measured, not asserted**: PASS — R4 protocol; gated bars untouched.
**Safe Rust**: PASS — SQL text generation over existing quoted-ident
helpers.

Post-design re-check: PASS — no new crates, no SPI surface, no unsafe;
the receipts table follows the established auxiliary-table pattern.

## Project Structure

### Documentation (this feature)

```text
specs/010-merge-refinements/
├── plan.md              # This file
├── research.md          # R1–R5 + recorded dlt deviations
├── data-model.md        # Options, survivor/scope semantics, receipts, errors
├── quickstart.md        # The two options, verify commands
├── contracts/
│   └── merge-refinements.md   # MR1–MR8 (amends 008 merge-strategies.md)
└── tasks.md             # /speckit-tasks output (NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
crates/rdlt-postgres/
├── src/dest/
│   ├── config.rs         # + DedupSort {column, order}, SortOrder asc|desc,
│   │                     #   PgTableOptions.{dedup_sort, merge_key};
│   │                     #   parse-time shape validation
│   ├── commit.rs         # MergePlan::deduped() ordering rewrite (R1);
│   │                     #   scope_delete step + _rdlt_scope_receipts (R2);
│   │                     #   receipts pruning at first touch
│   ├── ddl.rs            # ensure_table: open-time validation matrix (R3);
│   │                     #   _rdlt_scope_receipts creation alongside
│   │                     #   _rdlt_commits
│   └── mod.rs            # options plumbing (accessors like strategy_for)
├── tests/
│   ├── dest_conformance.rs   # + MR matrices: US1 (survivor × direction ×
│   │                         #   NULLs × flag × replay), US2 (scope
│   │                         #   replacement × untouched scopes × scope-move ×
│   │                         #   NULL scope × multi-unit × replay), US3
│   │                         #   validation errors
│   ├── dest_crash_sweep.rs   # + scoped/ordered arms under the registered
│   │                         #   fail points, armed-fire pins
│   └── config_schema.rs      # + dest-options cells for both fields
├── benches/run-merge-refinements.sh   # scoreboard cells (R4)
crates/rdlt-cli/src/main.rs   # no code change expected (serde passthrough);
                              # toml passthrough pinned by test
```

## Design Notes (delta-level)

- **deduped() rewrite (R1)**: `ORDER BY {key}, {col} {DESC|ASC} NULLS
  LAST, __rdlt_arrival DESC` when dedup_sort present. `flagged_roots()`
  (shredded path) is untouched — the option is rejected for shredded
  streams at open.
- **Scope delete (R2)**, per scoped table, inside the publish tx, before
  the strategy arm:
  1. `DELETE FROM target WHERE (scope) IN (SELECT DISTINCT scope FROM
     stage WHERE <no scope col NULL> AND ROW(scope)::text NOT IN
     (receipted this load))` — target-side NULL scopes can never match
     (SQL row equality), stage-side NULLs filtered explicitly.
  2. `INSERT INTO _rdlt_scope_receipts SELECT load, table, ROW(scope)::text
     … ON CONFLICT DO NOTHING`.
  Prune other loads' receipts for the table at the load's first unit
  (`load_committed_before == false`), mirroring replace-truncate-once.
- **Upsert composition**: scope delete first, then the existing
  hard-delete pre-delete + conflict-insert unchanged.
- **Validation (R3)**: parse-time in `PgDestOptions::validate`
  (shape); open-time in `ensure_table` next to M7/S1 (existence,
  collisions, shredded rejection, scd2+merge_key rejection).
- **No behavior drift**: absent options ⇒ byte-identical SQL to 009-era
  output (conformance already pins the existing matrices; FR-002 gets
  its own explicit cell).

## Verification Map (story → proof)

| Story | Proof surface |
|---|---|
| US1 dedup_sort | dest_conformance MR-US1 matrix: desc/asc survivor, unchanged last-wins absent the option, survivor's-flag hard-delete, NULL ordering, tie determinism, replay stability (SC-001) |
| US2 merge_key | dest_conformance MR-US2 matrix: delivered-scope replacement, untouched scopes, unseen scope insert, scope-moving update no-dup, NULL scope, MULTI-COMMIT-UNIT load (small commit thresholds), replay idempotence (SC-002) |
| US3 config | validation matrix cells (SC-004); config_schema round-trips + CLI toml passthrough (SC-005) |
| Crash | dest_crash_sweep arms with both options armed across registered fail points, post-recovery equality (SC-003) |
| Perf | run-merge-refinements.sh cells + gated bars within tolerance (SC-007) |
| Governance | semver-checks "no update required"; zero new deps (SC-006) |

## Phase 2 note for /speckit-tasks

Order: config surface + parse validation first (everything reads it) →
deduped() rewrite WELDED to the US1 matrix (an unproven survivor rule is
silent data corruption) → scope delete + receipts WELDED to the US2
matrix INCLUDING the multi-commit-unit cell (the S6/F2 lesson — this
cell is NON-OPTIONAL) → open-time validation matrix → crash-sweep arms →
schemas/CLI/docs → scoreboard + close-out. The FR-002
absence-means-no-change cell must land with the deduped() rewrite, not
later.
