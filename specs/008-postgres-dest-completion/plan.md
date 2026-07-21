# Implementation Plan: Postgres Destination Completion

**Branch**: `008-postgres-dest-completion` | **Date**: 2026-07-21 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/008-postgres-dest-completion/spec.md`

## Summary

Make the postgres destination feature-complete against the dlt
inventory while keeping every lead it already has (binary-COPY staging,
atomic publish, exactly-once receipts, TLS/mTLS). Modularization comes
FIRST as a pure relocation gated on a byte-identical suite (R9). Type
fidelity flips the existing `decimal`/`json_type` capability flags —
engine lowering is already capability-driven, so zero engine changes
(verified, R2) — and adds three hand-rolled wire encoders mirroring the
source's own decoders, no new dependencies (R1); pre-008 text columns
stay untouched with `rdlt::lossy` visibility (R3). Merge grows
destination-side strategies (R4–R6): upsert via conflict-update with an
auto-ensured unique index, hard-delete column, and SCD2 with
IS-DISTINCT-FROM change detection whose crash-redelivery safety rides
the EXISTING D3 receipt idempotency. Merge identities get supporting
indexes with a measured before/after (R7). The review-F6 error-chain
debt closes with one `describe()` helper (R8). All new config appears
in serde + CLI + schemars surfaces (R10). Zero SPI changes anywhere —
WriteMode stays frozen; strategies are how THIS destination executes
merge.

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies**: none new — the numeric/jsonb/uuid wire
encoders are in-crate mirrors of the source's decoders (R1);
tokio-postgres optional type features deliberately NOT enabled

**Storage**: destination-managed additions only — validity columns on
SCD2 tables, deterministic `rdlt_ix_*` indexes; `_rdlt_state`/
`_rdlt_commits` formats UNCHANGED (SCD2 boundary stability rides the
existing receipt idempotency, R6)

**Testing**: relocation gate = full existing suite byte-identical;
encoder↔decoder round-trip property tests (R1); type-fidelity
conformance (catalog types + SUM/JSON-path/uuid-join, SC-001); strategy
conformance per strategy incl. crash sweeps with armed-fire pins for
the upsert arm (SC-002) and redelivery-stability for SCD2 (SC-004);
F6 regression test (server message + SQLSTATE); schema round-trips for
the new config (SC-008)

**Target Platform**: Linux; reference machine unchanged

**Project Type**: Rust library workspace + dev CLI; net-zero crates

**Performance Goals**: existing gated bars (pg→pg ≥6×, iai ≤3%) stay
within tolerance — native encoding rides the same bulk path (SC-006);
NEW scoreboard cells: merge-heavy delete-insert vs upsert, and
unindexed-vs-indexed merge (R7/R11); measurement-first, no new gates

**Constraints**: safe Rust only, no new unsafe; ZERO rdlt-core/
rdlt-connector changes (semver-checks must stay "no update required");
WriteMode frozen — strategy is destination config (R10); additive-only
migrations (FR-003, R3); 005/006/007 benchmark records untouched

**Scale/Scope**: one crate's dest module: 613-line mod.rs → 5 modules;
3 wire encoders; 2 capability flips; 3 merge strategies + hard-delete;
index ensure; 1 error helper; config surface (serde + builder + CLI +
schemars); contracts: 3 new; 2 bench cells

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from features 001–007. **Seams sacred**: PASS — zero SPI
changes (capability flags are data on the existing struct; strategies
never cross the connector contract; SCD2 documented as a
destination-local semantic extension). **No silent failures**: PASS —
every new failure mode typed and named (duplicate keys under upsert
23505 → names key columns; hard-delete column shape; SCD2 validity
collision; JSONB-rejected documents name the column); pre-008 text
columns get suppression-proof `rdlt::lossy` visibility, never silent
retyping. **Correctness before speed**: PASS — upsert ships inside the
crash-sweep + armed-fire regime before its scoreboard number is
recorded; SCD2 redelivery stability is argued from the receipt
protocol AND conformance-tested. **Measured, not asserted**: PASS —
the index claim (FR-009) and the strategy comparison are measured
scoreboard entries; gated bars unchanged. **Safe Rust**: PASS — wire
encoders are plain byte construction.

Post-design re-check: PASS — no new crates, no SPI surface, no
unsafe, no state-format changes.

## Project Structure

### Documentation (this feature)

```text
specs/008-postgres-dest-completion/
├── plan.md              # This file
├── research.md          # R1–R11
├── data-model.md        # Config entities + DDL/index/validity rules
├── quickstart.md        # User-facing recipes per story
├── contracts/
│   ├── dest-types.md        # Native type mapping + migration visibility
│   ├── merge-strategies.md  # delete-insert / upsert / hard-delete / indexes
│   └── scd2.md              # Validity semantics, boundary, absence policy
└── tasks.md             # /speckit-tasks output (NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
crates/rdlt-postgres/
├── src/
│   ├── dest/
│   │   ├── mod.rs        # Postgres/PgSession orchestration + trait impls
│   │   │                 #   (capabilities: decimal:true, json_type:true)
│   │   ├── config.rs     # NEW: PgDestOptions {merge_strategy, hard_delete,
│   │   │                 #   scd2{valid_from,valid_to,absent}, per-table
│   │   │                 #   overrides}; serde + schemars + from_value
│   │   ├── ddl.rs        # sql_type (+NUMERIC(p,s)/JSONB/UUID/NOT NULL),
│   │   │                 #   create/migrate (drift + R3 lossy warn),
│   │   │                 #   ensure indexes (R7) + unique-for-upsert (R4)
│   │   ├── encode.rs     # copy_type/cell_value + NumericWire/JsonbWire/
│   │   │                 #   UuidWire ToSql impls (R1)
│   │   └── commit.rs     # publish transaction; strategy SQL: delete-insert
│   │                     #   (moved), upsert (R4), hard-delete (R5),
│   │                     #   scd2 (R6); describe() error helper (R8)
│   └── source/copy_decode.rs  # unchanged; its numeric/jsonb decoders are
│                              #   the round-trip oracle for encode.rs
├── tests/
│   ├── dest_conformance.rs    # + type-fidelity cells (SC-001), strategy
│   │                          #   conformance, F6 regression
│   ├── dest_crash_sweep.rs    # + upsert-strategy sweep with armed-fire pins
│   ├── scd2.rs                # NEW: SC-004 history/point-in-time/redelivery
│   └── config_schema.rs       # + PgDestOptions round-trips (SC-008)
├── benches / benches/run-pg.sh  # + merge-heavy + index scoreboard cells
crates/rdlt-cli/src/main.rs      # [destination.postgres] gains the options
```

## Design Notes (delta-level)

- **Relocation first** (R9): moves only, full suite green, then edits.
- **Fidelity** (R1–R3): `sql_type` gains `NUMERIC(p,s)` (from the
  lowered-no-more `ColumnType::Scalar(Decimal{p,s})`), `JSONB`, `UUID`,
  and NOT NULL from `ColumnDef.nullable` (CREATE only). `copy_type`
  maps Decimal128→NUMERIC, Json-logical Utf8→JSONB, Uuid-logical
  Utf8→UUID — per-column decisions come from the TABLE SCHEMA (logical
  types), not raw arrow, so Utf8-as-text vs Utf8-as-json/uuid cannot
  confuse. Encoders round-trip against the source decoders in property
  tests. Existing-text-column fallback per R3.
- **Strategies** (R4–R6): all inside the existing publish transaction;
  receipts/D3 untouched. Upsert = DISTINCT-ON dedup + ON CONFLICT DO
  UPDATE; hard-delete composes with both non-SCD2 strategies; SCD2
  retire+insert with IS DISTINCT FROM change detection, skip-unchanged,
  absence policy, boundary = now() at first execution (receipts make
  redelivery a no-op). Keyless + scd2/upsert = typed ensure-time error.
- **Indexes** (R7): deterministic names, `IF NOT EXISTS`, ensured with
  the table; unique-index failure under upsert names the key columns.
- **F6** (R8): one `describe()`; transient heuristic unchanged.
- **Config** (R10): destination-level defaults + per-table override
  map; validation errors name the field; CLI + schemars + from_value.

## Verification Map (story → proof)

| Story | Proof surface |
|---|---|
| US1 fidelity | dest_conformance type cells: catalog types, SUM equality, JSON-path, uuid-join, NOT NULL; encoder round-trip properties; R3 lossy-warn capture (SC-001) |
| US2 strategies | upsert convergence + idempotent re-runs; 23505 typed error; hard-delete exact totals; upsert crash sweep armed-fire green; index scoreboard measurement (SC-002/003/005) |
| US3 SCD2 | scd2.rs: three-round history, non-overlap, one active/key, point-in-time, redelivery-zero-dupes, absence policies, collision rejection (SC-004) |
| US4 shape | relocation commit = moves only + full suite; F6 regression (server message + SQLSTATE) (SC-007) |
| Config | config_schema round-trips for every new field (SC-008) |
| No-regression | make check + gated bars within tolerance (SC-006) |

## Phase 2 note for /speckit-tasks

Order: relocation (foundational, blocking) → US1 fidelity → US2
strategies → US3 SCD2 (builds on US2 machinery) → US4's F6 half can
ride the relocation task → config/schema + benches → close-out.
The relocation task MUST be its own commit with the moves-only rule
stated; the capability flip and encoder work MUST land with their
conformance cells in the same task (a flipped capability without the
round-trip proof is a silent-corruption risk).
