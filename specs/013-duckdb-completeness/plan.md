# Implementation Plan: DuckDB Destination Completeness

**Branch**: `013-duckdb-completeness` | **Date**: 2026-07-22 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/013-duckdb-completeness/spec.md`

## Summary

Extract the postgres destination's merge layer into a new internal
crate `rdlt-connector-sqlcore` (options vocabulary, validation, plan
shapes, single-unit rules) behind a `MergeDialect` trait that owns SQL
text only (R1/R2), proving the extraction with golden-SQL pins that
must survive byte-for-byte (R3) plus the untouched postgres suites,
sweeps, and gated bars. Implement the DuckDB dialect (R4 — DISTINCT ON,
ON CONFLICT vs auto-ensured unique index, tx-stable now() for scd2 per
R5), giving DuckDB the full 008/010 options vocabulary with identical
typed errors; any shape DuckDB cannot honor exactly becomes a typed
capability gap, never an approximation (SM3). Flip `json_type` to
native JSON via the existing stage→target SQL seam (R6). Verify to the
011 standard: traceability matrix, ≥80% measured coverage
(baseline-first), armed crash sweeps over the new arms, dlt parity
record — plus the cross-destination differential oracle at the facade
level (R7), and two scoreboard bench cells in the 012 harness (R8).
Zero SPI change, zero new external runtime deps, WriteMode frozen.

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies**: none new externally — sqlcore uses `serde` +
`thiserror` (existing); duckdb crate keeps `duckdb` bundled (JSON
extension statically included, probed in R6)

**Storage**: DuckDB database files (embedded); postgres via
testcontainers for the differential oracle only

**Testing**: golden-SQL pins (pre-extraction weld); sqlcore unit
cells; duckdb strategy/option/probe cells; differential suite in
`crates/rdlt/tests/`; failpoints sweeps; coverage via cargo-llvm-cov;
matrix citations to existing 006/008/010 suites first

**Target Platform**: Linux (dev/CI); DuckDB embedded — no network

**Project Type**: Rust workspace; ONE new internal crate
(rdlt-connector-sqlcore), no external-dependency changes

**Performance Goals**: none new — every existing gated bar within
tolerance (the extraction touches the postgres hot path's planning
code; the bars are the regression net); new duckdb cells scoreboard

**Constraints**: SPI frozen (semver "no update required"); postgres
behavior byte-identical (SM4); behavior unchanged when options absent
(FR-003); no silent dialect approximations (SM3); baseline coverage
measured before new cells (011 R2 rule)

**Scale/Scope**: sqlcore ≈ the movable ~600–800 lines of
commit.rs/config.rs planning + validation; duckdb dialect + arms ≈ the
008/010 surface; matrix ≈ the destination-options inventory; 2 bench
cells; README destination-options section becomes destination-neutral

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from 001–012. **Seams sacred**: PASS — SPI untouched; the new
seam (MergeDialect) is internal to the connector family and
contract-bound (SM1–SM3). **No silent failures**: PASS — typed
capability gaps replace approximations; differential mismatches fail
loudly. **Correctness before speed**: PASS — golden pins + sweeps +
differential before any parity claim. **Measured, not asserted**: PASS
— coverage floor with baseline-first, scoreboard cells with committed
artifacts, dlt parity recorded per option. **Safe Rust**: PASS —
SQL-text generation and an embedded database; no unsafe.

Post-design re-check: PASS.

## Project Structure

### Documentation (this feature)

```text
specs/013-duckdb-completeness/
├── plan.md              # This file
├── research.md          # R1–R8
├── data-model.md        # shared vocabulary / MergePlan / MergeDialect / oracle
├── quickstart.md        # use + verify + bench
├── contracts/
│   └── shared-merge-core.md  # SM1–SM8
├── matrix.md            # built in implementation (011 pattern)
└── tasks.md             # /speckit-tasks output (NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
crates/rdlt-connector-sqlcore/    # NEW internal crate (serde+thiserror only)
├── Cargo.toml
└── src/
    ├── lib.rs                    # options vocabulary (DestOptions/TableOptions/
    │                             #   MergeStrategy/Scd2Options) + validation
    ├── plan.rs                   # MergePlan shapes + single-unit rules
    └── dialect.rs                # MergeDialect trait

crates/rdlt-connector-postgres/
├── src/dest/config.rs            # re-exports shared types at existing paths
├── src/dest/commit.rs            # consumes sqlcore via the postgres dialect
├── src/dest/dialect.rs           # extracted SQL text (byte-identical, R3)
└── tests/golden_sql.rs           # NEW: pre-extraction pins (SM4 weld)

crates/rdlt-connector-duckdb/
├── src/lib.rs                    # split WHEN the code lands (008 precedent):
├── src/{config,commit,dialect}.rs  # options hook, strategy arms, DuckDB SQL
└── tests/                        # strategy cells, probes (R4/R5/R6),
                                  #   sweeps, param-matrix gap cells

crates/rdlt/tests/dest_differential.rs   # R7 cross-destination oracle

benches/cells/merge.toml          # + duckdb-strategy-* scoreboard cells (R8)
README.md                         # destination-options section goes neutral
benches/RESULTS.md                # coverage record + scoreboard rows
```

## Design Notes (delta-level)

- **Order is the weld**: golden-SQL pins land FIRST (against today's
  postgres code), then the extraction (pins + suites green, zero
  behavioral edits), then the duckdb dialect on the proven core. The
  extraction commit contains no duckdb code — reviewable as pure
  moves.
- **Probe-first dialect arms**: each R4 assumption (DISTINCT ON,
  ON CONFLICT semantics, unique-index conflict target, JSON extension)
  gets a probe cell BEFORE its arm ships; a failed probe converts the
  arm to a typed capability gap (SM3) and is recorded in the matrix —
  the feature cannot silently ship an approximation.
- **Explicit-vs-default merge_strategy** (011 R5) carries into shared
  validation: explicit under append/replace rejects on BOTH
  destinations, the unconfigured default never does.
- **Differential normalization**: canonical SELECT (ordered by
  identity, NULLs normalized, numerics compared through the documented
  affinity table) keeps the oracle strict without false positives.
- **dlt parity record**: per-option comparison against dlt 1.29.0's
  duckdb destination (010 format); staged-dataset-style features rdlt
  lacks everywhere are out of scope, recorded as such.

## Verification Map (story → proof)

| Story | Proof surface |
|---|---|
| US1 shared core + parity | golden_sql pins byte-identical; postgres suites/sweeps untouched-green; duckdb strategy/option cells per matrix; identical typed-error cells |
| US2 capabilities | JSON round-trip + json_extract cell; capability audit rows in matrix |
| US3 verification | matrix zero uncited rows; differential suite green; sweeps with armed-fire pins; coverage ≥80% recorded (baseline first); dlt parity record |
| Governance | make check + doc-tests + semver "no update required" + every gated bar within tolerance + scoreboard artifacts committed (SM8) |

## Phase 2 note for /speckit-tasks

Order: T001 coverage baseline + golden-SQL pins (the weld — nothing
moves before the pins exist) → sqlcore extraction with postgres
dialect (pure moves; pins + suites green) → duckdb probes (R4/R5/R6)
→ duckdb dialect + strategy arms welded to their behavior cells →
JSON capability flip → differential oracle → sweeps → matrix +
coverage close-out → bench cells + README + parity record. The
extraction and the duckdb work must be SEPARATE commits (SM4
reviewability); matrix commits WITH the cells that close its gaps
(011 rule).
