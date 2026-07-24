# Implementation Plan: Benchmark Refinement — Three-Way E2E Matrix

**Branch**: `018-bench-refinement` | **Date**: 2026-07-24 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/018-bench-refinement/spec.md`

## Summary

Execute BENCH_REFINMENT.md (v3.1) end-to-end: collapse the benchmark to an
e2e-only, three-way-comparable five-cell matrix (rdlt / dlt / Airbyte, same
conditions); delete the gated/scoreboard taxonomy, suites, two of three run
modes, 25 legacy benchmark cells, 10 fixtures, and their artifacts outright
(git history is the archive); relocate the cold-start guard to the
instruments track; add the Airbyte `driver` competitor kind behind five
recorded feasibility probes; rebuild the presentation (one generated matrix
+ GOVERNANCE.md split); retire all 8 bars via policy entry and re-introduce
enforcement measurement-first (≤ 1 bar/cell). Constitution Principle VIII
and the 012 bench contract are amended through their own procedures — the
first exercise of the governance machinery ratified in 017. Phases P0–P4,
each independently mergeable with the full gate green.

## Technical Context

**Language/Version**: Rust 1.96.0 workspace (harness `crates/rdlt-bench`,
publish = false) + Python (competitor scripts, venv-managed drivers —
016 pyiceberg precedent); no new Rust dependencies

**Primary Dependencies**: unchanged Rust side (clap, serde, existing
harness stack). New EXTERNAL machine prerequisites (not crate deps):
`abctl` + a kind-capable container runtime (P1 probes decide podman-rootless
vs docker — the #1 risk); dlt image gains `s3fs`, drops `duckdb` extras,
adds `connectorx` for pg extraction

**Storage**: benchmark artifacts (fingerprinted JSON, format_version 1→2:
`class` removed, optional `extra` object added), append-only
`benches/history.jsonl` (new), gitignored
`benches/competitors/airbyte/state.json` (connection-id cache).
Versioned-data rule: artifacts are re-recorded under v2, never silently
reshaped — v1 artifacts are deleted with their cells; git history keeps them

**Testing**: `cargo nextest run` (the harness selftest suite is EXEMPT test
machinery and stays); the full workspace gate at every phase merge;
measurement sessions are deliberate local acts (quiet guard), never CI

**Target Platform**: this podman-based Linux workstation (probes recorded
against it); hosted services out of scope

**Project Type**: dev-tool refactor (rdlt-bench crate + benches/ data) +
measurement program; no engine/connector code changes except deleting the
bench library-mode consumer

**Performance Goals**: none for the harness itself; the cold-start ≤ 40 ms
absolute check survives on the instruments track; matrix numbers are
recorded honestly, enforcement only via measurement-first bars (P4)

**Constraints**: full gate green at each of P0–P4; deletion is outright
(greenfield, no archive dir); every retired number remains checkout-able at
the cited pre-migration commit; RESULTS.md numbers 100% generated-or-cited;
no importance taxonomy reintroduced (spec FR-018)

**Scale/Scope**: rdlt-bench ~4.5k LOC shrinking (library_mode.rs deleted,
class/suite/mode branches removed); 25 of 26 cells deleted, 5 new; 10 of 13
fixtures deleted, 2 reshaped; 8 bars retired; dlt scripts 2 adapted + 3 new
+ 5 deleted; 1 new Airbyte scripts module

## Constitution Check

*Gate evaluated against constitution v1.0.0 pre-Phase-0; re-checked
post-Phase-1. One principle requires a recorded amendment — planned as an
explicit deliverable, not a violation.*

| # | Principle | Verdict | Notes |
|---|---|---|---|
| I | Small Core, Verified Breadth | PASS | Dev-tool only; the harness SHRINKS. Airbyte machinery is bench-module scripts + machine prerequisites, not product surface. |
| II | Library-First, Thin CLI | PASS | No engine/CLI capability change; deleting bench library mode removes a harness convenience, not library capability (the facade parser + its pins stay — spec FR-005). |
| III | One-Boundary Wrapping | PASS (n/a) | No connector boundaries touched. |
| IV | Exactly-Once Is Sacred | PASS (n/a) | No commit/replay paths touched; row-count verification is bench-side. |
| V | Typed Error Taxonomy | PASS | Harness errors stay offender-naming; `Missing{reason}` loud-skip retained. |
| VI | Self-Contained Comments | PASS | New/edited comments follow 017's standard; the migration record cites commits by hash (resolvable forever), the sanctioned evidence form. |
| VII | Test-and-Verification Gate | PASS | Full gate at each phase merge; selftest suite retained; coverage measured at close-out as usual. |
| VIII | Benchmark Governance | **AMENDMENT REQUIRED** | The principle's wording embeds the scoreboard taxonomy this feature deletes. Its MECHANISM (bars in bars.toml, enforced by the gate, no enforcement without recorded evidence + a governance entry) is preserved and STRENGTHENED (bars now also require a recorded session floor). Planned: amendment v1.1.0 (MINOR — materially reworded principle) with Sync Impact Report, landing IN P0 BEFORE the vocabulary deletion merges. The 012 contract's BH1/BH2/BH3/BH6 wording is amended the same recorded way (contracts/bench-refinement.md carries both amendment texts). |
| IX | Contracts & Persisted Formats Frozen | PASS | Artifact format_version increments 1→2 with the change list recorded (the sanctioned versioning path); v1 artifacts are deleted with their cells, not migrated. WAL/StateDoc/receipts untouched. |

**Violations requiring Complexity Tracking**: none (the Principle VIII
amendment is executed through the constitution's own governance — that is
compliance, not violation).

## Re-derived deletion list (plan-time, from the live tree — spec SC-001)

**Cells (26 total): 1 exempt, 25 deleted, 5 new.**
Exempt (test machinery): `selftest-protocol`.
Deleted: `pg-wide-duckdb-1m`, `pg-wide-pg-1m`*, `pg-jsonb-duckdb-200k`,
`strategy-delete-insert-1m`, `strategy-upsert-1m`,
`merge-index-incremental-unindexed`, `merge-index-incremental-indexed`,
`merge-index-half-indexed`, `merge-index-half-unindexed`,
`scope-replace-delete`, `scope-identity-delete`, `ordered-dedup`,
`lastwins-dedup`, `duckdb-strategy-delete-insert-1m`,
`duckdb-strategy-upsert-1m`, `cdc-change-apply-500k`,
`cdc-catchup-latency-1k`, `jsonl-duckdb-200k`, `shred-only-200k`,
`rest-pg-100k`, `parquet-passthrough`*, `parquet-duckdb`, `cold-start`*,
`file-s3-duckdb-200k`, `iceberg-polaris-200k`.
(*lineage: `pg-to-pg-1m` ← pg-wide-pg-1m; `s3jsonl-to-s3parquet-200k` ←
parquet-passthrough; `cold-start` RELOCATES to instruments rather than
dying.)
New matrix cells (benches/cells/e2e.toml, one file, no suites):
`pg-to-pg-1m`, `pg-to-s3parquet-1m`, `s3jsonl-to-pg-200k`,
`s3jsonl-to-s3parquet-200k`, `pg-to-pg-dedup-1m`.

**Fixtures (13): 1 exempt (`selftest-none`), 10 deleted (`jsonl-200k`,
`parquet-200k`, `cold-one-row`, `rest-pg`, `iceberg-polaris`, `strat-pg`,
`cdc-tp-pg`, `cdc-lat-pg`, `merge-index-pg`, `refine-pg`), 2 reshaped
(`pg-src` → `pg`: seeded 1M×12 source table + per-product destination
databases rdlt/dlt/airbyte on ONE server; `file-s3` → `rustfs`: buckets
`raw` + `lake`, per-product prefixes, image pin 1.0.0-beta.11 kept).**
`gen_jsonl.py` survives (its output is seeded INTO RUSTFS `raw`);
`seed_pg.sql` survives; `seed_merge_index.sql`, `seed_refine.sql`,
`polaris_bootstrap.py` and the CDC/strat/mock-API fixture material die with
their cells.

**Bars: all 8 retired** (7 cells; jsonl-duckdb-200k carried 2) via ONE
policy-log entry; bars.toml becomes empty-with-header until P4.

**Modes**: `Subprocess` stays; `Library` + `Hyperfine` deleted
(library_mode.rs deleted with its harness-side parity-pin test — the
fixture `benches/parity_specs.yaml` and the CLI-side parse/build pins
SURVIVE, spec FR-005; their comments are updated to name the CLI as the
remaining pin consumer).

**dlt module**: keep+adapt `pipeline_pg_pg.py`; `pipeline_parquet.py` →
`pipeline_s3jsonl_s3parquet.py`; NEW `pipeline_pg_s3parquet.py`,
`pipeline_s3jsonl_pg.py`, `pipeline_pg_pg_dedup.py`; DELETE
`cold_start.py`, `normalize_only.py`, `pipeline_jsonl_duckdb.py`,
`pipeline_pg_duckdb.py`, `pipeline_rest_pg.py`. Variants: `dlt`
(connectorx for pg sources), `dlt-pyarrow` (recorded context);
`dlt-sqlalchemy` DELETED. Image: +`s3fs`, +`connectorx`, −`duckdb` extras.

## Project Structure

### Documentation (this feature)

```text
specs/018-bench-refinement/
├── plan.md              # This file
├── research.md          # Phase 0 — decisions incl. probe design
├── data-model.md        # Phase 1 — cell/artifact/driver shapes
├── quickstart.md        # Phase 1 — per-phase run/verify commands
├── contracts/
│   └── bench-refinement.md   # BR1–BR8 + Principle-VIII/BH amendment texts
├── checklists/requirements.md
├── spike/               # P1 output — probe evidence + go/no-go (created then)
└── tasks.md             # Phase 2 (/speckit-tasks — not created here)
```

### Source Code (repository root)

```text
crates/rdlt-bench/src/   # class/suite/mode collapse; artifact v2; driver
│                        #  kind; variants discovery; history append; report
│                        #  rebuild; DELETE library_mode.rs
│   cells.rs artifact.rs runner.rs gate.rs report.rs main.rs protocol.rs
│   competitors.rs paths.rs template.rs
crates/rdlt-bench/tests/selftest.rs   # follows the new shapes
crates/rdlt-cli/         # UNTOUCHED (parity pins stay)
benches/
├── cells/e2e.toml               # the five cells (other cell files deleted)
├── cells/pipelines/*.yaml       # 5 rdlt pipeline specs
├── fixtures/{fixtures.toml, seed_pg.sql, gen_jsonl.py}   # slimmed
├── bars.toml                    # empty until P4
├── RESULTS.md                   # rebuilt: matrix/caveats/trends/milestones
├── GOVERNANCE.md                # NEW — coverage/semver/exclusions move here
├── history.jsonl                # NEW — append-only trends feed
├── check-cold-start.sh          # NEW — instruments-track cold-start ≤40ms
└── competitors/
    ├── dlt/                     # slimmed per the list above
    └── airbyte/                 # NEW (P3): setup.py driver.py variants.toml README.md
.specify/memory/constitution.md          # v1.1.0 amendment (P0)
specs/012-bench-harness/contracts/bench-harness.md  # BH amendment note (P0)
Makefile                                  # instruments verbs host cold-start
```

**Structure Decision**: everything stays in the existing rdlt-bench crate +
benches/ tree; the Airbyte module is scripts-only on a host venv (016
pyiceberg precedent) — no new crates, no new Rust deps (Principle I).

## Phase sequence (delivery order; spec FR-017)

| Phase | Content | Measurements | Merge gate |
|---|---|---|---|
| P0 | Constitution v1.1.0 + BH amendment FIRST; harness collapse (class/suite/modes, artifact v2, classless quiet guard); ONE migration commit (25 cells, 10 fixtures, artifacts, scripts, 8-bar retirement policy entry citing final values + archive commit); cold-start → instruments; presentation rebuild (RESULTS.md skeleton, GOVERNANCE.md, history plumbing); library_mode deletion (parity fixture + CLI pins stay) | none | full workspace gate |
| P1 | The five probes → `spike/` doc, go/no-go each (runtime FIRST — it decides P3's shape) | spike only | gate + recorded spike |
| P2 | Fixture reshape (pg + rustfs) + 5 rdlt pipeline specs + 5 cells + slimmed dlt image/scripts/variants → FIRST RECORDED SESSION rdlt vs dlt (10 arms, row-count-verified); matrix renders | recorded, unenforced | gate + session artifacts |
| P3 | `driver` kind + per-module variants discovery (flat namespace, collision = load-time error) + airbyte module + idempotent connections setup → FIRST 3-WAY SESSION (15 arms, or absent-with-reason per failed probe) | recorded, unenforced | gate + session artifacts |
| P4 | Bars measurement-first (≤ 1/cell, below recorded floors, policy entries, gate green against the justifying session). The conditional 3-way Iceberg cell is explicitly NOT taken unless the owner elevates lakehouse to a headline claim | per policy | gate incl. new bars |

## Complexity Tracking

No unjustified constitutional violations — table intentionally empty. (The
Principle VIII amendment goes through the constitution's own amendment
procedure; text in contracts/bench-refinement.md.)
