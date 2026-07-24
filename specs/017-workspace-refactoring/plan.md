# Implementation Plan: Workspace Refactoring Program

**Branch**: `017-workspace-refactoring` | **Date**: 2026-07-24 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/017-workspace-refactoring/spec.md`

## Summary

Execute the `REFACTORING.md` catalogue end-to-end: fix the 12 latent defects
(B1–B12) with red-before/green-after regression pins, then land the
cross-cutting refactors (R1–R13) and the delivery-surface items (D1–D15) as
independently mergeable increments in Part 4's value-per-risk order. All
behavior changes are confined to the catalogued defect fixes and
error-classification corrections; everything else is behavior-preserving
restructuring verified by the existing gate (conformance suites, golden SQL
pins, crash-point sweeps, container cells). No new dependencies. No semver
break: the one additive SPI change (`DestError::RateLimited`) rides the
`#[non_exhaustive]` enum; breaking renames ship as deprecated aliases or
named deferrals to the already-recorded 0.3 window.

## Technical Context

**Language/Version**: Rust, toolchain pinned 1.96.0 (`rust-toolchain.toml`);
workspace edition 2024; `unsafe_code` denied workspace-wide

**Primary Dependencies**: unchanged — no new crates. Touched pins for
reference: duckdb-rs 1.10505.0 (exposes structured `code`/`extended_code` on
`Error::DuckDBFailure` — basis for B5), iceberg 0.10.0 (error context chain
carries `status` values — basis for B6), arrow/parquet 58 tree, tokio,
object_store. Internal crates touched: all 13.

**Storage**: N/A (no persisted-format changes permitted — WAL, StateDoc,
receipts, snapshot properties stay byte-compatible; contract WR1)

**Testing**: `cargo nextest run` + `cargo test --doc`; testkit conformance
suites; failpoint crash sweeps (`--features failpoints`); container cells
(postgres, RUSTFS, Polaris) skip-not-fail; golden-SQL pins in sqlcore;
per-B-item regression tests new in this feature

**Target Platform**: unchanged (Linux CI, embeddable library)

**Project Type**: multi-crate Rust workspace (13 crates) — internal
restructuring feature, no new public capability

**Performance Goals**: no regression — the iai-callgrind perf gate and bench
scoreboard must be unaffected; no bar changes requested (Principle VIII)

**Constraints**: full gate green at every one of the ~12 merge increments;
public API additive-or-shimmed only; persisted formats and golden pins
byte-identical; coverage at/above pre-feature baseline (measured first)

**Scale/Scope**: ~54k lines across 208 Rust files + CI/bench/manifest
surfaces; catalogue: 12 B-items, 13 R-themes (with Part 3 sub-items),
15 D-items; close-out matrix must reach zero uncited dispositions

## Constitution Check

*GATE: evaluated against constitution v1.0.0 (pre-Phase-0 and re-checked
post-Phase-1 — both passes below).*

| # | Principle | Verdict | Notes |
|---|---|---|---|
| I | Small Core, Verified Breadth | PASS | No surface growth; the feature shrinks/tightens surface (R12 visibility narrowing, dead-code deletion). |
| II | Library-First, Thin CLI | PASS (strengthens) | B3/step-12 moves the pipeline-spec model into the `rdlt` library, making "CLI adds zero capability" structurally true. |
| III | One-Boundary Wrapping | PASS (strengthens) | B5/B6 move classification onto structured codes at the existing boundaries; duckdb's local `quote` bypass of its dialect seam is deleted (R2). |
| IV | Exactly-Once Is Sacred | PASS (guarded) | R2/R6/B7/B10 touch commit/replay paths → crash-point sweeps and duplicate-free pins re-run at each affected increment (WR1, WR6). |
| V | Typed Error Taxonomy | PASS (this feature enforces it) | B1/B5/B6/B8/B9/R8 close every catalogued violation; citation IDs stripped from user-facing strings (R1). |
| VI | Self-Contained Code & Comments | PASS (this feature enforces it) | R1 sweep is increment 2; new code written citation-free from the start. |
| VII | Test-and-Verification Gate | PASS | Gate green per merge; coverage baseline measured before increment 1; close-out matrix is the verification artifact (WR7). |
| VIII | Benchmark Governance | PASS | No new cells, no bar changes; bench-crate refactors (R4/3.8, D11–D12) alter tooling, not measurements — RESULTS.md regeneration must be diff-clean for unchanged cells. |
| IX | Contracts & Persisted Formats Frozen | PASS | WR1 pins formats byte-identical; golden SQL text frozen through the R2 extraction (planner emits identical SQL, pinned). |

**Violations requiring Complexity Tracking**: none.

## Project Structure

### Documentation (this feature)

```text
specs/017-workspace-refactoring/
├── plan.md              # This file
├── research.md          # Phase 0 — decisions per catalogue theme
├── data-model.md        # Phase 1 — shared abstractions + close-out matrix shape
├── quickstart.md        # Phase 1 — how to run/verify each increment
├── contracts/
│   └── workspace-refactoring.md   # WR1–WR8
├── checklists/requirements.md
└── tasks.md             # Phase 2 (/speckit-tasks — not created here)
```

### Source Code (repository root)

All 13 crates are touched; new/moved modules only (no new crates):

```text
crates/
├── rdlt-connector/          # +secret.rs (R3 one Secret), +channel.rs split (3.1),
│                            #  DestError::RateLimited (R8, non-breaking)
├── rdlt-core/               # counters unification, doc corrections (B12, 3.1)
├── rdlt-engine/             # runtime/graph.rs → runtime/run.rs (+ stream_task,
│                            #  ping-pong owner), load/apply.rs shared helpers (R6),
│                            #  wal/resume.rs two-pass replay (B10), fuzz repoint (B11)
├── rdlt-connector-sqlcore/  # plan/ split (R4), +commit protocol planner (R2:
│                            #  commit_script → Vec<Step>), shared quote/column_list/
│                            #  root_of/index-name helpers, typed validate errors (R8)
├── rdlt-connector-duckdb/   # commit split → executes sqlcore script (R2), structured
│                            #  error codes (B5), transient channel (B8)
├── rdlt-connector-postgres/ # tls/ + cdc/ splits (R4), pg_error_detail +
│                            #  is_transient_sqlstate unification (3.3), B4 typed
│                            #  ordering error, commit executes sqlcore script (R2)
├── rdlt-connector-file/     # location/ unification absorbs dest Store (R7; fixes
│                            #  B2/B9 root cause), dest/ split (R4)
├── rdlt-connector-rest/     # B1 classification-preserving context, read/ split (R4),
│                            #  typed Paginator error (R8)
├── rdlt-connector-iceberg/  # commit.rs split (R4), commit_with_retry (kills
│                            #  unreachable! tails), status-context classification (B6),
│                            #  SCOPE_HASH_LEN + state_key (B7), Secret re-export (R3)
├── rdlt/                    # +pipeline_spec module (B3 / step 12), facade doc fixes
├── rdlt-cli/                # consumes rdlt::pipeline_spec, main.rs split (R4)
├── rdlt-bench/              # consumes rdlt::pipeline_spec, cmd_run split, TOML
│                            #  dedup (D11–D12)
└── rdlt-testkit/            # +containers module (D1–D3: runtime_available, PgFixture),
                             #  +fixtures module (D4: batch_of/schema_for/meta_for),
                             #  memory dest commit split (R4)

.github/                     # +actions/free-disk (D6), workflow dedup (D7–D10)
Cargo.toml                   # rust-version (D13), inheritance stragglers (D14)
mutants.out.old/             # untracked (D15)
```

**Structure Decision**: no new crates and no crate moves — Principle I. All
extraction targets land in existing crates at their catalogued homes: shared
SPI code in `rdlt-connector`, SQL protocol in `rdlt-connector-sqlcore`, test
infrastructure in `rdlt-testkit`, the pipeline-spec model in the `rdlt`
facade. Module splits follow the R4 table's proposed seams verbatim unless
implementation records a justified deviation in the close-out matrix.

## Increment Sequence (delivery order)

Part 4's order, with Part 5 folded in. Each increment is independently
mergeable with the full gate green; later increments rebase on earlier ones.

| # | Increment | Catalogue items | Risk note |
|---|---|---|---|
| 1 | Defect fixes (small) + regression pins | B1, B2, B4–B9, B11, B12 (+B3 drift-sync stopgap) | Fixes on unmoved code; each pinned red-first |
| 2 | Citation/comment sweep | R1 (escalations 1–3) | Pure deletion/rewording; zero behavior change |
| 3 | Mechanical sweep: constants, dead code, visibility, delivery surfaces | R13, R12, D6–D15 | Before splits so less code moves |
| 4 | Shared infrastructure: Secret + testkit containers/fixtures | R3, D1–D5 | Unblocks connector + test dedup |
| 5 | Engine: shared apply → run_once split → two-pass replay | R6, R4(engine), B10, R9(engine), R11(engine) | Apply helpers make the split safe; split makes B10 obvious |
| 6 | sqlcore: commit splits → protocol extraction | R4(commits), R2, R8(sqlcore typed errors), B8 follow-through | Golden-SQL pins prove SQL-identical |
| 7 | Postgres: tls/cdc splits + error-detail unification | R4(pg), 3.3 DRY, R8(pg), R9(pg) | Classification helpers shared before alignment |
| 8 | File: Location/Store unification → dest split | R7, R4(file), B9 follow-through, R5(file) | Subsumes B2/B9 root causes |
| 9 | REST: read split + typed Paginator | R4(rest), R5(rest), R8(rest), R9(rest) | |
| 10 | Iceberg: commit split + commit_with_retry | R4(iceberg), R9(iceberg), 3.7 DRY | |
| 11 | Naming pass | R10 (non-breaking + shims/deferrals), R5 convention unification | Breaking rows → deprecated aliases or named 0.3 deferrals |
| 12 | CLI/bench: rdlt::pipeline_spec + bench splits | B3 (structural fix), R4(cli/bench), R11(bench) | Retires increment 1's stopgap |
| — | Close-out | FR-023 matrix, coverage vs baseline, WR sweep | Zero uncited dispositions |

## Complexity Tracking

No constitution violations to justify — table intentionally empty.
