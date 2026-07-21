# Implementation Plan: Postgres Connector Verification — Every Parameter Proven

**Branch**: `011-connector-verification` | **Date**: 2026-07-21 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/011-connector-verification/spec.md`

## Summary

A verification feature, not a feature feature: build the traceability
matrix (every parameter → behavioral cells, R3/R4), close every gap it
exposes, resolve every mismatch it surfaces (including the recorded
`merge_strategy`-under-append footnote via the R5 typed rejection), and
back the whole surface with a measured ≥80% line-coverage floor for the
connector crate (cargo-llvm-cov over nextest, R1, `make coverage`, R6).
Order of operations is audit-first: measure the baseline (T001), build
the matrix by CITING the existing deep suites (conformance,
differential, TLS matrix, CDC, sweeps), and only then write cells — for
genuine gaps, each stating its behavioral claim (PM2/PM3), never for
coverage's own sake. Mismatches found along the way are fixed with
pinned regression cells or resolved as documentation corrections, all
listed in the close-out (PM6). Zero SPI change; no new runtime
dependencies (dev-tooling only).

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies**: none new at runtime; dev tooling:
cargo-llvm-cov + llvm-tools-preview (verified/installed in T001)

**Storage**: none

**Testing**: this feature IS testing — matrix citations + new cells in
the existing suites (`dest_conformance`, `conformance`, `incremental`,
`tls_matrix`, `query_streams`, `config_schema`, `cdc*`, CLI unit tests);
crash-sweep coverage for any fix on a publish/read path

**Target Platform**: Linux; container runtime for live cells

**Project Type**: Rust library workspace + dev CLI; no new crates

**Performance Goals**: none new — gated bars stay within tolerance;
coverage runs are excluded from perf measurement (instrumented builds)

**Constraints**: SPI frozen (semver-checks "no update required");
`make check` semantics untouched (coverage is an additional target); no
coverage-only tests (PM5); behavior cells over parse cells (PM2);
baseline measured before any new cell lands (R2)

**Scale/Scope**: ~60 parameters + ~40 enumerated values + the FR-003
interaction rows; one matrix artifact; one Makefile target; one R5 code
fix; unknown-but-bounded gap cells (audit determines the count — the
matrix rows are the worklist)

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from 001–010. **Seams sacred**: PASS — zero SPI changes; the R5
fix is destination-config validation, the 008/010 pattern. **No silent
failures**: PASS — the feature exists to eliminate silent gaps; R5
removes the last recorded silent-inert option. **Correctness before
speed**: PASS — behavior-proving cells, sweeps for path-touching fixes.
**Measured, not asserted**: PASS — the coverage number is measured under
a recorded protocol with a pre-change baseline; expectations about the
baseline are explicitly labeled expectations (R2). **Safe Rust**: PASS.

Post-design re-check: PASS — audit artifacts + tests + one
posture-consistent validation fix.

## Project Structure

### Documentation (this feature)

```text
specs/011-connector-verification/
├── plan.md              # This file
├── research.md          # R1–R6 (tool, baseline protocol, matrix design,
│                        #   inventory, R5 resolution, Makefile wiring)
├── data-model.md        # Matrix schema + interaction rows + records
├── quickstart.md        # Audit a parameter; measure coverage
├── contracts/
│   └── parameter-matrix.md   # PM1–PM8
├── matrix.md            # THE traceability matrix (built in implementation)
└── tasks.md             # /speckit-tasks output (NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
Makefile                          # + coverage target (R6)
crates/rdlt-postgres/
├── src/dest/config.rs            # R5: explicit-vs-default merge_strategy
├── src/dest/commit.rs            # R5: open-time rejection under non-merge
├── tests/*.rs                    # gap cells land in the owning suites
crates/rdlt-cli/src/main.rs       # CLI spec rows' cells (unit)
benches/RESULTS.md                # coverage record (baseline + final +
                                  #   exclusions)
```

## Design Notes (delta-level)

- **Audit order**: T001 baseline → matrix skeleton with citations
  (existing suites) → gap list = rows without citations → cells → R5
  fix → final coverage + classification → close-out.
- **R5 shape**: parse layer keeps deserialization compatible while
  distinguishing explicit from defaulted `merge_strategy`
  (destination-wide and per-table); `ensure_table` rejects explicit
  configuration under a non-merge mode, typed, naming table + mode —
  exactly beside the 010 F5 rejections. Schema round-trip cells updated;
  no behavior change for unconfigured pipelines.
- **Cell placement**: each gap cell goes in the suite that owns its
  block (cursor rows → `incremental.rs`, hints → `conformance.rs`, CDC
  rows → `cdc.rs`, dest options → `dest_conformance.rs`, CLI rows →
  CLI unit tests) — the matrix cites across suites; no new "matrix test
  file" that duplicates ownership.
- **Coverage honesty**: instrumented runs use the same nextest
  invocations; failpoints feature included so sweep binaries count;
  exclusion classes expected: the subprocess release-CLI path
  (memory_bound), platform-conditional TLS store code, defensive
  unreachable arms — each classified, none waved through silently.

## Verification Map (story → proof)

| Story | Proof surface |
|---|---|
| US1 matrix | `matrix.md` complete, zero uncited rows (PM1); spot-audit protocol (SC-002) |
| US2 coverage | `make coverage` ≥80% recorded in RESULTS.md with baseline, per-file table, classified exclusions (PM5) |
| US3 mismatches | close-out mismatch list, each resolved (PM6); R5 rejection cells (PM7) |
| Governance | make check + doc-tests + sweeps + semver-checks green (PM8) |

## Phase 2 note for /speckit-tasks

Order: T001 tooling + baseline (WELDED to recording — an unmeasured
baseline poisons every later claim) → matrix skeleton + citation audit
(this task produces the GAP LIST that sizes the rest) → gap cells by
block (parallelizable per suite) → R5 fix welded to its cells → final
coverage + exclusion classification → close-out. The matrix must be
committed WITH the cells that close its gaps, never ahead of them.
