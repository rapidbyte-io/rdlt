# Implementation Plan: Hardening & Performance

**Branch**: `003-hardening-performance` | **Date**: 2026-07-20 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/003-hardening-performance/spec.md`

## Summary

Deepen the correctness net around the engine's hottest code (deterministic
crash-point sweep, mutation-testing pass, fuzz targets, end-to-end shredder
property test), THEN rewrite the hot path (streaming no-`Value` shred, cheaper
slab splitting, deliberate row-id hash choice, RSS closure), and prove it all by
completing the design doc §8 benchmark matrix with a blocking CI perf-regression
gate. Strict ordering is a spec requirement (FR-006): US1's net must exist and
pass before US3 touches the shredder.

The established architecture is feature 001's plan and contracts; feature 002's
artifacts stand. **This feature amends NO public contracts** — everything is
tests, benches, CI, and internal engine code. The only externally visible change
is a possible row-id algorithm switch (FR-008, clarified option A), which is
pre-release and recorded in the design doc.

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies** (all dev/test-side unless noted):
`cargo-mutants` (mutation pass), `fail` crate behind a `failpoints` feature
(crash-point injection; run-time no-op when disabled), `cargo-fuzz`/libFuzzer
(fuzz targets), `proptest` (already in workspace; shredder property test),
`iai-callgrind` + valgrind (deterministic perf gate), `hyperfine` (e2e run
statistics), `xxhash-rust` (hash candidate — runtime dep ONLY if the switch
wins), `memchr` (runtime: slab splitting).

**Storage**: n/a (feature adds no persistence; it tests the existing WAL/dest
protocols)

**Testing**: `cargo nextest run` (doc-tests via `cargo test --doc`); new suites:
`crash_sweep` (per-PR for in-process destinations), mutation + fuzz (scheduled
CI, too slow per-PR)

**Target Platform**: Linux (CI = ubuntu-latest; valgrind required for the perf
gate job)

**Project Type**: Rust library workspace + dev CLI (unchanged)

**Performance Goals**: design doc §8 — REST→Postgres ≥5×, shred-stage-only ≥20×,
cold start ≤1/20th, flagship RSS ≤1/5th of baseline; streaming shred ≥3× on the
shred microbench (SC-006)

**Constraints**: byte-identical shred output across old/new paths (rows,
identities, schemas); correctness suites land before the rewrite; every
optimization shows before/after on the benches; perf gate must be
machine-independent (instruction counts, not wall time)

**Scale/Scope**: ~0 new public API surface; 1 new fuzz workspace, ~4 new test
suites, ~3 bench harness additions, 2 CI jobs, internal rewrite of
`shred/{nest,infer,build}` hot path behind an equivalence gate

## Constitution Check

`.specify/memory/constitution.md` is the unfilled template (unchanged since
feature 001). Governing principles remain the approved design doc
(`2026-07-18-rdlt-engine-design.md`): correctness before speed, deep modules
with sacred seams, no silent failures, benchmark honesty (baseline first, no
multiple without both columns). Gate evaluation against those:

- **Correctness first**: PASS by construction — the feature's ordering constraint
  (FR-006) encodes it.
- **Seams sacred**: PASS — no `rdlt-core`/`rdlt-connector` changes except a
  possible internal swap inside `RowIdBuilder` (same API, same output type).
  Post-design re-check: still true; the `failpoints` feature lives in engine +
  destination crates, never in the seam crates.
- **Benchmark honesty**: PASS — all new cells measured baseline-first on the
  existing harness; misses documented, not hidden.

## Project Structure

### Documentation (this feature)

```text
specs/003-hardening-performance/
├── plan.md              # This file
├── research.md          # Phase 0 (R20–R30)
├── data-model.md        # Phase 1 (report/baseline formats, fault-point registry)
├── quickstart.md        # Phase 1 (how to run sweep/mutants/fuzz/benches)
├── contracts/
│   └── quality-gates.md # Phase 1 (CI gate semantics; no SPI amendments)
└── tasks.md             # Phase 2 (/speckit-tasks)
```

### Source Code (repository root)

```text
crates/rdlt-engine/
├── src/wal/…                  # fail::fail_point!() at write/fsync/rename/append boundaries (failpoints feature)
├── src/shred/stream.rs        # NEW: streaming no-Value shred path (tape + builders)
├── benches/shred.rs           # extended: old-vs-new path, hash candidates
├── benches/iai_hotpath.rs     # NEW: iai-callgrind instruction-count benches (perf gate)
└── tests/
    ├── crash_sweep.rs         # NEW: enumerate fault points × kill × recover × assert
    └── shred_equivalence.rs   # NEW: proptest old-path ≡ new-path + invariants
crates/rdlt-dest-parquet/src/lib.rs   # fail_point!() at truncate/rename/receipt boundaries
crates/rdlt-dest-duckdb/src/lib.rs    # fail_point!() at appender/tx boundaries (coarse; tx atomicity is DuckDB's)
crates/rdlt-core/src/identity.rs      # RowIdBuilder: hash swap IF R25 threshold met
crates/rdlt-source-file/src/jsonl.rs  # memchr slab splitting, zero-copy handoff
fuzz/                                  # NEW cargo-fuzz workspace (excluded from main workspace)
├── fuzz_targets/{jsonl_slab,cursor_decode,file_config,arrow_schema_map,shred_push}.rs
└── corpus/…                           # seeds + regression cases, committed
benches/
├── run-e2e.sh                 # + cold-start cell, hyperfine wrapping, RSS row rerun
├── baseline/pipeline_rest_pg.py      # NEW: dlt rest_api → postgres baseline
├── baseline/normalize_only.py        # NEW: dlt normalize-stage-only baseline
└── perf-baselines.json        # NEW: recorded instruction counts (gate compares ±3%)
.github/workflows/ci.yml       # + perf-gate job (valgrind + iai-callgrind, blocking)
.github/workflows/deep-checks.yml  # NEW scheduled: mutants + fuzz + full crash sweep
mutants.toml                   # cargo-mutants config (exclusions + timeout)
Makefile                       # NEW: canonical targets; CI invokes make, never inline commands
```

**Structure Decision**: everything lands in existing crates or test-only
additions; the `fuzz/` directory is a separate cargo workspace (standard
cargo-fuzz layout) so libFuzzer's nightly requirement never touches the main
build. The `failpoints` cargo feature is dev-facing, off by default, and
compiles to no-ops when disabled — zero release-path cost.

## Phase ordering (mirrors spec priorities)

1. **US1 net first**: fault points + crash sweep on the CURRENT code; mutation
   baseline + dispositions; fuzz targets + initial corpus; shredder property
   test. All green before any US3 code exists.
2. **US2 evidence**: three benchmark cells baseline-first; iai perf gate armed
   with baselines from the CURRENT code (so US3's wins are visible and its
   regressions blocked).
3. **US3 rewrite**: streaming shred behind equivalence gate; memchr/zero-copy
   slab work; hash decision (measure → switch iff >30% flagship e2e win); RSS
   closure; thin-LTO if it measures.

## Complexity Tracking

No constitution violations to justify. One deliberate scope guard: DuckDB and
Postgres internal transaction boundaries are NOT fault-instrumented — their
atomicity is the database's own guarantee; the sweep instruments every boundary
WE own (WAL, parquet dest, session protocol call sites) and treats each DB
transaction as one atomic step. Documented in contracts/quality-gates.md.
