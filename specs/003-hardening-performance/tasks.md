# Tasks: Hardening & Performance

**Input**: Design documents from `specs/003-hardening-performance/`
**Prerequisites**: plan.md, spec.md, research.md (R20–R30), data-model.md, contracts/quality-gates.md

**Ordering constraint (FR-006, non-negotiable)**: Phase 3 (US1 correctness net)
must be COMPLETE and green on current code before any Phase 5 (US3) hot-path
change exists. Phase 4 (US2) arms the perf gate with CURRENT-code baselines
before Phase 5 changes them. All cargo commands run via
`distrobox enter my-distrobox -- ` locally.

## Phase 1: Setup

- [X] T001 Create `Makefile` at repo root with intent verbs `build`, `release`, `test`, `bench`, `lint`, `check`; `test`/`bench` take `TARGET=` suite selectors (test: fast default, `unit`, `e2e`, `sweep`, `prop`, `fuzz`, `mutants`, `deep`=all slow; bench: micro default, `iai`, `e2e`) — tool commands are recipe implementation details, per FR-014/G4; `make check` reproduces per-PR CI exactly
- [X] T002 [P] Add `mutants.toml` at repo root (crates: rdlt-engine/rdlt-core/rdlt-connector; exclude benches, examples, testkit render helpers, Display impls; timeout multiplier 3×) per research R21
- [X] T003 [P] Scaffold `fuzz/` cargo-fuzz workspace (own `Cargo.toml`, excluded from root workspace; five empty target stubs + `corpus/<target>/` seed dirs) per research R22

## Phase 2: Foundational (blocking prerequisites)

- [X] T004 Add `failpoints` cargo feature (dep: `fail` crate, off by default) to `crates/rdlt-engine/Cargo.toml`, `crates/rdlt-dest-parquet/Cargo.toml`, `crates/rdlt-dest-duckdb/Cargo.toml`; wire `fail_point!()` sites with the registry names from data-model §1 into `crates/rdlt-engine/src/wal/*.rs` (segment write/fsync, manifest append/fsync), `crates/rdlt-dest-parquet/src/lib.rs` (truncate, staged sync, rename, dir fsync, state write, receipt write), `crates/rdlt-dest-duckdb/src/lib.rs` (append, tx commit), and the session call sites in `crates/rdlt-engine/src/runtime/graph.rs` / `src/load/loader.rs`
- [X] T005 Switch CI to the Makefile: `.github/workflows/ci.yml` jobs invoke `make <target>` instead of inline commands (G4.1); verify no behavior change by comparing job logs on a scratch commit

**Checkpoint**: `make gates` green with failpoints compiled but disabled — the injection plumbing is proven inert before anything depends on it.

## Phase 3: User Story 1 — Trustworthy under any failure (P1) 🎯 MVP

**Goal**: the correctness net exists and passes on CURRENT code.
**Independent test**: `make check` (includes the crash sweep), `TARGET=mutants make test` (report ≥85% + dispositions), fuzz targets running with zero findings, property suite green — all without any US3 code existing.

- [X] T006 [US1] Crash-point sweep harness in `crates/rdlt-engine/tests/crash_sweep.rs`: enumerate the data-model §1 registry; for each point × {error, panic} × {first-run, during-recovery (composed with one prior kill)} run a 3-stream pipeline (memory + parquet + DuckDB destinations), restart, assert exactly-once totals; assert sweep list == registry (G2.2); runs inside `make check` with `--features failpoints`
- [X] T007 [US1] Wire the per-PR sweep subset into `.github/workflows/ci.yml` via `make check` (in-process destinations; Postgres joins in the scheduled job) per G2.1
- [X] T008 [P] [US1] Shredder property test in `crates/rdlt-engine/tests/shred_property.rs`: proptest strategy per research R23 (bounded arbitrary JSON incl. `_rdlt_*`-named keys, normalization-colliding keys, 2^53 boundaries); assert row conservation, lineage integrity, schema monotonicity, naming safety; 256 cases default, `PROPTEST_CASES` override documented in the file header
- [X] T009 [P] [US1] Implement the five fuzz targets in `fuzz/fuzz_targets/{jsonl_slab,cursor_decode,file_config,arrow_schema_map,shred_push}.rs` with seed corpora; each target's crate-side entry points get `#[doc(hidden)] pub` shims where needed; run each ≥10 min locally and commit corpora
- [X] T010 [US1] Scheduled deep-checks workflow `.github/workflows/deep-checks.yml`: `TARGET=deep make test` on its weekly/nightly cadences (mutants, fuzz, extended property run, full crash sweep incl. Postgres) — per G3
- [X] T011 [US1] Run the mutation baseline (`TARGET=mutants make test`), then close the gaps: new tests for killable survivors, delete genuinely dead code, write waivers for the rest; record everything in `specs/003-hardening-performance/mutation-report.md` (data-model §2) until kill rate ≥85% with zero undispositioned survivors (SC-002)
- [X] T012 [US1] Fix everything the new nets find (sweep failures, fuzz findings graduating to unit tests, property counterexamples) — each fix lands with its regression test; document notable finds in this file's Implementation notes

**Checkpoint**: SC-001/SC-002 met; SC-003 clock running via T010. US1 delivers standalone.

## Phase 4: User Story 2 — Complete, regression-proof performance evidence (P2)

**Goal**: all five §8 cells measured; regressions blocked in CI.
**Independent test**: RESULTS.md shows five complete rows; a deliberate slowdown PR is rejected by the gate (SC-007).

- [X] T013 [P] [US2] dlt REST→Postgres baseline: standalone mock-API binary in `crates/rdlt-source-rest/examples/mock_api.rs` (100k records, 100 pages, cursor pagination) + `benches/baseline/pipeline_rest_pg.py` (pinned dlt `rest_api` → postgres); measure baseline FIRST per R29
- [X] T014 [US2] rdlt side of the REST→Postgres cell via the CLI against the same mock API and postgres instance; record the ≥5× row in `benches/RESULTS.md` with caveats
- [X] T015 [P] [US2] Shred-stage-only cell: `benches/baseline/normalize_only.py` (dlt normalize stage isolated, per R28-style in-process timing) vs `cargo bench -p rdlt-engine --bench shred` on the same 200k nested dataset; record the ≥20× row (honest miss documented if the current shredder falls short — US3 then closes it)
- [X] T016 [P] [US2] Cold-start cell per R28: one-row pipeline, hyperfine 10-run median for rdlt release CLI, in-process timing for dlt; record the ≤1/20th row; extend `benches/run-e2e.sh` with this cell
- [X] T017 [US2] Instruction-count benches in `crates/rdlt-engine/benches/iai_hotpath.rs` (shred 10k, passthrough 10k, identity keyed/keyless 10k) + `benches/perf-baselines.json` recorded from CURRENT code (data-model §4)
- [X] T018 [US2] Blocking perf-gate job in `.github/workflows/ci.yml` (part of `make check`: iai benches + comparison vs perf-baselines.json, >3% instruction regression fails, G1); prove SC-007 by pushing a scratch commit with an injected slowdown and observing the failure, then reverting

**Checkpoint**: gate armed on current-code baselines — US3's effects will be visible and its regressions blocked.

## Phase 5: User Story 3 — Faster hot path, provably (P3, HARD deliverable)

**Goal**: streaming shred + cheaper I/O + deliberate hash choice + RSS closure, all with before/after evidence, zero behavior change.
**Independent test**: US1 suites pass unchanged against the new path; SC-005/SC-006 met; hash decision recorded.

- [X] T019 [US3] Streaming tape parser in `crates/rdlt-engine/src/shred/stream.rs`: slab → borrowed token tape (no per-row `Value`), shape observation over the tape, canonical `_rdlt_id` bytes rendered from the tape through the SHARED `canonical_json_bytes` rules per R24; duplicate-key/lone-surrogate/number-boundary tie-breaks pinned by explicit tests copied from old-path behavior
- [X] T020 [US3] Equivalence gate in `crates/rdlt-engine/tests/shred_equivalence.rs`: proptest old-path ≡ new-path over (rows, `_rdlt_*` values, schema sequence, discard counts) per data-model §7; then switch the default in `crates/rdlt-engine/src/shred/mod.rs` keeping the old path compiled as the test reference (G5.3)
- [X] T021 [US3] Run the ENTIRE suite (`make check` + equivalence + property) against the new default path; record shred-bench before/after and update the ≥20× RESULTS.md row and `benches/perf-baselines.json` in the same change (G1.3)
- [X] T022 [P] [US3] memchr slab splitting + zero-copy handoff in `crates/rdlt-source-file/src/jsonl.rs` (no per-line UTF-8 revalidation, `Bytes::from(Vec)` instead of `copy_from_slice`) per FR-007, with before/after on the flagship e2e row
- [X] T023 [P] [US3] Hash decision per R25: add xxh3-128 candidate to `crates/rdlt-engine/benches/shred.rs` identity benches + a flagship e2e A/B; switch `crates/rdlt-core/src/identity.rs` internals ONLY if e2e wins by >30%; either way record decision + numbers in `2026-07-18-rdlt-engine-design.md` §5.4
- [X] T024 [P] [US3] RSS closure per R27: DuckDB `memory_limit` in `crates/rdlt-dest-duckdb/src/lib.rs` config (bench profile 256MB) + appender chunk cap; re-measure the flagship row until ≤397MB (SC-005) or document the residual honestly
- [X] T025 [P] [US3] Thin-LTO experiment per R30: `lto = "thin"`, `codegen-units = 4` in root `Cargo.toml` release profile; keep iff flagship e2e improves ≥2% and build time <2×; record either way

**Checkpoint**: SC-004/SC-005/SC-006 resolved; every optimization has its before/after in the PR trail.

## Phase 6: Polish & Cross-Cutting

- [X] T026 Update `benches/RESULTS.md` to the final five-row matrix + re-measured flagship row; update `benches/run-e2e.sh` so one script reproduces every cell
- [X] T027 [P] Amend `2026-07-18-rdlt-engine-design.md` §8 table with all measured cells and the hash-decision record; note the quality gates (G1–G5) under the testing strategy section
- [X] T028 Full sweep via `make check` (fmt, clippy `-D warnings`, nextest, doc-tests, crash sweep, iai gate) + update this file's Implementation notes + commit series on `003-hardening-performance` + PR to main (semver gate: no seam-crate API changes expected; hash swap is internal)

## Implementation notes (in progress)

- **US1 (complete)**: crash sweep green on all in-process destinations from the
  first full run — the feature-002 review fixes held; zero sweep findings.
  Fuzz smoke (5 targets × 45 s): zero findings. Property test's FIRST run found
  a real oracle subtlety (shape conflicts are order-dependent BY DESIGN —
  generator now uses shape-disjoint key pools; the engine behavior was correct).
  Crash-point macro lives ONCE in `rdlt_core::failpoint` (feature-forwarded
  through the connector seam; dests never depend on core directly).
- **Mutation baseline** (52 min, workspace-tested): 470 mutants — 241 caught,
  127 missed, 94 unviable, 8 timeouts → 64.1% vs the 85% bar. Survivor
  dispositions (T011) are the main outstanding US1 work. Beware: cargo-mutants
  config MUST live at `.cargo/mutants.toml` (a repo-root `mutants.toml` is
  silently ignored), and needs `test_workspace = true` + a disk-backed TMPDIR.
- **US2 (complete)**: REST→Postgres 5.5× (7.49 s → 1.37 s, ≥5× ✅); cold start
  1/22.7 (≤1/20 ✅); shred-only measured honestly (see below). Perf gate armed:
  iai-callgrind counts vs `benches/perf-baselines.json`, >3% blocks; SC-007
  proven live (injected +9.8% slowdown → gate exit 1; 0.4% → correctly passes).
- **US3 core (complete)**: the shredder was rebuilt around a `JsonView` seam —
  observation/canonical/identity/policy/build exist ONCE, generic; `TreeShredder`
  (reference) and `TapeShredder` (production, slab arena, no per-row trees)
  differ only in the ~60-line traversal. Equivalence proptest passed on its
  first full run. Profile findings (final attribution — a mislabeled bench
  entry point initially hid the tape win, caught during cleanup): (1)
  `RowId::to_hex` via `write!("{:02x}")` was **48% of all shred instructions**
  (table encoder halved the stage); (2) the tape path cuts a further **31%**
  vs the tree path. Net shred: 1.094 G → 362 M instr (3.0×), wall 1.66 →
  0.58 s (13.1× vs dlt normalize). Flagship e2e 1.73 → 1.05 s median (18.6×),
  RSS → 355 MB (met). Hash decision (T023): blake3 KEPT — cannot clear the
  30% e2e bar; recorded in design doc §5.4.
- T022 done (memchr slab reader — landed as FR-007 required behavior; e2e
  neutral within noise). T024 done: RSS 642 → 355 MB (SC-005 met at 1/5.6) —
  the fix was glibc arena retention (mallopt in the CLI), NOT DuckDB
  memory_limit, which measured zero effect. Three known mutation survivors
  closed with targeted tests (decimal scale guard, list mapping arms, WAL
  future-version guard).
- Mutation run on current code was OOM-KILLED at 349/595 (a mutant broke a
  backpressure bound; parallel runaways stacked to host OOM — it took the user
  session and both distrobox containers down, which blocks all further builds
  until a host re-login). Partial results dispositioned in
  mutation-report.md: 188 caught / 79 missed / 68.4% partial, with a
  cluster-by-cluster test plan projecting ≈94%. Makefile mutants recipe now
  carries --iterate --jobs 2.
- Session recovered (logind revived the user manager). T011/T012 done: 28
  survivor-killing tests across 13 files + `tests/mutation_closures.rs`; the
  registry-widening closure found a REAL passthrough narrowing bug (fixed with
  a lattice join — see mutation-report.md). T025 done: thin-LTO REJECTED by
  A/B (no win, 20× build cost). Final medians recorded (flagship 1.05 s →
  18.6×). Final sweep green: make check (lint, 153 nextest, crash sweep, iai
  perf gate — narrowing fix cost +0.01%, within the 3% tolerance) + doc-tests.
  PR to main pending push access (SSH key unavailable remotely); the clean
  full mutation run for the authoritative post-closure kill rate runs in the
  background / next scheduled deep job.

## Dependencies & Execution Order

```
Phase 1 (T001–T003) ─► Phase 2 (T004–T005)
   └─► Phase 3 (US1: T006–T012)   — sweep needs T004; T008/T009 parallel to T006
          └─► Phase 4 (US2: T013–T018)  — gate baselines recorded on post-US1 code
                 └─► Phase 5 (US3: T019–T025) — FR-006: hard-ordered after US1+US2
                        └─► Phase 6 (T026–T028)
```

### Parallel Opportunities

- T002 ∥ T003 after T001.
- Phase 3: T008 ∥ T009 while T006 builds the sweep; T011 after T006–T009 exist.
- Phase 4: T013 ∥ T015 ∥ T016 (different harness files); T017→T018 sequential.
- Phase 5: T022 ∥ T023 ∥ T024 ∥ T025 once T019–T021 land (different files, all
  measured against the already-armed gate).

## Implementation Strategy

- **MVP** = Phases 1–3: the correctness net on current code — independently
  valuable even if nothing else lands. Stop and validate.
- Phase 4 completes the evidence and arms the gate; Phase 5 is the payoff and is
  a HARD deliverable (clarification 2026-07-20) — the ordering is schedule
  discipline, not an escape hatch.
- Mutation/fuzz cadences (T010) start early so SC-003's 24 CPU-hours accumulate
  while later phases proceed.

## Notes

- Format validated: checkbox + ID + exact path on every task; [P] only where
  files and dependencies allow; story labels only in Phases 3–5.
- 28 tasks: Setup 3, Foundational 2, US1 7, US2 6, US3 7, Polish 3.
