# Tasks: Close or Re-baseline the Two Benchmark Misses

**Input**: Design documents from `/specs/004-close-perf-misses/`

**Prerequisites**: plan.md, spec.md, research.md (R1–R7), data-model.md,
contracts/measurement-protocol.md (P1–P6), quickstart.md

**Tests**: No new test-suite tasks — the feature's verification IS its
measurement records; the existing 003 nets (equivalence proptest, nextest,
doc-tests, crash sweep, perf gate) are acceptance conditions inside every A/B
task, not separate deliverables. Safe Rust only: `unsafe_code = "deny"`
stands; a candidate that only wins via unsafe is REJECTED (P4.4).

**Organization**: grouped by user story; US1 (shred) and US2 (cold start) are
independent after Phase 2 and may interleave; US3 (coherent record) is
hard-ordered after both.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup (measurement environment)

**Purpose**: restore and verify the environment every downstream number
depends on. Nothing is measured before this phase is green.

- [X] T001 Restore the distrobox build environment (missing from PATH at
      feature start; container may have lost gcc — see build-env history):
      verify `distrobox enter my-distrobox -- cargo --version`, valgrind,
      hyperfine, and that `rustc --version` matches the toolchain recorded in
      `benches/perf-baselines.json` (the gate's cross-toolchain refusal must
      NOT fire). Record the repair steps taken in
      `specs/004-close-perf-misses/evidence/environment.md`.
- [X] T002 Write the environment-identity header (R7) into
      `specs/004-close-perf-misses/evidence/environment.md`: CPU model,
      kernel, rustc, dataset identity (200k-row NDJSON row count + content
      hash regenerated via `benches/run-e2e.sh` datagen), and confirmation
      this is the 003 matrix machine. This file is the header template every
      later evidence artifact copies.
- [X] T003 Reproduce the close-out numbers on the restored environment before
      changing anything: `cargo bench -p rdlt-engine --bench shred` (expect
      ~0.50 s stage wall) and the run-e2e.sh cold cell (expect ~30 ms
      median); `make bench TARGET=iai` must pass against the recorded
      baselines. Record both in
      `specs/004-close-perf-misses/evidence/environment.md`. If either
      deviates materially, STOP and diagnose before any candidate work
      (protocol P2/P3 quiet-machine discipline).

---

## Phase 2: Foundational (record structure)

**Purpose**: land the record formats so every subsequent measurement is
written once, in final form.

**⚠️ CRITICAL**: no user story work before this phase completes.

- [X] T004 Restructure the matrix in `benches/RESULTS.md` per data-model §1:
      add the **Gated?** column (`gated` for every design-§8-target row,
      `scoreboard` for the parquet→DuckDB bonus row), no value changes; add
      the two-leaf `resolved (a)/(b)` status vocabulary note above the table.
- [X] T005 [P] Create
      `specs/004-close-perf-misses/evidence/README.md` stating the artifact
      formats (profile and A/B record required sections, data-model §3), the
      environment-header rule (copy from environment.md), and the P4
      accept/reject checklist verbatim — so each artifact is written against
      a checklist, not from memory.

**Checkpoint**: records land in final form; US1 and US2 may now proceed (in
either order or interleaved).

---

## Phase 3: User Story 1 — Resolve the shred-stage miss (Priority: P1) 🎯 MVP

**Goal**: shred-only cell (rdlt 0.50 s vs dlt 5.95 s = 12.0×) reaches a
decision-tree leaf: ≥20× (rdlt ≤ 297.5 ms) with the gate re-baselined, or an
evidence-backed bar adjustment.

**Independent Test**: the cell reports ≥20× on the frozen pin, OR the version
policy holds an adjustment entry linking committed profiling evidence in
which every R2 candidate carries a measured disposition.

- [X] T006 [US1] Fresh attribution profile of the shred stage on current
      code: callgrind via `cargo bench -p rdlt-engine --bench iai_hotpath` +
      `callgrind_annotate` (primary, gate units) AND
      `perf stat -e cycles,instructions,cache-misses,branch-misses` on
      `crates/rdlt-engine/benches/shred.rs` (secondary lens). Write
      `specs/004-close-perf-misses/evidence/profile-shred.md`: top-N table
      with shares, both lenses, environment header.
- [X] T007 [US1] Rank candidates C1–C5 (research R2) in
      `specs/004-close-perf-misses/evidence/profile-shred.md`: measured share
      per candidate, viable/exhausted classification against the 1.68×
      requirement (R3 arithmetic restated in the artifact), C5/blake3 reopen
      decision (only if identity hashing ≥ 25% share — otherwise T023 stands,
      one line). Attempt order = descending measured share.
- [ ] T008 [US1] Candidate C1 (structural scan, memchr-based stage-1 in
      `crates/rdlt-engine/src/shred/tape.rs`): implement per profile
      findings, then full A/B per protocol P4 — shred microbench both sides,
      e2e flagship cell, `make bench TARGET=iai` gate run,
      `cargo nextest run` + shred_equivalence + `cargo test --doc`. Verdict +
      data in `specs/004-close-perf-misses/evidence/ab-c1-structural-scan.md`.
      Skip with a one-line exhausted note in the resolution record if T007
      classified C1 non-viable.
- [ ] T009 [US1] Candidate C2 (UTF-8 validate-once through safe APIs — the
      type system carries the proof, NO unchecked conversions, P4.4) across
      `crates/rdlt-engine/src/shred/tape.rs` and the slab handoff: same A/B
      discipline → `specs/004-close-perf-misses/evidence/ab-c2-utf8-once.md`.
      Skip-note rule as T008.
- [ ] T010 [US1] Candidate C3 (arena/tape layout in
      `crates/rdlt-engine/src/shred/arena.rs` + `tape.rs`): memory-shaped, so
      the two-lens rule binds — acceptance requires the win in `perf stat`
      cycles AND wall time, not callgrind alone (R1). Same A/B discipline →
      `specs/004-close-perf-misses/evidence/ab-c3-arena-layout.md`.
      Skip-note rule as T008.
- [ ] T011 [US1] Candidate C4 (scalar fast paths in
      `crates/rdlt-engine/src/shred/build.rs` and `tape.rs`: integer-only
      number parse, escape-free string path, fixed-layout datetime parse
      replacing per-row chrono format-string interpretation): same A/B
      discipline → `specs/004-close-perf-misses/evidence/ab-c4-scalar-paths.md`.
      Skip-note rule as T008.
- [ ] T012 [US1] Resolve the cell per protocol P6: final 5-run-median shred
      measurement vs the frozen dlt 5.95 s on accepted code. Leaf (a): update
      the RESULTS.md row to `resolved (a)`, re-record
      `benches/perf-baselines.json` in a commit naming the accepted A/B
      records (P5). Leaf (b) (including the partial case — accepted wins
      land AND the bar adjusts): append the version-policy entry in
      `benches/RESULTS.md`, update the row's bar + `resolved (b)` status.
      Either way write
      `specs/004-close-perf-misses/evidence/resolution-shred.md` with the
      full candidates table (every C1–C5 disposition — a candidate without a
      row is a traceability failure, data-model §2).

**Checkpoint**: shred cell resolved, all evidence committed — independently
shippable as the MVP.

---

## Phase 4: User Story 2 — Cold-start absolute bar (Priority: P2)

**Goal**: gated criterion becomes `≤ N ms` absolute (N = measured floor ×
1.5, round up to 5 ms); dlt ratio demoted to scoreboard; cheap startup wins
taken if the profile surfaces them.

**Independent Test**: the criterion record shows an absolute bound + full
protocol; measured value passes it; re-pinning a faster dlt could change only
the scoreboard ratio, never the gated verdict (SC-003).

- [ ] T013 [P] [US2] Startup-composition profile (R5): temporary
      Instant-stamp instrumented build of `crates/rdlt-cli` (phase
      boundaries: main entry, config parse, DuckDB open, catalog/state init,
      first-batch ready, teardown — instrumentation is throwaway, never
      merged) corroborated by `strace -T -c -f` on the one-row pipeline;
      dynamic-link cost = hyperfine total minus main-entry stamp. Write
      `specs/004-close-perf-misses/evidence/profile-cold-start.md`: phase
      table, floor composition (irreducible vs reducible per phase),
      environment header.
- [ ] T014 [US2] Protocol measurement per P3: `hyperfine` ≥ 3 warmups / ≥ 20
      runs, median, warm FS cache, on the exact run-e2e.sh one-row pipeline
      command. Derive `N = floor × 1.5 rounded UP to nearest 5 ms`; record
      measurement + derivation in
      `specs/004-close-perf-misses/evidence/resolution-cold-start.md`
      (started here, finished in T016).
- [ ] T015 [US2] IF T013 classified a reducible phase worth taking (e.g.
      deferred DuckDB open/catalog work in `crates/rdlt-cli/src/main.rs`):
      implement and A/B under the full P4 rule (hyperfine both sides, e2e
      cells, gate run, nextest green) →
      `specs/004-close-perf-misses/evidence/ab-cold-startup.md`. If nothing
      reducible is worth it, record the one-line negative in the resolution
      record and skip.
- [ ] T016 [US2] Convert the criterion: split the RESULTS.md cold row into
      the gated-absolute row (bar `≤ N ms`, link to the protocol contract)
      and the scoreboard-ratio row (data-model §1); document the protocol
      (runs, aggregation, cache state) in the cold cell of
      `benches/run-e2e.sh`; append the version-policy entry recording the
      criterion conversion (ratio → absolute, with the derivation link);
      finish `specs/004-close-perf-misses/evidence/resolution-cold-start.md`
      — leaf (a) with the bar met, including the SC-003 invariance statement.

**Checkpoint**: cold-start criterion is baseline-tool-invariant and passing.

---

## Phase 5: User Story 3 — Coherent final record (Priority: P3)

**Goal**: one consistent story across matrix, policy, resolutions, evidence.

**Independent Test**: the SC-006 walk — every matrix row → resolution record
→ evidence artifact with no contradictions, stale numbers, or ambiguous
gated/scoreboard status.

- [ ] T017 [US3] Full-matrix re-measure on final accepted code, pin FROZEN at
      dlt 1.29.0 (P1): `benches/run-e2e.sh` (flagship + RSS, passthrough,
      cold cells) plus the REST→Postgres and normalize-only recipes; update
      every row of `benches/RESULTS.md` same-session, baseline-first; append
      the History entry.
- [ ] T018 [US3] SC-006 traceability walk, recorded at the bottom of
      `specs/004-close-perf-misses/evidence/README.md`: for each of the two
      resolved cells follow row → resolution record → every linked evidence
      artifact; verify no dangling links, every C1–C5 disposition present,
      policy entries cite resolutions, gate re-record commits name their
      accepted A/B records (P5), and design-doc §8 targets in
      `2026-07-18-rdlt-engine-design.md` reflect any adjusted bar (pointer to
      the policy entry, not a silent rewrite). Fix what the walk finds.
- [ ] T019 [US3] Full verification sweep green on the final tree:
      `make check` (lint, nextest, crash sweep, iai gate) + `cargo test
      --doc`; record the sweep output reference in
      `specs/004-close-perf-misses/evidence/README.md`.

---

## Phase 6: Polish

- [ ] T020 Implementation-notes block at the top of
      `specs/004-close-perf-misses/tasks.md` (003 convention): outcomes per
      cell, accepted/rejected candidates one-liners, and any follow-up
      backlog items surfaced by the profiles (candidates measured
      viable-but-not-taken, if any).

---

## Dependencies & Execution Order

```text
Phase 1 (T001→T002→T003) ─► Phase 2 (T004 ∥ T005)
   ├─► Phase 3 / US1: T006→T007→(T008→T009→T010→T011 in T007's measured order)→T012
   └─► Phase 4 / US2: T013→T014→T015→T016   (independent of US1; may interleave)
          both └─► Phase 5 / US3: T017→T018→T019 ─► Phase 6 (T020)
```

- T008–T011 are sequential (same files; each A/B measures against the
  currently-accepted state), and their real order comes from T007's ranking —
  the task numbering is the default, the profile's descending-share order
  wins.
- T013 is [P]: it can start any time after Phase 2, even mid-US1, on a
  different crate (`rdlt-cli`).

### Parallel Opportunities

- T004 ∥ T005 (different files).
- US2's T013 ∥ any US1 task (different crates, read-only profiling).
- Everything else is measurement-serialized by design: the P2/P3
  quiet-machine rule forbids concurrent heavy runs on the reference machine
  (the 003 contended-shred-measurement lesson), so cross-story parallelism is
  about calendar interleaving, not simultaneous execution.

## Implementation Strategy

- **MVP** = Phases 1–3: the shred cell resolved with committed evidence is
  independently valuable even if nothing else lands. Stop and validate.
- US2 is small and independent — natural interleave while long US1 A/B
  builds run.
- US3 is the audit trail that makes outcome (b) a first-class deliverable;
  it is cheap but hard-ordered last (P6: feature close requires it).
- Every task that produces a number commits its evidence artifact in the
  same change — evidence written after the fact is the failure mode the
  data-model's validation rules exist to prevent.

## Notes

- Format validated: checkbox + ID on every task; [P] only where files and
  the quiet-machine rule allow; story labels only in Phases 3–5.
- 20 tasks: Setup 3, Foundational 2, US1 7, US2 4, US3 3, Polish 1.
