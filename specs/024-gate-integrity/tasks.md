# Tasks: Test-gate integrity

**Feature**: `024-gate-integrity` | **Spec**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)

**Contract**: [contracts/gate-integrity.md](./contracts/gate-integrity.md) GI1–GI8

## Format: `[ID] [P?] [Story] Description`

- `[P]` — parallelizable: different files, no dependency on an incomplete task
- `[US1]`…`[US5]` — the user story the task serves (story phases only)

## Path Conventions

Repository root is `/var/home/netf/Repos/rapidbyte/rdlt`. Paths below are
repo-relative. Line numbers are as measured at `e77e5f23` — **re-measure before
editing**, because earlier tasks in this feature move lines in `Makefile`.

## Standing rules for every task

- Gate command is `env -u RUSTUP_TOOLCHAIN make check`. That variable silently
  overrides the 1.96.0 pin and only the perf gate notices, by refusing a
  comparison; never re-record baselines to clear that refusal.
- On this host, if a gate ran recently: `make reclaim`, then wait for
  `ss -tan | grep -c TIME-WAIT` to fall below ~20, then gate. Otherwise podman
  fails with `rootlessport … bind: address already in use`.
- **Never assert success from a filtered command whose failure the filter cannot
  see.** Check the exit code. This project has lost time to exactly that.
- Comments carry their reason and no task or feature ID (Principle VI).

---

## Phase 1: Setup (Shared Infrastructure)

- [X] T001 Record the pre-change baseline: run `env -u RUSTUP_TOOLCHAIN make check`
  and capture per-binary test and skip counts, plus total wall clock, into
  `specs/024-gate-integrity/evidence/baseline-before.txt`. This is the "before"
  half of every SC-010 and SC-008 comparison and cannot be reconstructed later.
- [X] T002 [P] Capture the nine permissive-selector match counts into
  `specs/024-gate-integrity/evidence/selector-counts-before.txt`, using
  `cargo nextest list` per selector (the eight non-empty results and the one zero
  from research R0). This is the evidence FR-002's dispositions are judged
  against.
- [X] T003 [P] Capture per-crate crash-site counts into
  `specs/024-gate-integrity/evidence/crash-sites-before.txt` for all seven crates
  that arm points, counting BOTH arming idioms per research R11 (engine 7,
  postgres 14, file 11, rest 4, iceberg 3, duckdb 2, snowflake 2+2). This is the
  cross-check on the scanner built in US3.

---

## Phase 2: Foundational (Blocking Prerequisites)

No foundational work blocks the stories. US1 is itself the prerequisite for
US2–US5 (see Dependencies), and it needs nothing built first: research R1
established the runner already defaults to failing on an empty selection, so US1
is a set of deletions rather than a construction.

---

## Phase 3: User Story 1 — An empty test selection fails instead of passing (Priority: P1) 🎯 MVP

**Goal**: a selector that matches nothing fails the gate, naming the selector.

**Independent test**: rename a selected test binary, run the gate, observe it
fail naming the empty selector; restore and observe it pass.

- [X] T004 [US1] Fix the dead selector in `Makefile` (the `TARGET=prop` line,
  currently 107): `test(shred_property)` → `binary(shred_property)`. The binary is
  `shred_property`; the single test inside it is `shred_invariants_hold`, so the
  test-name filter matches nothing (research R0). Verify with
  `cargo nextest list -p rdlt-engine -E 'binary(shred_property)'` returning 1.
- [X] T005 [US1] Remove `--no-tests=pass` from all nine `Makefile` lines
  (currently 97, 99, 101, 102, 103, 104, 105, 107, 205). The runner's default is
  already `fail` (research R1), so this is a deletion, not a replacement — do NOT
  add `--no-tests=fail`, which would be nine tokens asserting the status quo.
- [X] T006 [US1] Add a comment above the sweep-target block in `Makefile` stating
  the distinction the deleted flags conflated: `--no-tests` governs which tests
  the runner SELECTS, not whether they then skip. These binaries are selected and
  self-skip internally when a container runtime or credentials are absent, so
  removing the flag cannot fail a contributor's build for lacking resources.
- [X] T007 [US1] Record each of the nine dispositions in
  `specs/024-gate-integrity/evidence/selector-dispositions.md`: selector, its
  measured match count before, and its disposition (all nine expected STRICT — if
  any turns out to need `warn`, the reason goes at the `Makefile` site AND here,
  per FR-002).
- [X] T008 [US1] **Detection demonstration (FR-015)**: with the flags removed,
  temporarily rename one selected test binary file, run the gate, capture the
  failure output, revert, re-run and capture the pass. Record both in
  `specs/024-gate-integrity/evidence/demo-us1.md`. Then repeat for the R0 defect
  specifically: restore `test(shred_property)`, observe the gate now FAIL where it
  previously passed silently, revert.
- [X] T009 [US1] US1 gate: `env -u RUSTUP_TOOLCHAIN make check` green; the
  4,096-case property run now executes (confirm `make test TARGET=prop` reports 1
  test, not zero); close-out row.

**Checkpoint**: MVP — an empty selection can no longer pass, and the property
suite that had been silently empty runs.

---

## Phase 4: User Story 2 — Suites that exist are actually invoked (Priority: P1)

**Goal**: every test binary is invoked by the gate or exempt by name.

**Independent test**: enumerate every test binary; for each, show the gate path
that runs it or the recorded exemption with its reason.

- [X] T010 [US2] Add `$(MAKE) test TARGET=e2e` to the `check` target in
  `Makefile` (currently 263–268), between `test` and `test TARGET=sweep`. Its two
  binaries — `crates/rdlt-connector-file/tests/{e2e_copy.rs,e2e_duckdb.rs}` — are
  currently invoked by no target reachable from any gate (research R3: `deep`
  does not invoke `e2e` either).
- [X] T011 [US2] Build the exhaustive suite enumeration in
  `specs/024-gate-integrity/evidence/suite-reachability.md`: every
  `crates/*/tests/*.rs` binary, and for each the gate path that invokes it or a
  named exemption with its reason — and its MEASURED cost where cost is the
  reason. Derive the list mechanically (`ls crates/*/tests/*.rs` against
  `cargo nextest list` per gate target), never by hand.
- [X] T012 [US2] For every exemption T011 finds, write the reason at the site that
  excludes it — not only in the evidence file. An exemption a reader finds only in
  a spec directory is prose; one at the `Makefile` or config site is a decision.
- [X] T013 [US2] **Detection demonstration (FR-015)**: add a throwaway test binary
  that no gate path invokes, run T011's enumeration, observe it reported as
  unreachable, delete the binary. Record in
  `specs/024-gate-integrity/evidence/demo-us2.md`.
- [X] T014 [US2] Measure and record the wall-clock cost `TARGET=e2e` adds to
  `check`, with and without a container runtime present, in
  `specs/024-gate-integrity/evidence/gate-cost.md` (SC-010's first entry).
- [X] T015 [US2] US2 gate: full gate green; zero unreachable-and-unexplained
  suites; close-out row.

---

## Phase 5: User Story 3 — A dropped crash point is detected (Priority: P1)

**Goal**: every crash-point registry is verified against its own crate's sources,
so a dropped point fails.

**Independent test**: per registry, delete a site from source and registry
together and observe failure; add a site to source alone and observe failure;
restore and observe pass.

- [X] T016 [US3] Add the shared scanner to `crates/rdlt-testkit/src/crash.rs`,
  modelled on `crates/rdlt-engine/tests/crash_sweep.rs:196-233` (which walks
  `src/`, extracts each arming call's first string literal, sorts, and asserts
  set-equality — deliberately NOT deriving from the registry, because that is
  circular). Requirements specific to this port:
  - Recognise BOTH arming idioms — `crash_point!("…")` and `crash_at("…")` —
    because snowflake uses the second for two of its four points, for a stated
    correctness reason (research R11).
  - **FAIL LOUDLY on finding zero sites when the registry is non-empty.**
    "Scanned nothing, matched nothing" must never read as agreement; this is the
    specific way the new check could itself fail open.
  - Accept a subdirectory scope, because one crate holds sites for several
    registries (postgres 3, file 3).
  - Document in the doc comment that a third arming idiom requires updating this
    helper, and state the decision on commented-out arming calls (research open
    question 2 — decide here and say which).
- [X] T017 [P] [US3] Assert `rdlt-connector-postgres`'s three registries against
  their sources: `dest::FAIL_POINTS` (`src/dest/mod.rs:382`),
  `source::FAIL_POINTS` (`src/source/mod.rs:40`), `CDC_FAIL_POINTS`
  (`src/source/mod.rs:51`). 14 sites total; scope each scan so the three
  registries are checked separately, not against the union.
- [X] T018 [P] [US3] Assert `rdlt-connector-file`'s three registries:
  `dest::FAIL_POINTS` (`src/dest/mod.rs:56`), `S3_FAIL_POINTS`
  (`src/dest/mod.rs:67`), `source::FAIL_POINTS` (`src/source/mod.rs:21`). 11
  sites total.
- [X] T019 [P] [US3] Assert `rdlt-connector-snowflake`'s `FAIL_POINTS`
  (`src/dest/mod.rs:204`) — the crate that proves the two-idiom requirement: 2
  `crash_point!` in `src/dest/stage.rs` plus 2 `crash_at` in
  `src/dest/session.rs`, against a 4-entry registry. A scanner seeing only the
  macro would demand the registry shrink to 2, silently removing two points from
  the sweep matrix.
- [X] T020 [P] [US3] Assert `rdlt-connector-rest`'s `FAIL_POINTS`
  (`src/source/mod.rs:31`), 4 sites.
- [X] T021 [P] [US3] Assert `rdlt-connector-iceberg`'s `ICE_FAIL_POINTS`
  (`src/dest/session.rs:40`), 3 sites.
- [X] T022 [P] [US3] Assert `rdlt-connector-duckdb`'s `FAIL_POINTS`
  (`src/dest/mod.rs:262`), 2 sites.
- [X] T023 [US3] Migrate `rdlt-engine`'s existing in-test scanner to call the
  shared helper, so one implementation serves all eleven registries. Its
  assertion must stay behaviourally identical — it is the model the others copy,
  and a regression here would be invisible.
- [X] T024 [US3] **Detection demonstration (FR-015)**, per registry — ten
  demonstrations recorded in
  `specs/024-gate-integrity/evidence/demo-us3.md`: remove one site from source
  AND registry, observe failure; add a site to source only, observe failure;
  restore, observe pass. The snowflake case additionally demonstrates that
  removing only the `crash_at` sites is caught.
- [X] T025 [US3] US3 gate: full gate green; all ten registries asserted; the
  per-crate site counts match T003's record; close-out row.

---

## Phase 6: User Story 4 — A disarmed environment probe is visible (Priority: P2)

**Goal**: a maintainer can demand gated suites run, and a suite that silently
stops running shows as a difference.

**Independent test**: force a probe absent with resources present and observe a
count difference; run in demanding mode and observe outright failure.

- [X] T026 [US4] Add `RDLT_TESTKIT_REQUIRE_CONTAINERS` to
  `crates/rdlt-testkit/src/containers.rs`, mirroring the existing
  `FORCE_NO_CONTAINERS` at line 46: when set, `runtime_available()` panics naming
  the missing runtime instead of returning false. Setting both FORCE_NO and
  REQUIRE is itself an error — a run that forces absence and demands presence is a
  mistake in the invocation, and honouring either silently would hide it.
- [X] T027 [US4] Add `RDLT_TESTKIT_REQUIRE_SNOWFLAKE` to
  `crates/rdlt-testkit/src/snowflake.rs`, mirroring `FORCE_NO_SNOWFLAKE` at line
  18, with the same both-set-is-an-error rule.
- [X] T028 [US4] Add `crates/rdlt-testkit/tests/gating_pin.rs` asserting each
  probe's decision under each forced environment: resource present, absent,
  forced-absent, demanded-and-present, demanded-and-absent, and
  both-set-is-an-error. This is the test that makes the probe's behaviour a pinned
  contract rather than an implementation detail eight crates depend on.
- [X] T029 [US4] Produce the count baseline for the gate AS THIS FEATURE LEAVES
  IT — per binary, tests run and tests skipped — and commit it. Decide its
  location and granularity now that a real diff is visible (research open
  question 1) and record which was chosen and why.
- [X] T030 [US4] Add a `make` verb that emits the current counts in the baseline's
  exact shape, so comparison is a diff rather than a manual read. It reports; it
  does NOT fail on a difference — a check that fails on every legitimate test
  addition trains maintainers to bump it unread, which is how a pin stops
  pinning (GI5).
- [X] T031 [US4] Document in `crates/rdlt-testkit/README.md` how to read a count
  difference by direction: run-count up = a test was added; run-count down with
  skip-count up = a suite lost its resource or a probe regressed; run-count down
  with skip-count flat = tests disappeared.
- [X] T032 [US4] **Detection demonstration (FR-015)**: with a runtime present, set
  `FORCE_NO_CONTAINERS`, run the gate, observe the skip-count difference against
  the baseline; then set `REQUIRE_CONTAINERS` with the runtime absent and observe
  outright failure naming it. Record in
  `specs/024-gate-integrity/evidence/demo-us4.md`.
- [X] T033 [US4] US4 gate: full gate green in default mode; green in demanding
  mode on this host (resources present); close-out row.

---

## Phase 7: User Story 5 — Recorded gate practice is executable (Priority: P3)

**Goal**: every recorded gate practice is reproducible by a named command.

**Independent test**: run the named commands and reproduce the recorded figures.

- [X] T034 [US5] Add a `semver` target to `Makefile` running
  `cargo semver-checks` on `rdlt-core` and `rdlt-connector` against the recorded
  baseline `34ccd379`, with the sha and its derivation
  (`git merge-base main 024-gate-integrity`) in a comment at the site. Explain
  there why the baseline is PINNED rather than tracking a branch: a baseline that
  advances with every merge forgives the break it just accepted, and CI's
  `origin/main` is 73 commits stale so its result carries no information
  (research R5).
- [X] T035 [US5] Wire `$(MAKE) semver` into `check`.
- [X] T036 [US5] Add the snowflake type-check leg to the `lint` target:
  `cargo clippy -p rdlt-connector-snowflake --all-targets --features failpoints
  -- -D warnings`, with a comment stating that its `crash_sweep.rs` is
  `#![cfg(feature = "failpoints")]` and was compiled by no gate command — which is
  how it broke against deleted APIs during an earlier feature while the gate
  reported green. Do NOT add the feature to the workspace-wide clippy invocation:
  that would change what compiles in seven other crates (research R6).
- [X] T037 [US5] Codify the coverage exclusion in the `coverage` target:
  `-E 'not (package(rdlt-connector-snowflake) and binary(crash_sweep))'`, with
  the reason and the measured 101.5-minute cost at the site. This is the
  exclusion the recorded 87.22% figure was actually measured with, existing until
  now only as prose in a close-out (research R9).
- [X] T038 [US5] Pin the runner group membership in
  `crates/rdlt-connector-iceberg/tests/config_schema.rs`: assert the set of test
  binaries in the crate, partitioned into "inside the live group" (the 10 using
  the shared fixture) and "outside it" (`config_schema` alone). Keep
  `.config/nextest.toml`'s spelling as-is — research R7 rejected re-spelling it
  positively, because a positive list of ten fails the OTHER way when an eleventh
  live binary is added and not listed. Both spellings fail silently; the defect is
  that membership is implicit, so a test makes it explicit.
- [X] T039 [US5] Execute the FR-014 audit and record it in
  `specs/024-gate-integrity/evidence/gate-audit.md` with a disposition for EVERY
  check examined, including those found SOUND — a list of only the defects cannot
  show the search was exhaustive. Scope per research R10: every `cargo nextest`
  invocation, every `.config/nextest.toml` override, every environment-conditional
  step (`RDLT_DEEP`, `RDLT_HEAVY`, `PROPTEST_CASES`, the probes), the
  already-clean exit-swallowing class (pre-audited: zero occurrences), and the
  `docs`, `bench` and `coverage` targets, which were not among the original eight
  and remain unexamined.
- [X] T040 [US5] **Detection demonstration (FR-015)**: introduce a deliberate
  public-surface break in `rdlt-connector`, observe `make semver` catch it,
  revert; deliberately break `snowflake/tests/crash_sweep.rs`, observe the new
  lint leg catch it, revert; add an unlisted iceberg test binary, observe the
  membership pin fail, revert. Record in
  `specs/024-gate-integrity/evidence/demo-us5.md`.
- [X] T041 [US5] Reproduce each recorded figure with its now-named command and
  confirm agreement — the coverage figure against the recorded 87.22% in
  particular (SC-006). Record any disagreement with its explanation rather than
  adjusting the figure.
- [X] T042 [US5] US5 gate: full gate green; close-out row.

---

## Phase 8: Polish & Cross-Cutting Concerns

- [X] T043 [P] Update `crates/rdlt-testkit/README.md` and the root `README.md`
  where either describes how the gate is run, so the documented procedure matches
  the one that now exists.
- [X] T044 [P] Correct the two source comments that cite the property suite by a
  name the selector could not reach — `crates/rdlt-engine/src/shred/tape.rs:8`
  and `crates/rdlt-engine/tests/shred_identity_pin.rs:9` — so a reader is not
  sent looking for `shred_property.rs` under a filter that never matched it.
- [X] T045 Coverage at or above the 80% floor, measured with the now-codified
  exclusion, recorded with the before/after pair.
- [X] T046 Record the gate's total added wall-clock cost in
  `specs/024-gate-integrity/evidence/gate-cost.md` — before/after from T001 —
  so the price of the added integrity is a known quantity (SC-010).
- [X] T047 Close-out matrix in `specs/024-gate-integrity/close-out.md`: GI1–GI8
  all terminal; story matrix complete; every one of the counted items carrying a
  disposition (9 selectors, 10 registries, 1 orphaned target, 1 uncompiled file,
  1 group, 2 probe families, 2 recorded practices); every detection demonstration
  cited with its observed output; every unperformed verification named with its
  reason.
- [X] T048 SC-005 secret sweep at the final commit: account identifier, login
  name, and key material absent from every tracked file, checked by shape as well
  as by value (a credential file committed from another machine passes every
  value check).
- [X] T049 Final gate: `env -u RUSTUP_TOOLCHAIN make check` TWICE clean on a
  quiet machine, with test and skip counts matching the committed baseline on
  both runs; both results recorded in close-out.

---

## Contract traceability (GI1–GI8 → tasks)

Every clause maps to the tasks that satisfy it and the task that PROVES it. A
clause with no proving task would be satisfied by assertion, which GI8 forbids.

| clause | satisfied by | proven by |
|---|---|---|
| **GI1** empty selection fails | T004, T005, T006, T007 | T008 |
| **GI2** every suite reachable or exempt by name | T010, T011, T012 | T013 |
| **GI3** registry verified against sources, never itself | T016–T023 | T024 (ten demonstrations) |
| **GI4** a file the gate does not compile does not exist | T036 | T040 |
| **GI5** a skip is distinguishable from a pass | T026–T031 | T032 |
| **GI6** group constraint asserts its own membership | T038 | T040 |
| **GI7** recorded practice is executable | T034, T035, T037 | T041 |
| **GI8** strictly harder, and each fix proves detection | T039 (audit) | T008, T013, T024, T032, T040 + T047 |

Two clauses are proven by the same task by design: T040 demonstrates GI4 and GI6
together because both are US5-phase single-command checks, and separating them
would mean two gate runs for one increment.

## Dependencies & Execution Order

### Phase Dependencies

```text
Phase 1 (Setup)  ──▶ must precede everything: T001's "before" measurement
                     cannot be reconstructed once the gate changes
      │
      ▼
Phase 3 (US1)    ──▶ BLOCKS US2–US5. Until an empty selection fails, any
                     later fix can silently regress, and every later
                     demonstration would be unfalsifiable
      │
      ├──▶ Phase 4 (US2)  ─┐
      ├──▶ Phase 5 (US3)  ─┤ independent of each other
      │                     │
      ▼                     ▼
Phase 6 (US4)    ──▶ needs US1–US3 landed: its baseline must record the
                     FIXED gate, not the broken one
      │
      ▼
Phase 7 (US5)    ──▶ last: T039's audit must examine the gate as this
                     feature leaves it
      │
      ▼
Phase 8 (Polish)
```

### User Story Dependencies

- **US1** — no dependencies. Blocks all others.
- **US2** — needs US1. Independent of US3.
- **US3** — needs US1. Independent of US2.
- **US4** — needs US1, US2 and US3, because its committed baseline must describe
  the gate in its final shape.
- **US5** — needs everything, because the audit's subject is the finished gate.

### Within Each User Story

Implementation → detection demonstration → gate. The demonstration is not
optional polish: FR-015 makes it the story's actual evidence, and a story that
skipped it would have proven only that the gate still passes.

### Parallel Opportunities

- **T002, T003** (Phase 1) — different evidence files, no shared state.
- **T017–T022** (US3) — six crates, one registry-assertion task each, no shared
  file. The largest genuine parallel block in the feature. They all depend on
  T016's shared helper, so T016 lands first.
- **T043, T044** (Polish) — different files.
- **US2 and US3** can proceed on separate branches once US1 has merged; they touch
  different files (`Makefile` + evidence vs testkit + per-crate tests). The one
  overlap to watch: both may want to re-measure counts, and T029 in US4 is the
  task that owns the committed baseline.

**Not parallelizable, and worth stating**: nothing in US1 is parallel with
anything, because every task in it edits `Makefile`. Line numbers shift as tasks
land, which is why the Path Conventions note says to re-measure.

---

## Implementation Strategy

**MVP is US1 alone.** Nine deletions and one selector fix, and the gate can no
longer report success on an empty selection. That single increment also revives
the 4,096-case property run, which has been reporting green while executing
nothing — so the MVP delivers a working suite the project believed it already had.

**Incremental delivery after that** follows the dependency graph: US2 and US3 in
either order (or in parallel on separate branches), then US4 once the gate's shape
is final, then US5's audit last.

**If the feature had to stop early**, the value ordering is: US1 (the mechanism
by which everything else can silently evaporate) → US3 (the exactly-once matrix
cannot quietly shrink) → US2 (orphaned suites) → US4 → US5. US1 alone is worth
shipping; US1+US3 covers the two defects with real correctness consequences.
