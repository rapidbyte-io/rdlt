# Feature Specification: Hardening & Performance

**Feature Branch**: `003-hardening-performance`

**Created**: 2026-07-20

**Status**: Draft

**Input**: User description: "Hardening & performance (feature 003, fast-follow to 001/002): deepen the correctness net around the engine's hottest code, then make that code faster, and prove it with the design doc's remaining benchmark cells. Testing: mutation-testing pass, deterministic crash-point sweep harness, fuzz targets, end-to-end shredder property test. Performance: streaming no-Value shred path, memchr slab splitting, deliberate row-id hash decision, RSS target closure, thin-LTO. Benchmarks: REST→Postgres ≥5×, shred-only ≥20×, cold start ≤1/20th, CI perf-regression gate. Constraint: correctness changes land before the shred rewrite touches the hot path; every optimization must show its before/after."

## Clarifications

### Session 2026-07-20

- Q: May the row-id hash benchmark switch the algorithm, or measure only? → A: Measure AND switch if it wins by a meaningful margin (option A).
- Q: What margin qualifies as "meaningful" for the hash switch? → A: A genuine >30% improvement (raised from the planning default of >10%); below that, blake3 stays.
- Q: Additional deliverable? → A: A repository Makefile exposing the feature's suites and benches as canonical developer entry points.
- Q: Where does the feature's emphasis lie if scope pressure appears? → A: Correctness hardening (US1) and benchmarking rigor (US2) are the core of the slice.
- Q: Is the US3 streaming shred rewrite a hard deliverable or deferrable to a follow-up? → A: Hard deliverable — feature 003 does not merge without it (the US1→US2→US3 ordering is the schedule discipline, not a scope escape hatch).
- Q: Is the Makefile a canonical CI entry point or developer convenience? → A: Canonical — CI workflows invoke the same `make` targets contributors run; the Makefile is the single source of truth for every gate's exact command.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Trustworthy under any failure (Priority: P1)

An operator runs continuous incremental syncs on infrastructure that crashes, loses
power, restarts mid-commit, and feeds the engine malformed or adversarial input.
They need proof — not hope — that no committed row is ever lost, duplicated, or
corrupted under ANY interruption point, and that no input can crash or wedge the
engine. This story deepens the correctness net to the point where the test suite
itself finds the bugs a human review would: every write/publish/acknowledge
boundary is crash-tested mechanically, the test suite's blind spots are enumerated
and closed, and untrusted input surfaces (data files, persisted state, user
configuration) survive adversarial fuzzing.

**Why this priority**: Correctness is the project's #1 stated priority, and the
feature-002 review proved the existing 119-test suite still had a data-loss hole a
mechanical crash sweep would have caught. The hot-path rewrite in this same
feature (US3) must not begin until this net exists.

**Independent Test**: Run the crash-point sweep and mutation report on the
CURRENT engine (before any optimization lands). The sweep passes at every
interruption point across all bundled destinations; the mutation report shows the
agreed kill-rate threshold with every surviving mutant either tested or explicitly
waived with a reason.

**Acceptance Scenarios**:

1. **Given** a pipeline interrupted at ANY single write/publish/acknowledge
   boundary in the destination commit protocol or the engine's recovery log,
   **When** the pipeline restarts, **Then** every previously acknowledged row is
   still present exactly once and the run completes with correct totals — for
   every bundled destination.
2. **Given** the mutation-testing pass over the engine and the two seam crates,
   **When** it completes, **Then** at least the agreed percentage of generated
   mutants are killed by the suite, and each survivor is either covered by a new
   test or documented as a deliberate waiver.
3. **Given** adversarial byte streams fed to the data-file readers, the persisted
   state decoder, and the user-configuration parser for a sustained fuzzing
   session, **When** fuzzing ends, **Then** zero crashes, hangs, or memory
   errors were found (typed errors are the only acceptable failure mode).
4. **Given** randomly generated nested documents of arbitrary shape, **When**
   shredded, **Then** row conservation (every input row lands exactly once across
   the table family), lineage integrity (every child links to its real parent and
   root), and schema monotonicity (schemas only ever widen) hold for every case.

---

### User Story 2 - Complete, regression-proof performance evidence (Priority: P2)

A team evaluating the engine reads the benchmark table and finds every cell of the
original performance claim measured — API-to-warehouse, transform-stage-only, and
startup overhead — each against the pinned competitor baseline on the same
machine and dataset, with the methodology already established in features 001/002.
Once measured, the numbers cannot silently rot: a performance regression in the
hot paths fails a pull request the same way a contract break does.

**Why this priority**: The performance claim is the product's reason to exist, and
two of its five cells are still unmeasured promises. The regression gate must
exist BEFORE the hot-path rewrite (US3) so the rewrite's effect is provable and
its regressions are catchable.

**Independent Test**: The benchmark table shows all five cells with baseline and
engine columns filled; a deliberately slowed hot path submitted as a pull request
is rejected by the regression gate.

**Acceptance Scenarios**:

1. **Given** the mock API → relational-warehouse scenario at 100k+ records,
   **When** both the pinned baseline and the engine run it, **Then** the engine is
   at least 5× faster end-to-end.
2. **Given** the transform-stage-only comparison (shredding/normalizing alone, no
   I/O on either side), **When** both sides process the same nested dataset,
   **Then** the engine's stage is at least 20× faster.
3. **Given** a minimal one-row pipeline, **When** total startup-to-first-commit
   overhead is measured on both sides, **Then** the engine's overhead is at most
   1/20th of the baseline's.
4. **Given** a pull request that slows a measured hot path beyond the agreed
   tolerance, **When** continuous integration runs, **Then** the change is
   blocked with a report naming the regressed measurement.

---

### User Story 3 - Faster hot path, provably (Priority: P3)

A user ingesting large nested datasets gets materially higher throughput and lower
memory without any behavior change: the engine parses records straight into its
columnar output representation instead of building an intermediate tree per row,
splits input more cheaply, and its ingestion-side memory ceiling meets the
originally promised bound. Every optimization lands with a before/after
measurement on the established benches, and the identity-hashing algorithm — which
freezes forever at first release because persisted row identities depend on it —
is chosen deliberately from measured evidence rather than inherited by default.

**Why this priority**: Only safe AFTER US1's net exists (this rewrites the most
correctness-critical code) and only provable AFTER US2's regression gate exists.
Sequenced last by design — but a HARD deliverable (clarified 2026-07-20): the
feature does not merge without it. The ordering is schedule discipline, not an
escape hatch.

**Independent Test**: All US1 suites (crash sweep, property tests, full workspace
suite) pass unchanged on the rewritten path; the shred-stage bench shows the
improvement; the flagship end-to-end row improves or holds; RSS meets the ≤1/5th
target; the hash decision is recorded with its measurements.

**Acceptance Scenarios**:

1. **Given** the streaming shred path enabled, **When** the full test suite,
   crash sweep, and shredder property tests run, **Then** all pass with byte-identical
   outputs (same rows, same identities, same schemas) as the previous path.
2. **Given** the same 200k-record nested dataset as the existing benchmark rows,
   **When** the flagship end-to-end sync runs, **Then** wall time improves or
   holds, and peak memory is at most 1/5th of the pinned baseline's (closing the
   currently missed target).
3. **Given** the row-identity hash candidates measured on the shred bench,
   **When** the decision is made, **Then** the chosen algorithm, the measured gap,
   and the rationale are recorded in the design doc before any release tag.

---

### Edge Cases

- Crash DURING recovery (a second interruption while replaying the first) — the
  sweep must include recovery-path boundaries, not just first-run boundaries.
- A mutant that survives because the behavior it changes is genuinely
  unobservable (dead code, defensive redundancy): the waiver list must say so,
  and dead code found this way should be removed, not waived.
- Fuzzing finds an input that only misbehaves at scale (e.g. quadratic blowup):
  hangs and resource exhaustion count as findings, not just crashes.
- The property test generates a document shape the shredder maps to a name
  collision or capability lowering — invariants must hold through those paths too.
- The regression gate on shared CI runners: measurements must be
  machine-independent (deterministic counts, not wall time) or the gate will flap.
- The streaming parse path and the old path disagree on an obscure input (e.g.
  duplicate keys in one object, lone surrogates, 2^53-boundary numbers): the
  rewrite must define and test the tie-break identically to the old path.
- Baseline cold-start measurement must separate interpreter/runtime startup from
  engine work honestly — the comparison is engine overhead vs engine overhead.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The project MUST have a repeatable mutation-testing pass over the
  engine and both seam crates, with a recorded kill-rate threshold, and a tracked
  disposition (new test or written waiver) for every surviving mutant.
- **FR-002**: The project MUST have a deterministic crash-point sweep that
  interrupts a running pipeline at every distinct write/publish/acknowledge
  boundary of the recovery log and of each bundled destination's commit protocol
  — including boundaries reached during recovery itself — then restarts and
  verifies exactly-once visibility of all acknowledged data.
- **FR-003**: The crash-point sweep MUST run in continuous integration for at
  least the in-process destinations on every pull request.
- **FR-004**: The project MUST have sustained-fuzzing targets for every surface
  that consumes bytes the engine does not produce itself: data-file readers,
  persisted state/cursor decoding, and user configuration parsing. A fuzzing
  finding (crash, hang, or memory error) is release-blocking.
- **FR-005**: The project MUST have a generative property test driving arbitrary
  nested documents through the full shred path asserting row conservation,
  lineage integrity, and schema monotonicity.
- **FR-006**: The engine MUST provide a streaming shred path that converts raw
  input to columnar batches without materializing a per-row intermediate tree,
  producing byte-identical output (rows, identities, schemas) to the existing
  path, and it MUST NOT merge until the FR-001/FR-002/FR-005 suites exist and
  pass against it.
- **FR-007**: Input line-splitting MUST NOT re-validate or re-copy data the
  downstream parse already validates; ingestion buffers MUST be handed off
  without redundant copies.
- **FR-008**: The row-identity hashing algorithm MUST be benchmarked against at
  least one faster candidate on the shred bench, and — clarified 2026-07-20 —
  the engine SWITCHES to the winner when it beats the incumbent by a genuine
  >30% improvement on the flagship end-to-end row (not just the microbench;
  clarified 2026-07-20, raised from the >10% planning default). The decision and
  its measurements are recorded
  in the design doc before any release tag. Rationale: nothing is published yet,
  so this is the cheapest moment to switch a forever-frozen algorithm; row
  identities may change on dev machines only.
- **FR-009**: The flagship end-to-end scenario's peak memory MUST meet the design
  doc's ≤1/5th-of-baseline target, via destination-side memory configuration
  and/or batch tuning, measured on the established harness.
- **FR-010**: The three unmeasured design-doc benchmark cells (API→warehouse ≥5×,
  transform-stage-only ≥20×, cold start ≤1/20th) MUST be measured baseline-first
  on the established harness and recorded in the results table with the same
  honesty rules (no multiple without both columns, caveats stated).
- **FR-011**: Continuous integration MUST block pull requests that regress the
  measured hot-path benchmarks beyond a recorded tolerance, using
  machine-independent measurements, with the same blocking semantics as the
  existing contract gate.
- **FR-012**: Every optimization in this feature MUST land with a before/after
  measurement on the relevant bench in its change description; optimizations
  without a measurable win MUST NOT land.
- **FR-013**: The release build profile MUST be tuned (link-time optimization or
  equivalent) if and only if it shows a measured end-to-end improvement.
- **FR-014**: The repository MUST provide a Makefile with a SMALL set of
  intent-level verbs — `build`, `release`, `test`, `bench`, `lint`, `check`
  (everything a PR must pass) — where `test` and `bench` are parameterized by a
  `TARGET` variable selecting the suite (e.g. `make test` = fast suite,
  `TARGET=e2e make test`, `TARGET=fuzz make test`, `TARGET=mutants make test`;
  `make bench` = micro, `TARGET=e2e make bench`, `TARGET=iai make bench`).
  Tool specifics are recipe implementation details (clarified 2026-07-20).
  Single source of truth: CI invokes these `make` verbs rather than inline
  commands, so a contributor and CI run identical gates by construction.

### Key Entities

- **Crash-point sweep harness**: a test-only wrapper around the engine's
  filesystem/destination boundaries that enumerates interruption points, kills
  and restarts the pipeline at each, and asserts exactly-once outcomes.
- **Mutation report**: the recorded outcome of a mutation pass — kill rate,
  survivors, dispositions — checked into the feature's documentation.
- **Fuzz corpus**: seed inputs and regression cases for each fuzz target, kept in
  the repository so findings become permanent regression tests.
- **Benchmark results table**: the existing RESULTS.md, extended to all five
  design-doc cells with baseline-first methodology.
- **Perf-regression baseline**: recorded machine-independent measurements of the
  hot paths that CI compares against.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The crash-point sweep exercises 100% of enumerated interruption
  points across the recovery log and all bundled destinations, and passes.
- **SC-002**: The mutation pass reports its kill rate ≥ the agreed threshold
  (default 85% of viable mutants) with zero undispositioned survivors.
- **SC-003**: A cumulative 24 CPU-hours of fuzzing across all targets yields zero
  open crash/hang/memory findings at feature close.
- **SC-004**: All five design-doc benchmark cells are measured and recorded;
  the three new cells meet ≥5×, ≥20×, and ≤1/20th respectively — or the miss is
  documented with the same honesty rules as the existing RSS caveat.
- **SC-005**: The flagship end-to-end row's peak memory is ≤1/5th of the pinned
  baseline's on the same dataset (closing the feature-001 miss).
- **SC-006**: The streaming shred path shows ≥3× improvement on the
  shred-stage-only bench over the current path while producing identical output,
  and the end-to-end flagship row does not regress.
- **SC-007**: A deliberately introduced hot-path slowdown submitted as a pull
  request is blocked by the perf gate; a no-op change is not.

## Assumptions

- The pinned baseline (dlt 1.11.0 container) and the measurement machine remain
  the same as features 001/002 for comparability; RESULTS.md rows state their
  run date.
- "Bundled destinations" means the three shipped in-tree today (analytics file
  output, embedded analytical database, relational warehouse); the sweep's CI
  subset excludes only the destination requiring an external service container
  when unavailable.
- The mutation kill-rate threshold (85%) and perf-gate tolerance are defaults to
  be confirmed at planning; changing them later requires only a documented
  decision, not a spec change.
- The row-identity hash decision deadline is "before the first release tag", not
  "in this feature's merge" — but both the measurement AND (if the threshold is
  met) the switch are in scope for this feature.
- Fuzzing runs continuously in scheduled CI, not on every pull request; the
  24-CPU-hour budget is cumulative before feature close.

## Dependencies

- Features 001 (engine) and 002 (file & Arrow ingestion) merged — both are.
- The existing benchmark harness (`benches/`), pinned baseline container, and
  conformance/testkit infrastructure.
- Scheduled CI capacity for fuzzing and the mutation pass (both are too slow for
  per-PR runs; per-PR scope is the crash sweep subset and the perf gate).
