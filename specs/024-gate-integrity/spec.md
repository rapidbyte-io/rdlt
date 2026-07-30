# Feature Specification: Test-gate integrity

**Feature Branch**: `024-gate-integrity`

**Created**: 2026-07-30

**Status**: Draft

**Input**: User description: "TEST-GATE INTEGRITY — make the verification gate incapable of passing silently. Eight measured defects let `make check` report green while verifying less than it appears to, or nothing at all."

## Why this feature exists

Every feature this project has shipped — 001 through 023 — cites the local
verification gate as its evidence. A close-out that says "gate twice clean" is
making a claim about what was checked, and a reader who trusts that claim is
trusting the gate to have actually run what it appears to run.

Eight defects break that trust, and none is hypothetical: each was located in
the tree at commit `34ccd379`. The common shape is a check that **reports
success while verifying nothing** — a selector that matches no tests and is told
to pass anyway, a suite nothing invokes, a registry compared against itself, a
probe whose "unavailable" answer silently disarms whole suites.

One of these has already caused a real miss: during feature 023, a test file
failed to compile against APIs the feature had deleted, and the standard gate
never noticed, because no gate command builds that file at all. That is what
this feature exists to make impossible.

The feature changes **no product behavior**. It changes only what the gate can
get away with not checking.

## User Scenarios & Testing *(mandatory)*

The "user" here is a maintainer or an automated agent relying on the gate's
verdict to decide whether a change is safe. Each story is independently
valuable: any one of them, shipped alone, closes a specific way the gate can
lie.

### User Story 1 - An empty test selection fails instead of passing (Priority: P1)

A maintainer renames a test binary, deletes a test, or mistypes a filter. Today
the gate selects zero tests, is instructed to treat that as success, and reports
green. The maintainer believes a suite ran that did not exist.

After this story, a selector that matches nothing **fails**, naming the selector
that came up empty.

**Why this priority**: This is the mechanism by which every other
selector-based check in the gate can silently evaporate. Nine separate checks
currently depend on selectors that are permitted to match nothing — including
crash sweeps, the property-test suite, and the end-to-end suites. Fixing this
one defect makes eight other checks honest, and leaving it unfixed would let any
later fix silently regress.

**Independent Test**: Rename any selected test binary, run the gate, and observe
it fail naming the now-empty selector. Restore the name and observe it pass.

**Acceptance Scenarios**:

1. **Given** a gate check that selects tests by binary name, **When** the
   selector matches no tests, **Then** the check fails and names the selector.
2. **Given** every selector-based check in the gate, **When** the gate runs
   normally, **Then** each check reports a test count and the run passes.
3. **Given** a check whose selector is genuinely optional (its target may
   legitimately be absent in some environment), **When** the gate runs without
   that target, **Then** the check is skipped with an announcement — and the
   reason it is optional is recorded where the check is defined.

### User Story 2 - Suites that exist are actually invoked (Priority: P1)

A maintainer assumes every test suite in the repository is reached by the gate.
Two end-to-end suites are reached by nothing: a target exists to run them, and
no gate path invokes that target.

After this story, every test suite in the repository is either invoked by the
gate or carries a recorded reason why it is not.

**Why this priority**: An uninvoked suite is worse than a deleted one — it looks
like coverage in the file listing and provides none. Same priority as US1
because together they establish the gate's *extent*: US1 makes each check
honest, US2 makes the set of checks complete.

**Independent Test**: Enumerate every test suite in the repository; for each,
show either the gate path that invokes it or the recorded exemption with its
reason.

**Acceptance Scenarios**:

1. **Given** the full set of test suites in the repository, **When** the gate
   runs, **Then** every suite is either executed or has a recorded exemption.
2. **Given** a suite deliberately excluded because it is prohibitively slow,
   **When** a maintainer reads the exclusion, **Then** they find the measured
   cost and the separate path that does run it.

### User Story 3 - A dropped crash point is detected (Priority: P1)

Exactly-once delivery is the project's most load-bearing guarantee, and the
evidence for it is a set of crash sweeps that deliberately fail at instrumented
points and require recovery to converge. All but one of those sweeps verify their point
registry **against itself**: they check that every point in the list fired,
which stays true if a point is removed from both the code and the list. The
matrix quietly shrinks and every sweep still passes. Measured during planning,
the exposure is wider than first stated: **ten registries across six crates**,
not five.

One sweep does it correctly: it scans its own sources for instrumented points
and requires the found set to match the registry exactly, precisely because
deriving the list from the registry would be circular.

After this story, every crate that arms crash points detects a dropped point.

**Why this priority**: The guarantee this protects is the one the project calls
sacred, and the current check cannot distinguish "all points swept" from "fewer
points exist". Equal priority with US1/US2 because a gate that is complete and
non-vacuous can still be verifying a shrunken matrix.

**Independent Test**: For each registry, delete an instrumented point from the
source *and* from the registry, run that crate's verification, and observe it
fail. Restore both and observe it pass.

**Acceptance Scenarios**:

1. **Given** a crate that arms crash points, **When** an instrumented site is
   removed from the source and the registry together, **Then** that crate's
   sweep fails naming the divergence.
2. **Given** a crate that arms crash points, **When** a site is added to the
   source but not the registry, **Then** the sweep fails.
3. **Given** all crates that arm crash points, **When** the gate runs
   unmodified, **Then** every registry matches its sources exactly.

### User Story 4 - A disarmed environment probe is visible (Priority: P2)

Suites that need a container runtime or live credentials are designed to skip
rather than fail when those are absent — deliberately, so a contributor without
them can still run the gate. The cost is that a *wrongly* skipping suite is
indistinguishable from a passing one: a probe that reports "unavailable" when
the resource is present disarms every dependent suite across the workspace while
the gate reports green.

After this story, a maintainer can demand that gated suites actually run, and a
change in how many suites skip is visible as a difference rather than as green.

**Why this priority**: Lower than US1–US3 because it protects against a subtler
failure — the suites still exist and still pass when armed — but it is what
makes the other three trustworthy on a machine where resources are present.

**Independent Test**: With resources present, force a probe to report
unavailable; observe the gate report a skip-count difference against its
recorded baseline. Run in the demanding mode and observe outright failure.

**Acceptance Scenarios**:

1. **Given** an opt-in demanding mode, **When** a required resource is genuinely
   absent, **Then** the affected suite fails rather than skipping.
2. **Given** the default mode, **When** a resource is absent, **Then** the suite
   skips and announces the skip — unchanged from today.
3. **Given** a recorded per-suite count of tests run and tests skipped, **When**
   a suite silently stops running, **Then** the count differs from the record.

### User Story 5 - Recorded gate practice is executable (Priority: P3)

Two facts about how the gate is meant to be run exist only as prose in a
close-out document: one suite's public-surface comparison needs a baseline the
automated pipeline cannot currently provide meaningfully, and one coverage
measurement was taken with a documented exclusion that exists in no runnable
form. The next person to reproduce either gets a different result or a long
surprise.

After this story, a practice worth recording is worth encoding, and the recorded
figures are reproducible by running a named command.

**Why this priority**: It corrects a documentation-versus-reality gap rather
than a detection gap, so it ranks below the stories that stop the gate lying. It
is in scope because an unreproducible recorded figure is a claim nobody can
check.

**Independent Test**: Run the named commands and reproduce the recorded figures.

**Acceptance Scenarios**:

1. **Given** a documented gate practice, **When** a maintainer runs the named
   command, **Then** they reproduce the recorded result without reading prose.
2. **Given** a public-surface comparison, **When** it runs, **Then** its
   baseline is one whose result is interpretable, and the baseline is recorded
   as something a reader can re-derive.

### Edge Cases

- **A selector is legitimately empty in some environment.** Some suites need a
  container runtime or credentials absent on a contributor's machine. Such a
  selector may stay permissive, but the permission must be deliberate and
  recorded at the definition — not the current blanket default.
- **A crate arms no crash points at all.** The source-scanning check must pass
  trivially for such a crate rather than failing for having found nothing.
- **A crash point is armed inside a conditionally-compiled file.** The scan must
  see it when that configuration is active and must not report a spurious
  divergence when it is not.
- **A suite is prohibitively expensive.** One sweep costs over an hour and
  requires live credentials; it is correctly excluded from the routine gate.
  Exclusion is acceptable — silent exclusion is not.
- **The demanding mode is set on a machine genuinely lacking resources.** It
  must fail naming the missing resource, not with an obscure error.
- **Back-to-back gate runs on the development host.** A known host-level
  resource contention makes a second consecutive run fail on grounds unrelated
  to the change. Any new gate step must not worsen this, and the documented
  reclaim procedure must remain sufficient.
- **A test count changes for a legitimate reason.** Adding a test must not be
  reported as a defect; the recorded baseline must be updatable by the change
  that justifies it, with the update visible in review.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Every selector-based check in the gate MUST fail when its selector
  matches no tests, UNLESS it carries a recorded reason for being permitted to
  match nothing.
- **FR-002**: Each of the nine currently-permissive selectors MUST end this
  feature with a stated disposition: made strict, or recorded as deliberately
  permissive with its reason at the definition site.
- **FR-003**: Every test suite in the repository MUST be either invoked by the
  routine gate or covered by a recorded exemption naming its reason and, where
  the reason is cost, its measured cost.
- **FR-004**: Every crate that arms crash points MUST verify its point registry
  against the instrumented sites in its own sources, such that removing a point
  from both source and registry fails.
- **FR-005**: The crash-point verification MUST NOT derive the expected set from
  the registry it is checking.
- **FR-006**: A test file compiled only under a non-default configuration MUST
  be type-checked by the routine gate, so a change breaking it is caught when it
  is made rather than at the next manual run.
- **FR-007**: The system MUST provide an opt-in mode in which environment probes
  for gated resources fail instead of reporting the resource unavailable.
- **FR-008**: Default probe behavior MUST remain unchanged: absent resources
  cause an announced skip, never a failure.
- **FR-009**: The system MUST record, per suite, the number of tests run and the
  number skipped, such that a suite that silently stops running is detectable as
  a difference from the record.
- **FR-010**: Resource-group constraints on test execution MUST be expressed by
  naming what they include rather than by excluding what they do not, so adding
  or renaming a suite cannot silently change which constraint applies.
- **FR-011**: Public-surface comparison MUST be runnable as a named local
  command against a baseline whose result is interpretable, and that baseline
  MUST be recorded such that a reader can re-derive it.
- **FR-012**: Any gate practice recorded in a project document MUST exist in
  executable form, such that the recorded result is reproducible by running a
  named command.
- **FR-013**: The gate MUST NOT become easier to pass in any respect as a result
  of this feature; every change MUST make vacuous success strictly harder.
- **FR-014**: An audit MUST establish that no further check in the gate
  configuration can pass while verifying nothing, and MUST record each check's
  disposition — including checks found already sound.
- **FR-015**: For each defect fixed, the feature MUST demonstrate detection: the
  gate observed FAILING on a deliberately introduced regression that previously
  passed silently, and PASSING once reverted. Demonstrations MUST be recorded
  with their observed output.
- **FR-016**: No product behavior, persisted data format, generated SQL, or
  user-facing configuration or command-line vocabulary may change.

### Key Entities

- **Gate check**: One verification step with a selector, an invocation path, and
  a disposition (strict, or deliberately permissive with a recorded reason).
- **Crash-point registry**: A crate's declared list of instrumented failure
  sites, which must agree with the sites present in that crate's sources.
- **Environment probe**: A decision about whether a gated resource is available,
  with two modes — announce-and-skip (default) and demand-and-fail (opt-in).
- **Suite count record**: The per-suite tests-run and tests-skipped figures the
  gate is expected to produce, against which drift is detected.
- **Detection demonstration**: A recorded pairing of an introduced regression,
  the gate's failure on it, and the gate's return to green on reversion.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Zero checks in the gate can report success while running no tests,
  except those carrying a recorded reason; the count of unexplained permissive
  selectors falls from nine to zero.
- **SC-002**: Every test suite in the repository is reachable from the routine
  gate or carries a recorded exemption — zero suites unreachable and unexplained.
- **SC-003**: For every crash-point registry, removing one point from both its
  source and the registry causes verification to fail; demonstrated per
  registry. Ten registries across six crates currently cannot detect it; one
  (the engine's) already can and is the model.
- **SC-004**: A file compiled only under a non-default configuration is
  type-checked by the routine gate, demonstrated by a deliberate break being
  caught.
- **SC-005**: A wrongly-disarmed environment probe is detectable, demonstrated
  both by the count difference and by the opt-in demanding mode.
- **SC-006**: Every gate practice recorded in project documents is reproducible
  by running a named command, verified by reproducing each recorded figure.
- **SC-007**: Each fixed defect has a recorded detection demonstration showing
  the gate failing before and passing after reversion; the count of fixed
  defects without a demonstration is zero.
- **SC-008**: The routine gate passes twice consecutively on a quiet machine
  after the change, with test and skip counts matching the recorded baseline on
  both runs.
- **SC-009**: No product behavior changed: all pre-existing test expectations,
  generated SQL comparisons, and persisted-format checks pass unmodified.
- **SC-010**: The gate's added time cost is recorded as a measured figure, so the
  price of the added integrity is a known quantity rather than a surprise.

## Assumptions

- The audience is maintainers and automated agents reading the gate's verdict;
  there is no end-user-visible surface in this feature.
- The routine gate is the local one. Continuous integration remains out of scope
  for repair — it is blocked by organizational billing, and every CI-only
  verification stays recorded as unperformed rather than claimed green.
- The hour-plus sweep requiring live credentials stays OUT of the routine gate.
  Type-checking its file is in scope; running it is not.
- Suites needing a container runtime or credentials keep skip-not-fail as their
  default. The demanding mode is opt-in, for maintainers and for gate runs on
  machines where resources are known present.
- Adding a test legitimately changes a recorded count; the record is updatable
  by the change that justifies it, with the update visible in review.
- The version-window decision recorded as closed by the owner on 2026-07-30
  stays closed; nothing here proposes a version bump.
- The house-style refactor described in the repository's root refactoring
  document is a separate, later feature. This feature deliberately precedes it
  because that refactor's every increment depends on the gate detecting a
  regression — but each defect here stands on its own merits.
- The development host has a known contention issue on consecutive gate runs,
  mitigated by a documented reclaim procedure. This feature must not worsen it.
