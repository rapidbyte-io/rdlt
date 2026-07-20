# Feature Specification: Close or Re-baseline the Two Benchmark Misses

**Feature Branch**: `004-close-perf-misses`

**Created**: 2026-07-20

**Status**: Draft

**Input**: User description: "Close or re-baseline the two benchmark misses from feature 003's final re-measure against dlt 1.29.0: shred-only at 12.0× vs the ≥20× bar (profile for remaining headroom), and cold start at 1/14.2 vs the ≤1/20 bar (regressed only because the baseline tool improved its own startup — convert to an absolute bar, keep the ratio as a reported scoreboard number). Each miss must end in exactly one of two states: (a) bar met with maintainable, high-quality code and the regression gate re-baselined, or (b) an evidence-backed bar adjustment recorded in the benchmark version policy with the profiling data committed alongside. A documented negative result is a valid successful outcome, matching the 003 precedents."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Resolve the shred-stage miss (Priority: P1)

A maintainer profiles the shred (nested-document flattening) stage on current
code to find whatever headroom remains after the 003 rebuild, applies any
optimization that survives an evidence-based accept/reject decision, and drives
the cell to a resolved state: either the ≥20× bar is met, or the bar is
adjusted with the measured ceiling recorded as justification. The 003 work
already took the large structural wins and rejected the easy lever with A/B
evidence, so anything found here comes from fresh measurement, not
configuration changes.

**Why this priority**: This is the only cell where the gap is a genuine open
engineering question rather than a measurement-design flaw. It is also the
largest unresolved claim in the benchmark matrix: the shred stage is the
engine's differentiating fast path, and an unresolved "12.0× vs ≥20×" row
undermines the credibility of the whole matrix.

**Independent Test**: Can be fully tested by running the existing shred-stage
benchmark comparison on its own: the cell either reports ≥20× against the
pinned baseline, or the benchmark version policy contains a recorded
adjustment for this cell whose committed profiling evidence shows where the
remaining time goes and why each remaining candidate was rejected.

**Acceptance Scenarios**:

1. **Given** the current code at the 003 close-out state, **When** the
   maintainer profiles the shred stage, **Then** a committed evidence artifact
   attributes where the stage's cost goes, at sufficient granularity to
   name each remaining optimization candidate as viable or exhausted.
2. **Given** a candidate optimization, **When** it is evaluated, **Then** it is
   accepted only with a like-for-like A/B measurement showing a net win on the
   shred cell, no other gated criterion regressing beyond the gate tolerance,
   and code quality consistent with the engine's existing standards.
3. **Given** all candidates have been evaluated, **When** the cell still falls
   short of ≥20×, **Then** the bar is adjusted to the measured achievable
   value, the adjustment is recorded in the benchmark version policy citing
   the committed evidence, and the rejected candidates are listed with their
   measured results.
4. **Given** the resolved cell (either state), **When** the full benchmark
   matrix is re-measured, **Then** the shred row reports its final state with
   no unexplained discrepancy against the resolution record.

---

### User Story 2 - Redesign the cold-start criterion as an absolute bar (Priority: P2)

A maintainer converts the cold-start success criterion from a ratio against
the baseline tool's startup time to an absolute time bound on reference
hardware, so the criterion can never again flip from met to missed without any
change to this project's code. The ratio against the baseline tool remains in
the matrix as a reported scoreboard number, clearly marked as not gated. As
part of choosing the absolute bound, the maintainer profiles what the
engine's own startup actually spends time on and takes any cheap wins found.

**Why this priority**: The current miss is an artifact of measurement design,
not a performance problem — the engine got zero slower. Fixing the criterion's
design permanently removes a false-alarm class from the matrix. It is P2 only
because no code is currently at fault; the fix is smaller and independent of
User Story 1.

**Independent Test**: Can be fully tested by inspecting the success-criteria
record and re-running the cold-start measurement: the gated criterion is an
absolute bound with a defined measurement protocol, the measured value passes
it, and re-pinning a newer baseline-tool version changes only the scoreboard
ratio, never the gated verdict.

**Acceptance Scenarios**:

1. **Given** the engine's current startup, **When** the maintainer profiles a
   cold invocation end-to-end, **Then** a committed evidence artifact breaks
   the startup down into its major contributors and identifies which are
   reducible and which are floor costs.
2. **Given** the startup profile, **When** the absolute bar is chosen, **Then**
   its value is justified from the measured composition (floor plus explicit
   headroom) and its measurement protocol — hardware reference, run count,
   aggregation, cache state — is recorded alongside it.
3. **Given** the redesigned criterion, **When** the baseline tool releases a
   version with faster startup and the pin is updated, **Then** the scoreboard
   ratio changes but the gated cold-start verdict does not.
4. **Given** a cheap startup win identified by the profile, **When** it is
   applied, **Then** it passes the same accept/reject rule as User Story 1
   scenario 2 (net win, no gated regression, quality consistent).

---

### User Story 3 - Coherent final record (Priority: P3)

A maintainer (or future contributor) reads the benchmark matrix, the version
policy, and the resolution records after this feature closes and finds one
consistent story: every gated bar is either met or carries a recorded,
evidence-backed adjustment; scoreboard numbers are visibly distinct from gated
bars; and the regression gate's baselines reflect the final accepted code.

**Why this priority**: The 003 close-out demonstrated that honest documented
misses are only as valuable as their traceability. This story is the audit
trail that makes outcome (b) — the negative result — a first-class deliverable
rather than a shrug.

**Independent Test**: Can be fully tested by a documentation review pass: for
each of the two cells, follow the matrix row to its resolution record and
evidence artifact without finding contradictions, stale numbers, or an
ambiguous gated/scoreboard status.

**Acceptance Scenarios**:

1. **Given** the feature is complete, **When** the full matrix is re-measured
   and recorded, **Then** every row states whether it is gated or scoreboard,
   and the two formerly-missed cells reference their resolution records.
2. **Given** an accepted optimization changed performance characteristics,
   **When** the regression gate baselines are re-recorded, **Then** the
   re-record is tied to the accepting decision and the gate remains armed at
   its existing tolerance.

---

### Edge Cases

- A shred optimization clears the ≥20× bar but regresses another gated cell
  beyond the gate tolerance: it is rejected as-is; partial acceptance requires
  reworking it until no gated criterion regresses past tolerance.
- The measured ceiling lands between the current value and the bar (e.g. the
  stage reaches 17× with all viable candidates applied): both halves of the
  decision tree apply — ship the accepted improvements *and* adjust the bar to
  the measured value with evidence.
- The baseline tool releases a new version while this feature is in flight:
  the existing version-policy pin governs; all comparisons in this feature
  stay against the pinned version, and any re-pin is a separate recorded
  policy event.
- Cold-start measurements vary run-to-run on the reference hardware: the
  measurement protocol must define run count, aggregation (e.g. median), and
  cache state so the gated verdict is reproducible; a bar that flaps under its
  own protocol is treated as mis-set and revisited.
- Profiling tooling itself perturbs the measurement (observer effect): accept/
  reject decisions use the same measurement style on both sides of every A/B
  comparison, never a profiled run against an unprofiled one.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The shred stage MUST be re-profiled on current code, producing a
  committed evidence artifact that attributes the stage's cost with enough
  granularity to classify each remaining optimization candidate as viable or
  exhausted.
- **FR-002**: Every candidate optimization (shred or startup) MUST be accepted
  or rejected via a like-for-like A/B measurement, and MUST be rejected if it
  regresses any gated criterion beyond the regression gate's tolerance or
  falls below the project's existing code-quality standards.
- **FR-003**: The shred-stage cell MUST end in exactly one of two states:
  (a) the ≥20× bar met and the regression gate re-baselined on the accepted
  code, or (b) the bar adjusted to the measured achievable value with the
  justifying evidence committed and the adjustment recorded in the benchmark
  version policy.
- **FR-004**: The cold-start success criterion MUST be converted from a
  ratio-versus-baseline-tool bar to an absolute time bound whose value is
  justified by a committed startup-composition profile and whose measurement
  protocol (reference hardware, run count, aggregation, cache state) is
  recorded with it.
- **FR-005**: The ratio of engine cold start to baseline-tool cold start MUST
  remain in the benchmark matrix as a reported scoreboard number, explicitly
  marked as not gated.
- **FR-006**: Every bar adjustment made under this feature MUST be recorded in
  the benchmark version policy with a reference to the committed evidence that
  justifies it, following the format of the policy's existing records.
- **FR-007**: The performance regression gate MUST remain armed at its
  existing tolerance throughout the feature; its baselines MUST be
  re-recorded only as part of accepting a specific optimization, never to
  absorb an unexplained drift.
- **FR-008**: After both cells are resolved, the full benchmark matrix MUST be
  re-measured against the pinned baseline version and recorded, with every
  row's gated-versus-scoreboard status explicit.

### Key Entities

- **Benchmark cell**: One measured comparison in the matrix (e.g. shred-only,
  cold start), carrying a current value, a bar, and a met/missed/resolved
  state.
- **Gated bar vs. scoreboard number**: A gated bar participates in pass/fail
  decisions and can block; a scoreboard number is reported for context and
  cannot. Every cell must be exactly one of the two.
- **Evidence artifact**: A committed profiling or A/B measurement record that
  justifies an accept, reject, or bar-adjustment decision; referenced by the
  decision that relies on it.
- **Resolution record**: The record of which decision-tree outcome a cell
  reached — (a) closed or (b) re-baselined — linking the cell to its evidence
  artifacts.
- **Benchmark version policy record**: An entry in the existing version policy
  documenting a pin change or bar adjustment and why it happened.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Both formerly-missed cells reach a resolved state — 100% of the
  decision tree's leaves are (a) or (b), with zero cells left in an
  unresolved "missed" state after the final matrix re-measure.
- **SC-002**: The shred-stage cell either reports ≥20× against the pinned
  baseline, or its adjusted bar is accompanied by committed evidence in which
  every remaining named candidate carries a measured rejection result.
- **SC-003**: The cold-start gated verdict is invariant to the baseline tool:
  re-measuring against any baseline-tool version changes only scoreboard
  numbers, never a gated pass/fail.
- **SC-004**: No gated benchmark criterion other than the two under
  resolution regresses beyond the regression gate's existing tolerance at any
  accepted point in the feature.
- **SC-005**: The full existing verification suite (tests, crash sweep,
  regression gate, doc checks) passes at feature close.
- **SC-006**: A reviewer can trace each of the two cells from matrix row to
  resolution record to evidence artifact with no contradictions — verified by
  the User Story 3 documentation review pass.

## Assumptions

- The baseline-tool pin stays at the version recorded at 003 close-out
  (dlt 1.29.0) for the duration of this feature; any re-pin is out of scope
  and would be its own version-policy event.
- "Reference hardware" for the absolute cold-start bar is the same machine
  and environment used for the 003 final measurements; portability of the
  absolute bar to other hardware is out of scope.
- "High-quality, maintainable code" means the project's existing review and
  lint standards — matching the precedents where changes were rejected for
  cost or complexity despite being feasible (the 003 hash-swap and
  link-time-optimization decisions).
- The build/measurement environment must be restored to working order before
  profiling begins (it was degraded at session start); restoring it is a
  prerequisite, not a deliverable, of this feature.
- No new benchmark cells are added by this feature; it resolves the two
  existing misses and re-records the existing matrix.
