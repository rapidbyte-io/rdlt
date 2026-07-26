# Feature Specification: Benchmark Refinement — Three-Way E2E Matrix

**Feature Branch**: `018-bench-refinement`

**Created**: 2026-07-24

**Status**: Implemented — see [close-out.md](close-out.md) for the disposition of every requirement.

**Input**: User description: "Benchmark refinement per BENCH_REFINMENT.md (v3): rebuild the benchmark as an e2e-only, three-way-comparable matrix (rdlt/dlt/Airbyte, same conditions) of five cells; delete the gated/scoreboard taxonomy, 19 legacy cells, suites, three run modes, and six fixtures outright (git history is the archive); move cold-start to the instruments track; Airbyte driver competitor kind; presentation rebuild (generated matrix + GOVERNANCE.md split); bars start empty and are set measurement-first; phased P0-P4"

## Source Document

The authoritative design inventory is `BENCH_REFINMENT.md` at the repo root
(owner-authored, v3.1): the governing principle (e2e-only, three-way
comparable, no importance taxonomy), the three survival tests, the "same
conditions" definition, the five-cell matrix, the deletion list, the Airbyte
machinery, the presentation shape, and the P0–P4 phasing. This spec defines
the outcomes and how completion is judged; the document holds item-level
detail and its explicitly recorded decisions (v3.1: **deleted, not
archived** — git history is the archive) are adopted as requirements.

**Post-017 reconciliation** (the document predates feature 017's merge;
where its snapshot facts drifted, the RULE wins over the number):

- Cell/fixture counts have drifted (26 cells and 13 fixture entries in the
  tree today vs the document's 24/9). Survivorship is decided by the three
  tests of §2, not by the document's enumeration; the deletion list is
  re-derived at plan time from the live tree.
- The document's premise for deleting the parity fixture ("it exists to pin
  the harness's library-mode spec parser") is stale: since feature 017 the
  fixture pins the ONE shared pipeline-spec model in the facade, consumed by
  both the CLI and the harness's library mode. Deleting library mode removes
  only the harness-side consumer; the fixture and the CLI-side parse+build
  pins remain (they guard the shared parser the CLI still uses).
- The constitution (v1.0.0, Principle VIII) embeds the scoreboard vocabulary
  this feature deletes. The mechanism Principle VIII protects (no enforced
  bar without recorded evidence and a governance entry; bars enforced by the
  bench gate) is unchanged — but the wording must be amended through the
  constitution's own amendment procedure before or with the vocabulary
  deletion, not silently contradicted.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - The benchmark says one thing, simply (Priority: P1)

A maintainer or prospective user opening the benchmark finds a single small
matrix of end-to-end pipeline comparisons and nothing else: no importance
taxonomy (gated/scoreboard), no suites, no stage-level or product-internal
micro-cells, no half-dead archive. Everything that fails the three survival
tests (e2e pipeline; three-way comparable; claim-worthy) is deleted outright
in one migration, with git history as the archive and the final recorded
values cited from the migration record. The cold-start guard moves to the
instruments track so the embeddability claim stays protected outside the
comparison matrix. The results page is rebuilt: one generated matrix table,
curated caveats, generated trends, and a milestones section where retired
claims live on as quotable history with their evidence commit cited.

**Why this priority**: this is the phase with no new measurements and the
largest credibility gain — the benchmark's story becomes exactly the
owner-stated principle, and every later phase builds on the simplified
harness. Independently mergeable with the existing gate green.

**Independent Test**: after the phase, the cells directory contains only the
five matrix cells (plus harness self-test fixtures, which are test
machinery, not matrix rows); no taxonomy vocabulary survives anywhere in the
harness, its artifacts, or its documentation; the report regenerates from
the new shape; the full workspace gate is green.

**Acceptance Scenarios**:

1. **Given** the migrated tree, **When** the retired-vocabulary sweep runs
   (the class taxonomy, suite grouping, and retired run modes), **Then** it
   finds zero occurrences in harness code, cell definitions, artifacts, and
   generated documentation.
2. **Given** the migration commit, **When** a reader wants a retired
   number, **Then** the migration record names the final recorded value and
   the pre-migration commit where the full cell, fixture, and artifact are
   checkout-able.
3. **Given** the rebuilt results page, **When** it is regenerated, **Then**
   every number is either generated from artifacts or frozen with a
   citation, and the page carries the matrix, caveats, trends, and
   milestones sections.
4. **Given** the instruments track, **When** the embeddability check runs,
   **Then** the cold-start absolute bound (≤ 40 ms) is still enforced there.
5. **Given** the retired bars, **When** the governance log is read,
   **Then** one policy entry records the matrix rebuild and where the
   dlt-era claims live on.

---

### User Story 2 - Feasibility is proven before machinery is built (Priority: P2)

Before any Airbyte harness code exists, the five recorded probes answer the
open feasibility questions on THIS machine: the container-runtime question
(the platform stack expects a different runtime than this podman-based
host — the number-one risk), cluster-to-host networking for the shared
source/destination services, the exact job-API field names, the idle
cluster's load versus the quiet guard, and whether reset-plus-teardown
provably returns a destination to its initial state. The outcome is a
recorded spike document with a go/no-go per probe.

**Why this priority**: the document's own risk assessment — if the runtime
probe fails, the three-way phase is blocked and everything downstream
changes shape; probing first is this repo's established pattern.

**Independent Test**: the spike document exists with all five probes
answered by evidence gathered on this machine, each with a recorded
decision; no harness code was written in this phase.

**Acceptance Scenarios**:

1. **Given** the runtime probe's outcome, **When** it is recorded, **Then**
   it names the chosen path (rootless-podman provider, or installing the
   expected runtime, or no-go) with the evidence.
2. **Given** a no-go on any probe, **When** the phase closes, **Then** the
   spike records what the three-way matrix does instead (ship two-way with
   the third column visibly absent-with-reason) rather than silently
   shrinking scope.

---

### User Story 3 - The new matrix measures rdlt against dlt (Priority: P3)

The five cells exist as real pipelines with the consolidated fixtures (one
relational-database container carrying the seeded source table and
per-product destination databases; one object-store container with a raw
bucket for the nested-JSON dataset and a lake bucket for columnar output),
and the first recorded session lands: rdlt and dlt arms for all five cells
under the same-conditions protocol, with dlt in its fastest documented
configuration (the Rust-reader backend for database extraction — retiring
the old "pure-dlt" scoping) and the older backend kept as recorded context.
No bars yet: numbers are recorded, not enforced.

**Why this priority**: delivers the first honest numbers on the new matrix
and proves the five pipelines and fixtures end to end; the third product
rides on top of this in the next story.

**Independent Test**: one recorded session produces artifacts for all five
cells × (rdlt + dlt) with row-count verification passing; the generated
matrix table renders the session; no enforcement exists.

**Acceptance Scenarios**:

1. **Given** the five cells, **When** the session runs, **Then** each cell
   reads the same source data and writes to the same destination instance
   per the same-conditions definition, and every arm's destination row
   count equals the expected count.
2. **Given** the dedup cell, **When** its second load runs with half the
   rows changed, **Then** all three products' arms (when present) perform
   the same full-redelivery-plus-dedup regime, and the cell's recorded note
   states the regime honestly.
3. **Given** the fastest-configuration rule, **When** the database-source
   arms run, **Then** dlt uses its Rust-reader backend and the resulting
   (smaller) multiple is the recorded claim.

---

### User Story 4 - The matrix goes three-way (Priority: P4)

The harness gains a driver-style competitor kind (a host-side orchestrator
that manages the platform product's local cluster and its job API, emitting
the same last-line result convention the harness already consumes), the
Airbyte module (setup, driver, pinned versions, README), and the first
recorded three-way session. The platform's absence on a machine is a loud
recorded skip, never a silent one; its headline time is the job wall
(what a user experiences), with the attempt time recorded as labeled
context; cluster-wide resource numbers are recorded but never enforceable.

**Why this priority**: the three-way promise is the feature's reason to
exist, but it depends on the probes (US2) and the matrix (US3).

**Independent Test**: one recorded session carries all three products'
arms for the five cells (or a recorded absent-with-reason for any cell a
probe ruled out), rendered in the matrix with per-product timing boundaries
stated.

**Acceptance Scenarios**:

1. **Given** a machine without the platform prerequisite, **When** the
   session runs, **Then** the affected arms record a loud
   missing-with-reason and every other arm still measures.
2. **Given** the fairness policy, **When** the results page renders,
   **Then** the platform's headline includes orchestration (labeled), the
   caveats state what these cells do and do not measure about it, and its
   version is pinned with the same bump-means-re-measure rule as the other
   competitor.

---

### User Story 5 - Enforcement returns measurement-first (Priority: P5)

After the first recorded three-way session, at most one bar per cell is set
through the existing governance mechanism: each bar sits below its session
floor, cites a policy entry, and references an existing cell. Enforcement
resumes only where evidence exists; cells without bars inform.

**Why this priority**: last by design — bars without recorded three-way
evidence would violate the feature's own rules and the constitution's
mechanism.

**Independent Test**: the gate enforces only bars that cite recorded
sessions; every bar references an existing cell; no cell carries more than
one bar.

**Acceptance Scenarios**:

1. **Given** the first recorded session, **When** bars are proposed,
   **Then** each is below the recorded floor and carries a policy entry, and
   the gate goes green against the same session.
2. **Given** a cluster-resource statistic, **When** a bar is proposed on
   it, **Then** it is rejected: only single-process resource comparisons are
   bar material.

---

### Edge Cases

- What happens when the number-one feasibility risk lands (the container
  runtime cannot host the platform's cluster on this machine)? US2 records
  the no-go; the matrix ships two-way with the third column
  absent-with-reason; US4 blocks rather than shipping a silent 2-way
  pretending to be 3-way.
- What happens to the strongest legacy marketing number (the 13.5×
  flagship)? The document's owner-recorded recommendation — cut clean — is
  adopted: it survives as quotable history in milestones citing the
  pre-migration commit, and no legacy exception cell is kept.
- What happens when a survivor-set recount at plan time disagrees with the
  document's enumeration (counts have already drifted post-017)? The three
  survival tests decide; the plan records the re-derived deletion list.
- What happens to harness self-test cells (protocol-validation fixtures)?
  They are test machinery, not matrix rows — they survive as tests and are
  exempt from the three survival tests.
- What happens if the platform's job API fields differ from the document's
  pinned expectations? The probe (US2) pins the real fields before any
  harness code depends on them.
- What happens to in-flight recorded artifacts when cells are deleted?
  Deleted with their cells in the one migration commit; the migration
  record cites final values and the archive commit.
- What happens when the same-conditions rule collides with a product's
  cheaper-but-different regime (the platform's cursor-based incremental on
  the dedup cell)? The cheaper regime is not benched (no counterpart in
  the second product); the cell's note records the exclusion honestly.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The benchmark matrix MUST consist exactly of the five cells
  of the source document's §6, each an end-to-end pipeline runnable by all
  three products from the same data into the same destination instance
  under the same-conditions definition (§3).
- **FR-002**: Every existing cell, fixture, seed, competitor script, and
  recorded artifact that fails the three survival tests MUST be deleted
  outright in one migration commit — no archive directory, no disabled
  state — with the migration record citing each retired cell's final
  recorded value and the pre-migration commit. Harness self-test fixtures
  are exempt (test machinery, not matrix rows).
- **FR-003**: The importance taxonomy (gated/scoreboard classification)
  MUST be removed from the cell schema, artifacts, gate/report behavior,
  the quiet guard, and all documentation; what remains is cells (measured,
  reported) and bars (enforced); the artifact format version increments.
- **FR-004**: Suites MUST be removed (one matrix, one table, one generated
  region); the run-mode vocabulary MUST collapse to a single
  subprocess-wall-timing mode, retiring the modes that exist only for
  deleted cells, including the harness's in-process library mode.
- **FR-005**: The parity fixture and the CLI-side parse/build pins MUST
  survive the library-mode deletion (post-017 they guard the shared
  pipeline-spec model the CLI still consumes); only the harness-side
  consumer is deleted.
- **FR-006**: The cold-start absolute check (≤ 40 ms) MUST move to the
  instruments track and remain enforced there; it leaves the comparison
  matrix.
- **FR-007**: The quiet guard MUST become one classless rule (refuse or
  wait on a loaded machine for any run; a recorded force override), and
  bar cross-validation MUST reduce to "every bar references an existing
  cell".
- **FR-008**: All existing bars MUST be retired via one recorded policy
  entry; the bars file starts empty for the new matrix; after the first
  recorded three-way session at most ONE bar per cell may be set, each
  below its recorded session floor with a policy entry; cluster-wide
  resource statistics are never bar material.
- **FR-009**: The dlt competitor MUST run in its fastest documented
  configuration per cell (Rust-reader backend for database extraction),
  with the older backend retained as recorded context and the
  marketing-by-selection variant deleted; the competitor image gains
  object-store support and drops what left with the deleted cells.
- **FR-010**: The five feasibility probes of §7 MUST be answered with
  recorded evidence on the target machine BEFORE any platform-competitor
  harness code is written; a failed probe records its fallback (two-way
  with absent-with-reason) rather than silently shrinking scope.
- **FR-011**: The harness MUST gain a driver competitor kind: a host-side
  orchestrator per competitor module discovered from per-module variant
  files (one flat namespace, collision is a load-time error), emitting the
  existing last-line result convention so artifact, gate, and report paths
  need no changes.
- **FR-012**: The platform product MUST be treated as a machine
  prerequisite, not a fixture: absence is a loud recorded skip; setup is
  idempotent and its created connection identities are cached outside
  version control.
- **FR-013**: Per-product timing boundaries MUST be recorded honestly and
  stated in the results page: engine CLI wall; library in-process
  self-timed; platform job wall as headline with attempt time as labeled
  context; per-competitor run counts may differ and are recorded.
- **FR-014**: The results page MUST be rebuilt to: a short methodology +
  policy log header, ONE generated matrix table (medians with spread,
  ratios, bar status), curated caveats, generated trends from an
  append-only history, and a milestones section carrying retired claims
  with their evidence commits; governance records (coverage, exclusions,
  semver notes) move to a separate governance document; every number is
  generated or frozen-with-citation.
- **FR-015**: The verification rule for every arm MUST be destination row
  count equals expected count; the dedup cell MUST use the matching
  full-redelivery regime in all three products with its regime note
  recorded in the cell.
- **FR-016**: The constitution's benchmark principle MUST be amended
  through its own amendment procedure (version bump + sync report) so its
  vocabulary matches the cells/bars model while preserving its mechanism
  (no enforcement without recorded evidence and a governance entry); the
  012 harness contract's affected clauses are amended the same recorded
  way. Neither document may be silently contradicted.
- **FR-017**: Work MUST land as the phased sequence P0–P4, each phase
  independently mergeable with the full workspace gate green; phases that
  add measurements record them without enforcement until US5.
- **FR-018**: The explicit non-goals of §10 MUST hold: no importance
  taxonomy reintroduced, no product-specific cells, no CI wall-time
  gating, no hosted services, no bars without evidence + policy entry, no
  dashboard.

### Key Entities

- **Cell**: one end-to-end pipeline comparison (source data → product →
  destination), defined declaratively; carries its claim note and
  per-competitor arms; exactly five in the matrix.
- **Bar**: an enforced bound referencing one existing cell, justified by a
  policy entry citing a recorded session; at most one per cell.
- **Competitor arm**: one product's execution of a cell — container
  self-timed (existing kind) or driver-orchestrated (new kind) — emitting
  the shared result convention plus labeled context statistics.
- **Recorded session**: one same-machine, same-conditions measurement pass
  producing fingerprinted artifacts for every arm; the unit of evidence
  bars may cite.
- **Migration record**: the single deletion commit plus the policy-log
  entry citing retired cells' final values and the archive (pre-migration)
  commit.
- **Spike document**: the recorded outcomes of the five feasibility
  probes, each with evidence and a decision.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The matrix contains exactly 5 cells; every non-exempt legacy
  cell (21 of the 26 in the current tree, re-derived at plan time), its
  fixtures, seeds, scripts, and artifacts are gone from the working tree
  and recoverable at the cited archive commit.
- **SC-002**: A vocabulary sweep for the retired taxonomy and mode terms
  over harness code, cell files, artifacts, and generated docs returns
  zero hits after P0.
- **SC-003**: The results page shrinks to roughly a third of its current
  length with 100% of its numbers generated-from-artifacts or
  frozen-with-citation, structured as matrix/caveats/trends/milestones.
- **SC-004**: All five probes have recorded evidence-backed answers before
  any platform harness code lands; the number-one risk (runtime) has an
  explicit go/no-go decision.
- **SC-005**: The first recorded session covers 5 cells × 2 products
  (rdlt, dlt) with 10/10 row-count verifications passing; the three-way
  session covers 5 × 3 arms measured or absent-with-reason.
- **SC-006**: After US5, every bar (≤ 5 total, ≤ 1 per cell) sits below
  its cited recorded floor with a policy entry, and the gate passes
  against the same session that justified it.
- **SC-007**: The constitution and the 012 harness contract carry recorded
  amendments (version bump + sync report; amended clause wording) with
  zero remaining textual contradictions with the shipped behavior.
- **SC-008**: The full workspace gate (tests, lint, doc-tests, instruments
  track including the relocated cold-start check) is green at every
  phase's merge.

## Assumptions

- `BENCH_REFINMENT.md` is the authoritative design record; its v3.1
  owner decisions (delete-not-archive; the governing principle; the
  non-goals) are requirements, not options. Where its tree-state snapshot
  has drifted (cell/fixture counts, the parity-fixture premise), the rule
  decides and the plan records the re-derived lists.
- The flagship-number decision is settled as the document's own
  recommendation: cut clean, no legacy exception cell; the 13.5× lives in
  milestones with its evidence commit.
- Harness self-test cells are test machinery, exempt from the survival
  tests, and stay.
- The platform product's local cluster (via its documented local installer)
  is the only Airbyte deployment in scope; hosted services are out (§10).
- The greenfield policy from feature 017 applies (no compatibility shims);
  persisted benchmark artifacts are versioned data — the artifact format
  version increments rather than being silently reshaped.
- The probes may conclude no-go; the feature is still shippable through
  US3 (two-way matrix with recorded absence) — US4/US5 then wait on a
  future environment rather than blocking the cleanup value of P0–P2.
- Bench sessions require the established environment discipline (quiet
  machine, container runtime present, images pinned); measurement phases
  are run deliberately, not in CI.
