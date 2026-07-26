# Feature Specification: Workspace Refactoring Program

**Feature Branch**: `017-workspace-refactoring`

**Created**: 2026-07-24

**Status**: Implemented — see [close-out.md](close-out.md) for the disposition of every requirement.

**Input**: User description: "Workspace-wide refactoring program per REFACTORING.md: fix 12 latent bugs (B1-B12), then execute cross-cutting refactors R1-R13 (comment/citation policy, sqlcore commit-unit protocol extraction, Secret unification, god-file splits, validation decomposition, exactly-once apply-helper sharing, file-crate Location/Store unification, error-taxonomy alignment, panic-path removal, naming unification, arg-struct extraction, dead-code sweep, magic-constant centralization) in the value-per-risk order of Part 4"

## Source Catalogue

The authoritative inventory for this feature is `REFACTORING.md` at the repo
root: 12 defects (B1–B12), 13 cross-cutting themes (R1–R13), per-crate
findings (Part 3), a value-per-risk execution order (Part 4), and the
discovered opportunities from the follow-up sweep of non-Rust and
test-support surfaces (Part 5, items D1–D15). This spec
defines *what outcome* each group of findings must reach and *how completion
is judged*; the catalogue holds the item-level detail and is treated as the
requirement inventory. Line numbers in the catalogue are locators, not
contracts — items are identified by their IDs.

Constitution alignment: this feature is the enforcement vehicle for
Principle V (typed error taxonomy, no citation IDs in user-facing strings)
and Principle VI (self-contained comments) of the rdlt constitution v1.0.0,
and must preserve Principles IV (exactly-once) and IX (frozen persisted
formats) while restructuring.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Latent defects no longer corrupt or abort pipelines (Priority: P1)

An operator runs data pipelines against flaky networks, rate-limited
services, and crash-prone environments. Today, twelve catalogued defects
(B1–B12) can silently misbehave: recoverable failures abort runs instead of
retrying, row counts over-count neighbouring tables, out-of-order data
silently corrupts resume watermarks, recovery can exhaust memory, and
credential rejections can be misread as retryable. After this story, every
catalogued defect is fixed and pinned by a regression test that fails on the
old behavior.

**Why this priority**: these are correctness defects, not style; several
threaten the exactly-once promise or waste entire runs. The catalogue's own
Part 4 puts them first, before any restructuring, so fixes land on unmoved
code.

**Independent Test**: run the new regression tests against the pre-fix code
(each must fail) and post-fix code (each must pass); full existing gate stays
green.

**Acceptance Scenarios**:

1. **Given** a child-record fetch that fails with a recoverable (transient or
   rate-limited) condition, **When** the parent fan-out surfaces the error,
   **Then** the recoverable classification is preserved and retry budget is
   consumed instead of the run aborting (B1).
2. **Given** two output tables where one name is a prefix of the other,
   **When** rows are counted or truncated for the shorter-named table,
   **Then** only that table's own files are considered (B2).
3. **Given** a pipeline definition using any supported destination kind,
   **When** it is executed through either the command-line path or the
   embedded-library path, **Then** both paths parse it identically from one
   shared definition model (B3).
4. **Given** a source stream that yields rows out of the declared order,
   **When** the engine computes resume watermarks in a release build,
   **Then** the violation surfaces as a typed error instead of silently
   producing a wrong watermark (B4).
5. **Given** an upstream library that rewords its rendered error text,
   **When** failure classification runs (constraint violations B5,
   authentication rejections B6), **Then** classification keys on structured
   codes/context, or a pinned probe test fails loudly in CI before the
   reworded text can be misclassified.
6. **Given** a crash-recovery replay over a large uncommitted span, **When**
   recovery runs, **Then** memory use stays within the configured budget
   rather than buffering the whole span (B10).
7. **Given** the remaining catalogued defects (B7 paired literals, B8/B9
   error-channel gaps, B11 retired-parser fuzzing, B12 hashing-doc
   contradiction), **When** the story completes, **Then** each has a fix and
   a pin per the catalogue's stated resolution.

---

### User Story 2 - Every message and comment stands alone (Priority: P2)

An end user reading an error message, and a developer reading published crate
documentation, can understand what they see without access to the project's
internal `specs/` documents, review-finding numbers, or task IDs. All
user-facing strings are free of internal citation IDs; all comments state
their rule inline; already-rotted citations (stale charters, references to
removed crates, obsolete breadcrumbs) are corrected or deleted (R1).

**Why this priority**: pure deletion/rewording with zero behavior change, it
improves every future read of the code, and it is now a constitutional
requirement (Principles V and VI). Doing it before the structural moves means
less text moves twice.

**Independent Test**: a workspace sweep finds zero user-facing strings
containing internal citation IDs and zero comments whose meaning depends on
unresolvable references; the full test gate is unchanged.

**Acceptance Scenarios**:

1. **Given** any error or warning a user can see, **When** it is rendered,
   **Then** it contains no contract clause IDs, spec paths, or review-finding
   numbers.
2. **Given** any comment in a published crate, **When** read on its own,
   **Then** it states the rule or invariant itself (an optional short tag may
   follow); relocation breadcrumbs and historical narratives are gone.
3. **Given** the catalogued already-rotted citations (stale crate charter,
   removed-crate references, landed-task breadcrumbs), **When** the story
   completes, **Then** each is corrected or deleted.

---

### User Story 3 - One source of truth for correctness-critical logic (Priority: P3)

A maintainer changing commit protocol, crash-recovery apply logic, secret
masking, or pipeline-definition parsing edits exactly one place. The
catalogued live duplications are collapsed: the per-destination commit-unit
protocol into one shared planner (R2), the three secret-masking
implementations into one shared type (R3), the live-path and replay-path
apply logic into shared helpers (R6), and the CLI/bench pipeline-definition
model into one shared parser (with B3's drift fixed) — plus the mechanical
helper duplicates the catalogue lists alongside them.

**Why this priority**: these duplications have already drifted (the catalogue
documents the divergence), and every future feature widens the gap. They are
the highest-stakes copies because they guard exactly-once and credential
redaction.

**Independent Test**: for each named duplication, the workspace contains
exactly one implementation with all former copy sites consuming it;
behavior-pinning tests (golden outputs, conformance suites) pass unchanged.

**Acceptance Scenarios**:

1. **Given** the two relational destinations, **When** a commit-protocol rule
   changes (replay check, single-unit guard, publish ordering), **Then** the
   change is made once in the shared core and both destinations follow it
   (R2).
2. **Given** a secret value in any connector configuration, **When** it is
   debug-printed or serialized, **Then** one shared masking type governs the
   behavior everywhere (R3).
3. **Given** a batch applied on the live path and the same batch applied
   during crash-recovery replay, **When** either path runs, **Then** both
   execute the same shared apply helpers, and crash/replay conformance tests
   confirm identical outcomes (R6).
4. **Given** the file family's two parallel local-or-remote abstractions,
   **When** listing, key-joining, or error classification is needed, **Then**
   one unified abstraction serves both read and write halves (R7, closing the
   root cause of B2/B9).

---

### User Story 4 - Structural decomposition of god files and monoliths (Priority: P4)

A developer navigating the workspace finds each catalogued god file/function
(R4 table), validation monolith (R5), and over-long argument list (R11) split
along the catalogue's proposed seams: single-responsibility modules, named
per-rule validation functions, and context structs replacing repeated
argument prefixes.

**Why this priority**: pure structure; valuable but safe to do only after
bugs are fixed and duplications collapsed so code moves once. Several splits
are prerequisites for keeping the shared cores honest.

**Independent Test**: each catalogued location in the R4/R5/R11 tables is
either split as proposed or has a recorded, justified deviation; the full
gate is green and public API is unchanged (or additively changed) per crate.

**Acceptance Scenarios**:

1. **Given** the R4 table's fourteen locations, **When** the story completes,
   **Then** each is decomposed along its listed seams (or a deviation is
   recorded with rationale in the close-out matrix).
2. **Given** the five R5 validation monoliths, **When** a validation rule
   fails, **Then** the failing rule lives in a named function adjacent to its
   error message.
3. **Given** the R11 argument-list clusters, **When** the story completes,
   **Then** the repeated argument prefixes travel as named context structs
   and the corresponding lint suppressions are gone.

---

### User Story 5 - Honest error taxonomy and panic-free library paths (Priority: P5)

An embedding application can rely on the error taxonomy: recoverable
conditions are typed recoverable everywhere (destinations gain the missing
rate-limit channel or a documented waiver), stringly-typed error channels
become typed (R8), and the catalogued panic paths in library code become
typed errors or are made impossible by construction (R9), including the
missing transient channels (B8/B9 follow-through).

**Why this priority**: depends on the structural work (several panic paths
disappear inside the splits and shared helpers) and finishes the
constitution's Principle V across the workspace.

**Independent Test**: fault-injection and conformance tests exercise the
catalogued paths; none panic, and classification matches the taxonomy table
per connector.

**Acceptance Scenarios**:

1. **Given** a recoverable failure at any catalogued site (locked database
   file, mid-stream network reset, rate-limited catalog), **When** it
   surfaces, **Then** it is typed recoverable and the engine can retry (R8).
2. **Given** the catalogued panic clusters (ping-pong state, run-state
   expects, validated-at-parse expects, partial methods), **When** the story
   completes, **Then** each is a typed error or structurally impossible (R9).

---

### User Story 6 - One voice: naming, dead code, and constants (Priority: P6)

A reader encounters one naming convention across the workspace (R10:
non-breaking renames applied now; breaking renames staged for the already
recorded next semver window), no dead or over-exposed API surface (R12), and
named, centralized constants where magic literals were duplicated (R13).

**Why this priority**: polish with real value but no correctness risk;
breaking renames must wait for the recorded semver window, so this story is
last and partially deferred by design.

**Independent Test**: catalogue close-out matrix shows every R10/R12/R13 item
either applied, shimmed with a deprecation alias, or explicitly deferred to
the semver window with the deferral named.

**Acceptance Scenarios**:

1. **Given** the R10 rename table, **When** the story completes, **Then**
   non-breaking renames are applied, breaking ones carry a compatibility
   shim or a named deferral to the recorded semver window.
2. **Given** the R12 dead-code inventory, **When** the story completes,
   **Then** each item is deleted, narrowed in visibility, feature-gated, or
   kept with a recorded justification.
3. **Given** the R13 constant sites, **When** a paired or repeated literal
   must change, **Then** it changes in exactly one place.

---

### User Story 7 - Build, CI, and test-support surfaces share the discipline (Priority: P7)

A contributor touching the delivery machinery finds the same single-source
discipline the production code gets: one container-runtime probe with one
skip-not-fail posture shared by every integration test, shared test fixtures
owned by the test toolkit instead of copy-pasted across crates, deduplicated
CI steps and benchmark configuration, workspace-enforced toolchain facts
(minimum supported version declared where the build tool enforces it), and a
repository free of tracked artifacts that its own ignore rules exclude
(Part 5, D1–D15).

**Why this priority**: these surfaces don't ship to users, so they rank
below everything user-visible — but D1–D4 guard the credibility of the test
gate itself (a probe that panics instead of skipping, or fixtures that
drift, silently weakens Principle VII), and the fixture unification grows
the same shared test toolkit the P3 dedup work already touches.

**Independent Test**: the Part 5 close-out rows show each D-item applied or
justified; the full gate passes on a machine with and without a container
runtime, with identical skip behavior across all integration suites.

**Acceptance Scenarios**:

1. **Given** a machine without a container runtime, **When** the full test
   gate runs, **Then** every container-backed suite skips visibly through
   the one shared probe — none panic, and no two suites disagree (D1, D2).
2. **Given** a change to a shared test fixture (container image tag, commit
   metadata shape, canonical schema), **When** it is made, **Then** it is
   made in exactly one place in the test toolkit (D3, D4, D5).
3. **Given** a version pin that must agree across build surfaces (benchmark
   tooling, minimum supported toolchain, competitor baselines), **When** it
   is bumped, **Then** one declaration governs or a cross-check fails loudly
   (D7, D11, D13).
4. **Given** the remaining Part 5 items (CI step duplication, redundant
   toolchain installs, bench config duplication, tracked-but-ignored
   artifacts, stale prose facts), **When** the story completes, **Then**
   each is resolved or carries a recorded justification (D6, D8–D10, D12,
   D14, D15 and the catalogued low-severity notes).

---

### Edge Cases

- What happens when a refactor touches a persisted format or golden pin?
  The pin must remain byte-identical; any intentional change requires the
  explicit versioning path of constitution Principle IX and is out of scope
  for this feature.
- What happens when fixing a misclassification (B1, B8, B9) makes a
  previously fatal error retryable? Retry budgets must bound the new retries;
  tests cover the budget-exhaustion path so runs still terminate.
- What happens when an upstream library rewords error text after the pinned
  probe tests land (B5, B6)? The probe test fails loudly in CI rather than
  the classification silently degrading.
- What happens when a catalogued item turns out to be stale (code already
  changed since the review)? The close-out matrix records it as
  overtaken-by-events with the evidence, rather than forcing a no-op change.
- What happens when new refactoring opportunities are discovered during
  implementation? They are appended to the catalogue with the same severity
  triage and either scheduled into a story or explicitly deferred — never
  silently absorbed.
- What happens when a rename would break the published API outside the
  recorded semver window? It ships as a deprecated alias or a named deferral;
  the window itself is not opened by this feature.
- What happens if two independently mergeable increments conflict? Part 4's
  order is authoritative; the later increment rebases on the earlier one, and
  each merge lands with the full gate green.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Every defect B1–B12 MUST be fixed as catalogued, and each fix
  MUST carry a regression test that fails against the pre-fix behavior.
- **FR-002**: Recoverable failure classifications MUST be preserved across
  all propagation layers — no layer may downgrade a recoverable error to
  fatal while adding context (B1; generalized by R8).
- **FR-003**: Per-table file selection (counting and truncation) MUST use one
  shared ownership rule that cannot match a sibling table whose name shares a
  prefix (B2, folded into R7's unified abstraction).
- **FR-004**: One shared pipeline-definition model MUST serve both the
  command-line and embedded-library execution paths, supporting every
  destination kind the workspace ships (B3).
- **FR-005**: Ordering violations in cursor streams MUST surface as typed
  errors in all build profiles; key-format failures MUST propagate rather
  than collapse to empty key components (B4).
- **FR-006**: Failure classification MUST NOT depend on substring-matching
  rendered error text; where an upstream exposes nothing structured, the
  textual assumption MUST be pinned by a test that fails loudly on drift
  (B5, B6; constitution Principle V).
- **FR-007**: Write/read-paired literals MUST have a single definition shared
  by both sides (B7, R13).
- **FR-008**: Crash-recovery replay MUST honor the configured memory budget
  regardless of uncommitted-span size (B10).
- **FR-009**: Fuzzing MUST target the production parse path; retired parsers
  MUST be removed from production code or confined to tests (B11).
- **FR-010**: The schema-hash documentation and behavior MUST agree on one
  semantic for provenance, with the persisted hash format unchanged (B12).
- **FR-011**: No user-facing string (error, warning, CLI output) may contain
  contract clause IDs, spec paths, task IDs, or review-finding numbers; all
  comments MUST be self-contained; catalogued rotted citations MUST be
  corrected or deleted (R1).
- **FR-012**: The commit-unit protocol MUST have exactly one implementation
  consumed by all relational destinations, including the catalogued
  mechanical helpers (R2).
- **FR-013**: Secret masking MUST have exactly one implementation shared by
  all connectors; free-form header/parameter maps MUST have a documented
  redaction posture (R3).
- **FR-014**: Live-path and replay-path batch application MUST execute the
  same shared helpers (R6).
- **FR-015**: The file family MUST expose one local-or-remote location
  abstraction with read and write halves and one shared error
  classification (R7, subsuming B9).
- **FR-016**: Each location in the R4 god-file table and the R5 validation
  table MUST be decomposed along its catalogued seams or carry a recorded
  deviation with rationale.
- **FR-017**: The error taxonomy MUST be honest workspace-wide: recoverable
  channels exist where recoverable failures occur (including destinations),
  stringly-typed error channels named in R8 become typed, and error-variant
  misuse is corrected (R8, B8).
- **FR-018**: The catalogued panic paths in library code MUST become typed
  errors or be made impossible by construction (R9).
- **FR-019**: Naming MUST follow the R10 unification table: non-breaking
  renames applied; breaking renames shipped as deprecated aliases or named
  deferrals to the recorded semver window (R10).
- **FR-020**: The R11 argument-list clusters MUST be replaced by named
  context structs, removing the corresponding lint suppressions (R11).
- **FR-021**: Each R12 dead-code/visibility item MUST be deleted, narrowed,
  gated, or kept with a recorded justification; each R13 magic-constant site
  MUST be centralized into a named constant (R12, R13).
- **FR-022**: Work MUST land as independently mergeable increments following
  Part 4's value-per-risk order, with the full test gate green at every
  merge.
- **FR-023**: A close-out matrix MUST account for every catalogued item
  (B1–B12, R1–R13 including their Part 3 sub-items, and D1–D15 with Part 5's
  low-severity notes) with a terminal state: applied, shimmed,
  deferred-with-name, or overtaken-by-events-with-evidence — zero uncited
  dispositions.
- **FR-024**: Public API changes MUST be additive or shimmed within this
  feature; persisted formats, golden pins, and conformance clause behavior
  MUST be byte-for-byte / behavior-identical throughout (constitution
  Principles IV and IX).
- **FR-025**: Test-support and delivery surfaces MUST reach single-source
  state per Part 5: one shared container-runtime probe with a uniform
  skip-not-fail posture across all integration suites (D1, D2); shared test
  fixtures owned by the test toolkit (D3–D5); deduplicated CI steps and
  benchmark configuration (D6, D8–D12); version pins that must agree across
  surfaces governed by one declaration or a loud cross-check (D7, D13);
  inheritance stragglers and tracked-but-ignored artifacts resolved
  (D14, D15).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 12 of 12 catalogued defects are fixed, each with a regression
  test demonstrated to fail against the pre-fix code.
- **SC-002**: A workspace sweep finds zero user-facing strings containing
  internal citation IDs, down from the catalogued baseline (10+ sites).
- **SC-003**: Each named live duplication has exactly one implementation:
  secret masking 3→1, commit-unit protocol 2→1, pipeline-definition model
  2→1, live/replay apply logic 2→1, location abstraction 2→1.
- **SC-004**: The full test gate (unit, doc, conformance, container-backed
  integration) is green at every one of the feature's merges, and final
  coverage is at or above the pre-feature baseline.
- **SC-005**: All persisted-format golden pins are unchanged; crash-point
  sweep tests pass with duplicate-free results before and after the
  refactor.
- **SC-006**: The close-out matrix covers 100% of catalogued items with zero
  uncited dispositions, and every deferral is named with its target window.
- **SC-007**: Fault-injection tests over the catalogued panic and
  misclassification sites produce zero panics and zero
  recoverable-mistyped-as-fatal outcomes in library code.
- **SC-008**: No catalogued god file remains above its pre-split size, and
  every catalogued lint suppression for over-long argument lists is removed.
- **SC-009**: Delivery-surface duplication collapses to single sources:
  container-runtime probe 3→1, container fixture setup ~6→1, conformance
  fixture trio 6 files→1, CI disk-free step 5→1, benchmark-tooling version
  pin 3 declarations→1 — and the full gate produces identical skip behavior
  on machines with and without a container runtime.

## Assumptions

- `REFACTORING.md` is the authoritative item inventory; its line numbers are
  locators, not contracts. Items are tracked by ID (B*/R*) in the close-out
  matrix.
- The catalogue's Part 4 ordering is adopted as the delivery order; each
  numbered step is an independently mergeable increment.
- Breaking renames (R10's semver-breaking rows) are NOT shipped as breaks in
  this feature: they land as deprecated aliases where cheap, otherwise as
  named deferrals to the already recorded next-publish semver window (the
  0.2→0.3 major recorded by feature 014). This feature does not itself open
  that window.
- "User-facing strings" means anything a pipeline operator or CLI user can
  see in normal or failure operation: error/warning messages, CLI output,
  and log lines at info level or above. Internal trace/debug output and test
  assertion messages are exempt from FR-011's citation ban.
- The testkit conformance clause IDs (D1–D8/S1–S6) that failures actually
  print are the catalogue's stated good pattern and are retained.
- Behavior changes are limited to (a) the catalogued defect fixes and (b)
  error-classification corrections; everything else is
  behavior-preserving restructuring verified by the existing gate.
- The requested discovery pass beyond the original review was performed at
  specification time over the surfaces the Rust-file review skipped
  (test-support code, CI, build/bench configuration, manifests, repo-root
  hygiene); its findings are catalogued as Part 5 (D1–D15) and carried by
  User Story 7 / FR-025. Opportunities discovered later, during
  implementation, are still triaged into the catalogue rather than silently
  expanding scope; anything larger than a polish item becomes a recorded
  candidate for a future feature.
- No new external dependencies are needed; the work is internal
  restructuring within the existing workspace.
