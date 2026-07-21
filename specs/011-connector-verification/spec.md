# Feature Specification: Postgres Connector Verification — Every Parameter Proven

**Feature Branch**: `011-connector-verification`

**Created**: 2026-07-21

**Status**: Draft

**Input**: User description: "Full review of the postgres connectors'
(source/dest) configs and values: make sure ALL parameters are properly
tested and work correctly, aiming for 80+% test coverage of the
connector crate. After this feature, every config param is well tested,
we are 100% sure all params work as designed, and the connector is in
prod-quality shape."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - The parameter conformance matrix (Priority: P1)

An operator (or the rapidbyte platform) sets ANY documented parameter of
the postgres source, destination, or CLI pipeline spec and can trust it
does exactly what the README says — because every parameter, every
documented value, every default, and every documented interaction has a
test that proves the BEHAVIOR (against a real server where the behavior
is server-visible), not merely that the config parses. The proof is
auditable: a traceability matrix maps every parameter row to the
test cell(s) that pin it, so "is X tested?" is a lookup, not an
archaeology dig.

**Why this priority**: this is the feature. A parameter that parses but
misbehaves is worse than a missing parameter — it's a silent lie in the
product's contract. The connector already has deep suites; what's
missing is the systematic guarantee that NO parameter escaped them.

**Independent Test**: pick any parameter from the README reference
tables at random; the matrix names its cells; running those cells
exercises the documented default AND each documented value/behavior.

**Acceptance Scenarios**:

1. **Given** the full parameter inventory (source top-level, table
   entries, cursor block, query streams, type hints, CDC block, TLS
   block, destination connection, destination-wide and per-table
   options, SCD2 block, dedup/scope options, CLI pipeline spec),
   **When** the traceability matrix is built, **Then** every parameter
   has at least one behavioral cell per documented value or behavior,
   and every VALIDATION rule (typed error) has its own cell.
2. **Given** a parameter whose documented default is claimed in the
   README, **When** its default cell runs, **Then** the default behavior
   is observed (not inferred from code) — e.g. `boundary` defaulting to
   closed actually re-fetches and dedups watermark-equal rows.
3. **Given** parameters with documented interactions (cursor `lag` ×
   `boundary` × write mode; CDC × hard_delete composition; `dedup_sort`
   × strategies; TLS block × conn-string), **When** the matrix is
   audited, **Then** each documented interaction has a cell.
4. **Given** a gap found during the audit (parameter or value with no
   behavioral cell), **When** the feature completes, **Then** the gap is
   closed with a new cell — or the parameter's documentation is
   corrected if the behavior was misdocumented.

---

### User Story 2 - A measured coverage floor (Priority: P2)

The connector crate's test coverage is MEASURED with a recorded,
reproducible protocol, and reaches at least 80% line coverage. The
uncovered remainder is classified — every excluded region has a stated
reason (unreachable defensive arm, platform-specific branch, …), so the
number is honest rather than gamed.

**Why this priority**: the matrix (US1) proves the parameter surface;
coverage proves there are no dark corners BETWEEN parameters — decode
paths, error arms, lifecycle edges the parameter matrix doesn't name.

**Independent Test**: one command produces the coverage report; the
recorded number is ≥ 80% for the connector crate; the exclusion list is
short and each entry has a reason.

**Acceptance Scenarios**:

1. **Given** the coverage tooling wired into the house Makefile
   vocabulary, **When** the coverage target runs, **Then** it reports
   per-file and total line coverage for the connector crate,
   reproducibly.
2. **Given** the measured baseline (before this feature), **When** gaps
   are closed, **Then** total line coverage for the connector crate is
   ≥ 80%, and the number plus the command are RECORDED alongside the
   other measurements.
3. **Given** code that remains uncovered, **When** the feature closes,
   **Then** each uncovered cluster is classified with a reason in the
   recorded results — no silent dark corners.

---

### User Story 3 - Defects fixed, pinned, and the docs kept honest (Priority: P3)

Verification at this depth WILL surface mismatches: parameters that
misbehave, docs that overclaim, validation that under-rejects. Every
mismatch found becomes either a code fix with a pinned regression cell
or a documentation correction — never a silently-skipped row in the
matrix.

**Why this priority**: the exit criterion is "100% sure all params work
as designed" — which requires design, code, tests, and docs to agree at
the end, not just tests to exist.

**Independent Test**: the close-out lists every mismatch found with its
resolution (fix + cell, or doc correction); the matrix has zero
unresolved rows.

**Acceptance Scenarios**:

1. **Given** a parameter that misbehaves relative to its documentation,
   **When** found, **Then** the code is fixed and the new cell pins the
   corrected behavior (crash-swept if the fix touches a publish/read
   path).
2. **Given** a documented claim the implementation intentionally does
   not honor, **When** found, **Then** the documentation is corrected in
   the same change, with the discrepancy recorded.
3. **Given** the known minor discrepancy already on record
   (`merge_strategy` silently unused under append/replace write modes),
   **When** this feature completes, **Then** it is resolved like every
   other mismatch (typed rejection or documented-as-designed — decided
   and recorded).

---

### Edge Cases

- Parameters already covered by deep existing suites (TLS matrix, CDC
  cells, strategy conformance): the matrix CITES existing cells rather
  than duplicating them — traceability first, new tests only for real
  gaps.
- Parameters whose behavior is only observable under special server
  state (CDC WAL overrun, TOAST, container kill): the existing
  heavy/sweep cells are the citations; the matrix marks their runtime
  class so the audit stays honest about what runs where.
- Coverage measurement must not weaken the gates: the coverage run is an
  ADDITIONAL target; `make check` semantics stay untouched.
- Free-text parameters (`conn`, SQL in query streams, identifiers with
  hostile characters): the matrix includes the hostile-input rows
  (quoting, injection-shaped identifiers) — the existing quoting cells
  are citations, gaps get cells.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A COMPLETE parameter inventory MUST be produced covering
  every user-settable parameter of: source top-level, table entries,
  cursor block, query streams, type-hint vocabulary (all 12 hints), CDC
  block, TLS block (both directions), conn-string portability surface,
  destination connection, destination-wide options, per-table options
  (hard_delete, dedup_sort, merge_key, scd2 block), and the CLI pipeline
  spec (write_mode forms, source config/inline forms, workdir,
  pipeline).
- **FR-002**: Every inventory row MUST map to behavioral test cell(s)
  proving: the documented default, each documented value, and each typed
  validation error — with server-visible effects verified against a real
  server wherever the behavior is server-side.
- **FR-003**: Documented parameter INTERACTIONS MUST each have a cell
  (at minimum: cursor boundary×lag×write-mode, end_bound×end_value,
  nulls policies under resume, hints×cursor capability, CDC composition
  and exclusivity rules, TLS block×conn-string precedence, strategy×
  per-table option compositions, single-unit rules).
- **FR-004**: The traceability matrix MUST live in the repo as a
  reviewed artifact (parameter → cell names), and the close-out MUST
  show zero unresolved rows.
- **FR-005**: Line coverage for the connector crate MUST be measured
  with a reproducible command wired into the house tooling vocabulary,
  reach ≥ 80%, and be RECORDED (number, command, date) with the other
  measurements; the uncovered remainder MUST be classified with reasons.
- **FR-006**: Every mismatch discovered (behavior vs docs vs validation)
  MUST be resolved — code fix with pinned regression cell, or explicit
  documentation correction — and recorded in the close-out. This
  includes the known `merge_strategy`-under-append/replace footnote.
- **FR-007**: New cells MUST follow the house discipline: typed-error
  assertions name the offender; behavior cells assert server-visible
  state; fixes on publish/read paths get crash-sweep coverage; no
  parameter cell may assert parse-success alone where behavior is
  observable.
- **FR-008**: The engine SPI MUST NOT change (rdlt-core/rdlt-connector
  semver-checks stay "no update required"); no new runtime dependencies
  (test/tooling additions are permitted); existing gates and recorded
  bars stay green/within tolerance.

### Key Entities

- **Parameter inventory / traceability matrix**: the audited list —
  parameter, documented default, documented values/behaviors,
  validation rules, citing cell names, runtime class (unit / live
  server / sweep / heavy).
- **Coverage record**: crate, command, total + per-file numbers, date,
  exclusion classifications.

## Success Criteria *(mandatory)*

- **SC-001**: The traceability matrix exists in-repo and every parameter
  row cites at least one behavioral cell per documented value/behavior
  and per validation rule; an auditor can go from any README reference
  row to running proof in one lookup.
- **SC-002**: Randomized spot-audit holds: any sampled parameter's cells
  actually exercise the documented behavior (not parse-only), verified
  during review of this feature.
- **SC-003**: Connector-crate line coverage ≥ 80%, measured by the
  recorded command, with the number and exclusion classifications
  recorded alongside the house measurements.
- **SC-004**: All mismatches found are resolved and listed in the
  close-out (fix + pinned cell, or doc correction) — zero unresolved
  matrix rows, including the recorded `merge_strategy` footnote.
- **SC-005**: `make check`, doc-tests, crash sweeps, and semver-checks
  ("no update required" for rdlt-core/rdlt-connector) all green; gated
  perf bars within tolerance.

## Assumptions

- "Connector crate" = `rdlt-postgres` (source + dest + tls + CLI-facing
  config surface); the CLI spec parameters are verified through the
  CLI's own tests where they live.
- Coverage tooling choice (e.g. cargo-llvm-cov) is a planning decision;
  the requirement is reproducibility + recording, not a specific tool.
- 80% is a FLOOR for the crate total, not per-file; per-file numbers are
  recorded so outliers are visible and classified.
- Existing deep suites count as citations — the goal is complete
  traceability and closed gaps, not rewriting healthy tests.

## Out of Scope

- Non-postgres connectors (rest/file/duckdb/parquet) beyond the CLI
  spec fields shared by all pipelines.
- New connector features or parameters (this feature proves the surface
  that exists; anything found missing becomes a future feature, not
  scope creep here).
- Coverage gates in CI (recording and reaching the floor is in scope;
  turning it into a blocking gate is a separate governance decision per
  the 004 rules).
- Mutation testing beyond the existing `make test TARGET=mutants`
  vocabulary.
