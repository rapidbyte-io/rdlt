# Feature Specification: DuckDB Destination Completeness

**Feature Branch**: `013-duckdb-completeness`

**Created**: 2026-07-22

**Status**: Draft

**Input**: User description: "Model the duckdb connector after the
postgres connector — mirror whatever makes sense for duckdb: same
layout discipline, same test rigor, dlt feature parity. Approved shape
from the pre-spec discussion: extract a shared merge-planning core with
dialect hooks from the postgres dest (so destination #4 gets cheap),
bring duckdb to dlt-destination parity where duckdb semantics support
it (merge strategies, hard_delete, dedup_sort, merge_key, native JSON),
and verify to the postgres standard (parameter matrix, conformance
depth, crash sweeps, coverage floor, plus a cross-destination
differential oracle). Explicitly NOT mirrored: TLS/conn-string (no
network), CDC (no WAL), a duckdb source, preemptive modularization."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - One merge vocabulary, two destinations (Priority: P1)

An operator who knows the postgres destination's options points a
pipeline at DuckDB and everything they already learned carries over:
`merge_strategy` (`delete_insert` | `upsert` | `scd2`), per-table
options (`hard_delete`, `dedup_sort`, `merge_key`, the `scd2` block) —
same names, same YAML shape, same validation rules, same typed errors
naming the offender, same documented behavior. Under the hood the two
destinations share ONE merge-planning core; only the SQL dialect
differs (DuckDB's dedup/upsert idioms vs postgres'), so a fix or
refinement to the shared shapes lands on both destinations at once —
and the third SQL destination becomes an exercise in writing a dialect,
not a connector.

**Why this priority**: this is the feature — parity users can feel, and
the extraction that makes the connector family scale. dlt supports
merge/scd2 on its duckdb destination; rdlt currently loses that
comparison on features while winning on speed.

**Independent Test**: take the 008/010 option documentation, replace
`postgres:` with `duckdb:` in the destination block, and every
documented option either behaves as documented against a real DuckDB
database or fails with the same typed error postgres would give.

**Acceptance Scenarios**:

1. **Given** a keyed structured stream into DuckDB with
   `merge_strategy: delete_insert` (or unset — the default), **When**
   rows are re-delivered with changes, **Then** matched keys are
   replaced and totals equal source truth, exactly as on postgres.
2. **Given** `merge_strategy: upsert`, **When** a load re-delivers
   matched keys, **Then** rows update in place (no delete-visibility
   window) and the strategy composes with `hard_delete`.
3. **Given** `merge_strategy: scd2` with the documented validity-column
   options, **When** rows change / disappear across loads, **Then**
   history rows close and open per the 008 contract, including the
   per-table single-commit-unit rule and absent-handling vocabulary.
4. **Given** `dedup_sort` and/or `merge_key` on a DuckDB table, **When**
   a load carries in-load duplicates / a scoped feed, **Then** ordered
   survivor selection and scope replacement behave per the 010
   contract (values beat NULL; ties keep deterministic last-wins;
   scope delete before the strategy arm; per-table single-unit rule).
5. **Given** any option misuse postgres rejects (dedup on keyless,
   explicit strategy under non-merge write mode, unknown scd2 column
   collisions, NULL in a merge key), **When** attempted on DuckDB,
   **Then** the SAME typed rejection fires, naming the same offender.
6. **Given** the shared core extraction, **When** the postgres suites
   and gated benchmark bars run, **Then** postgres behavior and
   performance are UNCHANGED (the refactor is behavior-preserving by
   construction and proven by the existing nets).

---

### User Story 2 - Honest capabilities closed (Priority: P2)

The DuckDB destination's capability declarations stop carrying "false"
where DuckDB itself has the feature: `Json` columns land as native
DuckDB JSON (queryable with DuckDB's JSON functions) instead of
lowering to VARCHAR, and the capability matrix in the docs reflects
measured reality. Anything that stays unsupported remains DECLARED
false and documented — honesty either way.

**Why this priority**: capability flips ripple into engine planning
(the Json escape hatch, hint vocabulary) — closing them removes a
documented caveat users hit with nested/jsonb-shaped data (the
pg_jsonb→DuckDB scoreboard cell exercises exactly this path).

**Independent Test**: a pipeline carrying a Json-typed column into
DuckDB produces a native JSON column queryable via DuckDB JSON
functions; the capability declaration and README row agree.

**Acceptance Scenarios**:

1. **Given** a source column carrying Json values, **When** loaded into
   DuckDB, **Then** the destination column is DuckDB's JSON type and
   round-trips content intact (including the jsonb escape-hatch path
   from the postgres source).
2. **Given** the capability audit, **When** the feature closes, **Then**
   every `DestCapabilities` field for DuckDB is either true-and-proven
   or false-and-documented — no silent gaps.

---

### User Story 3 - Verified to the postgres standard (Priority: P3)

The DuckDB destination gets the 011 treatment, proportional to its
surface: a traceability matrix mapping every user-settable option to
behavioral cells, conformance depth matching the postgres dest suites,
armed crash sweeps across the new strategy arms, a measured coverage
floor for the crate — and one oracle postgres could never have: a
CROSS-DESTINATION DIFFERENTIAL that feeds identical streams to both
destinations and requires equivalent outcomes (same rows, same merge
results, same rejection behavior), making each connector a correctness
net for the other.

**Why this priority**: parity claims without the verification standard
are exactly the "silent lie in the product's contract" 011 exists to
prevent; and the differential oracle hardens BOTH destinations.

**Independent Test**: the matrix has zero uncited rows; the coverage
number is measured and recorded with classified exclusions; the
differential suite runs identical feeds through both destinations and
passes; sweeps run the new arms with armed-fire pins.

**Acceptance Scenarios**:

1. **Given** the option inventory (destination-wide + per-table + scd2
   block), **When** the matrix is built, **Then** every row cites
   behavioral cells (defaults observed, values proven, typed errors
   pinned) — citations to existing suites where they exist, new cells
   only for genuine gaps.
2. **Given** identical keyed/structured feeds (including redeliveries,
   duplicates, deletes, scoped loads), **When** run through postgres
   and DuckDB destinations, **Then** destination-visible outcomes are
   equivalent modulo documented type-system differences, and the
   differential suite pins this.
3. **Given** the crash discipline, **When** the new strategy arms run
   under the crate's fail points, **Then** armed-fire sweeps prove
   crash/rerun convergence for each strategy (the D4 recovery protocol
   extended to the new arms).
4. **Given** the coverage protocol, **When** measured, **Then** the
   crate meets the recorded floor with classified exclusions, baseline
   measured BEFORE new cells (the 011 rules).

---

### Edge Cases

- **Dialect divergence is real, not hidden**: DuckDB has no
  `DISTINCT ON`/`ON CONFLICT` equivalents with identical semantics —
  the shared core owns the SHAPE (dedup ordering, scope delete,
  strategy arms, single-unit rules) and dialects own the SQL; where a
  dialect cannot honor a shape's semantics exactly, that is a typed
  capability gap, never a silent approximation.
- **Single-writer reality**: DuckDB is embedded/single-process; the
  concurrency-adjacent behaviors (session death, staged temp tables)
  follow the existing D4 clauses, and the sweeps prove them for the new
  arms rather than assuming postgres reasoning transfers.
- **SCD2 boundary timestamp**: postgres uses the transaction timestamp;
  DuckDB's equivalent must be pinned to ONE documented source of time
  with redelivery-stable semantics (the 008 receipts/stability rules).
- **dlt parity is recorded, not assumed**: like 010, parity claims
  against dlt's duckdb destination are verified against the pinned dlt
  and deviations are documented individually.
- **Benchmarks**: new duckdb merge cells enter the 012 harness as
  SCOREBOARD cells (declarative TOML, no new gates); every existing
  gated bar must stay green — the shared-core refactor touches the
  postgres hot path's planning code and the bars are the regression
  net for it.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The merge-planning core (strategy arms, dedup/survivor
  shapes, scope replacement, single-commit-unit rules, option
  validation) MUST be shared between the postgres and DuckDB
  destinations, with per-destination dialect hooks owning only SQL
  generation; postgres behavior MUST be provably unchanged (existing
  suites, sweeps, and gated bars green with zero test edits beyond
  mechanical renames).
- **FR-002**: The DuckDB destination MUST accept the SAME destination
  options vocabulary as postgres (`merge_strategy` incl. per-table,
  `hard_delete`, `dedup_sort`, `merge_key`, `scd2` block) with
  identical YAML shape, identical two-layer validation, and identical
  typed-error posture — implementing each option's documented behavior
  where DuckDB semantics permit, and rejecting with a typed
  capability error where they do not (no silent approximations).
- **FR-003**: Behavior MUST be unchanged when options are absent
  (existing duckdb pipelines see zero difference; the 006 keyed-merge
  default remains the default).
- **FR-004**: `Json` columns MUST land as native DuckDB JSON with
  content round-trip, and the capability declaration MUST flip
  accordingly; every remaining false capability MUST be documented.
- **FR-005**: A traceability matrix for the DuckDB destination surface
  MUST exist with zero uncited rows at close-out (011 rules: behavior
  cells, observed defaults, citations over rewrites).
- **FR-006**: A cross-destination differential suite MUST feed
  identical streams (append, replace, keyed merge under each strategy,
  duplicates, deletes, scoped loads, typed-error cases) to both
  destinations and assert equivalent destination-visible outcomes,
  modulo documented type-system differences.
- **FR-007**: The new strategy arms MUST be crash-swept under the
  crate's fail points with armed-fire pins, including crash/rerun
  convergence per strategy and the per-table single-unit rules.
- **FR-008**: Crate line coverage MUST be measured (baseline first),
  reach a recorded floor of ≥ 80%, and classify exclusions (011
  protocol, `make coverage` vocabulary).
- **FR-009**: dlt parity MUST be recorded like 010: each option's
  behavior compared against pinned dlt's duckdb destination, with
  deviations documented individually.
- **FR-010**: Governance unchanged: zero engine-SPI change (semver
  "no update required" for rdlt-core/rdlt-connector), WriteMode
  vocabulary frozen, zero new runtime dependencies, every existing
  gated benchmark bar within tolerance; new duckdb cells are
  scoreboard-only via the 012 harness.

### Key Entities

- **Shared merge core**: the destination-agnostic planning layer —
  strategy arms, dedup/survivor shapes, scope replacement, validation,
  single-unit rules — consumed by both destinations.
- **Dialect**: a destination's SQL generation for the shared shapes
  (postgres dialect = today's SQL, extracted; duckdb dialect = new).
- **Capability gap (typed)**: a shape a dialect cannot honor exactly —
  surfaces as a typed error naming the option and destination.
- **Differential oracle**: the identical-feed, equivalent-outcome
  cross-destination suite.
- **Traceability matrix / coverage record**: as in 011, scoped to the
  DuckDB destination surface.

## Success Criteria *(mandatory)*

- **SC-001**: The 008/010 destination-options documentation applies to
  DuckDB by swapping the destination block; every documented option
  behaves as documented or fails typed — proven by the matrix with
  zero uncited rows.
- **SC-002**: The differential suite passes: identical feeds produce
  equivalent outcomes on both destinations across strategies, options,
  and rejection cases.
- **SC-003**: Json → native DuckDB JSON round-trips; the capability
  matrix row flips with proof.
- **SC-004**: Postgres is provably untouched: its suites and sweeps
  green without behavioral edits, semver "no update required", and
  every gated bar within tolerance after the shared-core extraction.
- **SC-005**: DuckDB crate coverage ≥ 80% measured and recorded with
  classified exclusions; new strategy arms crash-swept with
  armed-fire pins.
- **SC-006**: dlt parity record exists with individually documented
  deviations; new duckdb merge scoreboard cells run under the 012
  harness with committed artifacts.

## Assumptions

- "DuckDB surface" = the destination only; a DuckDB source is out of
  scope (the naming pattern keeps it possible later).
- The shared core lives where planning decides (likely a module of the
  connector family, NOT in rdlt-core/rdlt-connector — the SPI stays
  frozen); its extraction is behavior-preserving refactoring of the
  postgres dest proven by existing nets, not a rewrite.
- Equivalence in the differential oracle is defined modulo documented
  type-system differences (e.g. numeric affinities), each recorded.
- The dlt pin for parity comparison is the benchmark module's pin
  (dlt 1.29.0) unless bumped under the version policy first.

## Out of Scope

- A DuckDB source; MotherDuck / remote DuckDB.
- TLS/mTLS, conn-string portability (no network surface), CDC (no WAL).
- New engine WriteMode vocabulary or any rdlt-core/rdlt-connector
  change.
- Preemptive crate modularization beyond what the strategy code
  actually needs (mirror 008: split when the code arrives).
- Gated benchmark bars for the new duckdb cells (scoreboard first;
  bars are a later governance decision under the 004 rules).
- Parity with dlt duckdb features rdlt lacks EVERYWHERE (e.g. staging
  datasets) — future features, not scope creep here.
