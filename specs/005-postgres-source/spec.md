# Feature Specification: Postgres SQL Source Connector

**Feature Branch**: `005-postgres-source`

**Created**: 2026-07-20

**Status**: Draft

**Input**: User description: "Postgres SQL source connector (005): snapshot + incremental (cursor-column) reads from Postgres tables/queries as a new rdlt source, informed by a review of how Python dlt's sql_database source does it (~/Repos/rapidbyte/dlt) but designed with Rust idioms and best practices, not a port; super performant (binary protocol, streaming, backpressure-aware); extend the benchmark matrix with postgres→duckdb and postgres→postgres cells vs pinned dlt; thorough test + crash-test coverage to the 003 hardening standard, 100% robustness"

> **Baseline review**: the capability surface and lessons below are grounded in
> a code review of dlt's `sql_database` source (dlt/sources/sql_database/,
> reviewed 2026-07-20). Three findings shape this spec: (1) dlt's fast path
> (its "pyarrow backend", documented 20–30× over its own default) wins by
> skipping its JSON normalizer, while extraction itself remains row-by-row —
> so the genuine performance lever is producing columnar batches directly
> from the database wire format; (2) dlt's incremental boundary semantics
> (open/closed ranges, NULL cursor handling, closed-boundary re-fetch with
> deduplication) are well designed and this spec adopts them as the parity
> bar; (3) dlt ships three robustness gaps this feature explicitly beats:
> no retry/reconnect, no mid-table resume, and no consistent snapshot per
> table run. This is a reimagining under rdlt's architecture, not a port.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Snapshot a Postgres database into a destination (Priority: P1) 🎯 MVP

A data engineer points rdlt at a Postgres database (connection string +
a list of tables, or "all tables in a schema"), runs a pipeline to DuckDB,
Postgres, or Parquet, and gets every selected table replicated with
faithful types — numbers stay numbers with their precision, timestamps
keep their timezone semantics, and the destination tables match what the
source schema declared. No per-table configuration is required for the
common case; the source discovers table structure itself.

**Why this priority**: reading databases is the single largest source
class for an ELT platform, and rdlt currently cannot read any database.
A working snapshot path is the minimum that delivers value and completes
the flagship platform demo (Postgres → DuckDB). Everything else layers
on it.

**Independent Test**: seed a Postgres instance with tables covering the
supported type matrix; run a snapshot pipeline into DuckDB; verify row
counts, column types, and values match the source exactly.

**Acceptance Scenarios**:

1. **Given** a Postgres database with populated tables, **When** the user
   configures the source with just a connection string and table names,
   **Then** the pipeline replicates every selected table's rows and
   declared column types to the destination without manual schema input.
2. **Given** a table selection by schema (no explicit table list),
   **When** the pipeline runs, **Then** all tables in that schema are
   discovered and loaded, and the discovery cost is bounded (discovery
   happens once per run, not per batch).
3. **Given** a source column whose type has no faithful destination
   representation, **When** the pipeline runs, **Then** the value is
   either converted by a documented, tested rule or the row/value is
   handled per the pipeline's existing schema policy — never silently
   corrupted.
4. **Given** a large table (≥ 10× available memory), **When** it is
   snapshotted, **Then** the run completes with bounded memory (streamed
   in batches end-to-end, never materializing the table).
5. **Given** each selected table, **When** its rows are read, **Then**
   all rows of that table reflect a single consistent point-in-time view
   of the table (a concurrent writer cannot cause a torn read within one
   table's extraction).

---

### User Story 2 - Incremental sync on a cursor column (Priority: P2)

A data engineer marks a column (e.g. `updated_at`, `id`) as the cursor
for a table. The first run loads everything; every later run loads only
rows whose cursor value advanced past the stored watermark, and the
watermark survives restarts because it lives in rdlt's existing pipeline
state. Boundary rows are never lost and never double-applied.

**Why this priority**: incremental loading is what makes database syncs
operationally viable (hourly syncs of large tables). It builds directly
on the snapshot path and rdlt's existing checkpoint/state machinery.

**Independent Test**: run once, insert/update rows around the boundary
value (including duplicates of the exact watermark), run again; verify
exactly the new rows land, dedup at the closed boundary works, and the
watermark in state matches the max cursor value seen.

**Acceptance Scenarios**:

1. **Given** a completed initial run with cursor column configured,
   **When** new rows with higher cursor values are inserted and the
   pipeline re-runs, **Then** only those rows are fetched and loaded.
2. **Given** rows that share the exact watermark value across two runs,
   **When** the closed-boundary (≥) default re-fetches them, **Then**
   the engine deduplicates so no row is applied twice (dlt-parity
   semantics), and an open-boundary (>) opt-out exists for strictly
   monotonic cursors.
3. **Given** rows whose cursor column is NULL, **When** the run executes,
   **Then** the configured NULL policy (include or exclude) is applied
   and recorded — never an undefined behavior.
4. **Given** a crash or cancellation mid-run, **When** the pipeline is
   re-run, **Then** it resumes from the last committed checkpoint with
   zero lost and zero duplicated rows (the 003 crash-matrix guarantee
   extended to this source).
5. **Given** an incremental stream with a merge key, **When** updated
   rows are re-fetched, **Then** the existing Merge write mode applies
   them as upserts.

---

### User Story 3 - Performance proven against the pinned baseline (Priority: P3)

A maintainer extends the benchmark matrix with two new cells —
Postgres → DuckDB and Postgres → Postgres — measured baseline-first
against pinned Python dlt reading the same Postgres instance, and records
honest multiples with the same discipline (version policy, gated vs
scoreboard, evidence artifacts) established by features 003/004.

**Why this priority**: "super performant" is a claim; the matrix is how
this project makes claims. The 004 close-out established that bars are
set from measurement, not aspiration — these cells follow that protocol
from day one.

**Independent Test**: run the extended harness; both new rows appear
with same-session baseline-first pairs, dataset identity recorded, and
gated bars whose derivation links committed evidence.

**Acceptance Scenarios**:

1. **Given** the benchmark harness, **When** the new cells run, **Then**
   dlt is measured FIRST on the same Postgres data, using its fastest
   documented configuration for this job as the gated baseline (its
   default configuration may be reported as an additional scoreboard
   row, and its Rust-powered reader backend as scoreboard context).
2. **Given** the measured baseline, **When** the gated bars are set,
   **Then** they follow the 004 protocol: measurement first, headroom
   explicit, recorded in the version policy with evidence links —
   never a bar invented before both sides are measured.
3. **Given** the new cells, **When** the full matrix re-measures, **Then**
   existing gated cells stay within the regression gate's tolerance
   (adding a source must not regress the engine hot paths).

---

### User Story 4 - Robust under failure, proven by crash testing (Priority: P4)

An operator can kill the pipeline at any point — mid-fetch, mid-commit,
between tables, during watermark persistence — or lose the database
connection, and the next run recovers to a correct state: no lost rows,
no duplicates, no silent partial data, no stuck state. Failures surface
as typed, actionable errors.

**Why this priority**: it is the project's "no silent failures"
principle applied to the new connector; priority P4 only because the
crash-sweep infrastructure (fail-point registry, sweep suites, WAL
recovery tests) already exists from 003 and this story extends it rather
than inventing it. It is a release gate for the feature regardless of
priority order.

**Independent Test**: a crash sweep over registered fail points in the
source's read/checkpoint path (including a second-occurrence pass, per
the 003 lesson), plus forced connection drops mid-stream; every sweep
case ends with a verified-correct destination after re-run.

**Acceptance Scenarios**:

1. **Given** any registered fail point in the source path, **When** the
   process is killed there and the pipeline re-runs, **Then** the
   destination converges to exactly-once results (crash matrix green).
2. **Given** a connection loss mid-table, **When** the run fails, **Then**
   the error is a typed source error naming the table and phase, the
   already-committed work is preserved, and re-run resumes correctly.
3. **Given** transient connection failures, **When** the source connects
   (or reconnects at a safe boundary), **Then** a bounded, configured
   retry policy applies — and mid-stream failures are never retried in
   a way that could double-apply rows.
4. **Given** schema drift between discovery and read (column added,
   dropped, or retyped mid-run), **Then** the run either applies the
   pipeline's schema policy or fails with a typed error — never loads
   misaligned data.

---

### Edge Cases

- Empty tables, tables with only NULL cursor values, and tables whose
  cursor column is not indexed (correctness unaffected; document the
  performance caveat).
- Numeric precision beyond the destination's native range (e.g.
  arbitrary-precision NUMERIC): lossless rule or policy-driven handling,
  documented and tested — never silent truncation.
- Postgres-specific values with no universal representation: infinite
  timestamps/dates, arrays, enums, JSON/JSONB columns, UUIDs,
  mixed-case/quoted identifiers, non-default schemas.
- JSON/JSONB columns may carry arbitrary nested documents — decide and
  test whether they flow as opaque JSON values (default) with shredding
  as an explicit opt-in, or shred by default (consistency with the file
  source's behavior matters more than either choice).
- A table dropped or renamed between discovery and read; a table with
  zero user-visible columns after exclusions.
- Cursor values that regress (clock skew on `updated_at`): watermark
  must never move backward; the behavior is defined and tested.
- Very wide tables (hundreds of columns) and very narrow ones (one
  column); batch sizing must adapt so memory stays bounded in both.
- The pinned dlt version for the new baseline cells: pin and record in
  the version policy at first measurement, consistent with the existing
  matrix pin (dlt 1.29.0 unless bumped by its own policy event).
- Cancellation (SIGINT / engine cancel) mid-stream: same convergence
  guarantee as crashes.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST provide a Postgres source connector,
  configurable declaratively (connection + stream selection), exposing
  one stream per selected table; selection MUST support explicit table
  lists and schema-wide discovery, and MAY support views.
- **FR-002**: The source MUST discover table structure (columns, declared
  types, nullability, primary keys) from the database itself, once per
  run, and publish it as the stream's schema so destinations receive
  declared types rather than inferred ones.
- **FR-003**: The source MUST stream rows as typed columnar batches on
  the engine's structured path (the same path the parquet source uses),
  with a documented mapping from Postgres types to the engine's logical
  types; values with no faithful mapping MUST follow a documented rule or
  the pipeline's existing schema policies — silent corruption is
  forbidden.
- **FR-004**: Extraction MUST be streaming and backpressure-aware
  end-to-end: bounded batch sizes, bounded in-flight memory independent
  of table size, and no full-table materialization anywhere.
- **FR-005**: Each table's extraction MUST present a single consistent
  point-in-time view of that table. (Cross-table consistency of one run
  is OUT of v1 scope; see Assumptions.)
- **FR-006**: The source MUST support incremental reads on a configured
  cursor column with dlt-parity boundary semantics: closed (≥, default)
  or open (>) lower bound; optional upper bound; max or min direction;
  NULL-cursor include/exclude policy; closed-boundary re-fetch
  deduplicated via primary key (or configured key). The watermark MUST
  persist in the engine's existing pipeline state and survive restarts.
- **FR-007**: The watermark MUST never move backward, MUST only advance
  on committed loads, and crash/cancel at any point MUST leave the
  pipeline resumable with zero lost and zero duplicated rows (003
  crash-matrix guarantee).
- **FR-008**: Connection establishment MUST support a bounded retry
  policy; mid-stream failures MUST surface as typed errors naming table
  and phase, and MUST NOT auto-retry in a way that could double-apply
  rows. All failure modes MUST be typed `SourceError`s — no panics, no
  silent skips.
- **FR-009**: The source MUST register fail points in its read and
  checkpoint paths in the existing crash-sweep registry, and the sweep
  suites (including the second-occurrence pass) MUST cover them.
- **FR-010**: The benchmark harness MUST gain Postgres → DuckDB and
  Postgres → Postgres cells: baseline-first against pinned dlt on
  identical data (dataset identity recorded), dlt measured in its
  fastest documented configuration as the gated baseline, with gated
  bars set measurement-first per the 004 version-policy protocol; the
  new cells MUST carry explicit gated/scoreboard status.
- **FR-011**: Adding the source MUST NOT regress any existing gated
  benchmark criterion beyond the armed gate's tolerance, and the full
  existing verification suite MUST remain green.
- **FR-012**: The connector MUST live behind the existing connector SPI
  with no public-API breakage to `rdlt-core`/`rdlt-connector`; any
  unavoidable SPI change is a recorded semver event justified in the
  plan. The workspace memory-safety policy stands: no new unsafe-code
  exceptions.

### Key Entities

- **Postgres source stream**: one selected table (or discovered table);
  carries the reflected schema, optional cursor configuration, and
  key/merge configuration.
- **Reflected table schema**: the declared structure discovered from the
  database — columns, types, nullability, primary key — the authority
  for the stream's published schema.
- **Type mapping**: the documented correspondence from source database
  types to engine logical types, including the explicit list of lossy or
  policy-driven cases.
- **Cursor watermark**: the per-stream incremental position stored in
  pipeline state; advances only on commit; never regresses.
- **Benchmark cells (new)**: Postgres → DuckDB and Postgres → Postgres
  rows in the matrix, each a baseline-first measured pair with recorded
  dataset identity and gated/scoreboard status.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A user can replicate a multi-table Postgres database to
  DuckDB with a configuration naming only the connection and the tables
  (or schema), and the destination matches the source: row counts equal,
  values equal under the documented type mapping, for the full supported
  type matrix.
- **SC-002**: A table at least 10× larger than available memory
  snapshots successfully with peak memory bounded by configuration (not
  by table size) — demonstrated in the test suite with an enforced
  memory ceiling.
- **SC-003**: Incremental runs fetch only rows past the watermark; the
  boundary tests (equal-watermark duplicates, NULL cursors, regressing
  clocks, open vs closed) all pass; after any crash-sweep interruption
  the re-run converges to exactly-once results — 100% of registered
  fail points green, both sweep passes.
- **SC-004**: The two new benchmark rows exist with same-session
  baseline-first pairs against pinned dlt, dataset identity recorded,
  gated bars set measurement-first with version-policy entries linking
  evidence — zero cells with aspirational (unmeasured) bars.
- **SC-005**: The full verification suite (existing tests + new
  conformance, property, and sweep suites; lint; doc-tests; perf gate)
  is green at feature close; no existing gated benchmark regresses
  beyond gate tolerance.
- **SC-006**: The feature's records pass the 004-style traceability
  walk: every claim in the matrix and design doc about this source
  resolves to committed evidence with no contradictions.

## Assumptions

- **Postgres first, dialect seam later**: only Postgres is in scope;
  the design should avoid hard-coding Postgres where a clean seam is
  free, but no second database is implemented or tested in v1.
- **Tables and views only**: custom SQL queries per stream are out of
  v1 scope (dlt's query-adapter escape hatch is noted for the backlog).
- **CDC / logical replication is out of scope** (design-doc stance
  unchanged); cursor-column incremental is the v1 mechanism.
- **Cross-table snapshot consistency is out of v1 scope**: each table
  is internally consistent (FR-005); a shared snapshot across all
  tables of a run — which dlt does not offer either — is recorded as a
  deliberate deferral and backlog candidate, not silently ignored.
- **Cursor is a real column** (no computed/JSON-path cursors in v1),
  with max (ascending) as the default direction; custom aggregation
  functions (dlt's `last_value_func` beyond max/min) are out of scope.
- **Auth**: connection-string authentication including TLS as supported
  by the underlying driver; enterprise auth schemes (Kerberos, IAM
  tokens) are out of scope.
- **Baseline configuration**: "dlt's fastest documented configuration"
  means its arrow-producing backend as the gated baseline; its
  Rust-powered reader backend (connectorx) is reported as a scoreboard
  row where feasible — beating another Rust reader is context, not a
  gate. Pin per the existing version policy.
- **Benchmark datasets**: defined in the harness at measurement time
  with recorded identity (row count + content hash), covering at least
  one wide typed table and one JSONB-bearing table, sized comparably to
  the existing cells (10⁵–10⁶ rows).
- **Existing machinery is reused, not duplicated**: pipeline state and
  checkpoints, write modes (Append/Replace/Merge), schema policies,
  WAL/recovery, the testkit, and the crash-sweep registry.
