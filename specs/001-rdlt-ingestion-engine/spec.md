# Feature Specification: rdlt — Data Ingestion Engine Library

**Feature Branch**: `001-rdlt-ingestion-engine`

**Created**: 2026-07-19

**Status**: Draft

**Input**: User description: "I am working on rust based modern and performant dlt like ingestion engine/library"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - First full sync from source to destination (Priority: P1)

A pipeline developer embeds the library in their application, points a supported source
(e.g. a paginated web API) at a supported analytical destination, and runs the pipeline.
Raw, semi-structured records — including nested objects and lists — arrive in the
destination as well-typed, queryable tables without the developer declaring any schema up
front. Nested lists become linked child tables; every row carries lineage identifiers that
tie it back to the run and its parent record.

**Why this priority**: This is the product. A single successful source→destination sync
with automatic typing and nesting is the minimum thing anyone would adopt the library for,
and every other story builds on it.

**Independent Test**: Run a pipeline against an in-memory source with nested sample
records and an in-memory destination; verify tables, inferred types, child-table linkage,
and lineage columns — no network or external services needed.

**Acceptance Scenarios**:

1. **Given** a source emitting records with scalars, nested objects, and lists of objects,
   **When** the pipeline runs to completion, **Then** the destination contains a root table
   and child tables with correct types, and every child row references its parent and root
   row.
2. **Given** records whose values for one field vary in type across rows (e.g. integer then
   text), **When** the pipeline runs, **Then** the field is widened to a single common type
   without data loss and without creating duplicate variant columns.
3. **Given** a value that cannot be represented in any typed column without loss, **When**
   the pipeline runs, **Then** the value is preserved verbatim in a raw/JSON-typed column —
   never dropped, never corrupted.

---

### User Story 2 - Incremental sync (Priority: P2)

On the second and subsequent runs, the pipeline moves only data that is new or changed
since the last successful run. The developer chooses per-stream whether new data is
appended, replaces the table, or is merged by key (updates overwrite prior versions,
including their nested child rows).

**Why this priority**: Without incrementality every sync is a full reload; recurring
pipelines — the dominant real-world use — are impractical. Depends on P1.

**Independent Test**: Run twice against a source that records which portion of its data was
requested; verify the second run requests only data after the first run's committed
position, and that merge mode replaces updated records and their child rows exactly.

**Acceptance Scenarios**:

1. **Given** a completed first run, **When** a second run starts, **Then** the source is
   asked to resume from the last committed position, not from the beginning.
2. **Given** merge mode keyed on a record identifier, **When** an updated version of a
   record with different nested children arrives, **Then** the destination holds exactly
   the new version — old child rows are gone, new ones present.

---

### User Story 3 - Crash-safe, resumable runs (Priority: P3)

A run is killed at an arbitrary point — process crash, cancellation, machine loss. When
the pipeline is started again it resumes from the last durable position and finishes. The
destination never exposes partial data from an uncommitted portion of a run, never
double-counts rows, and never skips a range of source data.

**Why this priority**: Correctness under failure is the trust foundation for unattended
operation; a platform cannot be built on an engine that duplicates or loses data when
interrupted. Depends on P1/P2.

**Independent Test**: A fault-injection harness kills the run at every distinct stage of
processing, restarts it, and asserts the destination's final content is identical to an
uninterrupted run — byte-for-byte, including lineage and committed positions.

**Acceptance Scenarios**:

1. **Given** a run killed after data was durably buffered locally but before publication,
   **When** the pipeline restarts, **Then** buffered work is replayed without re-reading
   the source and the final tables match an uninterrupted run.
2. **Given** a run killed mid-publication, **When** the pipeline restarts, **Then** the
   publication completes exactly once (the destination recognizes the repeated attempt).
3. **Given** the local work directory is completely lost, **When** the pipeline restarts,
   **Then** it re-reads only from the last committed source position and the outcome is
   still correct (slower, never wrong).

---

### User Story 4 - Schema evolution under policy (Priority: P4)

Upstream data changes shape mid-stream — new fields appear, types widen. By default the
destination schema evolves automatically and the run continues. Where stability matters,
the developer sets a per-table or per-column policy: freeze (fail fast with an actionable
error before any bad data lands), discard offending rows, or discard offending values —
with every discard counted and reported, never silent.

**Why this priority**: Real sources drift; how drift is handled is a top adoption
criterion. Evolve-by-default already works via P1's widening — this story adds the policy
controls.

**Independent Test**: Feed a stream that changes shape mid-run under each policy; verify
evolved schema, or typed failure before any write, or exact discard counts in the final
report.

**Acceptance Scenarios**:

1. **Given** the default evolve policy, **When** a new field appears mid-run, **Then** the
   destination gains the column and previously loaded rows read as null for it.
2. **Given** a frozen table, **When** an incompatible change arrives, **Then** the run
   fails with an error naming the table, column, and offending change, and no row of the
   violating batch was published.
3. **Given** a discard policy, **When** non-conforming rows arrive, **Then** conforming
   data loads and the final report states exactly how many rows/values were discarded and
   where.

---

### User Story 5 - Observable runs and verifiable connectors (Priority: P5)

An operator (or the future rapidbyte platform) watches a run through a typed event stream
— stream started, batch loaded, schema evolved, position committed — and receives a final
machine-readable report: rows and bytes per table, schema changes applied, discards,
committed positions, whether the run resumed, and elapsed time. A connector author builds a
new source or destination against the public integration contract and certifies it with the
provided conformance test suite instead of reverse-engineering engine behavior.

**Why this priority**: Programmatic observability and a certification story are what make
the library a platform foundation rather than a script; they can land after the engine
semantics above are proven.

**Independent Test**: Run a pipeline and assert the event sequence and report contents
match the data actually moved; run the conformance suite against a deliberately
non-compliant connector and verify it fails with actionable diagnostics.

**Acceptance Scenarios**:

1. **Given** a running pipeline, **When** batches load and schemas evolve, **Then**
   corresponding typed events are emitted in order and the final report totals match the
   destination's actual contents.
2. **Given** a connector that violates the resume-from-position contract, **When** the
   conformance suite runs, **Then** it fails with a message identifying the violated
   guarantee.

---

### Edge Cases

- Integer values too large to fit a floating-point representation exactly: widening must
  detect the precision hazard per value and escalate to a lossless textual type rather
  than silently rounding.
- Two distinct nested field names that collide after flattening for a destination:
  disambiguated deterministically; distinct source fields never silently merge.
- A source that cannot rewind (queue/webhook): after a crash, redelivered records must not
  produce duplicates in merge mode (deterministic row identity dedups them).
- Identical records with no declared key in merge mode: collapse to one row — documented
  dedup behavior, not data loss.
- Destination unavailable mid-run: the run fails with a destination-classified error;
  already-committed data stays intact; a later run resumes normally.
- A slow destination: extraction slows to match (bounded buffering); memory stays capped
  regardless of source speed or row width.
- Cancellation mid-run: treated identically to a crash — one recovery path, same
  guarantees.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST move data from a source connector to a destination connector
  as a series of streams, each landing in its own destination table (plus derived child
  tables), with exactly one stream owning any given table per run.
- **FR-002**: The system MUST infer column types from observed data without requiring an
  up-front schema, and MUST widen types only along a defined, order-insensitive lattice in
  which every widening is verifiably lossless (value-checked, with escalation to a textual
  or raw type when exactness cannot be preserved).
- **FR-003**: The system MUST preserve nested structures: nested objects retain structure
  where the destination supports it (flattened deterministically and collision-safely where
  not); lists of objects become child tables linked to their parent and root rows.
- **FR-004**: Every loaded row MUST carry lineage: the run that loaded it, a deterministic
  row identity, and — for child rows — parent and root identities at every nesting depth,
  sufficient to merge or trace any subtree without recursive lookups.
- **FR-005**: The system MUST support three write modes per stream: append, full replace,
  and merge by key; merge MUST replace an updated record's entire subtree of child rows.
- **FR-006**: The system MUST support incremental extraction: sources are given the last
  committed position and MUST NOT be asked to re-read completed ranges in normal operation.
- **FR-007**: Data visibility in the destination MUST be exactly-once: a reader of the
  destination never observes partial, duplicated, or skipped data from any run, regardless
  of crashes, restarts, or cancellation at any point.
- **FR-008**: Source positions MUST be committed atomically with the data they cover, and
  recorded in the destination itself, so correctness survives total loss of any local
  working state.
- **FR-009**: After interruption, a restarted run MUST resume: replaying locally buffered
  work when available (without re-reading the source), otherwise re-extracting from the
  last committed position; repeated publication attempts MUST be recognized and not
  double-applied.
- **FR-010**: Schema change handling MUST be policy-driven per table and column: evolve
  (default), freeze (typed, actionable failure before any violating data is published),
  discard row, or discard value; all discards MUST be counted and reported.
- **FR-011**: Every schema version MUST be identifiable and every schema change auditable
  as a delta from one identified version to another.
- **FR-012**: The system MUST expose a typed event stream during runs and produce a final
  machine-readable report (per-table rows/bytes, schema changes, discard counts, committed
  positions, resume indicator, elapsed time) whose totals match destination contents; there
  MUST be no silent failures — retries, widenings, and discards all surface in it.
- **FR-013**: Errors MUST be classified by required operator action (configuration,
  contract violation, source-side, destination-side, local storage), naming the affected
  stream/table where applicable.
- **FR-014**: Transient source and destination failures MUST be retried by the engine with
  backoff (honoring rate-limit hints); connectors MUST NOT need their own retry logic;
  non-transient failures MUST abort with a classified error.
- **FR-015**: Memory use MUST remain bounded and configurable regardless of source volume,
  row width, or destination speed; a slow stage slows upstream stages rather than growing
  buffers.
- **FR-016**: The system MUST ship a public conformance test suite that certifies
  third-party connectors against the documented contract (resume behavior, ordering,
  idempotent publication, crash-safe staging), and the bundled connectors MUST pass it.
- **FR-017**: The library MUST be embeddable: no owned daemon, scheduler, or UI; all
  capability available programmatically, with a thin command-line wrapper for development
  use.
- **FR-018**: v1 MUST include a configurable web-API source and two relational/analytical
  destinations (one embedded, one server-based), all built on the same public connector
  contract as third-party connectors — no privileged internal access.

### Key Entities

- **Pipeline**: A named, repeatable unit of data movement binding one source, one
  destination, per-stream write modes, and schema policies; owns its run history.
- **Stream**: A named sequence of records offered by a source (e.g. one API collection);
  maps to one root table and its child tables.
- **Table schema (versioned)**: The typed shape of a destination table; each version is
  content-identified; changes exist only as auditable deltas between versions.
- **Position (cursor)**: A source-defined marker of extraction progress; the unit of
  incremental resume; committed atomically with covered data.
- **Run / Run report**: One execution of a pipeline and its machine-readable outcome
  (volumes, schema changes, discards, positions, resume status, duration).
- **Commit unit**: The atomic publication of a span of data together with the positions
  and schema versions it covers; repeat attempts are recognized by identity.
- **Lineage identity**: Deterministic per-row identifiers (row, parent, root, run) that
  make merge, dedup, and subtree tracing possible without recursive lookups.
- **Connector (source / destination)**: A third-party-implementable component satisfying
  the public contract; destinations additionally declare capabilities (merge support, type
  matrix, identifier rules, nesting support) that the engine plans around.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer new to the library gets a first successful source→destination
  sync — nested data, no schema declared — in under 30 minutes using only public
  documentation.
- **SC-002**: The crash-injection suite kills runs at every distinct processing stage
  (100% stage coverage); in every case the restarted run converges to a destination state
  identical to an uninterrupted run — zero duplicated, lost, or partially visible rows.
- **SC-003**: On the reference nested-data workload, end-to-end file-to-warehouse
  ingestion completes at least 10× faster than the incumbent Python-based tool on
  identical hardware and data, measured by a one-command reproducible harness with the
  baseline measured first.
- **SC-004**: Peak memory during the reference workload is at most one-fifth of the
  incumbent baseline and stays flat (±10%) as input volume grows 100×.
- **SC-005**: The engine-bound API-to-database benchmark sustains at least 5× the
  incumbent's throughput; pass-through of already-structured data at least 2×; results
  published with methodology.
- **SC-006**: Property-based tests of the typing rules (order-insensitivity, no lossy
  widening, deterministic row identity, collision-safe naming) pass across the full
  generated input space; any counterexample is a release blocker.
- **SC-007**: 100% of bundled connectors pass the public conformance suite in CI; a
  deliberately non-compliant test connector fails it with a diagnostic naming the violated
  guarantee.
- **SC-008**: Every run ends in either a success report or a classified, actionable error;
  zero silent failure modes — audited by fault-injection review of the final report against
  injected faults (discards, retries, widenings all accounted for).

## Assumptions

- The library is the foundation for a future multi-connector platform (rapidbyte);
  platform concerns — scheduling, multi-tenancy, process isolation, secrets management,
  catalog UI, user auth — are explicitly out of scope for the engine.
- v1 connector scope: one declaratively configured web-API source; two destinations (one
  embedded analytical database, one server-based relational database). Additional SQL
  sources, change-data-capture, sandboxed/out-of-process connectors, and language bindings
  are deferred.
- One process runs one pipeline; isolation between pipelines is the embedder's concern.
- Sources are either resumable from a position or tolerate a bounded redelivery window
  after failure (mitigated by frequent commits and merge-mode dedup).
- Destination systems provide atomic publication primitives (transactions or equivalent
  staged-swap), which the exactly-once visibility guarantee builds on.
- Performance claims are always relative to a pinned version of the incumbent tool,
  measured on the same hardware and datasets via the published harness.
- An approved technical design exists (`2026-07-18-rdlt-engine-design.md`) covering
  architecture, contracts, and semantics; this specification states the product
  requirements that design satisfies.
