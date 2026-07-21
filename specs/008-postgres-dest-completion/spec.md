# Feature Specification: Postgres Destination Completion

**Feature Branch**: `008-postgres-dest-completion`

**Created**: 2026-07-21

**Status**: Draft

**Input**: User description: "Postgres destination completion (008): make the postgres destination feature-complete and performant, informed by a full dlt postgres-destination inventory. Modularize the destination first — dest/ currently one 613-line mod.rs — into a source-mirroring layout (config, DDL/type-mapping, COPY encoding, commit/merge modules) as pure relocation. Type fidelity (the biggest gap): land decimals as native NUMERIC(p,s) instead of engine-lowered TEXT (flip the decimal capability, binary-COPY numeric wire encoding mirroring the source's decoder), JSON as JSONB (flip json_type), UUID as native uuid, and honor NOT NULL in created tables; only-additive migration rules stay, with documented behavior for pre-existing TEXT columns. Merge power and performance: keep atomic delete-insert as the default, add an upsert strategy (ON CONFLICT DO UPDATE on the merge key, with the required unique index created automatically), create supporting indexes for merge paths generally (today's DELETE..IN scans an unindexed target — measured before/after), and a hard-delete column option (rows flagged in a configured column delete instead of upsert). SCD2 as a distinct merge strategy (validity from/to columns, retire-changed-and-absent then insert-active, destination-side config) — dlt parity's last big block. Also close the review-F6 debt: destination error helpers must render source chains (server message + SQLSTATE) like the rest of the crate. Replace stays atomic truncate+insert-from-stage in one transaction (already stronger than dlt's default); an optional drop-and-swap strategy for very large replaces may be considered but is not required. Strategy selection is destination-side configuration — zero rdlt-core/rdlt-connector SPI changes (WriteMode stays frozen). Benchmarks: pg→pg gated bars must stay within tolerance; add merge-heavy and upsert scoreboard measurements. Explicitly OUT: PostGIS/geometry, external object-storage staging, csv/parquet loader formats (Arrow-native COPY only), parallel load jobs (engine-level future), foreign-key constraints."

## Context

The 008 audit inventoried dlt's postgres destination end to end and compared
it with ours. Where we already lead — and must not regress: bulk loading goes
through the database's fastest wire path into staging tables with a single
atomic publish transaction (dlt's default is large INSERT statements),
delivery is exactly-once under a crash model proven by sweeps, and the
connection layer carries the full TLS/mTLS matrix. The genuine gaps this
feature closes: values that HAVE precise database types today land as plain
text (decimals, JSON documents, UUIDs — a warehouse consumer sees strings
where dlt's users get `numeric`, `jsonb`); merge knows only one strategy
(delete-then-insert) while dlt offers upsert, insert-only, SCD2 history
tracking, hard-delete flags, and dedup ordering; merge deletes scan an
unindexed target table (a real performance cliff at scale — dlt at least
creates a unique index on its row identity); created tables ignore
nullability; and destination-side error messages still drop the server's own
explanation (the one review-F6 debt item). The destination also remains a
single 613-line file, which was the original motivation for this feature.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Values keep their database types (Priority: P1)

A data engineer syncs a table with money amounts (decimal), JSON documents,
and UUID identifiers into Postgres. Downstream consumers — BI tools, SQL
analysts, other services — find `numeric(p,s)`, `jsonb`, and `uuid` columns
they can aggregate, index, and query with native operators, instead of text
they must cast on every read. Columns the source declared as required are
created NOT NULL.

**Why this priority**: this is the largest fidelity gap versus every serious
warehouse loader; text-typed numbers silently break downstream SUM/AVG and
JSON path queries. It changes what every future load produces, so it lands
first.

**Independent Test**: a round trip of decimal/JSON/UUID/required columns from
a Postgres source through the pipeline into a Postgres destination yields
native column types with exact values, verified by querying the destination's
catalog and aggregating loaded values.

**Acceptance Scenarios**:

1. **Given** a stream with a decimal(12,4) column, **When** it loads, **Then**
   the destination column is numeric with matching precision/scale and
   SUM/AVG over loaded rows equals the source's exactly (no float detour,
   no text).
2. **Given** JSON-typed values, **Then** the destination column is the
   database's binary JSON type and a JSON path query returns the expected
   fields.
3. **Given** UUID values, **Then** the destination column is the native uuid
   type and equality joins against uuid literals work.
4. **Given** a source column declared non-nullable, **Then** the created
   destination column is NOT NULL; nullable columns stay nullable.
5. **Given** a table created by an EARLIER rdlt version with text columns
   where native types would now be chosen, **When** new loads arrive,
   **Then** behavior follows the documented additive-only migration rule —
   existing columns are never silently retyped, the situation is visible,
   and the documented path to native types (fresh table or explicit
   migration) works.
6. **Given** values at the edges (decimal precision limits, deeply nested
   JSON, NULLs in every new type), **Then** round-trip equality holds — the
   full type matrix conformance extends to the new native types.

---

### User Story 2 - Merge strategies: upsert, hard delete, and speed (Priority: P2)

A data engineer with a large frequently-updated table chooses an UPSERT
strategy for merge: matched keys update in place, new keys insert — without
the full delete window of delete-insert. The unique index the strategy needs
is created automatically. A CDC-shaped feed marks deletions in a column; rows
so flagged are deleted at the destination instead of upserted. Merge on large
tables stops scanning: supporting indexes exist for the merge identity, and
the before/after difference is measured.

**Why this priority**: merge is the production write mode; today's single
strategy has a delete-visibility window and unindexed scans that hurt at
scale. This story is both capability (upsert, hard delete) and measured
performance.

**Independent Test**: update-heavy merge under the upsert strategy converges
to one row per key with newest values; flagged rows disappear; a
large-table merge shows a measured improvement over the unindexed baseline.

**Acceptance Scenarios**:

1. **Given** merge with the upsert strategy and a declared key, **When**
   updated and new rows load, **Then** matched keys are updated in place,
   new keys inserted, totals exact, re-runs idempotent (exactly-once
   semantics preserved under the crash model).
2. **Given** the upsert strategy on a table without the required unique
   index, **Then** the index is created automatically at table-ensure time;
   pre-existing tables where the index cannot be created (duplicate keys
   already present) fail with a typed error naming the conflict.
3. **Given** a configured hard-delete column, **When** a batch carries rows
   flagged deleted, **Then** those keys are removed at the destination and
   NOT re-inserted; unflagged rows merge normally; totals reflect the
   deletions exactly.
4. **Given** the default (delete-insert) strategy on a large keyed table,
   **Then** a supporting index on the merge identity exists and a
   merge-heavy measurement shows the improvement over the unindexed
   baseline (recorded as a scoreboard number, not a new gate).
5. **Given** existing pipelines with no strategy configured, **Then**
   behavior is byte-identical to today (delete-insert, atomic).
6. **Given** the upsert strategy under the crash sweep (every registered
   fail point, both occurrence passes, armed-fire pins), **Then**
   exactly-once holds — the same discipline the delete-insert arm already
   passes.

---

### User Story 3 - SCD2 history tracking (Priority: P3)

A data engineer configures SCD2 for a dimension table: instead of
overwriting, the destination keeps every version of each row with validity
timestamps. When a row changes, its current version is retired (valid-to
set) and the new version inserted as active; rows absent from the feed can
be retired too. Analysts query "as of" any point in time.

**Why this priority**: the last big dlt-parity block; a genuinely different
write semantic that unlocks dimension-history use cases — but it builds on
US2's strategy machinery, so it comes after.

**Independent Test**: an initial load plus two update rounds produce, for a
changed key, multiple versions with correct, non-overlapping validity
ranges and exactly one active version; point-in-time queries return the
right version.

**Acceptance Scenarios**:

1. **Given** SCD2 configured for a keyed stream, **When** the first load
   runs, **Then** every row is active (valid-from set, valid-to open) and
   totals match the source.
2. **Given** a subsequent load where some rows changed, **Then** changed
   keys get their old version retired at a consistent boundary timestamp
   and a new active version inserted; unchanged rows keep their original
   version untouched (no churn).
3. **Given** a key absent from the new load, **Then** the configured
   absence policy applies (retire the missing key's active version, or
   keep it — both expressible); the default keeps it (incremental feeds
   are partial by nature).
4. **Given** re-delivery of the same batch after a crash, **Then** no
   duplicate versions appear — the redelivery window collapses to the same
   history (exactly-once under the crash model).
5. **Given** validity-column names that collide with source columns,
   **Then** a typed error names the collision at open; the names are
   configurable.

---

### User Story 4 - A destination codebase shaped like the source (Priority: P4)

A contributor opening the destination finds the same layout discipline as
the source: configuration, type/DDL decisions, wire encoding, and the
commit/merge protocol each live in their own focused module, and every
destination error carries the database server's own message (never a bare
"db error").

**Why this priority**: maintainability and the review-F6 debt; pure
relocation plus an error-surface fix, no behavior change beyond messages.

**Independent Test**: the full existing suite passes unchanged after the
relocation; a forced database error at the destination surfaces the
server's message and SQLSTATE in the pipeline error.

**Acceptance Scenarios**:

1. **Given** the modularized destination, **Then** the full suite (unit,
   conformance, recovery, both crash sweeps) passes with zero behavioral
   diffs — the relocation commit contains moves, not edits.
2. **Given** any database failure during write or commit (e.g. a dropped
   table mid-load, a constraint violation), **Then** the surfaced error
   contains the server's message and SQLSTATE, matching the discipline the
   source and TLS layers already follow.

---

### Edge Cases

- Decimal values exceeding the destination's numeric limits; negative
  scale/oversized-precision declarations (source policy already lowers
  those to text — the boundary between native-decimal and textual stays
  contract-defined).
- JSON documents containing NUL escapes or invalid surrogate pairs that the
  binary JSON type rejects — typed error naming the column, never silent
  truncation.
- Upsert strategy with a multi-column key; key columns containing NULLs
  (rejected — same rule as keyed merge today).
- Hard-delete column that is absent from a batch, or non-boolean/non-null
  shaped — typed config error at open.
- Hard-delete flag on a key never previously loaded (delete of a
  non-existent row — a no-op, not an error).
- SCD2 with a batch that contains the same key twice (in-batch last-wins
  before versioning; deterministic).
- SCD2 boundary timestamp stability across a crash-recovery redelivery
  (same load = same boundary, or versions would duplicate).
- Index auto-creation on very large pre-existing tables (documented: the
  first ensure may be slow; the alternative — silent slow merges forever —
  is worse).
- Strategy changed between runs on the same table (delete-insert →
  upsert): allowed when compatible, typed error when the table state
  cannot support it (e.g. duplicate keys).
- Replace and append modes: byte-identical behavior to today (only merge
  gains strategies).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The destination MUST create native column types for decimal
  (with the declared precision/scale), JSON, and UUID values, and honor
  source-declared non-nullability in created tables. The engine-level
  capability declarations MUST reflect this so lowering stops converting
  these types to text.
- **FR-002**: Loaded values MUST round-trip exactly into the native types
  through the same bulk wire path used today (no per-row fallback, no
  throughput regression beyond the stated tolerance) — the destination's
  encoding mirrors the source's existing decoding of the same wire formats.
- **FR-003**: Migration stays ADDITIVE-ONLY: existing columns are never
  silently retyped. Tables created before this feature keep working; the
  situation where a text column would now be native MUST be visible (not
  silent) and the documented paths to native types MUST work.
- **FR-004**: Merge strategy MUST be selectable per destination (and
  overridable per table) among: delete-insert (default, unchanged), upsert,
  and SCD2. No strategy configured = today's behavior exactly. Strategy
  selection lives entirely in destination configuration — zero changes to
  the engine/connector public contracts; the engine's write modes stay
  frozen.
- **FR-005**: The upsert strategy MUST update matched keys in place and
  insert new keys, with in-batch last-wins determinism, exactly-once under
  the crash model, and D3 idempotent re-commit — proven by the same
  crash-sweep + armed-fire discipline as the existing merge arm.
- **FR-006**: Indexes required by a strategy MUST be created automatically
  at table-ensure time (unique on the key for upsert; supporting index on
  the merge identity for delete-insert). Failure to create (e.g.
  pre-existing duplicate keys under upsert) is a typed error naming the
  conflict. Index creation MUST be idempotent across sessions.
- **FR-007**: A hard-delete column MUST be configurable per table: rows
  whose flag is set delete the key at the destination instead of
  upserting/inserting; the flag column's shape is validated at open;
  deletes of never-loaded keys are no-ops; totals stay exact.
- **FR-008**: SCD2 MUST keep full version history per key: validity
  from/to columns (names configurable, collision-checked), one active
  version per key, retirement at a per-load boundary timestamp that is
  stable across crash-recovery redelivery, an absence policy
  (retire-absent or keep-absent, default keep), and in-batch dedup before
  versioning. Point-in-time correctness is conformance-tested.
- **FR-009**: Merge paths MUST NOT scan unindexed targets: the identity
  used for deletion/matching gets a supporting index, and the improvement
  on a large-table merge is MEASURED (scoreboard) against the unindexed
  baseline.
- **FR-010**: Every destination error that originates from the database
  MUST carry the server's message and error code (SQLSTATE) — closing
  review F6; no path may surface a bare driver Display string.
- **FR-011**: The destination module MUST be reorganized to mirror the
  source's layout (configuration / type-DDL / wire-encoding /
  commit-protocol concerns separated) as a pure relocation with zero
  behavior change, gated by the full existing suite.
- **FR-012**: All new configuration (strategy, hard-delete column, SCD2
  settings) MUST appear in generated config surfaces/documentation with
  validation errors that name the offending field, consistent with the 006
  schema discipline. The CLI's destination configuration MUST expose the
  same options.
- **FR-013**: Existing gated pg→pg benchmark bars stay within tolerance;
  new merge-heavy and upsert measurements are recorded as scoreboard
  numbers (measurement-first; no new gates without a version-policy
  entry).
- **FR-014**: Replace and append behavior stays byte-identical; the
  optional drop-and-swap replace strategy is OUT unless it costs nothing
  extra (not required for completion).

### Key Entities

- **Merge strategy**: per-destination (per-table overridable) choice:
  delete-insert | upsert | scd2; orthogonal to the engine's frozen write
  modes.
- **Hard-delete column**: configured column name whose truthy/non-null
  value marks a row for deletion during merge.
- **SCD2 settings**: validity from/to column names, absence policy,
  boundary-timestamp source; per-table.
- **Supporting index**: automatically ensured index tied to a table's
  merge identity/strategy; idempotent, typed failure on conflict.
- **Native-type mapping**: decimal(p,s)/JSON/UUID/NOT NULL now first-class
  in table creation and the bulk wire path.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The type-matrix round trip extended to native decimal, JSON,
  and UUID passes against a real destination: catalog types match, SUM
  over a decimal column equals the source's total exactly, a JSON path
  query and a uuid-literal join both work — zero text-cast steps for
  consumers.
- **SC-002**: Update-heavy upsert converges to one row per key with newest
  values; three consecutive re-runs keep destination totals exactly equal
  to the source; the full crash sweep (every registered fail point, both
  passes, armed-fire pins) is green for the upsert arm on the real
  database.
- **SC-003**: A hard-delete feed removes exactly the flagged keys — final
  totals equal source-minus-deletions; re-delivery changes nothing.
- **SC-004**: SCD2 across three load rounds yields correct version counts,
  non-overlapping validity ranges, exactly one active version per key,
  and correct point-in-time query results; crash-redelivery adds zero
  duplicate versions.
- **SC-005**: A merge-heavy large-table measurement (recorded protocol,
  quiet machine) shows the indexed merge path faster than the unindexed
  baseline, published as a scoreboard entry with both numbers.
- **SC-006**: All existing gated benchmark bars remain within tolerance
  after the changes (perf gate + e2e bars); the bulk-load path shows no
  regression from native-type encoding.
- **SC-007**: After modularization, the complete existing test suite
  passes unchanged, and a forced database failure surfaces the server's
  message + SQLSTATE in the pipeline error (F6 closed, regression-tested).
- **SC-008**: Every new config field validates with errors naming the
  field; documented examples parse; unknown fields fail — extending the
  006 generated-schema guarantee to the destination's new surface.

## Assumptions

- Strategy selection is destination-side configuration because the
  engine's write-mode vocabulary is a frozen public contract; a strategy
  is an EXECUTION choice for the same merge semantics (except SCD2, which
  is deliberately a destination-local semantic extension — documented as
  such).
- Upsert requires a database version with native conflict-update support
  (universally available in supported versions); no compatibility shim
  for older servers.
- The hard-delete column is a source-provided data column (CDC-style
  flag); rdlt does not synthesize deletions (that is CDC's job, a future
  feature).
- SCD2 applies to keyed streams only (same keyed requirement as merge);
  keyless SCD2 is rejected typed.
- Existing text columns from earlier versions are NOT auto-migrated;
  visibility + documented manual paths satisfy FR-003 (additive-only is a
  standing contract).
- The uuid/json capability flip affects only destinations that declare
  it; other destinations (DuckDB, parquet) are untouched in this feature.
- Explicitly OUT: PostGIS/geometry types, external object-storage
  staging, csv/parquet loader file formats, parallel load jobs,
  foreign-key constraints, dedup-sort ordering hints (arrival-order
  last-wins stays the deterministic rule).
