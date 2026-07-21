# Feature Specification: Merge Refinements — Ordered Dedup + Scope Keys

**Feature Branch**: `010-merge-refinements`

**Created**: 2026-07-21

**Status**: Draft

**Input**: User description: "dedup_sort + independent merge_key — the two
dlt merge knobs real migrations are most likely to hit (identified by the
post-009 completeness audit as the remaining destination-side parity gaps).
Ordered survivor selection when one load carries several rows for the same
row identity, and a non-unique scope key that replaces whole partitions of
the target during a merge load, independent of the row-identity key."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Ordered survivor selection (`dedup_sort`) (Priority: P1)

A pipeline's source delivers several versions of the same row within one
load — an event feed with an `updated_at` or sequence column, a backfill
that overlaps a live window, an at-least-once upstream that re-delivers.
Today the destination keeps the LAST-arriving version (deterministic, but
arrival order is a property of the read, not of the data). The operator
declares a dedup ordering — a column and a direction — and the surviving
version is the one that column says is newest (or oldest), regardless of
arrival order.

**Why this priority**: this is the knob migrations from dlt hit first, and
the only one whose absence can produce a WRONG destination row (stale
version wins because it arrived last) rather than a missing feature.

**Independent Test**: one merge load carrying two versions of the same key
in "wrong" arrival order (newest first); the destination holds the newest
version under `desc`, the oldest under `asc`; without the option, the
last-arriving row still wins (behavior unchanged).

**Acceptance Scenarios**:

1. **Given** a merge-loaded table with dedup ordering on `seq` descending,
   **When** one load delivers (key=1, seq=5) then (key=1, seq=3), **Then**
   the destination holds seq=5 — the ordering, not arrival, decided.
2. **Given** the same table WITHOUT a dedup ordering, **When** the same
   load runs, **Then** the destination holds seq=3 (the existing
   deterministic last-wins is unchanged — no silent behavior change).
3. **Given** dedup ordering on `seq` descending and a hard-delete flag
   column, **When** one load delivers (key=1, seq=5, flag=TRUE) then
   (key=1, seq=3, flag=NULL), **Then** the row is DELETED — the surviving
   version's flag decides, exactly as if it had arrived alone.
4. **Given** two versions where the dedup column is NULL on one, **When**
   the load runs, **Then** the version with a value survives; when ALL
   versions are NULL the existing last-wins order applies (documented,
   deterministic).
5. **Given** a replayed load (crash redelivery), **When** the same rows
   arrive again, **Then** the destination converges to the same surviving
   version — redelivery-stable.

---

### User Story 2 - Scope-key replacement (`merge_key`) (Priority: P2)

A pipeline loads a rolling window — "yesterday's events", "this tenant's
refresh" — where the incoming batch is the complete truth for its scope
(a load date, a tenant id), but row identities inside the scope come and
go. The operator declares a scope key: a non-unique column set,
independent of the row-identity key. A merge load then REPLACES every
scope present in the incoming batch: target rows in those scopes that the
batch no longer carries disappear, and the batch's rows land — while
scopes the batch does not touch are left alone.

**Why this priority**: without it, rows deleted upstream inside a
re-delivered window survive forever at the destination (the delete-insert
strategy only replaces identities it sees again). It is the second dlt
merge knob, and the standard shape for window/partition refreshes.

**Independent Test**: seed two scopes; re-deliver one scope with a row
missing; the missing row is gone, the untouched scope is intact,
row-identity merging still applies within the delivered scope.

**Acceptance Scenarios**:

1. **Given** a table scope-keyed on `day` holding rows for day 1 and
   day 2, **When** a merge load delivers day 1 WITHOUT one of its previous
   rows, **Then** that row is gone from the destination, the remaining
   day-1 rows match the batch exactly, and day 2 is untouched.
2. **Given** the same shape, **When** the batch delivers a day the target
   has never seen, **Then** its rows simply land (scope replacement of an
   empty scope is an insert).
3. **Given** scope-key replacement combined with identity-keyed updates,
   **When** a batch updates a row whose scope column ITSELF changed (the
   row moved from day 1 to day 2), **Then** the destination holds the row
   once, in its new scope — identity replacement and scope replacement
   compose without duplicating.
4. **Given** rows whose scope column is NULL, **When** a load runs,
   **Then** NULL scopes never match any target scope — such rows are
   replaced by identity only (documented; NULL is not a scope).
5. **Given** a replayed load, **When** the same batch re-applies, **Then**
   the outcome is identical — scope replacement is idempotent.

---

### User Story 3 - Loud, typed configuration surface (Priority: P3)

An operator declaring either control gets the same discipline as every
other destination option: misconfiguration fails loudly at open with the
table and column named; the options ride the generated config schemas and
the CLI passthrough; composition rules with the existing strategies are
validated, not discovered at load time.

**Why this priority**: the controls change which rows survive — a silently
ignored or misapplied declaration is data corruption from the user's view.

**Independent Test**: the validation matrix — each invalid shape produces
its own typed error naming the offender; valid shapes round-trip the
generated schema and the CLI.

**Acceptance Scenarios**:

1. **Given** a dedup ordering naming a column the table does not carry,
   **When** the pipeline opens, **Then** a typed error names table and
   column before any data moves.
2. **Given** a scope key naming the hard-delete flag column or a validity
   column, **When** the pipeline opens, **Then** the collision is a typed
   error.
3. **Given** valid declarations, **When** configs are validated against
   the generated schema, **Then** examples validate, unknown fields fail
   both layers, and the CLI passes both options through per-table.

---

### Edge Cases

- Dedup-ordering ties (equal sort values): the existing deterministic
  last-wins order breaks the tie — documented, stable across replays.
- The dedup ordering applies wherever same-identity rows collapse within
  one load (delete-insert, upsert, and SCD2's base-row selection alike) —
  no strategy silently ignores it.
- A scope key equal to (or a superset of) the identity key is legal but
  pointless — accepted, documented as such (identity replacement already
  covers it).
- Scope replacement with an EMPTY incoming batch for a stream deletes
  nothing (no scopes present = nothing matched) — a no-op load stays a
  no-op.
- Both controls declared together on one table compose: scope deletion
  first widens what is removed; ordered dedup then picks the surviving
  version per identity among the incoming rows.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The postgres destination MUST accept a per-table dedup
  ordering — one column plus a direction (ascending/descending) — that
  selects the surviving version among same-identity rows within one load,
  for every merge strategy that collapses such rows.
- **FR-002**: Absent a dedup ordering, survivor selection MUST remain the
  existing deterministic last-wins — zero behavior change for existing
  pipelines.
- **FR-003**: The surviving version (and only it) MUST drive every
  downstream merge decision — hard-delete flag, SCD2 change detection,
  upsert content.
- **FR-004**: The postgres destination MUST accept a per-table scope key —
  a non-unique column set independent of the row-identity key — such that
  a merge load removes every target row whose scope matches a scope
  present in the incoming batch, then applies the batch (scope
  replacement OR'd with identity replacement).
- **FR-005**: Scope replacement MUST leave untouched every target scope
  absent from the incoming batch, and MUST treat NULL scope values as
  matching nothing.
- **FR-006**: Both controls MUST be redelivery-stable: replaying a load
  (crash recovery, at-least-once upstreams) converges to the same
  destination state — the exactly-once-outcomes guarantee is preserved
  and re-proven under the crash sweeps for every new code path.
- **FR-007**: Both controls MUST compose with the existing strategy
  surface (delete-insert, upsert, hard_delete; SCD2 scope-key
  interaction is OUT — see Out of Scope) and with each other; every
  undefined combination is a typed configuration error, never silence.
- **FR-008**: Misconfiguration — unknown columns, collisions with
  reserved/flag/validity columns, non-orderable dedup columns — MUST fail
  typed at open, naming table and column, before any data moves.
- **FR-009**: Both options MUST ride the generated destination config
  schemas (examples validate; unknown fields fail schema AND parser) and
  the CLI's per-table destination options passthrough.
- **FR-010**: The engine SPI and write-mode vocabulary MUST NOT change
  (rdlt-core/rdlt-connector semver-checks stay "no update required");
  streams remain keyed exactly as today — the controls are destination
  configuration.
- **FR-011**: Performance MUST be measured, not asserted: the scope-delete
  shape gets a measurement-first scoreboard entry under the recorded
  protocol; existing gated bars stay within tolerance.

### Key Entities

- **Dedup ordering** (per-table destination option): column name +
  direction; selects the surviving version among same-identity rows in
  one load.
- **Scope key** (per-table destination option): ordered list of column
  names; defines the replacement scope of a merge load, independent of
  the row-identity key.

## Success Criteria *(mandatory)*

- **SC-001**: The US1 matrix passes against a real server: ordered
  survivor under both directions, unchanged last-wins without the option,
  flag-of-survivor hard-delete, NULL ordering policy, replay stability.
- **SC-002**: The US2 matrix passes against a real server: scope
  replacement removes undelivered rows in delivered scopes only, empty
  and unseen scopes behave as specified, scope-moving updates never
  duplicate, NULL scopes match nothing, replay is idempotent.
- **SC-003**: Every new merge path is crash-swept under the existing
  registered fail points (both occurrence passes, armed-fire pins) with
  post-recovery equality — exactly-once outcomes hold.
- **SC-004**: The validation matrix produces a distinct typed error per
  invalid shape, each naming table + column.
- **SC-005**: Config schemas round-trip both options; the CLI passes them
  through; docs (README + dest-options contract) are truthful.
- **SC-006**: semver-checks on rdlt-core and rdlt-connector report "no
  update required"; zero new dependencies; safe Rust only.
- **SC-007**: A scoreboard entry records the scope-delete cost under the
  5-run-median protocol; existing gated bars remain within tolerance.

## Assumptions

- Both controls are POSTGRES-DESTINATION options (the 008 options
  surface). The DuckDB destination keeps its current merge behavior;
  parity there is a separate decision.
- dlt-parity semantics are the reference where they are well-defined
  (survivor by sort column; scope delete OR identity delete), but rdlt's
  stricter posture wins where dlt is loose: no append-fallback when keys
  are missing, no silently-arbitrary survivor, NULL policies documented.
- Streams remain identity-keyed (engine clause B4 as amended): a scope
  key never substitutes for the row-identity key.

## Out of Scope

- Scope-key-ONLY merges (no row identity) and dlt's append-fallback when
  keys are absent — both would weaken the frozen engine merge contract.
- SCD2 retirement scoping by scope key (dlt's scd2 merge_key): SCD2
  already has its own absence policy (008 S6); extending it is a separate
  decision with its own history semantics.
- Any engine/SPI vocabulary change; DuckDB destination parity; dedup
  across LOADS (the controls act within one load's delivered set).
