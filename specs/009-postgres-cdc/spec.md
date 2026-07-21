# Feature Specification: Postgres CDC via Logical Replication

**Feature Branch**: `009-postgres-cdc`

**Created**: 2026-07-21

**Status**: Draft

**Input**: User description: "Postgres CDC via logical replication (009): the last big source gap — capture inserts, UPDATES, and DELETES from Postgres continuously, with exactly-once outcomes end to end. Initial snapshot + streaming handoff with no gap and no overlap: the replication slot's exported consistent snapshot is the boundary — snapshot rows load first, then changes stream from exactly that point. Streaming uses the database's built-in logical decoding output (publication-based; no third-party decoder plugins). Changes apply in transaction-commit order; resume positions (LSN watermarks) ride the existing engine checkpoint/state machinery, and the slot's acknowledged position advances only after the destination durably commits — a crash replays from the last commit with no loss, and re-applied changes converge because updates upsert by key and deletes are idempotent (composition with feature 008: CDC emits a deletion-flag column; destinations with hard-delete support apply real deletions, others carry the flag as data = documented soft-delete). Requirements surfaced clearly: tables need a primary key (or REPLICA IDENTITY configured) — typed error naming the table otherwise; TOAST columns unchanged in an update follow a documented policy; mid-stream schema changes follow the existing additive drift rules. Two run modes: bounded catch-up (consume the backlog to the current WAL position, then finish — cron-able, the MVP) and continuous tail (stream until cancelled, checkpointing as it goes). Slot and publication lifecycle managed with care: create-if-missing behind explicit config, never silently drop, clear typed errors for missing/replaced slots and WAL-retention overruns, and visibility into replication lag. Crash discipline as everywhere: fail-point sweeps across snapshot handoff, stream read, and acknowledgement boundaries with armed-fire pins; a container-kill mid-stream test; measured performance (snapshot throughput consistent with the existing pg source bars; streaming apply throughput and catch-up latency as new measurement-first scoreboard entries). All connection features inherited (TLS/mTLS, conn-string portability, application_name). Explicitly OUT: DDL replication beyond additive column drift, multi-database/multi-slot coordination, non-postgres CDC, transformations on the change stream, exactly-once for destinations without keyed merge (documented at-least-once there)."

## Context

Cursor-based incremental loading (features 005–007) cannot see DELETES at
all, and sees UPDATES only when the table maintains a trustworthy
modification timestamp. Change data capture through the database's own
change feed closes both holes and unlocks near-real-time replication —
the one capability where competing tools currently lead this project.
Everything else the change feed needs already exists in the stack: the
engine's checkpoint/resume machinery carries positions, the destination's
keyed upsert makes re-applied updates converge, and the hard-delete
column (feature 008) turns captured deletions into real destination
deletions. This feature is the composition of those pieces with the
database's replication protocol.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Bounded catch-up replication (Priority: P1)

A data engineer enables CDC for a set of tables. The FIRST run takes a
consistent snapshot of the tables and remembers the exact change-feed
position that snapshot corresponds to. Every LATER run consumes the
changes accumulated since the last run — inserts, updates, AND deletes —
applies them in transaction order, and finishes when it has caught up to
the feed's current position. Scheduled every few minutes, this keeps the
destination a faithful, delete-aware mirror with no timestamp columns or
source schema changes required.

**Why this priority**: this is the MVP — it delivers delete capture and
update capture on a cron cadence with the existing run-based operational
model; everything else builds on it.

**Independent Test**: seed a table; run 1 snapshots it; mutate the source
(inserts + updates + deletes); run 2 applies exactly those changes; the
destination equals the source row-for-row; a third run with no source
changes moves nothing.

**Acceptance Scenarios**:

1. **Given** CDC enabled for a table with a primary key, **When** the
   first run executes, **Then** the destination holds a consistent
   snapshot and the run records the feed position that snapshot
   corresponds to.
2. **Given** inserts, updates, and deletes on the source after run 1,
   **When** run 2 executes, **Then** the destination matches the source
   exactly: new rows present, updated rows show newest values, deleted
   rows are GONE (destination hard-delete applied); totals equal the
   source's.
3. **Given** no source changes since the last run, **Then** a run
   consumes nothing, moves nothing, and finishes promptly.
4. **Given** a transaction on the source touching several rows and
   tables, **Then** its changes apply in commit order and land atomically
   with respect to the pipeline's commit units — never a torn transaction
   visible across a completed run.
5. **Given** the snapshot-to-stream boundary, **Then** no change is
   skipped, and any change applied twice in the boundary window
   converges: a row modified DURING the snapshot run appears exactly
   once with its correct final state *(refined per research R4)*.

---

### User Story 2 - Crash-safe resume with exactly-once outcomes (Priority: P2)

The pipeline dies mid-catch-up — process kill, network loss, database
restart. The next run resumes from the last durably committed position:
no captured change is lost, and re-delivered changes converge to the same
destination state because updates apply by key and deletes are
idempotent. The change feed's retained backlog is only released once the
destination has durably committed past it.

**Why this priority**: CDC without crash discipline silently corrupts
mirrors; this is the project's signature guarantee extended to the change
feed, and it must hold before continuous mode multiplies exposure time.

**Independent Test**: the crash sweep — inject failures at every
registered boundary (snapshot handoff, stream read, acknowledgement,
destination publish), both occurrence passes; after recovery the
destination equals the source exactly; the armed-fire pins prove every
boundary actually fired.

**Acceptance Scenarios**:

1. **Given** a crash at ANY registered fail point during snapshot,
   streaming, or acknowledgement, **When** the pipeline restarts,
   **Then** it resumes from the last committed position and the
   destination converges to exactly the source state (no loss, no
   duplicates in effect).
2. **Given** the feed's acknowledged position, **Then** it advances ONLY
   after the destination durably commits the corresponding changes — a
   crash between destination commit and acknowledgement re-reads
   already-applied changes, which converge (upsert/delete idempotency).
3. **Given** a real container kill mid-stream, **Then** the surfaced
   error is typed (naming the phase), committed work survives, and the
   next run completes the catch-up.
4. **Given** re-delivery of an already-applied delete, **Then** it is a
   no-op; re-delivery of an already-applied update leaves the same final
   state.

---

### User Story 3 - Continuous tail mode (Priority: P3)

For latency-sensitive mirrors, the same pipeline runs in continuous mode:
it keeps consuming the change feed, applying and checkpointing changes
as they arrive, until deliberately cancelled. Freshness is bounded by
the apply cadence (v1: a chunked catch-up loop with a short idle wait —
research R6), not by an external schedule.

**Why this priority**: the near-real-time story; operationally heavier
(long-lived process), so it layers on the proven bounded mode.

**Independent Test**: start continuous mode; apply a burst of source
changes; observe them in the destination without restarting the pipeline;
cancel cleanly; restart resumes without loss or duplication.

**Acceptance Scenarios**:

1. **Given** continuous mode running, **When** changes commit on the
   source, **Then** they appear at the destination without operator
   action, and checkpoints advance as data flows (a later crash resumes
   from the last checkpoint, not from mode start).
2. **Given** a cancellation request, **Then** the pipeline stops at a
   clean commit boundary — the next run (bounded or continuous) resumes
   exactly there.
3. **Given** a quiet source (no changes), **Then** continuous mode idles
   without busy-work and picks up the next change promptly.

---

### User Story 4 - Operational clarity: prerequisites, lifecycle, lag (Priority: P4)

The database-side prerequisites are surfaced, never assumed: a table
without the identity the change feed needs is rejected with a typed error
naming the table and the fix. The feed's server-side resources (the
subscription point and the table-set declaration) are created only when
explicitly requested, never silently dropped, and their absence,
replacement, or backlog overrun produce distinguished typed errors.
Replication lag — how far the destination trails the source — is visible
per run.

**Why this priority**: CDC failures are notoriously opaque (silent WAL
bloat, dropped slots, missing identities); making them loud and typed is
what makes the feature operable.

**Acceptance Scenarios**:

1. **Given** a table without a primary key or configured replica
   identity, **Then** enabling CDC fails typed, naming the table and the
   identity requirement; updates/deletes for tables whose identity was
   dropped later fail loudly, never silently mis-apply.
2. **Given** `create-if-missing` NOT enabled and no server-side feed
   resources, **Then** a typed error explains exactly what to create;
   with it enabled, resources are created idempotently and never dropped
   by rdlt.
3. **Given** the feed's backlog was discarded by the server (retention
   overrun / slot invalidated), **Then** the error is typed, names the
   condition, and states the recovery (fresh snapshot).
4. **Given** any completed run, **Then** the run report shows the
   replication lag (position delta and/or time delta) at completion.
5. **Given** an update whose oversized (out-of-line stored) column values
   were not included in the change record, **Then** the documented policy
   applies deterministically and visibly — never silently nulled.

---

### Edge Cases

- A row inserted and deleted within one source transaction (net no-op)
  — applies cleanly, leaves nothing.
- Updates that change the PRIMARY KEY value itself (old key must be
  deleted, new key inserted).
- A snapshot taken while large transactions are in flight (they
  committed after the snapshot point → they arrive via the stream, once).
- Very large single transactions (bigger than one commit unit's memory
  budget) — bounded memory holds; the transaction still applies
  atomically with respect to completed runs.
- Tables added to the CDC set after the slot exists (new table needs its
  own snapshot without disturbing the others').
- Source table dropped or renamed mid-stream — typed error, not silent
  skipping.
- The deletion-flag column name colliding with a source column — typed
  error at open (same collision discipline as SCD2 validity columns).
- Keyed-merge-less destinations (parquet append): the change stream
  lands as an audit log with operation flags — documented at-least-once,
  never claimed exactly-once.
- Restart after the SOURCE database itself was restored from backup
  (feed position no longer valid) — the invalidated-slot typed error
  path, with the documented fresh-snapshot recovery.
- Two pipelines accidentally sharing one subscription point — detected
  and rejected (a feed position is single-consumer state).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A CDC-enabled stream MUST capture inserts, updates, and
  deletes from the database's built-in logical change feed
  (publication-based; no third-party decoder plugins required on the
  server).
- **FR-002** *(refined per research R4)*: The initial snapshot and the
  change stream MUST join with NO GAP: the stream replays from a feed
  position at or before the snapshot's point, so no change is ever
  lost. The window between the two points applies twice and MUST
  CONVERGE (upsert-by-key + idempotent deletes) — a row's final state
  is correct and it appears once, regardless of concurrent writes
  during the snapshot.
- **FR-003**: Changes MUST apply in transaction-commit order, and a
  completed run MUST never expose a torn source transaction.
- **FR-004**: Resume positions MUST ride the existing engine
  checkpoint/state machinery; the feed's acknowledged position MUST
  advance only after the destination durably commits the corresponding
  changes. Crash recovery from any point yields convergent destination
  state (updates by key, deletes idempotent) — exactly-once OUTCOMES on
  keyed-merge destinations.
- **FR-005**: Deletions MUST be expressed via the feature-008
  composition: the stream carries an operation/deletion flag column;
  destinations with hard-delete support apply REAL deletions;
  destinations without it receive the flag as data (documented
  soft-delete). The flag column's name is collision-checked.
- **FR-006**: Updates MUST apply by the table's replica identity;
  primary-key-changing updates produce delete-old-key + insert-new-key.
  Tables lacking a usable identity are rejected typed at enable time,
  naming the table and the requirement.
- **FR-007**: Oversized out-of-line column values omitted from update
  records MUST follow one documented, deterministic policy, visible in
  configuration — never silent nulling.
- **FR-008**: Mid-stream schema changes MUST follow the existing
  additive drift rules (new columns appear; anything else is a typed
  error); source table drop/rename mid-stream is a typed error.
- **FR-009**: Two run modes MUST exist: bounded catch-up (consume to
  the feed's current position, then finish — schedulable) and
  continuous tail (apply until cancelled, checkpointing continuously,
  cancelling cleanly at a commit boundary). Both share the same resume
  state.
- **FR-010**: Server-side feed resources MUST be created only under
  explicit create-if-missing configuration (idempotently), NEVER
  dropped by rdlt; missing, replaced, invalidated, or
  retention-overrun feed states each produce a DISTINGUISHED typed
  error with its recovery path; concurrent consumption of one
  subscription point by two pipelines is rejected.
- **FR-011**: Replication lag (position and/or time delta at run
  completion) MUST be visible in the run report; bounded runs MUST
  terminate promptly on a quiet feed.
- **FR-012**: Memory MUST stay bounded regardless of source
  transaction size; large transactions may span multiple commit units
  while preserving FR-003's no-torn-transaction guarantee for
  completed runs.
- **FR-013**: All connection features are inherited unchanged: TLS/mTLS
  policy, conn-string portability, application_name identification.
- **FR-014**: The crash discipline applies in full: registered fail
  points at the snapshot handoff, stream read, and acknowledgement
  boundaries; sweeps with both occurrence passes and armed-fire pins; a
  real container-kill mid-stream test.
- **FR-015**: Performance is measurement-first: snapshot throughput
  must remain consistent with the existing gated pg-source bars;
  streaming apply throughput and catch-up latency are NEW scoreboard
  measurements with a recorded protocol (no new gates without a
  version-policy entry).
- **FR-016**: All new configuration (CDC enablement, slot/publication
  names, create-if-missing, flag-column name, TOAST policy, run mode)
  appears in the generated config schemas with field-naming validation
  errors, per the 006 schema discipline.

### Key Entities

- **Change feed subscription**: the server-side slot/publication pair a
  pipeline consumes; single-consumer; explicitly created; never
  auto-dropped.
- **Feed position (LSN watermark)**: the resume cursor carried by engine
  state; acknowledged upstream only after destination commit.
- **Snapshot boundary**: the exact position the initial snapshot
  corresponds to; the no-gap/no-overlap joint.
- **Change record**: row image + operation kind (insert/update/delete)
  + source transaction ordering; deletion expressed via the flag column
  at the destination seam.
- **Run mode**: bounded catch-up | continuous tail; shared state.
- **TOAST policy**: the documented handling for omitted oversized
  values in update records.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The snapshot→stream→mutate→catch-up cycle yields a
  destination EXACTLY equal to the source (row counts and values,
  including deletions applied) across the independent test's three
  runs; a no-change run moves zero rows.
- **SC-002**: The full crash sweep across every registered CDC fail
  point (both occurrence passes, armed-fire pins) plus the
  container-kill test all converge to source-equal state — no loss, no
  duplicate effects.
- **SC-003**: Continuous mode applies a burst of source changes without
  restart, cancels cleanly, and resumes without loss or duplication;
  the quiet-feed case shows no busy-work.
- **SC-004**: Every operational failure mode in FR-010 (missing
  resources, invalidated slot, retention overrun, identity missing,
  concurrent consumer) produces its distinguished typed error in a
  test; zero silent failure paths.
- **SC-005**: Snapshot throughput stays within the existing gated
  pg-source tolerances; streaming apply throughput and catch-up
  latency are recorded as scoreboard entries under a written protocol.
- **SC-006**: Replication lag appears in the run report for every
  completed run in both modes.
- **SC-007**: Config schemas round-trip every new field; documented
  examples parse; unknown fields fail both layers.
- **SC-008**: On keyed-merge destinations the end-to-end guarantee is
  exactly-once OUTCOMES (verified by the sweep + equality checks); on
  destinations without keyed merge the behavior is documented
  at-least-once with the operation flag carried as data — and the docs
  say so truthfully.

## Assumptions

- The database user has replication privileges; the server allows
  logical replication (standard managed-Postgres settings). Preflight
  checks produce typed errors when not.
- One pipeline consumes one subscription point; fan-out to multiple
  destinations is done by running multiple pipelines with their own
  slots (multi-slot coordination is OUT).
- The deletion-flag column defaults to a system-prefixed name and is
  configurable; hard-delete application at the destination reuses the
  feature-008 machinery unchanged.
- TOAST policy default: an omitted oversized value in an update record
  means "unchanged" and the destination retains the existing stored
  value where the write mode permits it; where it cannot be retained,
  the run fails typed rather than writing a wrong value. The chosen
  default and its boundaries are documented with the policy config.
- Continuous mode integrates with the existing run-based engine as a
  run that ends only on cancellation or fatal error; scheduling/
  supervision of the long-lived process belongs to the embedder
  (rapidbyte platform later).
- Snapshot reuses the existing COPY-based read path (its performance
  bars and crash discipline carry over).
- Explicitly OUT: DDL replication beyond additive column drift,
  multi-database/multi-slot coordination, non-postgres CDC, change-
  stream transformations, exactly-once claims for destinations without
  keyed merge.
