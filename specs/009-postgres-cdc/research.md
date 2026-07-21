# Research: Postgres CDC via Logical Replication

Decisions verified against the code on branch `009-postgres-cdc`
(main @ fb7955f) and the crates.io registry sources. Two sanctioned
spec refinements are recorded at the bottom.

## R1 — Consumption path: SQL-level logical decoding, zero new dependencies

**Fact (verified in the registry source)**: tokio-postgres 0.7.18 — our
pinned, released driver — has NO replication-protocol support: no
`copy_both`, no replication connection mode (those exist only on the
unreleased git master and in third-party forks/crates).

**Decision**: consume the change feed through the SQL interface over a
NORMAL connection (the one connect path with TLS/mTLS intact):
- `pg_create_logical_replication_slot(slot, 'pgoutput')` — creation
  (behind `create_if_missing`), returns the consistent point.
- `pg_logical_slot_peek_binary_changes(slot, upto_lsn, NULL,
  'proto_version','1', 'publication_names', pub)` — read WITHOUT
  consuming; this is what makes the per-table pass design (R3) sound.
- `pg_replication_slot_advance(slot, lsn)` — acknowledge (advance
  `confirmed_flush_lsn`) explicitly, exactly when WE decide (R6).

**Rationale**: zero new dependencies (house rule); the peek/advance
split maps EXACTLY onto the ack-after-destination-commit requirement;
plain connections inherit the whole 006/007 TLS + portability stack for
free. The replication-protocol client (streaming START_REPLICATION)
becomes a future optimization for continuous mode, not a v1 need.

**Alternatives considered**: third-party replication crates — rejected
(new dependency, varying maintenance, and we lose the shared connect
path); hand-rolling the wire-level replication session — rejected
(large surface for v1; the SQL interface covers every v1 requirement).

## R2 — pgoutput binary parsing: hand-rolled, fuzzed, in-crate

**Crate survey (owner question, 2026-07-21)**: no mature alternative
exists. Upstream rust-postgres merged replication support but never
released it (tokio-postgres 0.7.18 AND postgres-protocol 0.6.12 — both
verified in registry sources — have none). Standalone parsers are
pre-release (`pgoutput` 0.0.7, `postgres-replication-types` 0.1.1,
`pg_replicate` 0.1.0) — auditing a 0.0.x crate line-by-line costs more
than owning the ~250 lines with our own fuzz target. Full clients
(`pgwire-replication` 0.4.0) carry their OWN TLS/connection stacks,
which would fork the single parse_conn/mTLS gate 006/007 built. The
format is 9 message types, documented, unchanged since PG10. If
upstream ever releases its replication support, swapping the parser is
a contained refactor.

**Decision**: `src/source/cdc/pgoutput.rs` parses the documented
logical-replication message set (Begin, Commit, Relation, Type, Insert,
Update, Delete, Truncate, Origin; TupleData with text/binary/null/
unchanged-TOAST markers) — the same house discipline as the 005 COPY
decoder: hand-rolled, property-tested, and a NEW fuzz target
(`pg_pgoutput_decode`) registered in fuzz/Cargo.toml. Tuple values
arrive in TEXT form under proto_version 1 and decode through the
EXISTING typed parse paths (the source's config-literal/text machinery),
keeping one conversion vocabulary.

## R3 — Multi-table delivery through the frozen SPI: per-table passes over a peeked range

**Problem**: one slot interleaves many tables' changes; the engine
reads streams SEQUENTIALLY and each `read()` is bound to one stream.

**Decision**: bounded catch-up works in per-table PASSES over the same
WAL range: at run start the source pins `target_lsn` (the feed's
current position); each CDC stream's `read()` peeks the range
`(its cursor, target_lsn]`, filters ITS table's changes, and pushes
them as structured batches with LSN checkpoints. Peeking never
consumes, so N streams = N passes over the same range, each
independently resumable. Cross-table ordering: within a table,
commit order is preserved exactly; across tables, a mid-run crash can
leave table A committed ahead of table B — the next run's replay
converges (R5), and a COMPLETED run always covers every table to the
same `target_lsn`, which is precisely the spec's completed-run
consistency guarantee (FR-003).

**Batch/checkpoint cutting**: only at transaction-commit boundaries in
the change stream — a checkpoint never lands mid-transaction for its
table, so resume replays whole transactions (FR-003 + FR-012; large
transactions may span multiple pushed batches BETWEEN checkpoints,
keeping memory bounded by batch size, not transaction size).

**Alternatives considered**: engine-level multi-table push seam —
rejected (SPI change, frozen); single audit stream for all tables —
rejected (loses per-table schemas and merge semantics).

## R4 — Snapshot boundary: slot-first, then snapshot; overlap CONVERGES (spec refinement 1)

**Fact**: the exported-snapshot handshake (`CREATE_REPLICATION_SLOT …
EXPORT_SNAPSHOT`) exists only on the replication protocol, which our
driver doesn't speak (R1). The SQL slot-creation function exports no
snapshot.

**Decision**: first run creates the slot FIRST, then opens a
REPEATABLE READ transaction and snapshots every CDC table through the
EXISTING COPY path (all tables under ONE snapshot — incidentally
closing the cross-table snapshot-consistency gap noted in the 008
audit). The stream then replays from the slot's consistent point,
which is ≤ the snapshot's point: NO GAP by construction, and the
window between the two points applies TWICE — once via the snapshot,
once via the stream — converging because updates upsert by key and
deletes are idempotent (the exact crash-redelivery argument the spec
already makes). The spec's "no overlap" phrasing is REFINED to the
outcome it was protecting: a row's final state is correct and appears
once (US1-AS5 holds verbatim); the boundary contract is
"no-gap + overlap-converges".

**Consequence pinned in conformance**: a row inserted after slot
creation but before the snapshot ends appears EXACTLY ONCE with its
final state (it is the overlap case).

## R5 — Exactly-once outcomes: cursors, acks, and the 008 composition

**Decision**:
- Cursor = LSN (u64, rendered `X/Y` for humans) per CDC stream,
  carried by the EXISTING engine checkpoint/state machinery.
- The slot's acknowledged position advances via
  `pg_replication_slot_advance` ONCE per run, to
  `min(committed cursor over ALL CDC streams)` — computed after every
  stream's `read()` has reported its resume cursor (the source
  accumulates them; if a run dies early, no ack happens, which is
  always safe — acking is WAL-retention hygiene, never correctness).
- Change application: updates land as upserts by the table's key;
  deletes land as rows with the deletion-flag column set (feature-008
  `hard_delete` applies real deletions on postgres; other destinations
  carry the flag = documented soft-delete). PK-changing updates emit
  delete(old key) + insert(new key), in that order.
- Convergence under replay/overlap: upsert idempotent, delete
  idempotent, flag-only rows idempotent — exactly-once OUTCOMES on
  keyed-merge destinations (SC-008).

**Recommended pipeline shape** (quickstart + validation warning if
missing): `write_mode = merge{key=PK}` + destination
`merge_strategy = upsert` + `hard_delete = <flag column>`.

## R6 — Run modes

**Bounded catch-up (MVP)**: pin `target_lsn` at run start; passes per
R3; run finishes when every stream reaches the target. Quiet feed =
empty peeks = prompt finish (FR-011).

**Continuous tail (spec refinement 2)**: v1 implements the tail as a
CHUNKED LOOP inside one engine run — repeat {bounded catch-up to the
current position; short idle wait when quiet} until cancellation, with
checkpoints flowing every chunk and cancellation honored at commit
boundaries (the engine's existing cancel token). Freshness is bounded
by the chunk cadence (idle wait default 1s) rather than a wire-held
tail — recorded honestly; a protocol-level streaming tail is future
work riding a replication-capable driver (R1 note).

## R7 — TOAST policy (FR-007)

**Decision**: pgoutput marks unchanged out-of-line values in update
records. Policy: if the table's REPLICA IDENTITY is FULL, the old
tuple carries the value — the source substitutes it (retain
semantics, deterministic). Otherwise, encountering an unchanged-TOAST
marker is a TYPED error naming table + column and advising
`ALTER TABLE … REPLICA IDENTITY FULL` — never silent nulling. Config
documents exactly this; no other modes in v1.

## R8 — Identity requirements + preflight (FR-006/US4)

**Decision**: enabling CDC preflights each table: `relreplident` must
be 'd' (default, with a PK), 'f' (full), or 'i' (usable unique index);
'n' or PK-less default → typed error naming the table and the fix.
Update/delete records lacking key data at runtime (identity dropped
later) → typed error, never mis-application. Publication membership
is preflighted too (table in publication, or created via
create-if-missing with the configured table set).

## R9 — Lifecycle + distinguished errors (FR-010)

**Decision**: slot and publication are created only under
`create_if_missing: true` (idempotent: `IF NOT EXISTS` semantics via
catalog checks); rdlt NEVER drops either. Distinguished typed errors:
slot missing; slot exists with a different plugin; slot invalidated /
WAL retention overrun (`pg_replication_slots.wal_status` =
lost/unreserved → error names the condition and the fresh-snapshot
recovery); slot actively consumed by another pid (`active` = true →
the concurrent-consumer rejection); publication missing or not
covering a configured table. Replication lag (feed position minus
committed position, plus wall-clock delta when available) lands in
the run report per FR-011.

## R10 — Test rig

**Decision**: `CdcPgFixture` = the existing container fixture with
`-c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8`
command args. Fail points: `cdc.slot.create`, `cdc.snapshot.copy`
(handoff), `cdc.stream.peek`, `cdc.ack.advance` — swept with both
occurrence passes + armed-fire pins; container-kill mid-catch-up cell
mirrors the 005 pattern (poll-for-first-commit before killing).

## R11 — Performance protocol (FR-015/SC-005)

**Decision**: snapshot rides the EXISTING COPY path — the gated
pg-source bars apply unchanged and must stay within tolerance. New
scoreboard cells (5-run medians, recorded protocol): change-apply
throughput (1M-row table, 500k-change backlog → catch-up wall time)
and catch-up latency (quiet-to-caught-up on a small delta). No new
gates without a version-policy entry.

## R12 — Module + config shape

**Decision**: `crates/rdlt-postgres/src/source/cdc/{mod.rs, pgoutput.rs,
slot.rs}` inside the existing `source` feature; `PostgresConfig` gains
an optional `cdc:` block (slot, publication, create_if_missing,
mode: catchup|tail, idle_wait, flag_column default `_rdlt_deleted`
collision-checked, ack: auto|off) — schemars-derived, round-trip
tested per the 006 discipline. CDC and cursor config on the same table
are mutually exclusive (typed). Zero rdlt-core/rdlt-connector changes:
LSN cursors are ordinary `Cursor` values, CDC streams are ordinary
structured streams with a declared key.

## Sanctioned spec refinements (recorded per 007 precedent)

1. **FR-002 / US1-AS5 boundary wording**: "no gap and no overlap" →
   "no gap; the slot-to-snapshot window applies twice and CONVERGES"
   (R4 — the exported-snapshot handshake needs a replication-protocol
   client our released driver lacks; the outcome guarantee — final
   state correct, row appears once — is unchanged and is what
   conformance pins).
2. **US3 continuous tail**: v1 is a chunked catch-up loop with
   checkpoint-per-chunk and idle waits (default 1s), not a wire-held
   stream; freshness is bounded by the chunk cadence (R6). Acceptance
   scenarios hold as written; the mechanism is recorded honestly.
