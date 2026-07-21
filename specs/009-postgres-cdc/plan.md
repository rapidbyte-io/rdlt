# Implementation Plan: Postgres CDC via Logical Replication

**Branch**: `009-postgres-cdc` | **Date**: 2026-07-21 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/009-postgres-cdc/spec.md`

## Summary

CDC lands as a COMPOSITION of machinery the stack already trusts, with
one decisive constraint discovered up front: the released tokio-postgres
(0.7.18, verified in the registry) has NO replication-protocol support,
so v1 consumes the feed through the SQL logical-decoding interface over
the ordinary connection path — `pg_logical_slot_peek_binary_changes`
to read WITHOUT consuming, `pg_replication_slot_advance` to acknowledge
exactly when we choose (R1). That peek/advance split is what makes the
frozen-SPI delivery design sound: bounded catch-up pins a target LSN
and runs one PASS per CDC stream over the same peeked range, filtering
per table, checkpointing only at transaction-commit boundaries (R3).
The slot is created first, the snapshot (all tables under ONE repeatable
read — incidentally closing the 008 audit's cross-table-consistency
note) follows, and the slot-to-snapshot window applies twice and
CONVERGES — recorded as sanctioned spec refinement 1 (R4). pgoutput
messages are hand-rolled + fuzzed like the 005 COPY decoder (R2).
Deletes ride the 008 hard-delete composition; updates upsert by key;
acks advance to the min committed cursor across streams, once per run
(R5). Continuous tail v1 = chunked catch-up loop with idle waits —
sanctioned refinement 2 (R6). TOAST policy: substitute-from-old-image
under REPLICA IDENTITY FULL, typed error otherwise (R7). Preflights,
lifecycle errors, and lag reporting per R8/R9. Zero rdlt-core/
rdlt-connector changes: LSN cursors are ordinary Cursor values on
ordinary keyed structured streams.

## Technical Context

**Language/Version**: Rust stable (2024 edition), workspace v0.2.0

**Primary Dependencies**: none new — SQL-level decoding keeps
tokio-postgres 0.7.18 sufficient (R1, verified); pgoutput parser is
in-crate (R2)

**Storage**: engine state carries LSN cursors (ordinary Cursor
values); server-side slot/publication are user-owned resources managed
per R9; no rdlt state-format changes

**Testing**: `CdcPgFixture` (wal_level=logical container, R10); the
snapshot→mutate→catch-up equality suite; crash sweep over the four new
fail points (cdc.slot.create, cdc.snapshot.copy, cdc.stream.peek,
cdc.ack.advance) with both passes + armed-fire pins; container-kill
mid-catch-up; pgoutput property tests + NEW fuzz target
(pg_pgoutput_decode); lifecycle/preflight distinguished-error matrix;
config schema round-trips

**Target Platform**: Linux; reference machine unchanged

**Project Type**: Rust library workspace + dev CLI; net-zero crates
(new `source/cdc/` module inside rdlt-postgres)

**Performance Goals**: snapshot rides the EXISTING gated COPY bars
(must stay within tolerance); NEW scoreboard cells (5-run medians,
recorded protocol): change-apply throughput (500k-change backlog
catch-up) and quiet catch-up latency (R11); measurement-first, no new
gates

**Constraints**: safe Rust only; ZERO rdlt-core/rdlt-connector changes
(semver-checks stays "no update required"); no new dependencies; slot/
publication NEVER dropped by rdlt; ack only after destination commit;
additive-only drift rules unchanged; the two spec refinements (R4
boundary convergence, R6 chunked tail) are RECORDED, not silent

**Scale/Scope**: one new module family (cdc/{mod,pgoutput,slot}.rs),
one config block, four fail points, one fixture variant, one fuzz
target, three contracts, two scoreboard cells

## Constitution Check

Constitution file remains the unfilled template; governing principles
carried from features 001–008. **Seams sacred**: PASS — zero SPI
changes (verified design: LSN cursors are ordinary Cursors, CDC
streams are ordinary keyed structured streams; delivery works through
per-table passes, not new push seams); both spec deviations are
recorded refinements with rationale (R4, R6). **No silent failures**:
PASS — every operational failure mode is a DISTINGUISHED typed error
(R9); TOAST never silently nulls (R7); identity loss never
mis-applies (R8). **Correctness before speed**: PASS — the
peek-don't-consume design makes every pass resumable; acks are
correctness-independent hygiene; the sweep + container-kill discipline
applies before any scoreboard number is recorded. **Measured, not
asserted**: PASS — R11 protocol; existing bars untouched. **Safe
Rust**: PASS — pgoutput parsing is plain byte handling, fuzzed.

Post-design re-check: PASS — no new crates, no SPI surface, no
unsafe, no state-format changes; the chunked-tail refinement keeps
the long-lived mode inside the existing run/cancellation model.

## Project Structure

### Documentation (this feature)

```text
specs/009-postgres-cdc/
├── plan.md              # This file
├── research.md          # R1–R12 + two sanctioned spec refinements
├── data-model.md        # Config block, cursors, change records, errors
├── quickstart.md        # Enable CDC, recommended pipeline shape, ops
├── contracts/
│   ├── cdc-protocol.md      # Snapshot boundary, passes, ordering, acks
│   ├── cdc-config.md        # The cdc: block + composition requirements
│   └── cdc-operability.md   # Preflights, lifecycle errors, lag, TOAST
└── tasks.md             # /speckit-tasks output (NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
crates/rdlt-postgres/
├── src/source/
│   ├── cdc/
│   │   ├── mod.rs        # stream integration: target-LSN pinning, per-table
│   │   │                 #   passes, tx-boundary checkpointing, chunked tail
│   │   │                 #   loop, ack accumulation (min over streams),
│   │   │                 #   preflights, lag computation, fail points
│   │   ├── pgoutput.rs   # hand-rolled message parser (Begin/Commit/Relation/
│   │   │                 #   Insert/Update/Delete/Truncate/TupleData incl.
│   │   │                 #   unchanged-TOAST markers) — fuzzed + property-tested
│   │   └── slot.rs       # SQL-interface lifecycle: create-if-missing, peek,
│   │                     #   advance, wal_status/active checks → distinguished
│   │                     #   typed errors (R9)
│   ├── config.rs         # + CdcConfig block (schemars; mutually exclusive
│   │                     #   with cursor config per table; flag-column
│   │                     #   collision check)
│   └── mod.rs            # streams(): CDC streams declared keyed structured;
│                         #   read(): dispatch snapshot pass vs change pass
├── tests/
│   ├── cdc.rs            # NEW: US1 equality cycle, boundary-overlap cell,
│   │                     #   PK-changing update, net no-op tx, TOAST matrix,
│   │                     #   preflight/lifecycle distinguished errors, lag
│   ├── cdc_crash_sweep.rs# NEW: four fail points × both passes + armed-fire
│   │                     #   pins + container-kill mid-catch-up
│   └── common/mod.rs     # + CdcPgFixture (wal_level=logical)
├── fuzz/fuzz_targets/pg_pgoutput_decode.rs   # NEW target + Makefile line
└── benches/run-cdc.sh    # scoreboard: apply throughput + catch-up latency
```

## Design Notes (delta-level)

- **Passes (R3)**: `read()` for a CDC stream = peek `(cursor, target]`
  with the publication filter, decode pgoutput, keep only this table's
  changes, batch by size, CHECKPOINT ONLY AT COMMIT LSNs. The peeked
  range is identical across streams; nothing is consumed until ack.
- **Snapshot handoff (R4)**: first run (no cursor): slot ensure →
  REPEATABLE READ tx → COPY every CDC table (existing path, one
  snapshot) → cursor starts at the slot's consistent point. Overlap
  converges; conformance pins the overlap cell explicitly.
- **Apply semantics (R5)**: insert/update → row with flag NULL; delete
  → key columns + flag TRUE (non-key columns NULL); PK-change → two
  records ordered delete-then-insert. Quickstart + a validation
  warning steer pipelines to merge{key}+upsert+hard_delete.
- **Ack (R5)**: source accumulates each stream's committed cursor
  (`since`) as reads begin; after the LAST stream's pass, advance the
  slot to min(cursors). Early death = no ack = safe.
- **Tail (R6)**: loop {catch-up chunk; if quiet, idle_wait}; honors
  the engine cancel token between chunks; checkpoints flow per chunk.
- **TOAST (R7)**: unchanged-TOAST + identity FULL → substitute from
  old image; otherwise typed error naming table/column + the ALTER
  advice.
- **Preflights (R8/R9)**: replica identity, publication membership,
  slot plugin/activity/wal_status — each failure its own typed error;
  lag (target LSN − committed LSN, plus time delta from
  `pg_last_committed_xact` when available) in the run report.

## Verification Map (story → proof)

| Story | Proof surface |
|---|---|
| US1 catch-up | cdc.rs equality cycle (snapshot → mutate → catch-up → equal; no-change run moves nothing); boundary-overlap cell; commit-order + net-no-op-tx cells (SC-001) |
| US2 crash | cdc_crash_sweep.rs: 4 points × both passes, armed-fire pins; ack-after-commit pin; container-kill mid-catch-up (SC-002) |
| US3 tail | chunked-loop cell: burst applies without restart, clean cancel at commit boundary, quiet idle (SC-003) |
| US4 ops | distinguished-error matrix (identity, missing/foreign slot, invalidated/overrun, concurrent consumer, publication gaps); lag in report (SC-004, SC-006) |
| pgoutput | property tests + fuzz target registered (Makefile/fuzz list) |
| Perf | existing bars within tolerance + run-cdc.sh scoreboard cells (SC-005) |
| Schemas | config_schema.rs round-trips for the cdc block (SC-007) |

## Phase 2 note for /speckit-tasks

Order: pgoutput parser first (pure, fuzzable, everything depends on
it) → slot lifecycle + preflights → US1 bounded catch-up (snapshot
handoff + passes + apply semantics, WELDED to the equality cycle and
boundary-overlap cells) → US2 sweep + container-kill → US3 chunked
tail → US4 error matrix + lag → config/schema + CLI → benches +
close-out. The ack logic MUST land in the same task as its
ack-after-commit pin (an unpinned ack is a silent-data-loss risk);
the boundary-overlap conformance cell is NON-OPTIONAL (it is the
recorded refinement's proof).
