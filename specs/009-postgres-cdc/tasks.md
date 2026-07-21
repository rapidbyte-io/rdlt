# Tasks: Postgres CDC via Logical Replication

**Input**: Design documents from `/specs/009-postgres-cdc/`

**Prerequisites**: plan.md, spec.md (as refined per research R4/R6),
research.md (R1–R12), data-model.md,
contracts/{cdc-protocol, cdc-config, cdc-operability}.md, quickstart.md

**Tests**: INCLUDED — every success criterion is test-defined (equality
cycle SC-001, sweep SC-002, tail SC-003, error matrix SC-004,
scoreboard SC-005, lag SC-006, schemas SC-007, outcome guarantees
SC-008). Safe Rust only; ZERO new dependencies; ZERO SPI changes.

**Organization**: pgoutput parser and the fixture are foundational
(everything depends on them); slot lifecycle + config next; then US1
bounded catch-up (MVP), US2 crash discipline, US3 tail, US4
operability, polish.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

No setup tasks — no new dependencies, no workspace changes (plan
Technical Context).

## Phase 2: Foundational (BLOCKING)

- [X] T001 [P] CDC test fixture in
      `crates/rdlt-postgres/tests/common/mod.rs`: `CdcPgFixture` — the
      existing container with `-c wal_level=logical
      -c max_replication_slots=8 -c max_wal_senders=8` command args
      (R10); helper for seeding + mutating tables and reading
      `pg_replication_slots` state.
- [X] T002 [P] pgoutput parser in
      `crates/rdlt-postgres/src/source/cdc/pgoutput.rs`: hand-rolled
      decode of Begin/Commit/Relation/Type/Insert/Update/Delete/
      Truncate/Origin + TupleData (text/binary/null/unchanged-TOAST
      markers), proto_version 1 (R2); unit tests over hand-built
      message bytes incl. malformed inputs (typed errors, no panics);
      property test; NEW fuzz target
      `fuzz/fuzz_targets/pg_pgoutput_decode.rs` registered in
      `fuzz/Cargo.toml` + the Makefile FUZZ_TARGETS list.
- [X] T003 [P] Config surface in
      `crates/rdlt-postgres/src/source/config.rs`: `CdcConfig` block
      (slot, publication, create_if_missing, mode catchup|tail,
      idle_wait — 007 duration vocabulary, flag_column default
      `_rdlt_deleted`, ack auto|off) per data-model; schemars derive;
      validation: cdc+cursor mutually exclusive per table (typed,
      names the table), flag-column collision check deferred to open
      (needs reflection), empty names rejected; unit validation
      matrix (C1/C2/C4).
- [X] T004 Slot lifecycle in
      `crates/rdlt-postgres/src/source/cdc/slot.rs`: SQL-interface
      wrappers — create-if-missing (idempotent via catalog checks;
      NEVER drop), `pg_logical_slot_peek_binary_changes` range peek,
      `pg_replication_slot_advance` ack, and the DISTINGUISHED typed
      errors from `pg_replication_slots` state: missing slot, wrong
      plugin, invalidated/WAL-overrun (`wal_status`), active
      concurrent consumer (names pid), publication missing/gap (O2);
      integration cells against `CdcPgFixture` for each error class
      + create_if_missing idempotency (depends on T001).

**Checkpoint**: the protocol substrate exists, fuzzed and typed.

## Phase 3: User Story 1 — Bounded catch-up (Priority: P1) 🎯 MVP

**Goal**: snapshot + delete-aware catch-up mirroring on a cron
cadence; the convergent-overlap boundary proven.

**Independent Test**: spec US1 — seed → run 1 snapshots → mutate
(inserts/updates/deletes) → run 2 equals the source row-for-row → a
no-change run moves nothing.

- [X] T005 [US1] Stream declaration + snapshot pass in
      `crates/rdlt-postgres/src/source/mod.rs` +
      `src/source/cdc/mod.rs`: CDC tables declare keyed structured
      streams with the key from the replica-identity preflight
      (relreplident 'd'-with-PK/'f'/'i'; else typed error naming
      table + fix — O1); first run (no cursor): ensure slot FIRST,
      then ONE `REPEATABLE READ` transaction snapshots every CDC
      table through the EXISTING COPY path, cursor initialized to the
      slot's consistent point (R4); WELDED conformance: the
      boundary-overlap cell in `crates/rdlt-postgres/tests/cdc.rs` —
      a row mutated between slot creation and snapshot end appears
      exactly once with its final state (P2 — the recorded
      refinement's NON-OPTIONAL proof).
- [X] T006 [US1] Change pass in
      `crates/rdlt-postgres/src/source/cdc/mod.rs`: pin `target_lsn`
      at run start; per-stream peek over `(cursor, target_lsn]`
      filtered to the stream's table; decode via pgoutput into
      structured arrow batches carrying `flag_column` (NULL for
      insert/update; TRUE + key-only for delete); PK-changing update
      emits delete(old)+insert(new) in order (P5); checkpoints ONLY
      at transaction-commit LSNs (P4); batches bounded by the
      existing batch knobs (memory bounded regardless of tx size).
- [X] T007 [US1] Acknowledgement in
      `crates/rdlt-postgres/src/source/cdc/mod.rs`: accumulate each
      stream's resume cursor; after the LAST stream's pass, advance
      the slot to min(committed cursors); `ack: off` skips; dying
      early acks nothing (P6); WELDED (plan rule — an unpinned ack is
      silent data loss): the ack-after-commit pin in
      `tests/cdc.rs` — the slot's confirmed position never exceeds
      the least committed stream cursor, verified across a partial
      run.
- [X] T008 [US1] Equality-cycle conformance in
      `crates/rdlt-postgres/tests/cdc.rs` (new): full US1 cycle with
      the recommended composition (merge{key} + upsert +
      hard_delete): snapshot → mutate (inserts, updates, DELETES) →
      catch-up → destination equals source row-for-row (deleted rows
      GONE); third run moves nothing; net no-op transaction leaves
      nothing; multi-row multi-table source transaction applies in
      commit order; totals exact (SC-001, SC-008).

**Checkpoint**: delete-aware mirroring works — shippable MVP.

## Phase 4: User Story 2 — Crash discipline (Priority: P2)

**Goal**: the project's signature guarantee extended to the feed.

**Independent Test**: spec US2 — sweep every registered CDC fail
point, both passes, armed-fire pins; container-kill mid-catch-up;
destination converges to source-equal state.

- [X] T009 [US2] Crash sweep in
      `crates/rdlt-postgres/tests/cdc_crash_sweep.rs` (new):
      register `cdc.slot.create`, `cdc.snapshot.copy`,
      `cdc.stream.peek`, `cdc.ack.advance` crash points in the cdc
      modules (registry-pinned like G2.2); sweep all four × three
      actions × both occurrence passes with armed-fire pins;
      post-recovery equality checks; container-kill mid-catch-up
      cell (poll-for-first-commit before killing, 007 pattern);
      redelivered-delete no-op and redelivered-update convergence
      cells (SC-002, US2-AS4).

## Phase 5: User Story 3 — Continuous tail (Priority: P3)

**Goal**: chunked-loop tail (recorded refinement 2) — apply until
cancelled, checkpoint per chunk, cancel cleanly.

**Independent Test**: spec US3 — burst applies without restart; clean
cancel at a commit boundary; resume without loss/duplication; quiet
idle.

- [X] T010 [US3] Tail mode in
      `crates/rdlt-postgres/src/source/cdc/mod.rs`: `mode: tail`
      loops bounded catch-up chunks, waits `idle_wait` when quiet,
      honors the engine cancel token between chunks (P7);
      conformance in `tests/cdc.rs`: source mutations during a
      running tail appear at the destination without restart;
      cancellation stops at a commit boundary; a subsequent run
      (either mode) resumes exactly; quiet feed idles without
      busy-work (SC-003).

## Phase 6: User Story 4 — Operability (Priority: P4)

**Goal**: every ugly CDC failure mode loud, typed, and distinguished;
lag visible; TOAST deterministic.

**Independent Test**: spec US4 — each failure mode produces its own
typed error in a test; lag appears per run.

- [X] T011 [US4] TOAST policy in
      `crates/rdlt-postgres/src/source/cdc/mod.rs` + `pgoutput.rs`:
      unchanged-TOAST marker + REPLICA IDENTITY FULL → substitute
      the value from the old tuple image (deterministic retain);
      without FULL → typed error naming table + column + the ALTER
      advice (O3, R7); conformance matrix in `tests/cdc.rs`: FULL
      table round-trips a large TOAST value through an unrelated
      update; default-identity table fails typed on the same shape.
- [X] T012 [US4] Error matrix + lag in `tests/cdc.rs` +
      `src/source/cdc/{mod,slot}.rs`: conformance cells for EVERY
      distinguished error (identity unusable; identity dropped
      mid-stream never mis-applies; missing slot without
      create_if_missing; wrong-plugin slot; concurrent consumer;
      publication gap; cdc+cursor exclusivity; flag-column
      collision) (SC-004); replication lag surfaced per completed
      run — LSN delta + time delta where available — via the run
      report/structured tracing seam chosen at implementation
      (engine-internal additions allowed; rdlt-core/rdlt-connector
      untouched), with a capture test (SC-006, O5).

## Phase 7: Polish & close-out

- [ ] T013 [P] Config/CLI/schemas: cdc block in
      `crates/rdlt-postgres/tests/config_schema.rs` round-trips
      (examples validate, unknown fields fail both, exclusivity
      cell); CLI passthrough verified (source yaml already carries
      cdc:); the C3 composition WARNING (merge+upsert+hard_delete
      recommended) emitted when absent, with a capture test; README
      + quickstart truthful (SC-007).
- [ ] T014 [P] Scoreboard in `benches/run-cdc.sh` (new): change-apply
      throughput (1M-row table, 500k-change backlog → catch-up wall
      time) and quiet catch-up latency; 5-run medians; recorded as
      scoreboard entries + history line in `benches/RESULTS.md`
      (SC-005; existing gated bars must stay within tolerance — no
      new gates).
- [ ] T015 Close-out: `make check` + `cargo test --doc` +
      `cargo semver-checks check-release --baseline-rev origin/main
      -p rdlt-core -p rdlt-connector` ("no update required");
      fuzz target listed and smoke-run; implementation-notes block at
      the top of this file (incl. proofs that both recorded
      refinements have their conformance cells); memory/README
      updates; all tasks [X].

## Dependencies & Execution Order

```text
Phase 2: T001 ∥ T002 ∥ T003 → T004 (needs T001)
   └► US1: T005 → T006 → T007 → T008   (MVP; needs T002+T003+T004)
        └► US2: T009
        └► US3: T010                    (independent of US2)
        └► US4: T011 ∥ T012             (independent of US2/US3)
   Polish: T013 ∥ T014 (need US1) → T015 (last)
```

- T001/T002/T003 are genuinely parallel (different files, no shared
  state); single-session order as listed.
- T005's boundary-overlap cell and T007's ack pin are WELDED to their
  implementation tasks (plan rules) — do not split.
- T009's fail points must be registered in the same task as the sweep
  that pins them (armed-fire discipline).

## Implementation Strategy

MVP = Phases 2+3 (bounded catch-up with the convergent boundary and
pinned acks). US2 hardens it; US3/US4 layer independently. Nothing
outside `crates/rdlt-postgres` (+ fuzz list, benches, possibly an
engine-internal lag field) changes; semver-checks arbitrates the
zero-SPI promise and the perf gate arbitrates every step.
