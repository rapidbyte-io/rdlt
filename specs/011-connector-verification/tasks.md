# Tasks: Postgres Connector Verification — Every Parameter Proven

**Input**: Design documents from `/specs/011-connector-verification/`

**Prerequisites**: plan.md, spec.md, research.md (R1–R6 incl. the
measured 87.69% baseline), data-model.md (matrix schema + interaction
rows), contracts/parameter-matrix.md (PM1–PM8), quickstart.md

**Tests**: this feature IS tests — every gap cell states a one-sentence
behavioral claim (PM2/PM3); coverage-only tests are forbidden (PM5).
Mismatches found in ANY task are resolved IN that task (fix + pinned
cell, or doc correction) and logged in the close-out list (PM6).

**Organization**: tooling+baseline first; the matrix skeleton's citation
audit produces the gap list that sizes everything; per-block gap tasks
are parallel (different suites); the R5 fix is its own welded task;
final coverage + classification close US2.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

No setup tasks beyond T001 (tooling is part of the measurement weld).

## Phase 2: Foundational (BLOCKING)

- [X] T001 Coverage tooling + official baseline: add the `coverage`
      target to `Makefile` (R6: `cargo llvm-cov nextest -p rdlt-postgres
      --features failpoints`, total + per-file table; NOT part of
      `check`); verify cargo-llvm-cov + llvm-tools-preview; run TWICE to
      confirm stability; record the official baseline (total, per-file
      table, command, date) in `benches/RESULTS.md` — WELDED (plan rule:
      an unrecorded baseline poisons every later claim). Confirm or
      correct research R2's planning-time numbers (87.69% lines).

## Phase 3: User Story 1 — The parameter conformance matrix (Priority: P1) 🎯 MVP

**Goal**: every parameter row cited by behavioral cells; gaps closed in
the suites that own them.

**Independent Test**: sample any README parameter → matrix row → run the
cited cells → the documented behavior is exercised (SC-002).

- [ ] T002 [US1] Matrix skeleton + citation audit in
      `specs/011-connector-verification/matrix.md`: create every table
      per data-model schema with ALL R4 inventory rows + FR-003
      interaction rows; audit the EXISTING suites
      (`crates/rdlt-postgres/tests/*.rs`, `crates/rdlt-cli/src/main.rs`
      tests) and fill citations where cells already pin the row
      (verifying each citation actually proves the claim — PM4); mark
      every uncited row `GAP` and every misdocumented behavior
      `MISMATCH`. Output: the gap/mismatch worklist that drives
      T003–T010. The matrix commits WITH gap-closing tasks, never ahead
      of them (plan rule) — this task may land with T003.
- [ ] T003 [P] [US1] Source top-level + table-entry + selection rows in
      `crates/rdlt-postgres/tests/conformance.rs` (+ `reflect_live.rs`
      where reflection-owned): close the T002 gaps for `schema`,
      `include_views`, `tables` discover-all vs listed, `primary_key`
      override, included/excluded columns (incl. hostile identifiers),
      `batch_target_bytes`/`batch_max_rows` observable batch cutting;
      fill matrix citations.
- [ ] T004 [P] [US1] Cursor-block rows in
      `crates/rdlt-postgres/tests/incremental.rs`: close gaps across
      `column`, `initial_value`, `boundary`×`lag`, `direction: min`,
      `end_value`×`end_bound`, all three `nulls` policies under resume,
      `lag` families (duration/magnitude/date whole-days) × write mode;
      fill matrix citations.
- [ ] T005 [P] [US1] Type-hint rows (all 12 values + closed-table
      rejections + hint×cursor-capability) in
      `crates/rdlt-postgres/tests/conformance.rs`; fill matrix
      citations.
- [ ] T006 [P] [US1] Query-stream rows in
      `crates/rdlt-postgres/tests/query_streams.rs`: `name`
      uniqueness, `sql` read-only enforcement + describe, query cursor
      parity, declared `primary_key`, query `type_hints`; fill matrix
      citations.
- [ ] T007 [P] [US1] CDC-block rows in
      `crates/rdlt-postgres/tests/cdc.rs`: close gaps across all 7
      params (incl. `idle_wait` observable pacing class, `ack: off`
      retention behavior, `flag_column` custom name end-to-end) + the
      CDC interaction rows; fill matrix citations.
- [ ] T008 [P] [US1] TLS + conn-string rows in
      `crates/rdlt-postgres/tests/tls_matrix.rs`: close gaps across the
      5 modes × both directions, root_cert forms (path/inline/system),
      client-cert both-or-neither, conn-string translation/precedence/
      contradiction/rejected-by-name/percent-escape/application_name
      rows; fill matrix citations.
- [ ] T009 [P] [US1] Destination rows in
      `crates/rdlt-postgres/tests/dest_conformance.rs` (+
      `scd2.rs`): close gaps across `dataset`, destination `tls`
      passthrough, strategy values, `hard_delete` bool vs non-bool
      flags, `dedup_sort`, `merge_key`, scd2 block (`valid_from`/
      `valid_to` custom names, `absent` both values); fill matrix
      citations.
- [ ] T010 [P] [US1] CLI pipeline-spec rows in
      `crates/rdlt-cli/src/main.rs` tests: `pipeline`, `workdir`
      default+custom, `write_mode` three forms, postgres source inline
      XOR `{config: path}` + mixing rejection, destination kinds parse
      rows; fill matrix citations.

**Checkpoint**: matrix has zero GAP rows; MISMATCH rows resolved or
carried explicitly into T011.

## Phase 4: User Story 3 — Mismatches resolved (Priority: P3)

**Goal**: design, code, tests, and docs agree; the recorded footnote
dies here.

**Independent Test**: close-out mismatch list complete; every entry
fixed+pinned or doc-corrected.

- [ ] T011 [US3] R5 fix in `crates/rdlt-postgres/src/dest/config.rs` +
      `crates/rdlt-postgres/src/dest/commit.rs`: distinguish explicit
      from defaulted `merge_strategy` (parse-compatible shape);
      `ensure_table` rejects EXPLICIT configuration (destination-wide or
      per-table) under append/replace, typed, naming table + mode;
      unconfigured default never rejects — WELDED cells in
      `crates/rdlt-postgres/tests/dest_conformance.rs` (explicit
      rejected under append AND replace; default accepted; merge
      unaffected) + `crates/rdlt-postgres/tests/config_schema.rs`
      round-trip unchanged; README/contract wording updated. Any OTHER
      mismatches surfaced by T003–T010 that need code fixes land here
      too (sweep coverage if on a publish/read path), each with its
      pinned cell.

## Phase 5: User Story 2 — The measured floor (Priority: P2)

**Goal**: ≥80% recorded honestly with a classified remainder.

**Independent Test**: `make coverage` reproduces the recorded number;
exclusion list short with reasons.

- [ ] T012 [US2] Final coverage + classification: re-run
      `make coverage`; record final total + per-file table in
      `benches/RESULTS.md` next to the T001 baseline; classify EVERY
      remaining uncovered cluster (research R2 hypotheses verified —
      e.g. testhook bench/fuzz entries, subprocess CLI path, platform
      TLS arms, defensive unreachables) with file+reason; confirm ≥80%
      floor (SC-003). If a cluster turns out to be a REAL untested
      branch, it goes back to the owning T003–T010 suite as a cell, not
      into the exclusion list.

## Phase 6: Polish & close-out

- [ ] T013 Close-out: `make check` + `cargo test --doc` +
      `cargo semver-checks check-release --baseline-rev origin/main
      -p rdlt-core -p rdlt-connector` ("no update required"); spot-audit
      three random matrix rows end-to-end (SC-002, recorded);
      implementation-notes block at the top of this file with the FULL
      mismatch list and resolutions (PM6) + coverage numbers; memory
      update; all tasks [X].

## Dependencies & Execution Order

```text
Phase 2: T001
   └► US1: T002 → (T003 ∥ T004 ∥ T005 ∥ T006 ∥ T007 ∥ T008 ∥ T009 ∥ T010)
        └► US3: T011   (needs the T002–T010 mismatch list; R5 standalone
                        part can start after T002)
             └► US2: T012 (after ALL cells land)
                  └► T013 (last)
```

- T003–T010 are genuinely parallel (different suites); single-session
  order as listed.
- T001's baseline recording and T011's rejection cells are WELDED to
  their implementation (plan rules).
- Matrix commits ride WITH their gap-closing cells (T002 lands with
  T003 at the earliest).

## Implementation Strategy

MVP = Phases 2+3 (the matrix with zero gaps — the product claim "every
parameter proven" is deliverable from there). US3 makes the surface
honest, US2 seals it with the measured floor. Nothing outside tests,
the matrix, the Makefile target, and the R5 validation fix changes;
semver-checks arbitrates the frozen SPI and the recorded bars stay
within tolerance.
