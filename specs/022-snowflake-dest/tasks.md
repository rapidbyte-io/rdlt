# Tasks: Snowflake Destination Connector

**Input**: Design documents from `/specs/022-snowflake-dest/`
**Prerequisites**: plan.md, spec.md, research.md (D1–D10), data-model.md,
contracts/snowflake-dest.md (SD1–SD8), quickstart.md

**Conventions**: every increment merges on a green full local gate
(`make check`); tests run via `cargo nextest run`; live legs gate
skip-not-fail on the credential convention (research D8) and never name the
qual account in committed files; existing postgres/duckdb golden pins are
byte-identical at every merge; all performance choices are
measure-then-take (research D10).

## Phase 1: Setup

- [X] T001 Environment gate + capability probes (record ALL verdicts as
  research.md addenda, correcting assumptions): (a) pin
  `snowflake-connector-rs = "1.1"` in a scratch member and record the lock
  impact (the reqwest 0.13 tree arrives feature-gated — the recorded
  double-tree shape); (b) re-run the plan-time auth probe through the
  CRATE (not the SQL API): `KeyPairConfig::from_encrypted_pem` with the
  qual key + passphrase file, `SELECT CURRENT_VERSION()`, a
  `BEGIN; INSERT; ROLLBACK` pair across `query()` calls proving
  cross-statement transactions on one session; (c) fakesnow fidelity
  probe: venv per `tools/interop/` pattern, start `fakesnow` server mode,
  point the crate at it — login, query, BEGIN/COMMIT, `MERGE … QUALIFY`
  transpilation; ADOPT as hermetic leg or REJECT with the transcript
  recorded either way; (d) arrow-written-parquet COPY check: write a small
  parquet with the workspace writer, SigV4-PUT it to the qual stage
  bucket, `COPY INTO … MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE`, verify
  rowcount — the one remaining unknown on the proven stage path; (e)
  verify `~/.config/rdlt/snowflake/` convention resolves (key, passphrase,
  stage.env) and record the exact resolution order; (f) PAT probe: **ASK THE
  OWNER at this point** to mint a PAT for the qual user (Snowsight, ~2
  min), then authenticate through the crate's password channel with it
  and record whether the PAT-rides-password assumption holds — the `pat`
  config arm commits only on this verdict. The password test user and
  OAuth integration are NOT requested here — they are requested when
  their live cells are built (T030).
- [X] T002 [P] Create `specs/022-snowflake-dest/close-out.md` skeleton:
  contract matrix SD1–SD8 (all OPEN), story matrix (US1–US5 NOT STARTED),
  deviations section, and the ledger conventions from the 020 pattern
  (every disposition cited, none silent).

## Phase 2: Foundational (blocking all stories)

- [X] T003 sqlcore ensure-choreography extraction (the fired 020 trigger):
  move the shared table-legs / column-ensure / index-ensure choreography
  from `crates/rdlt-connector-postgres/src/dest/commit.rs` and
  `crates/rdlt-connector-duckdb/src/dest/` into
  `crates/rdlt-connector-sqlcore/src/ensure.rs` behind dialect seams,
  with BOTH executors' emitted SQL proven byte-identical (golden pins +
  full suites green before/after). Record the trigger's terminal
  disposition in close-out.md.
- [X] T004 sqlcore session-protocol extraction (the fired 013 trigger):
  extract the commit/receipt/state execute-side choreography shared by the
  postgres and duckdb sessions into
  `crates/rdlt-connector-sqlcore/src/protocol/execute.rs` (planner's
  companion; DML-transaction and DDL-outside-unit constraints
  parameterized), both existing destinations rewired, behavior and pins
  byte-identical. If byte-identity proves endangered, extract only the
  shared shapes and re-record the remainder with a named trigger — never a
  silent partial (research D7).
- [X] T005 Crate skeleton + workspace wiring:
  `crates/rdlt-connector-snowflake/` (workspace member;
  `snowflake-connector-rs` pinned per T001; `[features] failpoints`;
  workspace lints incl. `unsafe_code` deny), thin `src/lib.rs`; facade
  feature `snowflake` + `rdlt::connector::snowflake` module alias in
  `crates/rdlt/src/lib.rs`; compiles empty with the feature on and off.
- [X] T006 [P] Config vocabulary: `src/config.rs` per data-model §1 —
  account/user + the closed `auth` enum (key_pair {private_key Secret
  PEM-or-`path:`, key_passphrase}, password {password, mfa_passcode},
  oauth {token}, pat {token} — every secret Secret-wrapped)/role/
  warehouse/database/schema + `table_type` (transient|permanent),
  `session_parameters` map, `query_tag`, `host` override + the shared
  `DestOptions` vocabulary; eager typed validation naming the field;
  `#[non_exhaustive]`; schemars schema; from_yaml/from_json/from_value;
  tests in `tests/config_schema.rs` (round-trip corpus, unknown-field
  rejection, validation matrix, Secret grep-proof for EVERY secret field: key,
  passphrase, password, passcode, oauth token, PAT).
- [ ] T007 [P] The one boundary: `src/boundary.rs` — Client/Session
  construction from config mapping the FULL auth vocabulary
  (`KeyPairConfig::from_pem`/`from_encrypted_pem`,
  `AuthConfig::password` + passcode, `AuthConfig::oauth`, PAT via the
  T001-probed channel; external-browser SSO typed-unsupported),
  `EndpointConfig` host override, session parameters + QUERY_TAG applied
  at session open,
  an internal executor seam over the crate's `Session` (mockable for
  statement-count and retry tests), and error translation in ONE place:
  `Error::snowflake_code()` + `ErrorKind` → SPI taxonomy (Auth /
  Permission / Transient: Network,SessionExpired,Timeout,throttle codes /
  Fatal; code 100090 carries the duplicate-merge-key shape). The
  unit-scoped executor REFUSES DDL statement kinds (SD3 — the guard is
  code, not convention). Unit tests over the mock seam incl. the full
  classification matrix; no substring matching anywhere.
- [ ] T008 [P] Testkit credential probe: `snowflake_available()` in
  `crates/rdlt-testkit/src/` — env-first
  (`RDLT_SNOWFLAKE_*`), config-dir fallback, Option-returning
  skip-not-fail (the container posture with credential presence in place
  of runtime presence); plus a per-test-run scratch-schema helper with
  teardown mirroring container-fixture isolation.

**Checkpoint**: both extractions merged green with pins byte-identical; the
crate compiles behind its feature; config validates everywhere; the live
convention resolves.

## Phase 3: User Story 1 — Exactly-once loads with one YAML document (P1) 🎯 MVP

**Goal**: Append/Replace loads land exact totals on the qual account with
key-pair auth, replay publishes nothing, crashes converge.
**Independent test**: pg→snowflake pipeline, exact rowcount; re-run = zero
new rows; kill at each crash point and converge duplicate-free.

- [ ] T009 [US1] Identifier policy + describe-once ensure: `src/dest/ddl.rs`
  — ONE emission function producing quoted-uppercase identifiers (research
  D3); ensure reads existing structure once per session (DESCRIBE or
  INFORMATION_SCHEMA, one read), creates tables, emits additive
  `ADD COLUMN` only when the column is genuinely absent; `_rdlt_` tables
  via the sqlcore constants uppercased at emission only; hostile-identifier
  round-trip pins (mixed case, reserved words, quoted specials) in-file.
- [ ] T010 [P] [US1] Closed type mapping: enforce research D9 in ddl +
  batch encoding — Json→VARIANT, Decimal→NUMBER(p,s) with p≤38 refused
  typed at write when exceeded, VARIANT >16MB refused typed,
  Uuid→VARCHAR(36), TIMESTAMP_TZ/NTZ split; unmappable columns typed at
  ensure naming the column; unit cells in-file; the mapping table goes in
  the crate README (T041 finalizes).
- [ ] T011 [US1] Commit protocol on the sqlcore execute skeleton:
  `src/dest/commit.rs` — pure-DML unit transaction (publish + receipt +
  state in ONE explicit txn through the T004 skeleton), replay of a
  receipted (load, seq) publishes nothing and returns the prior receipt,
  rowcount-verification hook (INSERT counts now; COPY results in T013),
  crash points `sf.stage.write` / `sf.unit.publish` /
  `sf.receipt.visible` behind the failpoints feature.
- [ ] T012 [US1] INSERT ingestion path: `src/dest/ingest.rs` — batched
  multi-row INSERT through the boundary's bind machinery inside the unit;
  batch size a named constant with its measurement deferred to T037
  (comment says so); NULL-in-merge-key typed write-time refusal (house
  rule); arrival column assigned at insert (mechanism per D4, cost noted
  for T037).
- [ ] T013 [US1] External-stage COPY ingestion path: `src/dest/ingest.rs` +
  `src/dest/stage.rs` — parquet parts via the file-family writer under a
  pipeline-scoped prefix in the configured stage bucket (`object_store`),
  `CREATE STAGE` (DDL, outside units) + `COPY INTO …
  MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE` inside the unit, per-COPY
  `rows_loaded` verified against staged counts (mismatch fails the unit
  typed, SD6), `REMOVE`/object cleanup idempotent and ownership-precise
  (part-name shape); stage config optional — absent means INSERT-only.
- [ ] T014 [US1] Destination impl + open wiring: `src/dest/mod.rs` —
  `Snowflake` Destination entry, session open (context statements, state
  read, replay check), stale-staged-artifact cleanup on open (only THIS
  pipeline's), typed error when neither config nor server default supplies
  a warehouse.
- [ ] T015 [P] [US1] Pipeline-spec + CLI wiring: `destination: snowflake:`
  block in `crates/rdlt/src/pipeline_spec.rs` (embedding the crate's
  config type — the 020 DestSpec::File precedent) + spec-parse pin; CLI
  feature plumbing in `crates/rdlt-cli/`.
- [ ] T016 [US1] Live smoke leg (credential-gated) in `tests/live_dest.rs`:
  the five US1 acceptance scenarios — fresh Append exact totals + receipt;
  replay publishes nothing; Replace never observed cleared-but-unfilled;
  wrong/rotated key → typed auth error naming account+user, zero key
  material in the rendered error (asserted); suspended-warehouse
  auto-resume absorbed by timeouts.
- [ ] T017 [US1] Crash sweep in `tests/crash_sweep.rs`: `sf.*` points × 3
  actions against the live qual account (credential-gated), each point
  proven ARMED (armed-fire pins), converging to exactly-once totals with
  zero duplicate publishes — both ingestion paths covered (SD7).
- [ ] T018 [US1] US1 gate: full local gate green; close-out story row +
  SD-clause progress recorded; any deviation cited.

**Checkpoint**: MVP — a correct, crash-disciplined Snowflake destination
for Append/Replace with both ingestion paths.

## Phase 4: User Story 2 — Full merge parity without enforced constraints (P1)

**Goal**: the complete shared strategy/options vocabulary, outcome-equal to
postgres, on a dialect with no ON CONFLICT / DISTINCT ON / enforced uniques.
**Independent test**: differential oracle — identical seeded inputs land
canonical-row-equal results vs the postgres destination for every strategy.

- [ ] T019 [US2] `SnowflakeDialect` on the sqlcore MergeDialect seam:
  `src/dest/dialect.rs` — dedup via `QUALIFY ROW_NUMBER() OVER (PARTITION
  BY key ORDER BY __rdlt_arrival DESC) = 1` inside the USING subquery
  (proven live, research D4); `MERGE INTO` upsert, delete_insert, scd2
  statement sets (validity windows, markers, absent keep/retire with the
  recorded single-unit rule), merge_scope replacement, hard_delete;
  informational PRIMARY KEY declared, never relied on.
- [ ] T020 [US2] Golden pins: `tests/golden_sql.rs` — every emitted
  statement for every strategy × options arm pinned byte-for-byte; AND
  re-run the postgres + duckdb golden suites proving their pins
  byte-identical through T003/T004/T019 (SD4 both halves).
- [ ] T021 [P] [US2] Options validation parity: matrix test asserting every
  strategy × options combination accepts/rejects identically to the other
  SQL destinations with the identical shared-core error text (spec US2
  scenario 2).
- [ ] T022 [US2] Duplicate-merge-key diagnosis: map structured code 100090
  at the boundary into the shared diagnosis (columns + both remedies, same
  text as the other destinations); typed-shape test via the mock seam AND
  one live provocation cell (undeduped duplicate source rows).
- [ ] T023 [US2] Live strategy matrix (credential-gated) in
  `tests/live_dest.rs`: upsert / delete_insert / scd2 × hard_delete /
  dedup_sort / merge_scope land exact totals with last-wins semantics on
  the qual account; scd2 single-unit violation typed.
- [ ] T024 [US2] Differential oracle vs postgres: canonical-row equality
  incl. SCD2 HISTORY OPENNESS on identical seeded inputs for the full
  matrix (gated on credentials AND the pg container; lives in
  `tests/live_dest.rs` beside the strategy matrix).
- [ ] T025 [US2] US2 gate: full gate + both golden suites; close-out row.

**Checkpoint**: merge parity delivered and PROVEN equal, not asserted.

## Phase 5: User Story 3 — Frugal with round trips (P2)

**Goal**: statement economy is measured and contractual (SD7 second half).
**Independent test**: same load twice against an unchanged schema — second
run issues zero schema-mutation statements; totals constant per table.

- [ ] T026 [US3] Statement-count instrumentation on the executor seam:
  `tests/boundary_mock.rs` — counts by class (schema-read / schema-mutate /
  DML / control) recorded per load through the mock transport.
- [ ] T027 [US3] Economy pins (mock): unchanged schema → ZERO mutations and
  exactly one schema READ per table per session; one added nullable column
  → exactly one `ADD COLUMN` and nothing else; total statements constant
  in table count, independent of column count (spec US3 scenarios).
- [ ] T028 [US3] Live verification: run the same load twice on the qual
  account, count statements server-side (QUERY_HISTORY) and client-side,
  record both counts in close-out.md (SC-004).
- [ ] T029 [US3] US3 gate: full gate; close-out row.

## Phase 6: User Story 4 — Verified like the other connectors (P2)

**Goal**: the certification surface — conformance, posture, hygiene.
**Independent test**: suite green with credentials absent (visible skips);
live legs run with credentials present; tree mechanically clean of secrets.

- [ ] T030 [US4] Conformance certification: wire the testkit
  dest-conformance harness in `tests/live_dest.rs` (credential-gated) and
  pass every clause; deviations (if any) typed and recorded. PLUS the
  auth-matrix live cells: one load per auth method — key-pair, PAT,
  password (test user), OAuth token — each gated on ITS OWN credential
  entry (skip-not-fail independently), each asserting the same typed
  auth-failure shape on a corrupted secret with zero secret material in
  the rendered error. **ASK THE OWNER at this point** for the password
  test user and the OAuth integration + token-mint details (research D8
  names the env/file entries); legs whose credentials have not arrived
  skip with reason and are recorded as such.
- [ ] T031 [P] [US4] Gating posture tests: credentials absent → every
  snowflake live test reports skipped-with-reason and the workspace suite
  is green; suite-timeout audit so SaaS latency (warehouse resume, WAN)
  fits without weakening container-leg timings.
- [ ] T032 [US4] fakesnow hermetic leg — CONDITIONAL on T001's verdict:
  if ADOPTED, wire the server fixture (venv pattern) and move
  protocol-level tests onto it in the standard gate; if REJECTED, record
  the rejection + transcript as the task's terminal disposition in
  close-out.md (either way the qual account remains the leg of record).
- [ ] T033 [P] [US4] Secret + identity hygiene: grep-proof cells for key,
  passphrase, and stage secret across Debug/serialize/error/log output;
  plus the SC-005 mechanical tree search (account identifier, user name,
  key markers → zero hits) as a repeatable script recorded in close-out.
- [ ] T034 [US4] US4 gate: full gate with and without credentials present;
  close-out row.

## Phase 7: User Story 5 — Recorded performance standing (P3)

**Goal**: numbers with provenance; UNBARRED; defaults cite measurements.
**Independent test**: close-out contains the session with dataset identity,
timings, configuration; every shipped default cites a measurement.

- [ ] T035 [US5] INSERT batch-knee measurement on the qual account: sweep
  batch sizes (rows × bytes) on the bench-shaped dataset, medians of
  repeated runs, pick the knee, replace T012's placeholder constant WITH
  the citation at the site; record the sweep in close-out.md.
- [ ] T036 [US5] Recorded ingestion session: pg→snowflake, bench-shaped
  1M×12 dataset, BOTH paths (INSERT and external-stage COPY), wall +
  rows/s + statement counts + configuration recorded in close-out.md;
  determine the INSERT-vs-COPY crossover and encode it as the documented
  default selection rule (with its numbers); UNBARRED — verify
  `make bench TARGET=gate` untouched and green (SC-007).
- [ ] T037 [US5] US5 gate: full gate; close-out row; every declined
  optimization carries its number (the D-13/D-21 null hypothesis stands).

## Phase 8: Polish & close-out

- [ ] T038 [P] Crate README: closed type mapping table, identifier policy,
  auth setup walk for EVERY method (key generation → ALTER USER; PAT
  minting; password caveats — MFA enforcement, TYPE=SERVICE refusal;
  OAuth integration sketch), credential convention, s3-compatible-endpoint allowlist caveat (research D6),
  internal-stage PUT status; `make docs` clean (missing_docs on all public
  items).
- [ ] T039 [P] Quickstart verified verbatim against the qual account
  (SC-006), corrections folded back into
  `specs/022-snowflake-dest/quickstart.md`.
- [ ] T040 dlt-parity matrix with named deviations (internal-stage PUT gap
  foremost) in `specs/022-snowflake-dest/parity.md`; file the upstream
  `snowflake-connector-rs` issue for PUT/raw-response support and record
  its URL as the deferral's trigger reference in close-out.md.
- [ ] T041 Coverage ≥80% baseline-first (`make coverage`, workspace-wide,
  recorded) and semver additive-proof (`cargo semver-checks` on rdlt-core
  + rdlt-connector: "no update required") at the final increment.
- [ ] T042 Close-out matrix complete: SD1–SD8 all terminal, story matrix
  complete, zero uncited dispositions, both extraction triggers
  dispositioned (SC-008), every UNPERFORMED verification named with its
  reason.
- [ ] T043 Final `make check` twice clean on a quiet machine, recorded in
  close-out.md with the SC-005 mechanical sweep re-run at the final
  commit.

## Dependencies

- **Phase 1 → everything** (T001's verdicts steer T005 pin, T032, T013).
- **T003, T004 → T009, T011** (the snowflake ensure and commit build ON
  the extracted sqlcore shapes; extractions merge first with pins proven).
- **T005–T008 → all US phases**; T006/T007/T008 are parallel after T005.
- **US1 → US2** (dialect and strategies build on session/commit/ingest);
  **US1 → US3** (economy instruments the US1 ensure/executor);
  **US1+US2 → US4** (things to certify) **→ US5** (things to measure).
- Within US1: T009→T011→T012→T013 sequential (same files/protocol);
  T010, T015 parallel to the protocol chain; T016/T017 after T014.
- **US5 before Polish's T040 numbers**; T038/T039 parallel anytime after
  US2.

## Implementation strategy

**MVP = Phases 1–3** (setup, foundations incl. both extractions, US1): an
independently shippable Append/Replace Snowflake destination with
exactly-once discipline and both ingestion paths. Each subsequent phase is
an independently mergeable increment in value-per-risk order: merge parity
(US2) carries the most design risk and lands immediately after MVP while
the sqlcore work is fresh; economy (US3) and certification (US4) harden;
measurement (US5) runs when the shape is final so its numbers describe what
ships. Every phase ends with the full local gate and a close-out row —
nothing merges red, nothing lands unrecorded.
