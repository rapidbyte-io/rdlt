# Tasks: Iceberg Destination (Provider-Agnostic REST Catalog)

**Input**: Design documents from `/specs/016-iceberg-dest/`

**Prerequisites**: plan.md, research.md (R1–R10, survey verdict
recorded), data-model.md, contracts/iceberg-dest.md (ID1–ID8),
quickstart.md

**Tests**: included — the standing discipline: every capability lands
WITH its cells; container cells skip-not-fail; interop (pyiceberg) is
the correctness oracle, not an afterthought; the matrix commits WITH
the cells that close its gaps (011 rule). The T001 probe verdicts are
RECORDED decisions — a narrowed capability is typed and documented,
never silent (ID5).

**Organization**: tasks grouped by user story; US order is build
order. Every task leaves the whole suite green.

## Phase 1: Setup

- [X] T001 Environment gate + capability probes (the recorded
  go/narrow decisions): verify the podman shim and socket; pull and
  verify the Polaris image (confirm image/tag, ports, bootstrap
  credential env, in-memory mode flags by starting it against the 015
  RUSTFS container and completing a real catalog handshake —
  `GET /v1/config` + OAuth2 token — record verified facts in
  `specs/016-iceberg-dest/research.md` R8, correcting assumptions);
  verify the UC OSS image's Iceberg REST surface (usable → the bearer
  leg exists; else record DEFERRED in R8); PROBE iceberg-rust 0.10
  capabilities in a scratch project against the live Polaris:
  (a) fast-append transaction commit, (b) OVERWRITE surface — record
  the FR-008/R7 verdict (Replace ships vs v1 narrows to Append with
  Replace typed-unsupported), (c) vended-credentials config, (d)
  whether iceberg-catalog-rest permits custom auth/signing hooks (the
  R4 Glue phase-2 door). Set up the pyiceberg venv
  (`tools/interop/requirements.txt`, pinned) and prove it can read a
  Polaris-cataloged table the probe wrote. Record ALL verdicts in
  research.md addenda.

## Phase 2: Foundational (blocking all stories)

- [X] T002 Crate skeleton + config vocabulary:
  `crates/rdlt-connector-iceberg/` (workspace member; deps per R1:
  iceberg/iceberg-catalog-rest/iceberg-storage-opendal pinned 0.10,
  `[features] failpoints`), `src/lib.rs` thin façade;
  `src/config.rs` per data-model §1 — catalog block (uri, warehouse,
  auth: oauth2_client_credentials | bearer, props escape hatch),
  namespace + create_namespace, storage override (family S3 spelling,
  local `Secret` newtype per the 014/015 pattern), tables +
  partition_by (identity|year|month|day|hour, closed spellings),
  eager typed validation, `#[non_exhaustive]` vocabulary with
  constructors, from_yaml/from_json/from_value, generated schema;
  cells in `crates/rdlt-connector-iceberg/tests/config_schema.rs`
  (round-trip corpus, unknown-field rejection, validation matrix,
  Secret grep-proof).
- [X] T003 [P] Closed type mapping + error boundary:
  `src/schema.rs` — the data-model §2 table (engine LogicalType +
  arrow nested shapes → iceberg types; unmappable typed naming the
  column; unit cells in-file); `src/errors.rs` — `iceberg::Error` →
  typed classification in ONE place (transient: network/5xx/429/
  credential-expiry; fatal: auth/missing-warehouse/schema-conflict/
  retry-exhaustion; every message names catalog/namespace/table/
  column; unit cells over constructed errors).
- [X] T004 Polaris+RUSTFS fixture:
  `crates/rdlt-connector-iceberg/tests/common/mod.rs` — testcontainers
  fixture starting RUSTFS (reuse the 015 pattern) + Polaris wired to
  it (T001-verified image/env), catalog health = /v1/config with the
  bootstrap credential, helper yielding a ready IcebergConfig;
  skip-not-fail without a runtime socket; a smoke cell proving
  namespace create + empty table create/load round-trip.

**Checkpoint**: config parses/validates everywhere; the local
catalog world is reachable from tests.

## Phase 3: User Story 1 — Exactly-once tables through the catalog (P1) 🎯 MVP

**Goal**: the Destination/LoadSession implementation — append path,
snapshot-native receipts, conflict retry, Replace-or-narrowing, crash
discipline.

**Independent test**: engine runs against Polaris+RUSTFS land exact
totals with one snapshot per non-empty commit; replayed commits
publish nothing; crash sweep converges with a duplicate-free snapshot
history.

- [X] T005 [US1] Append path: `src/dest.rs` (Destination +
  LoadSession: capabilities merge:false, open/ensure_table/write/
  commit skeleton) + `src/commit.rs` (batch staging → data-file write
  via the library writer, fast-append transaction per commit) +
  ensure_table (namespace create iff configured, load-or-create
  table via the closed mapping); engine-driven cells in
  `crates/rdlt-connector-iceberg/tests/catalog_live.rs`: exact totals
  through the ENGINE into a Polaris-cataloged table, one snapshot per
  non-empty commit, empty commit publishes nothing, typed
  unreachable/unauthorized/missing-warehouse cells.
- [ ] T006 [US1] Exactly-once + state (ID2): commit identity
  properties (rdlt.pipeline/load-id/commit-seq) on every snapshot;
  replay detection walking snapshot history BEFORE building the
  transaction (discard staged, return prior receipt); StateDoc in
  the `rdlt.state` table property updated in the same commit;
  read_state from the catalog; cells: replayed (load, seq) publishes
  NOTHING (snapshot count unchanged), a two-run incremental pipeline
  resumes from the state property, receipts visible via raw snapshot
  summaries.
- [ ] T007 [US1] Bounded conflict retry (ID3): refresh→rebuild→commit
  ×4 with jitter in `src/commit.rs`; exhaustion typed naming table +
  competing snapshot; cell: a competing writer (second session)
  commits between rdlt's table load and commit — rdlt retries and
  lands WITHOUT losing the foreign snapshot; exhaustion cell with a
  hammering competitor asserts the typed error.
- [ ] T008 [US1] Replace per the T001 verdict (ID5): if overwrite
  probed GREEN — Iceberg overwrite once-per-load with the durable
  guard read from snapshot history (cells: replace replaces exactly
  once per load, crash-recovery session does not re-truncate, new
  load replaces again); if probed RED — Replace = typed
  "not supported by this release" at ensure_table (cell), the
  narrowing recorded in spec/FR-008 + parity + README.
- [ ] T009 [US1] Crash discipline (ID7): `ice.files.write`,
  `ice.commit`, `ice.receipt.visible` points in commit.rs +
  `ICE_FAIL_POINTS` registry; `tests/sweep.rs` sweeping ×3 actions
  against the LIVE fixture (skip-not-fail), asserting exactly-once
  totals AND duplicate-free snapshot history; Makefile TARGET=sweep
  gains the binary.

**Checkpoint**: US1 = a correct, crash-disciplined Iceberg
destination against a real catalog.

## Phase 4: User Story 2 — Provider matrix (P2)

**Goal**: the auth vocabulary proven, credential vending default,
UC bearer leg if the gate verified it.

**Independent test**: OAuth2 + vended runs against Polaris with zero
user storage keys; bearer leg per gate verdict; schema round-trips
for every auth spelling.

- [ ] T010 [US2] Vending + storage override: vended-credentials
  catalog props as the DEFAULT storage path (no user keys), the
  family-S3 storage override as the explicit alternative; cells:
  a full engine run with NO storage block against Polaris (vended),
  the same with the explicit override, expiry-mid-run classification
  cell (transient) if the fixture permits simulating it — else the
  classification unit cell in errors.rs stands and the live gap is
  recorded in the matrix.
- [ ] T011 [P] [US2] Bearer leg per the T001 verdict: if UC OSS was
  verified — a `tests/catalog_live.rs` bearer arm against the UC
  container (append + read-back); else the bearer scheme is proven at
  the config/attachment level (schema round-trip + grep-proof + a
  wiremock-style header assertion if feasible) and the deferred live
  leg is recorded in R8/matrix. SigV4/Glue stays phase-2 (recorded —
  no task).

**Checkpoint**: one document, provider-swappable auth, no storage
keys required on vending catalogs.

## Phase 5: User Story 3 — Interop is the oracle (P3)

**Goal**: independent engines read what rdlt writes.

**Independent test**: pyiceberg (standard gate) and Spark (deep tier)
read every table shape the cells write.

- [ ] T012 [US3] Partitioning: partition_by → Iceberg partition spec
  (identity + temporal transforms) at table create in
  `src/dest.rs`/`src/schema.rs`; unknown column/transform typed at
  parse (config cells); live cells: partitioned writes land, the
  spec is visible in table metadata, per-partition file layout
  observable in the bucket.
- [ ] T013 [US3] pyiceberg read-back:
  `tools/interop/pyiceberg_readback.py` (pinned venv from T001;
  reads via the SAME REST catalog: counts, column names/types,
  partition spec, snapshot count) + `tests/interop.rs` invoking it
  (container-gated, skip-not-fail; also SKIPS visibly if the venv is
  absent) over three shapes: plain append ×2 commits, partitioned,
  post-additive-drift.
- [ ] T014 [US3] Spark read-back (deep tier):
  `tools/interop/spark_readback.sh` + job (Spark container with the
  Iceberg runtime jar, REST catalog config) asserting the same over
  the same shapes; wired into `make test TARGET=deep` ONLY; a
  RECORDED first-run result in the task notes (image/tag verified at
  run time, the 015 posture).

**Checkpoint**: rdlt's Iceberg output is provably nobody's private
format.

## Phase 6: Polish & close-out

- [ ] T015 Facade + CLI + bench: `crates/rdlt/` feature `iceberg` +
  `rdlt::connector::iceberg` re-export; CLI `DestSpec::Iceberg` block
  (spec-parse cell in `crates/rdlt-cli/src/main.rs`); scoreboard cell
  `iceberg-polaris-200k` in `benches/cells/e2e.toml` + polaris
  fixture in `benches/fixtures/fixtures.toml` (Container kind from
  015; rustfs fixture reused) + pipeline yaml, recorded
  baseline-first (no dlt pair — recorded).
- [ ] T016 [P] Traceability matrix
  `specs/016-iceberg-dest/matrix.md` (every config row → cells, zero
  uncited; gap cells land WITH this task) + parity record
  `specs/016-iceberg-dest/dlt-parity.md` (vs dlt's Iceberg support;
  deferrals named: Glue/SigV4 phase-2, merge-on-read, maintenance,
  the T001-verdict narrowings if any).
- [ ] T017 Close-out: coverage ≥80% baseline-first with classified
  exclusions in `benches/RESULTS.md`; comprehensive
  `crates/rdlt-connector-iceberg/README.md` (013/014/015 standard —
  full options reference incl. provider notes and maintenance
  guidance); config-schema round-trips re-checked; `make check` +
  doc-tests + semver (additive crate — verify no new break beyond
  the standing 0.2→0.3); quickstart.md walked verbatim; existing
  gated bars untouched (re-verify the two file-family bars only if
  any shared code moved — expected: none).

## Dependencies

- T001 → T002 (the probe verdicts shape config/commit code paths)
- T002 → T003 [P] and T004 (config feeds both; T003 parallel with
  T004)
- US1: T005 after T003+T004; T006 after T005; T007 after T006; T008
  after T006 (verdict-dependent); T009 after T008
- US2: T010 after T005 (needs the append path); T011 [P] after T002
  (config-level) with its live arm after T004
- US3: T012 after T005; T013 after T012; T014 after T013
- T015 after T005 (facade/CLI can land once the dest exists; bench
  needs the append path); T016 [P] after all cells exist; T017 last
- Parallel: T003 beside T004; T011 beside T010; T016 beside T017 prep

## Implementation strategy

MVP = Phases 1–3 (a correct exactly-once append destination against a
real catalog IS the product; Replace may be verdict-narrowed). The
non-negotiables: T001 verdicts are RECORDED before dependent code
(overwrite, vending, UC leg, signing hook — four written decisions);
container cells skip-not-fail; iceberg-rust types never cross the
public surface (ID1 — reviewable by grepping the public API);
interop cells are the acceptance bar for anything touching
schema/partitioning. Bench and Spark run under the quiet-machine and
deep-tier disciplines respectively.
