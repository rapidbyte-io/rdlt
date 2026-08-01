# 028 — Snowflake second generation: `rdlt-connector-snowflake-v2`

A TRUE greenfield rewrite — the discipline the owner corrected into the
record three times now: generation 1 is a REFERENCE IMPLEMENTATION, read
for its contract (behavior, frozen operator needles, SQL/wire text,
classification rules, semantic dispositions) and never transcribed. Code,
comments, module boundaries, and tests are designed fresh, following the
best current crates — `rdlt-connector-postgres` and `rdlt-connector-rest`
— as the structural exemplars. An earlier attempt on this branch
transcribed gen-1 files nearly verbatim and was RESET on owner
instruction; this plan supersedes it.

Branch `028-snowflake-v2` off main @ 8cc1c22e (the 027 merge). The first
connector BORN on the sdk.

## Decision record

- D1. BORN ON THE SDK: `Snowflake` implements `DestinationConnector`
  (`type Backend = Load`); the SPI face is `destination::Shell`; the
  one-dependency rule from birth (sdk + the sqlcore exception; SPI via
  `spi::`); the sdk's `test_dependency_rule` ADOPTED list gains the
  crate the moment it exists.
- D2. TYPED CONFIG ERRORS close the recorded asymmetry: `ConfigError`
  gains `Yaml(String)`/`Json(String)` (rendered-payload variants keep
  `Clone + PartialEq + Eq`) with the two `From` impls `Document`
  requires; the `Result<_, String>` constructors die with gen 1.
  Display renders the parser text BARE — byte-identical to what the
  `String` era carried; only the type changes.
- D3. FROZEN SURFACES — contracts, spelled identically because they are
  contracts (not because files are reused):
  - YAML/JSON vocabulary: field set and serde shapes (`account`, `user`,
    additive `auth` struct with key_pair/password/oauth_token/pat,
    `database`, `schema`, `warehouse`, `role`, `table_type`,
    `session_parameters`, `query_tag`, `host`, flattened
    `DestinationOptions`; `deny_unknown_fields` everywhere).
  - Message spellings: the `snowflake: ` family verbatim — the
    Missing/Auth/Invalid renderings, the connect-failure identity frame
    ("connecting as user `U` on account `A` failed: …"), the DDL-in-unit
    refusal ("cannot run inside a commit unit — it would commit the
    transaction and publish rows the unit had not finished writing;
    schema work belongs before the unit opens"), the upload refusals
    (no-result / non-UPLOADED status with optional message / renamed
    target), the load-shortfall abandonment ("staged N rows in K part(s)
    but the service reported M loaded; the unit is abandoned rather than
    committed short"), local-fs classifications naming TMPDIR (storage
    full TRANSIENT, read-only/permission FATAL), the shared
    duplicate-merge-key diagnosis with "Snowflake error 100090" as the
    cause, the load-mismatch and unknown/non-merge-table internal
    fatals, the scalar/column-shape probe errors.
  - Classification: kinds Network/Timeout/SessionExpired transient;
    Server transient ONLY on the allowlist {000629, 000630, 000625};
    everything else fatal; `DUPLICATE_ROW_IN_DML` = "100090" recognised
    by CODE never text; the code walk descends the source chain so
    added context cannot hide it.
  - Crash points, IDs and positions: `sf.stage.write` (local part
    exists, nothing uploaded), `sf.stage.upload` (uploaded, unrecorded —
    different debris, different reclaimer), `sf.unit.publish` (written,
    nothing durable; rollback+discard first), `sf.receipt.visible`
    (durable, caller about to be told otherwise; discard only). The two
    unit-edge points use the VALUE form (cleanup before propagation);
    the stage points use the macro form — the two-spellings fact the
    testkit scanner exists for.
  - SQL text: quoted-UPPER identifiers (doubling embedded quotes); the
    type map (Bool→BOOLEAN, Int64→NUMBER(19,0), Float64→FLOAT,
    Utf8→VARCHAR, Uuid→VARCHAR(36), Json→VARIANT, Binary→BINARY,
    TimestampTz→TIMESTAMP_TZ ≠ TimestampNaive→TIMESTAMP_NTZ, Date/Time,
    Decimal(p,s)→NUMBER(p,s), lowered nesteds→VARCHAR);
    read-before-write ensure (steady state emits NOTHING; absent table
    → one CREATE; missing column → one ALTER ADD COLUMN IF NOT EXISTS;
    widen → ALTER COLUMN SET DATA TYPE; TRANSIENT prefix applies to
    every created table incl. bookkeeping); the stage leg's
    `__RDLT_ARRIVAL NUMBER AUTOINCREMENT` and no NOT NULL on stage
    columns; scd2 validity columns TIMESTAMP_TZ without NOT NULL and NO
    index steps ever; `INFORMATION_SCHEMA` describe bound (`?`) with
    database-qualified path and schema as filter; DELETE-never-TRUNCATE;
    `QUALIFY ROW_NUMBER()` dedup with declared sort before
    `__RDLT_ARRIVAL DESC`; `MERGE INTO … TGT/SRC` upsert with the
    empty-assignment MATCHED arm elided and `=` (never null-safe) key
    comparison; hard-delete `= TRUE` / `IS DISTINCT FROM TRUE`;
    `$rdlt_tx_ts` captured once per unit via
    `SET rdlt_tx_ts = CURRENT_TIMESTAMP()` (per-statement clock is a
    pinned service fact); the bookkeeping state MERGE and receipt
    INSERT through `sql_literal_body` (quote AND backslash doubling);
    `BEGIN`/`COMMIT`/`ROLLBACK` literals; the PUT statement (verb
    first — the library switches result format on the first token —
    `AUTO_COMPRESS = FALSE`, `OVERWRITE = TRUE`); the COPY statement
    (catalog-case target list, file-case `$1:"col"` projection — the
    case asymmetry is load-bearing, never MATCH_BY_COLUMN_NAME —
    explicit `FILES = (…)` from recorded parts, `FILE_FORMAT = ( TYPE =
    PARQUET )`, `FORCE = TRUE` because service-side file dedup would
    skip a rolled-back unit's re-load, `PURGE = FALSE`,
    `ON_ERROR = ABORT_STATEMENT`); `CREATE STAGE IF NOT EXISTS` (never
    OR REPLACE); `REMOVE @stage/scope/`; the reclaim listing via
    `LIST` + `RESULT_SCAN(LAST_QUERY_ID())` comparing
    `TO_TIMESTAMP_TZ("last_modified", 'DY, DD MON YYYY HH24:MI:SS
    TZD') < DATEADD(hour, -N, CURRENT_TIMESTAMP())` on the SERVICE's
    clock, removing one named object at a time.
  - Staging semantics: stage object `{STAGE_PREFIX}int_{hash8(pipeline)}`
    (distinct namespace from merge stage TABLES); per-load
    hash12 segments and per-table hash16 prefixes (free-text names are
    hashed, never sanitised); part names `{index:08}.parquet` with the
    counter never reset within a session; one local part at a time,
    removed on every path; per-row upload verification (status
    UPLOADED, target == local basename); best-effort removal/reclaim
    (a cleanup failure never fails a committed load); 24 h stale
    window; local reclaim of THIS load's directory only.
  - Protocol dispositions (the Backend split preserves gen 1's exact
    error paths): `existing_receipt` = load-mismatch guard (bare) →
    begin unit (captures the instant) → receipt probe (bare);
    `replay` = rollback + discard staged, NO single-unit re-marking
    (gen 1 computed no plan on replay — recorded divergence from
    postgres); `publish` = load staged parts via one COPY per table
    with rowcount verification (err → rollback+discard) → staged
    probe (bare) → plan (bare) → steps (err → rollback+discard) →
    `sf.unit.publish` (rollback+discard) → COMMIT (bare) → mark
    single_unit_done from staged_nonempty → `sf.receipt.visible`
    (discard only) → discard → receipt. `write` = ensured lookup
    (defensive; the sdk session fronts it) → begin unit →
    planner-driven Replace prepare (clear inside the unit, guard
    seeded per load) → empty-batch early return AFTER the prepare →
    encode part → PUT → record pending. `ensure_table` = observe
    (once per table per session) → phase-1 DDL → fold into catalog →
    phase-2 merge ensures → record; DDL strictly outside any unit.
    `connect` = bookkeeping tables + `CREATE STAGE IF NOT EXISTS` +
    local reclaim + remote reclaim, all before any unit.
  - Statement economy: begin+capture once per unit; one COPY per table
    per unit; describe once per table per session; steady-state ensure
    emits zero schema statements.
- D4. FRESH DESIGN — the improvements this rewrite makes (exemplars:
  postgres's destination layout, rest's config/test organization):
  - Module boundaries: `unit.rs` gets its own noun (the transaction
    lifecycle AND what may run inside it — Unit + is_ddl + DmlOnly +
    the refusal — one coherent concern where gen 1 split it across
    client.rs and inline session state); `catalog.rs` splits the
    catalog image + describe/observe out of gen 1's mixed ddl.rs;
    `client.rs` stays the pure library boundary; `load.rs` is the
    Backend coordinator (postgres's load.rs precedent). All comments
    and docs written fresh.
  - Test organization: the house layout — `tests/integration.rs` +
    `cases/test_<noun>.rs` — replacing gen 1's sixteen top-level
    binaries (fewer link steps, one credential-gate module at
    `cases/common.rs`); `crash_sweep.rs` KEEPS its own binary because
    the by-hand sweep selects it by name (023's recorded convention).
    Tests are RE-AUTHORED: same invariants and frozen needles, fresh
    structure and names.
- D5. PARITY DEFINITION (differs from the ported-byte-identical form —
  the owner's correction): every invariant generation 1's suites
  assert is covered by a fresh test asserting the SAME frozen needles;
  the live legs prove behavior against the qual account (conformance
  kit, merge/load/auth/economy/reclaim/semantics, the differential
  oracle vs postgres); the SQL pins compare rendered statements as
  data. A coverage map in the close-out lists gen-1 suite → fresh
  test(s).
- D6. COEXISTS UNCONSUMED (publish = false; facade/CLI keep gen 1)
  until the owner-decided swap.

## The crate

```
crates/rdlt-connector-snowflake-v2/
  Cargo.toml            — publish=false; sdk(schema)+sqlcore+fork+substrate
  src/lib.rs            — façade: pub mod destination (the three service
                          facts in the crate docs)
  src/destination/
    mod.rs              — pure TOC + re-exports + Shell alias
    config.rs           — vocabulary + typed ConfigError + Document
    connector.rs        — Snowflake + DestinationConnector + FAIL_POINTS
                          + the testhook the live cells need
    client.rs           — the ONE library boundary: Executor,
                          SessionExecutor, classify/code_in, connect
    unit.rs             — the commit unit: Unit lifecycle + the captured
                          instant + is_ddl + DmlOnly + the refusal
    catalog.rs          — the catalog image + describe/observe
    ddl.rs              — quote + the type map + ensure rendering
    dialect.rs          — the MergeDialect spellings
    encode.rs           — batch→parquet + sql_literal_body
    stage.rs            — parts, PUT, COPY, reclaim
    load.rs             — Load (Backend): the coordinator
  tests/
    integration.rs cases/{mod,common}.rs
    cases/test_<noun>.rs — config, conformance, ingestion, load, merge,
                          auth_matrix, economy, semantics, reclaim,
                          oracle, quickstart, options
    crash_sweep.rs      — own binary (by-hand sweep selects by name)
```

## STATUS — BUILT, TESTS RE-AUTHORED, ALL GREEN LIVE (2026-08-01)

SRC: every module designed fresh (unit.rs owns the transaction AND the
DML-only discipline; catalog.rs split from ddl; client.rs slimmed to a
four-method executor seam with uniform binds). 57/57 unit tests.
IMPROVEMENT FOUND BY THE FRESH SUITE: generation 1 never validated the
shared merge options at parse — a document with contradictory options
parsed clean and failed mid-load; the gate now runs
`DestinationOptions::validate` with sqlcore's frozen sentence as the
detail (the same asymmetry class 027 closed for pg-dest).

TESTS: re-authored per D4/D5 under the house layout —
tests/integration.rs + cases/{common + 14 suites} + crash_sweep.rs (own
binary, by-hand). 88/88 PASSING INCLUDING EVERY LIVE LEG against the
qual account: the conformance kit through the shell, ingestion
end-to-end with the no-local-residue check, replace, the live
steady-state ensure economy, upsert/hard-delete/scd2 single-instant
read-backs, the auth matrix (password/oauth legs written and gated,
UNPERFORMED without their entries — the standing 022/023 call), the
three service-fact pins (DDL commits, SET does not, per-statement
clock), aged reclaim BOTH ways, live classification, and the
differential oracle vs postgres across 4 strategy arms (69.7 s).

COVERAGE MAP (gen-1 suite → fresh home): conformance→test_conformance;
ingestion_session→test_ingestion; live_load+live_economy→test_load;
live_merge→test_merge; live_auth_matrix→test_auth;
live_semantics→test_semantics; scratch_reclaim→test_reclaim;
live_client→test_client; differential_oracle→test_oracle;
quickstart_doc→test_quickstart (quickstart matches the fresh README);
secret_hygiene→test_secret_hygiene; options_parity→test_options;
statement_economy→test_economy (+ src unit pins);
credential_gating→test_gating; crash_sweep→crash_sweep.rs (fresh
armed-then-recover shape iterating FAIL_POINTS + the registry check;
by-hand, not run here). Zero warnings all shapes. Review + gates
follow.


## REVIEW ROUND 1 (two lenses, 2026-08-01)

CONTRACT FIDELITY vs the reference implementation: CLEAN in all seven
areas — every frozen message/SQL/classification/crash-point surface
byte-identical, the protocol dispositions verified line-by-line against
gen 1's commit, the options-validation delta confirmed as the only
vocabulary-behavior change, and the anti-transcription check confirms
genuinely fresh authorship (v2-only module boundaries, the 4-method
executor seam, independent prose throughout — no near-verbatim file).

FRESH EYES: ONE CRITICAL FINDING — inherited from generation 1, and
exactly what the rewrite-as-review discipline exists to catch. The
engine legitimately calls ensure_table MID-UNIT when a source's schema
evolves between batches; generation 1 had no handling (a debug build
panics on its assertion; a release build lets the DDL auto-commit the
partial unit — exactly-once broken silently) and additionally captured
each table's COPY column list on the FIRST write only, so a mid-unit
added column's values loaded NULL for the whole unit with matching row
counts and no error. FIXED, both halves, the snowflake-shaped way:
when real schema work is owed while a unit is open the unit is ENDED
by rollback first (staged parts are FILES and survive; `pending` stays
valid; the only transactional work a unit holds pre-publish is a
Replace clear, so `cleared` tracking split into committed vs in-unit —
promoted at COMMIT, dropped at rollback — and the next write
re-clears); and the pending column list now follows the LATEST write's
schema (additive evolution makes it a superset; earlier parts load
NULL for the new column, which is correct). PINNED LIVE:
`a_column_added_mid_unit_keeps_its_data` drives two batches and a
mid-unit widening through the engine against the qual account and
reads the added column's value back. The reviewer also verified the
bare-error paths safe under the engine's run-level retry model (fresh
session per attempt; a poisoned Load is dropped, never reused).
