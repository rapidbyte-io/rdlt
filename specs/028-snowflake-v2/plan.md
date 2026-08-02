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
    cases/test_<noun>.rs — auth, client, conformance, economy, gating,
                          ingestion, load, merge, oracle, options,
                          quickstart, reclaim, secret_hygiene,
                          semantics (config coverage lives as unit
                          tests inside src/destination/config.rs)
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


## REVIEW ROUND 2 (verification lens on the round-1 fix, 2026-08-02)

The round-1 fix touches exactly-once machinery, so it got its own
adversarial pass: eight attack scenarios against the mid-unit-rollback
design and the `cleared` split. Five verdicts SOUND (unit contents
enumerated; publish always runs inside a unit the receipt probe opened;
promotion discipline; PUT/`pending` validity under rollback+retry;
`$1:"COL"` on an absent parquet key is NULL, not an ON_ERROR abort).
TWO DEFECTS CONFIRMED AND FIXED, one observation recorded:

1. CROSS-TABLE OWED CLEARS (data integrity, reachable): the mid-unit
   rollback dropped EVERY table's in-unit Replace-clear mark, but only
   a subsequent write of the SAME table re-cleared — and that write may
   never come. A unit holding Replace table A's executed DELETE that is
   then rolled back by table B's mid-unit ensure would publish A's
   parts with the clear silently gone (old rows coexist with new), and
   the un-promoted mark would let a LATER unit re-clear rows this load
   had committed. FIX: the marks move to `reclear_owed`, and every
   unit-opening path now goes through `open_unit`, which re-executes
   owed clears before any planner consults `cleared_union` (write, the
   receipt probe, and publish — which now asserts the open instead of
   assuming it). Replay PROMOTES in-unit marks into `cleared` instead
   of dropping them: the receipt proves a prior incarnation committed
   that very unit, durable clear included — generation 1's single
   never-rolled-back set had exactly this observable state. PINNED
   offline (`a_rolled_back_clear_is_owed_to_the_next_unit_not_the_next_write`
   drives the whole choreography over a recording executor) and live
   (`a_replace_clear_survives_another_tables_mid_unit_evolution`: a
   two-run, two-stream load where the second table's mid-unit ensure
   rolls back the first table's clear, and only run 2's row survives).

2. PHASE-2 DDL BYPASSED THE GATE (exactly-once, narrow): the rollback
   gate read only phase 1's statements, while the scd2 validity ALTERs
   of phase 2 ran through the UNGUARDED executor — a validity-only
   ensure arriving mid-unit would auto-commit the partial unit, the
   exact class round 1 closed. FIX: phase 2 is rendered up front and
   the gate is computed over ALL owed DDL (rendering against the
   pre-`record_created` image is equivalent — validity columns never
   appear in `schema.columns` — and stops a re-ensure from wiping
   recorded validity columns and re-emitting their ALTERs). PINNED
   offline: `a_validity_only_ensure_still_ends_the_open_unit_first`.

OBSERVATION, RECORDED NOT FIXED (inherited, both generations): the
Replace guard's committed half lives only in session memory — sqlcore's
planner contract expects `cleared_targets` seeded durably (postgres
writes `names::CLEARED_TABLE`; snowflake never has). A crash between
two units of one load that both write the same Replace table lets the
recovery session re-emit the clear and delete rows the first unit
committed. Fixing it means adding the third bookkeeping table to the
connect surface and the publish transaction — a deliberate scope the
owner schedules, recorded here so it cannot pass as reviewed-and-fine.

Suite after round 2: 92/92 (three pins added), clippy clean both
feature shapes.


## REVIEW ROUND 3 (verification lens on the round-2 machinery, 2026-08-02) — CLEAN

Eight attack angles against the owed-reclear design, all SOUND: mark
round-trips across repeated mid-unit ensures lose and duplicate
nothing (and `reclear_owed` can never intersect `cleared`); every
error path's abandonment of in-memory marks is safe under the engine's
fresh-session-per-attempt retry (verified against the loader's drain);
the phase-2 hoist renders strictly less DDL, never more, and moves
option validation ahead of any executed statement; DDL failing after
the rollback leaves nothing dangling; a bare publish-side failure
leaks only age-reclaimed remote parts (deterministic names +
OVERWRITE=TRUE + FILES=() make the retry exact); the offline pins bind
the recorded statement ORDER, not fields. TERMINUS: no defect found.

Two hardening notes taken/recorded: the live cross-table pin's stream
interleaving is now PINNED with a testkit `batch_delay` on the second
stream (the race could otherwise let it pass vacuously — the offline
pin was already deterministic); and `record_created` still REPLACES a
table's catalog image with `schema.columns`, wiping recorded validity
columns, so an scd2 re-ensure in one session re-renders no-op ALTERs
and can trigger a spurious-but-safe rollback/reclear cycle — round
trips, not correctness; recorded for the owner, deliberately not
changed inside the review loop.


## GATE OF RECORD (2026-08-02, tree @ 2e58b0db)

`make check` TWICE CLEAN, untouched between runs, `env -u
RUSTUP_TOOLCHAIN`, reclaim + TIME_WAIT drain before each. COUNT
PREDICTED AND VERIFIED: 1106 (the 1014 pre-028 workspace + this
crate's 92: 59 in the unit binary + 33 in the integration binary — 89
at round 1, +3 review pins, two of which landed in the unit binary). Run 1: 1106/1106, 2 skips (both #[ignore]d instruments),
six in-gate sweep suites green (postgres source sweep 64.7 s the one
SLOW), semver no update required, 6 benches 0 regressed, cold start
22.9 ms (bar <= 40). Run 2: 1106/1106, same 2 skips, semver clean, 0
regressed, cold start 23.4 ms.

ENVIRONMENT EVENTS, recorded not re-rolled: a FIRST gate attempt
failed on the KNOWN rootlessport bind flake (`rdlt-connector-file::
s3_live s3_replace_never_deletes_user_files`, port 46011 — the
intra-run concurrency mechanism; crate untouched by 028); the
isolation rerun then exposed that the dev toolbox (Fedora 44 / gcc 16)
no longer carried the `libstdc++.so` link symlink — Fedora moved it
from `libstdc++-devel` into `gcc-c++`, which was absent, so ANY relink
of a duckdb-linked test binary failed with `unable to find -lstdc++`
(the first attempt's 412 passes ran from cached binaries). `gcc-c++`
installed; the flaked cell then passed 1/1 in isolation and both
recorded gates ran start-to-finish clean.

STATUS: the fresh rewrite is COMPLETE on the branch — built greenfield
on the sdk (D1-D6), three review rounds (round 1: the mid-unit
schema-evolution defect, both halves; round 2: the cross-table owed
clears and the phase-2 gate bypass; round 3: CLEAN, terminus), every
fix pinned offline or live, 92/92 crate suite, gates twice clean at
1106. The crate coexists UNCONSUMED as `rdlt-connector-snowflake-v2`;
the swap (delete generation 1, rename, take the `snowflake` name) is
the owner's decision, as 025/026 precedent. The crash sweep remains
by-hand and was NOT run in these gates (its own binary, failpoints-
gated, spends real account time). One recorded owner item stands open:
the non-durable Replace-clear guard (review round 2's observation).


## REVIEW ROUND 4 (full-crate, four parallel lenses, 2026-08-02)

Fresh-eyes bug scan, house-style compliance, test adequacy, and
docs/plan accuracy over the whole crate. NOT clean — findings and
dispositions:

FIXED, with pins (suite 92 → 108: 71 unit binary + 37 integration):

1. THE DURABLE CLEAR GUARD (bug lens, escalating round 2's
   observation; two lenses independently): Replace's once-per-load
   clear guard lived only in session memory, against sqlcore's OWN
   contract for a DirectToTarget destination (the executor seeds
   `cleared_targets` from a durable record; `names::CLEARED_TABLE`
   exists for exactly this; postgres implements it). A crash between
   two units of one load re-cleared — DELETED — rows the first unit
   committed, silently. NOW IMPLEMENTED the snowflake-shaped way: the
   third bookkeeping table `_rdlt_cleared` created at connect; every
   ClearTarget executes through `clear_target`, which writes the
   durable record in the SAME unit transaction (rolled back together
   or durable together — this covers the owed re-clears too, whose
   first record rolled back with the unit); `cleared` is SEEDED once
   at connect from the record (one round trip, not per-write probes —
   the SaaS economy). Pinned offline: record-beside-DELETE order, the
   seed-then-never-reclear path, the cross-unit promoted-never-rerun
   path, and the COPY-shortfall abandon. Generation 1 never wrote the
   record — inherited, now CLOSED rather than re-recorded.

2. THE FULL-FEED PROBE WAS UNREADABLE (bug lens, MEASURED LIVE before
   fixing): sqlcore's stage probe `SELECT EXISTS (SELECT 1 FROM …)`
   COMPILES on the service and answers BOOLEAN — which `cell_as_u64`'s
   numeric-only arms refused ("expected one integer … got nothing
   usable", reproduced against the qual account). Every publish of a
   full-feed merge config (`merge_scope`, scd2 `absent: retire`)
   failed — in BOTH generations, which ran the identical probe through
   identical arms with zero coverage of the vocabulary. FIX: a Boolean
   arm (plus the "true"/"false" string forms). Pinned offline
   (`a_count_is_read_from_every_representation_the_service_uses`) and
   live end-to-end
   (`an_absent_key_retires_through_the_full_feed_probe`).

3. THE STAGE LEG'S CATALOG IMAGE (bug lens): a merge stage table
   created this session was never `record_created`, so a later
   same-session evolution SKIPPED the stage's ADD COLUMN (the
   re-rendered CREATE IF NOT EXISTS is a service no-op) and the COPY
   named a column the stage never gained. Inherited from generation 1.
   FIX: the stage leg folds into the image (columns + arrival), and
   `record_created` now EXTENDS rather than replaces — closing round
   3's recorded observation too (replacement wiped recorded validity
   columns, re-rendering their ALTERs and spuriously ending a unit).
   Pinned live: `a_column_added_mid_unit_reaches_the_merge_stage`
   (delete_insert merge, mid-unit widening, value read back through
   the stage).

4. TEST ADEQUACY (that lens's nine findings): the one genuinely
   vacuous test (the PUT pin asserted on a string the test itself
   wrote) now pins the statement `upload` actually renders (`put_sql`
   extracted); per-row upload verification pinned against scripted
   mixed/empty/renamed reports — the 023-measured service fact finally
   has a test that can fail; the transient decision table extracted
   (`transient_rule`) and pinned exhaustively with the allowlist
   contents; `reclaim_local` pinned (own directory only, the other
   load's survives); the shipped 24 h window pinned; scd2 now also
   asserts exactly ONE current version; dedup_sort finally READ BACK
   (the declared survivor, not a count — 023 D-32's failure class);
   the crash sweep asserts the settled load's local directory empty.

5. GATE WIRING (compliance + adequacy — the 024 "compiled by no gate
   command" class): the Makefile's failpoints type-check named
   generation 1 only, so v2's sweep was compiled by NO gate command;
   the coverage exclusion likewise missed v2 (its live sweep would
   have RUN inside a routine coverage pass on this machine). Both
   lines now name the v2 crate and die naturally at swap-in. The
   registry-vs-sources check also runs UNGATED now
   (tests/cases/test_gating.rs) — drift fails a routine gate instead
   of waiting for the next by-hand sweep.

6. NAMING (compliance): `SnowflakeConfig` → `destination::Config` and
   `SnowflakeDialect` → `Dialect`, per the second-generation naming
   rules both reference crates follow (types don't repeat the crate
   noun; 025 made the same rename for postgres). Rust API names are
   not a frozen surface.

7. COMMENT HONESTY (compliance + bug lens): the key-pair passphrase
   doc claimed an up-front check that never existed in either
   generation (the mismatch surfaces at connect through the library —
   the comment now says so); `explain_merge_failure`'s doc claimed the
   service error "is kept as the cause" while the code (frozen,
   generation-1 parity) replaces it with the diagnosis naming the
   code — the comment now matches the code. The plan's design-era test
   list and the gate block's binary split corrected above.

RECORDED, NOT CHANGED:

- Widen (bug lens): `ALTER COLUMN … SET DATA TYPE` renders for
  conversions the service refuses (only VARCHAR length and NUMBER
  precision widen in place); a genuine cross-type widen fails LOUDLY
  at execution, generation-1 parity, and a typed refusal would leave
  the pipeline exactly as stuck. Documented at the render site.
- README title says `rdlt-connector-snowflake`: deliberate pre-swap
  naming — 025/026 renamed the crate at swap-in with zero doc edits;
  the title is written for the name the crate will carry.
