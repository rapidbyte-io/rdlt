# rdlt-connector-postgres v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking. Update checkboxes as tasks complete — this file is the durable
> record across context windows.

**Goal:** Rewrite `crates/rdlt-connector-postgres` in place, module by module,
into the six-seam v2 design — fresh code and naming throughout, zero duplicated
logic, full functional parity proven by the existing test suite and gate.

**Architecture:** In-place progressive rewrite ("ship of Theseus"): each task
replaces one zone with freshly written code behind the same frozen public
surface, deletes the old files in the same commit, and ends with the crate's
tests green. The existing ~9.6k-line test suite is the parity oracle; the
golden-SQL pins, conformance suites, crash sweeps, and iai benches are the
regression net. The six seams: (1) `session/` owns connect+TLS-handshake+GUC
profiles+classification; (2) `types/` is the single Postgres type rulebook
(map/binary/text/literal/encode); (3) dest decomposes by owned state
(catalog/unit/executor); (4) source validates each fact once (`plan.rs`,
`cursor/`); (5) CDC gets a typestate runtime and an `ack` module; (6) one
`testsupport/` seam replaces four ad-hoc test-access mechanisms.

**Tech Stack:** tokio-postgres + rustls (unchanged deps; the manifest's
dependency set must not grow), arrow 58.3, rdlt-connector SPI,
rdlt-connector-sqlcore planner.

## Decision record

- **D1 — crate name and path unchanged.** "v2" names the design generation,
  not the package. Keeping `rdlt-connector-postgres` keeps the facade path
  `rdlt::connector::postgres`, the gate's `-p` selections, the four named test
  binaries, and external users (duckdb/snowflake tests, fuzz targets) intact.
- **D2 — branch `postgres-v2` off main @ d92cec06.** Merge is the owner's
  call; done = gate twice clean on the branch.
- **D3 — no copying.** Each task: read the old module, extract its behavioral
  contract (Appendix A), write fresh code + naming against that contract,
  delete the old file in the same commit. Public frozen names (Appendix B) are
  the only carried-over spellings.
- **D4 — `tls/` stays a public top-level module** holding the portable policy
  vocabulary (`TlsPolicy` — constructed by `rdlt::pipeline_spec`); the
  Postgres-specific connect machinery moves to a new private `session/`.
- **D5 — dest gains config entry points** (`from_yaml/from_json/from_value` +
  `config_schema()`), freezing the facade's existing YAML field set
  `{conn, dataset, tls, merge_strategy, tables}`; the facade delegates.
- **D6 — hot paths keep their measured algorithms** (single-pass COPY decode,
  borrowed-view encode, no per-row allocation). Fresh code, same shape; the
  iai gate (6 benches, 0 regressed) adjudicates.

## Global Constraints

- Workspace denies `unsafe_code`; workspace lints apply; `cargo fmt` every touched crate.
- Tests via `cargo nextest run` (doc-tests via `cargo test --doc`); gates with `env -u RUSTUP_TOOLCHAIN`, launched from repo root, Makefile untouched during a run.
- House layout: pure-TOC `mod.rs`, code under its noun, tests = `integration.rs` + `cases/test_<noun>.rs`; no `name.rs` beside `name/`.
- Comments self-contained (no citation IDs); typed error taxonomy; no substring-matching rendered errors in production code.
- Greenfield: no compat shims or aliases; renames land directly with all consumers updated in the same commit. Persisted DATA formats stay frozen.
- The dependency list in Cargo.toml may not grow.
- Every task ends: crate compiles with `--all-targets --features failpoints`, affected suites green, one commit ending with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Container suites need `systemctl --user start podman.socket`; first full-crate run after a rebuild may hit the recorded container-burst flake — rerun once before diagnosing.

---

### Task 0: Branch + baseline

**Files:** none (git only)

- [x] **Step 1:** `git checkout -b postgres-v2` (from main @ d92cec06)
- [x] **Step 2:** Baseline: `env -u RUSTUP_TOOLCHAIN cargo nextest run -p rdlt-connector-postgres --features failpoints` — record the pass count; this is the oracle's starting shape.
- [x] **Step 3:** Commit this plan file: `git add specs/025-postgres-v2/ && git commit -m "plan(postgres-v2): the six-seam rewrite plan"`

### Task 1: `session/` — the connection module

**Files:**
- Create: `src/session/mod.rs` (TOC), `src/session/conn_string.rs`, `src/session/establish.rs`, `src/session/profile.rs`, `src/session/classify.rs`
- Modify: `src/source/connector.rs` (drop `connect()`), `src/source/cdc/runtime.rs`, `src/source/testhook.rs`, `src/dest/config.rs` (drop `client()`), `src/dest/connector.rs`, `src/tls/mod.rs`
- Delete: `src/tls/connstring.rs`, `src/tls/connect.rs`, `src/driver_error.rs`
- Test: existing `tests/cases/test_connstring.rs`, `tests/cases/test_tls_matrix.rs` (repoint imports), inline unit tests in the new files

**Interfaces (produces):**
```rust
pub(crate) struct Conn { /* tokio_postgres::Client + spawned io-driver handle */ }
impl Conn {
    pub(crate) fn client(&self) -> &tokio_postgres::Client;
}
pub(crate) enum Profile { Plain, CdcControl }   // CdcControl pins datestyle=ISO, bytea_output=hex
pub(crate) struct ParsedConn { pub config: tokio_postgres::Config, pub tls: crate::tls::TlsPolicy }
pub(crate) fn parse_conn(s: &str) -> Result<ParsedConn, ConnStringError>;   // libpq forms, percent-escapes, sslmode↔tls translation, unsupported params rejected BY NAME, application_name=rdlt default
pub(crate) async fn establish(parsed: &ParsedConn, profile: Profile) -> Result<Conn, EstablishError>;
pub(crate) enum EstablishError { Config(String), Transient(String), Fatal(String) }
// classify.rs: the two opposite-polarity SQLSTATE rulebooks, side by side:
pub(crate) fn connect_sqlstate_is_transient(code: &str) -> bool;
pub(crate) fn statement_sqlstate_is_permanent(code: &str) -> bool;   // classes 22/23/42
pub(crate) fn detail_of(e: &tokio_postgres::Error) -> String;        // server message + SQLSTATE, always
```

**Behavioral contract:** Appendix A §Session. Headline invariants: conn-string
parse failure is always Config/Fatal; network-shaped connect failure is
Transient; TLS alert + SQLSTATE 28000 → ClientCert class; the
`EstablishError → SourceError/DestinationError` mapping lives in ONE place
(two tiny adapters, one per half — deleting the twice-copied 4-arm match).

- [ ] **Step 1:** Read old `tls/connstring.rs`, `tls/connect.rs`, `driver_error.rs`; write the contract notes into Appendix A §Session (below) if anything there is missing.
- [ ] **Step 2:** Write `session/` fresh per the interfaces above, unit tests inline (conn-string matrix: libpq spellings, sslmode trio, percent-escape strictness, rejected params by name).
- [ ] **Step 3:** Migrate all six old connection call sites (source read/reflect, cdc control/snapshot/preflight, dest open) to `session::establish`; delete the three old files; repoint `test_connstring.rs`/`test_tls_matrix.rs` imports.
- [ ] **Step 4:** `cargo nextest run -p rdlt-connector-postgres --features failpoints` green (container suites included).
- [ ] **Step 5:** Commit: `refactor(postgres): session/ owns connect — one parse, one handshake, one rulebook`

### Task 2: `tls/` — portable policy, fresh internals

**Files:**
- Create: `src/tls/rustls.rs`, `src/tls/verify.rs` (fresh rewrites), keep `src/tls/policy.rs` rewritten in place
- Delete: `src/tls/rustls_config.rs`
- Test: `tests/cases/test_tls_matrix.rs` (unchanged assertions)

**Interfaces:** `tls::TlsPolicy` serde shape FROZEN (mode strings incl. the
verify-* spellings, `client_cert`/`client_key`, PemSource fields). Internals:
`pub(crate) fn client_config(policy: &TlsPolicy) -> Result<rustls::ClientConfig, TlsError>` —
require = accept-any verifier, verify_ca = webpki minus hostname, verify_full =
stock webpki.

- [ ] **Step 1:** Rewrite the three files fresh (policy vocabulary serde-identical; verifiers with their invariants stated in self-contained comments).
- [ ] **Step 2:** Full TLS matrix suite green (needs containers): `cargo nextest run -p rdlt-connector-postgres -E 'test(tls)'`
- [ ] **Step 3:** Commit: `refactor(postgres): tls/ is the portable policy home — nothing postgres in it`

### Task 3: `types/` — one type rulebook (source half)

**Files:**
- Create: `src/types/mod.rs`, `src/types/map.rs`, `src/types/binary.rs`, `src/types/text.rs`, `src/types/literal.rs`
- Modify: `src/source/{connector,reflect,cursor,testhook}.rs`, `src/source/cdc/{apply,read,values}.rs`, `src/source/config/vocabulary.rs` (HintType home)
- Delete: `src/source/type_map.rs`, `src/source/copy_decode.rs`, parser halves of `src/source/cdc/values.rs` and `src/source/cursor.rs`
- Test: existing `test_native_types.rs`, `test_incremental.rs`, cdc suites, fuzz entry via testhook (Task 9 repoints the fuzz crate)

**Interfaces (produces):**
```rust
pub(crate) enum Kind { Bool, Int2, Int4, Int8, Float4, Float8, Numeric { precision: u16, scale: i16 }, Text, Bytea, TimestampUs, TimestamptzUs, DateDays, TimeUs, Uuid, Json, /* full set derived from old type_map.rs during Step 1 */ }
impl Kind { pub(crate) fn arrow(&self) -> arrow_schema::DataType; }
pub(crate) struct ColumnPlan { pub name: String, pub kind: Kind }   // replaces FieldPlan, lives HERE not in the decoder
// map.rs:  catalog (typname/oid, typmod) + HintType → Kind; lossy-mapping warn on `rdlt::lossy`, once per column per read
// binary.rs: pub(crate) struct Decoder { … }  — COPY BINARY → RecordBatch, single-pass, bounded buffers (memory_bound suite is the proof)
// text.rs: pub(crate) fn parse(kind: &Kind, s: &str) -> Result<ScalarValue, ParseError>   — the ONE text-form parser (CDC tuples + cursor watermarks)
// literal.rs: pub(crate) fn render(kind: &Kind, v: &ScalarValue) -> String                — the ONE SQL-literal renderer (watermark resume predicates)
```
`HintType` public spelling stays `source::HintType`; its 5 string-newtype trait
impls collapse into one `string_newtype!`-style macro shared with `Lag`/`Wait`
(the ~200-line triplication dies here or in Task 6, whichever touches it first).

**Behavioral contract:** Appendix A §Types. A new `Kind` variant must be a
compiler-forced edit in every face (exhaustive matches, no `_` arms over Kind).

- [ ] **Step 1:** Read old `type_map.rs` + the three conversion tables; enumerate the full Kind set and each face's per-kind behavior into Appendix A §Types (uuid server forms! numeric text domain! timestamp µs precision!).
- [ ] **Step 2:** Write `types/` fresh; port the inline unit tests' *cases* (values and expectations are contract, wording fresh).
- [ ] **Step 3:** Migrate consumers (source read path, cdc apply/values, cursor parse/render); delete old files.
- [ ] **Step 4:** Suites: native_types, incremental, cdc_cycle, memory_bound (`RDLT_HEAVY=1 … -E 'binary(memory_bound)'`), differential, property (`-E 'binary(shred_property)'` is engine-side; postgres property lives in the copy decode fuzz — run the proptest suites in-crate).
- [ ] **Step 5:** Commit: `refactor(postgres): types/ is the one rulebook — a new kind is a compiler-forced edit`

### Task 4: `types/encode.rs` — the dest wire face

**Files:**
- Create: `src/types/encode.rs` (cfg dest)
- Modify: `src/dest/write.rs` (consumer), `src/dest/testhook.rs`
- Delete: `src/dest/encode.rs`
- Test: `test_dest_conformance.rs`, `test_native_types.rs` round-trip legs, encode pins in dest testhook

**Interfaces:** `pub(crate) struct Encoder` — Arrow → COPY BINARY over a
borrowed column view, `ToSql`-based, 64 KiB flush discipline preserved.
Round-trip property: `binary::Decoder` is the oracle for `Encoder` (the 008
review's hand-rolled-encoder-vs-decoder oracle stays a test).

- [ ] **Step 1:** Read old `dest/encode.rs`; contract to Appendix A §Types (numeric string-domain grouping — the u128 overflow lesson; NULL bitmap; per-kind wire forms).
- [ ] **Step 2:** Write fresh; migrate `write.rs`; delete old.
- [ ] **Step 3:** Dest suites green incl. conformance + iai bench compiles: `cargo bench -p rdlt-connector-postgres --bench iai_pg -- --list`
- [ ] **Step 4:** Commit: `refactor(postgres): encode joins the rulebook — decoder is the encoder's oracle`

### Task 5: dest decomposition — catalog / unit / executor

**Files:**
- Create: `src/dest/catalog.rs`, `src/dest/unit.rs`, `src/dest/executor.rs`, `src/dest/load.rs` (the `LoadSession` impl, fresh name `PgLoad`), `src/dest/errors.rs` (phase-tagged)
- Modify: `src/dest/connector.rs` (open() thins; state-tables DDL moves to catalog), `src/dest/dialect.rs` (rewrite in place, 3 hooks + why-defaults comment), `src/dest/mod.rs`
- Delete: `src/dest/commit.rs`, `src/dest/write.rs`, `src/dest/ddl.rs`, `src/dest/sqlgen.rs`, `src/dest/classify.rs`
- Test: golden pins (`test_golden_sql.rs`, `test_golden_ensure_sql.rs` — SQL text BYTE-IDENTICAL), dest_conformance, merge suites, dest_crash_sweep, direct_publish, unit_isolation

**Interfaces (produces):**
```rust
struct PgLoad { conn: session::Conn, catalog: Catalog, unit: Unit, load_id: LoadId, pipeline: PipelineId }
pub(super) struct Catalog { tables: BTreeMap<TableName, (TableSchema, WriteMode)> }
impl Catalog {   // consumes sqlcore ensure planner; renders DDL text (types lowering, UNLOGGED stages, identity CACHE 32, USING casts)
    pub(super) async fn ensure(&mut self, conn: &Conn, schema: &TableSchema, mode: &WriteMode) -> Result<(), DestError>;
    pub(super) fn planner_input(&self) -> …;   // what plan_commit needs
}
pub(super) struct Unit { state: UnitState, cleared: BTreeSet<TableName> }  // UnitState = Closed | Open — no bool
impl Unit {   // literal BEGIN ISOLATION LEVEL READ COMMITTED / SET LOCAL work_mem='64MB' / COMMIT / ROLLBACK
    pub(super) async fn begin_if_closed(&mut self, conn: &Conn) -> Result<(), DestError>;
    pub(super) async fn commit(&mut self, conn: &Conn) -> Result<(), DestError>;
    pub(super) async fn rollback(&mut self, conn: &Conn) -> Result<(), DestError>;   // EVERY error path — 25P02 poisoning rule
}
// executor.rs: methods take (&Conn, &Catalog, &mut Unit) — disjoint structs, no free fn, no pub(super) field pokes
pub(super) async fn run_step(conn: &Conn, catalog: &Catalog, unit: &mut Unit, step: &Step) -> Result<(), DestError>;
// errors.rs: phase-tagged like the source half:
pub(super) enum DestPhase { Connect, Ensure, Write, Commit }
```

**Behavioral contract:** Appendix A §Dest. Non-negotiables: rollback-on-every-
error wrapper pattern; `ClearTarget`/`InsertSelect` unreachable → fatal
"internal:"; replay = sqlcore `replay_disposition` (DiscardUnit) + still apply
`script.marks` + return prior receipt; Replace clear at write time via
`prepare_target`, at most once per (load, target), durable record in same txn;
23505 → `duplicate_merge_key_diagnosis`; reclamation TRUNCATE scoped to the
pipeline's hash prefix; capabilities values verbatim.

- [ ] **Step 1:** Read old commit/write/ddl/connector; write §Dest contract incl. every literal SQL string (they are golden-pinned).
- [ ] **Step 2:** Write catalog.rs + unit.rs fresh with inline unit tests.
- [ ] **Step 3:** Write executor.rs + load.rs (the LoadSession impl) + errors.rs; open() thins; goldens pin through `testsupport::dest` (temporary shim until Task 9 lands the real one).
- [ ] **Step 4:** Delete the five old files; suites: golden × 2 (byte-identical or STOP and fix), dest_conformance, merge_strategies, scd2, merge_refinements, unit_isolation, direct_publish.
- [ ] **Step 5:** Crash sweep: `cargo nextest run -p rdlt-connector-postgres --features failpoints -E 'binary(dest_crash_sweep)'`
- [ ] **Step 6:** Commit: `refactor(postgres): dest splits by owned state — catalog, unit, executor`

### Task 6: dest config entry points + facade delegation

**Files:**
- Create: `src/dest/config.rs` rewritten: builder (frozen) + `PostgresDestConfig { conn, dataset, tls, merge_strategy, tables }` with `from_yaml/from_json/from_value`, `config_schema()`
- Modify: `src/dest/connector.rs` (`spec()` now carries the schema), `crates/rdlt/src/pipeline_spec.rs` (delegate the postgres dest arm), string-newtype macro adoption for `Lag`/`Wait`/`HintType` if not already done in Task 3
- Test: new `tests/cases/test_dest_config.rs` (entry-point trio + schema round-trip via `jsonschema`, mirroring `test_config_schema.rs`), facade's own pipeline_spec tests

**Behavioral contract:** the facade's YAML field set is FROZEN (existing bench-
generated specs must parse unchanged); explicit `merge_strategy` under
append/replace stays a typed error at open, absent stays permissive
(the load-bearing `Option`); `from_yaml → ConfigError` (family rule).

- [ ] **Step 1:** Write config type + entry points + schema fresh; wire `spec()`.
- [ ] **Step 2:** Delegate facade arm; run facade tests: `cargo nextest run -p rdlt`.
- [ ] **Step 3:** New test file; crate suites green.
- [ ] **Step 4:** Commit: `feat(postgres): the destination is describable — config entry points reach parity with the source`

### Task 7: source — one validation gate, cursor by noun

**Files:**
- Create: `src/source/plan.rs`, `src/source/cursor/mod.rs`, `src/source/cursor/watermark.rs`, `src/source/cursor/tracker.rs`, `src/source/cursor/prepare.rs`, `src/source/pump.rs`
- Modify: `src/source/connector.rs` (thins to SPI dispatch), `src/source/sqlgen.rs` (rewrite in place; `wrap_query` used by BOTH describe and read paths), `src/source/reflect.rs` (rewrite in place), `src/source/errors.rs` (rewrite in place), `src/source/config/*` (rewrite in place)
- Delete: `src/source/cursor.rs`, `src/source/copy_pump.rs`
- Test: `test_incremental.rs`, `test_source_conformance.rs`, `test_query_streams.rs`, `test_config.rs`, `test_option_edges.rs`, crash_sweep

**Interfaces (produces):**
```rust
pub(crate) enum StreamPlan { Table(TablePlan), Query(QueryPlan), Cdc(CdcPlan) }
pub(crate) async fn plan_streams(cfg: &PostgresConfig, reflection: &Reflection) -> Result<Vec<StreamPlan>, PgSourceError>;
// THE gate: cursor column selected/capable, lag prerequisites (inclusive boundary + sql_delta + PK),
// CDC replica-identity keys survive column selection, query-name collisions.
// streams() maps plans → StreamSpec (always .with_structured()); read() looks up its plan — NO revalidation, ONE error text per fact.
pub(crate) struct Watermark …;   // serde JSON shape FROZEN (CursorState); literal rendering via types::literal
pub(crate) struct Tracker …;     // built from a TrackerSpec struct, not 8 positional args; single pass per batch, row_key computed once
pub(crate) fn prepare(plan: &TablePlan, since: Option<&Cursor>) -> Result<Incremental, PgSourceError>;  // cursor column resolved ONCE
// pump.rs: pub(crate) async fn pump(client, sql, on_batch) — the shared COPY read loop; crash hook behind #[cfg(feature="failpoints")], NOT a production enum variant
```

**Behavioral contract:** Appendix A §Source. Cursorless streams never
checkpoint; `ChannelClosed` = cancellation = `Ok(())`; watermark never lowered;
lag re-delivers window under Append (documented); boundary matrix
(inclusive/exclusive × closed/open) preserved exactly; dedup semantics of
tracker (values beat NULL, ties keep arrival last-wins).

- [ ] **Step 1:** Read old connector/cursor/copy_pump/sqlgen; contracts to §Source (the boundary matrix verbatim, the tracker dedup rules, the 019 perf notes on render_cell).
- [ ] **Step 2:** Write plan.rs + cursor/ + pump.rs fresh; thin connector.rs; rewrite sqlgen/reflect/errors/config in place.
- [ ] **Step 3:** Delete old files; suites green incl. `-E 'binary(crash_sweep)'` armed.
- [ ] **Step 4:** Commit: `refactor(postgres): every source fact validated once — plan.rs is the gate`

### Task 8: CDC — typestate runtime, ack has a home

**Files:**
- Create: `src/source/cdc/ack.rs`; rewrite in place: `runtime.rs`, `read.rs`, `slot.rs`, `tail.rs`, `apply.rs`, `pgoutput.rs`, `mod.rs`
- Delete: `src/source/cdc/values.rs` (remainder folded into types/text in Task 3)
- Test: `test_cdc_cycle.rs`, `test_cdc_slot.rs`, `test_cdc_identity.rs`, `test_cdc_recovery.rs`, cdc_crash_sweep

**Interfaces (produces):**
```rust
// runtime.rs — the Option<Arc<Client>>+expect() pair becomes a typestate:
pub(super) struct Control { conn: Arc<session::Conn> }      // established with Profile::CdcControl
pub(super) struct Snapshot { conn: Arc<session::Conn> }     // holds the REPEATABLE READ txn
pub(super) enum RunPhase { Idle, Controlled(Control), Snapshotting(Control, Snapshot) }
// read.rs: phase functions with lock scopes — validate_slot_gap / snapshot_pass / change_pass / complete_run — each takes the guard, none straddles an await it shouldn't
// ack.rs: pub(super) async fn advance_and_report(…) — run-completion ack + lag_bytes on `rdlt::cdc`; called by BOTH tail and non-tail paths
// slot.rs: ensure() decomposed into a pipeline of named checks, one EnsureOutcome
```

**Behavioral contract:** Appendix A §CDC. Slot-first snapshot; ONE
repeatable-read view; snapshot cursor read BEFORE the RR BEGIN (visibility
horizon — confirmed_flush would wedge recovery); ACK trails one run behind,
failed runs ack nothing; peek session GUC pins; TOAST substitute-under-FULL /
typed without; PK-change = delete+insert; chunked tail with checkpoint-probe
cancellation; distinct CdcCursor JSON shape (misrouted state fails typed);
R9's distinguished slot errors (WAL-retention overrun, concurrent consumer,
invalidation, recreated-slot gap, dropped-index empty key).

- [ ] **Step 1:** Read all eight old files; §CDC contract (this is the subtlest zone — every recorded 009-review fix is an invariant).
- [ ] **Step 2:** Rewrite fresh (pgoutput parser last — it is self-contained and fuzzed; keep its exact accepted grammar).
- [ ] **Step 3:** CDC suites + `-E 'binary(cdc_crash_sweep)'` green.
- [ ] **Step 4:** Commit: `refactor(postgres): cdc runtime is a typestate — the audited panics retire`

### Task 9: `testsupport/` — one seam, and the outside world

**Files:**
- Create: `src/testsupport/mod.rs` (doc-hidden TOC), `src/testsupport/source.rs`, `src/testsupport/dest.rs`, `src/testsupport/session.rs`, shared fixture data `src/testsupport/data.rs`
- Modify: `src/fixtures.rs` (dedup `PgFixture`/`CdcPgFixture` via one private `start_with(flags)`; keep public API), `fuzz/fuzz_targets/pg_copy_decode.rs`, `fuzz/fuzz_targets/pg_pgoutput_decode.rs`, `benches/iai_pg.rs`, `src/tls/mod.rs` (doc-hidden block retires), test files importing `testhook`/`dest::sqlgen`
- Delete: `src/source/testhook.rs`, `src/dest/testhook.rs`, the Task-5 temporary shim
- Test: whole crate + `cargo check` in `fuzz/` (out-of-workspace — the compiler won't catch it from here)

- [ ] **Step 1:** Write testsupport/ fresh; the duplicated literal FieldPlan vectors become one fixture table in data.rs.
- [ ] **Step 2:** Repoint fuzz targets, bench, and every test import; delete old seams.
- [ ] **Step 3:** `cargo nextest run -p rdlt-connector-postgres --features failpoints,fixtures` + `cd fuzz && cargo check` + bench `--list`.
- [ ] **Step 4:** Commit: `refactor(postgres): one test-access seam — testsupport/ replaces four conventions`

### Task 10: front page, naming sweep, duplicate audit

**Files:**
- Modify: `src/lib.rs` (front-page doctest stays runnable; module map updated), `README.md`, every `mod.rs` TOC comment
- Audit: whole crate

- [ ] **Step 1:** lib.rs front page rewritten for the v2 map (keep the doctest contract: yaml → validated source; contradiction refused at parse time with "contradicts" in the message).
- [ ] **Step 2:** Naming sweep against the crate rule (public = `Postgres*`, internal = `Pg*` or bare; one spelling per concept — grep for the old names: `FieldPlan|MappedType|Decode::|PgSession|testhook|driver_error|copy_decode|copy_pump|rustls_config` must return zero hits in src/).
- [ ] **Step 3:** Duplicate audit: `quote` aliases (ONE: sqlcore's, spelled at use sites), parse helpers (`grep -rn "parse_date_days\|parse_time_us"` → exactly one home), the 4-arm connect match (one home), string-newtype impls (one macro).
- [ ] **Step 4:** `cargo clippy -p rdlt-connector-postgres --all-targets --all-features -- -D warnings`; `cargo fmt`; `RUSTDOCFLAGS="-D warnings" cargo doc -p rdlt-connector-postgres --no-deps --all-features`.
- [ ] **Step 5:** Commit: `docs(postgres): the front page describes v2 — and nothing is spelled twice`

### Task 11: the gate, twice

- [ ] **Step 1:** Doc-tests: `env -u RUSTUP_TOOLCHAIN cargo test --doc -p rdlt-connector-postgres`
- [ ] **Step 2:** Full workspace gate from repo root, no edits during the run: `env -u RUSTUP_TOOLCHAIN make check` — wait on the log's completion marker, not a pgrep PID.
- [ ] **Step 3:** Inspect: 964/964 (2 instrument skips), all sweeps, semver clean, 6 benches 0 regressed (the iai compare adjudicates D6 — if a bench regressed, fix the hot path, do NOT re-record baselines), cold start ≤ 40 ms.
- [ ] **Step 4:** Second untouched gate run; both clean → done.
- [ ] **Step 5:** Update memory (plan-series status); report. Merge stays the owner's call (D2).

## Appendix A — Behavioral contracts (filled during each task's Step 1)

Seeded from the three survey reports; each task's Step 1 verifies against the
old source and completes its section BEFORE fresh code is written.

**§Session** — six call sites today; CDC control pins `datestyle=ISO`,
`bytea_output=hex` (pgoutput TEXT forms depend on them); dest sets
`search_path` AFTER schema creation (stays in dest, not a profile); parse:
libpq trio + verify-* sslmode spellings translate, unsupported params rejected
by name, strict percent-escape errors, `application_name=rdlt` default,
sslmode-contradicts-tls-block refused at source-config parse time.

**§Types** — full Kind set from old `type_map.rs`; uuid accepts server forms
(urn:, braces, bare hex); numeric stays in the string domain end-to-end (u128
wraps silently in release at 38 digits — the 008 lesson); timestamps µs;
`rdlt::lossy` warn once per column per read; hint vocabulary is the closed
"decimal(p,s)" string table; binary decoder bounded-memory (memory_bound
proves); encoder 64 KiB flush; decoder-as-encoder-oracle round-trip test.

**§Dest** — capabilities: merge/json/decimal true, structs/lists false,
ident max_len 63; open(): create schema → search_path → state/commits/cleared
tables → TRUNCATE stages scoped to pipeline hash prefix; unit tx literals
(`BEGIN ISOLATION LEVEL READ COMMITTED`, `SET LOCAL work_mem = '64MB'`);
rollback on EVERY mutating error (25P02); `DirectToTarget` publish;
ClearTarget/InsertSelect unreachable→fatal "internal:"; replay: rollback +
apply marks + prior receipt; ensure-before-validate phase order FROZEN;
23505→duplicate_merge_key_diagnosis; golden SQL byte-identical (21 pins).

**§Source** — streams() always structured; validation facts (cursor
selected/capable; lag needs inclusive+delta+PK; CDC keys survive selection;
query name collisions); cursorless never checkpoints; ChannelClosed=Ok;
boundary matrix inclusive/exclusive; watermark never lowered; tracker dedup
(values>NULL, ties→arrival last-wins); query streams wrapped ONE way
(`SELECT * FROM (sql) AS q`) for describe AND read.

**§CDC** — slot-first snapshot; one RR view; snapshot cursor = visibility
horizon read before RR BEGIN; ack trails one run, failed runs ack nothing;
floors = dest-committed since + fresh snapshot points; TOAST
substitute-under-FULL/typed-without; PK-change delete+insert; chunked tail +
checkpoint-probe cancellation; retention warn @256MiB; primary_key override
wins under REPLICA IDENTITY FULL; distinguished slot errors (retention
overrun, concurrent pid, invalidation, recreated gap, dropped-'i'-index);
per-chunk targets; distinct CdcCursor JSON.

## Appendix B — Frozen surfaces (never change; verified by existing tests)

1. Public paths: `source::{PostgresSource, PostgresConfig, ConfigError, config_schema, HintType}`, `dest::{Postgres, DestinationOptions, TableOptions, MergeStrategy, Scd2Options, DedupSort, SortOrder, AbsentPolicy}`, `tls::TlsPolicy` (+serde), `fixtures::{PgFixture, CdcPgFixture}`; doc-hidden: `FAIL_POINTS`/`CDC_FAIL_POINTS` registries (names + every crash-point ID string).
2. Config vocabularies: source YAML document; sqlcore `DestinationOptions` serde; facade dest fields `{conn, dataset, tls, merge_strategy, tables}`.
3. Persisted: cursor-state JSON, CdcCursor JSON, `_rdlt_*` names (sqlcore's), receipt semantics, `__rdlt_arrival` identity column DDL.
4. Wire: COPY BINARY both directions; pgoutput v1 accepted grammar; golden SQL text; DDL text.
5. Gate names: test binaries `integration`, `crash_sweep`, `dest_crash_sweep`, `cdc_crash_sweep`, `memory_bound`; bench `iai_pg` + its bench fn names; crate name + feature names (`source`, `dest`, `failpoints`, `fixtures`).
6. Engine-visible behavior: everything in Appendix A.
