# 031 — GENERATION-1 CONTRACT INVENTORY: rdlt-connector-duckdb

Prepared for the true-greenfield rewrite (`031-duckdb-v2`, main @ 8a10d0fe).
Source of record: `crates/rdlt-connector-duckdb` (src 922 lines: lib.rs 7,
dest/mod.rs 374, dest/commit.rs 510, dest/dialect.rs 24; tests 2,596 lines
across 11 binaries + tests/common) plus consumers in `crates/rdlt`,
`crates/rdlt-cli`, `crates/rdlt-engine`, `crates/rdlt-connector-file`,
`crates/rdlt-testkit`, `Makefile`, `benches/`. All quoted spellings are EXACT
(byte-level) unless flagged; `\` inside a quoted Rust format string marks a
string-literal line continuation — the rendered message has a SINGLE space
there.

Layout of gen 1: `lib.rs` thin façade (`pub mod dest;` only — "a future
source slots in beside it"), `dest/mod.rs` the builder + Destination impl +
DDL text helpers, `dest/commit.rs` the LoadSession protocol + ensure
rendering + step execution, `dest/dialect.rs` the MergeDialect seam.
DESTINATION-ONLY crate; no source half exists.

**HEADLINE CORRECTION of the 017-era record**: CLAUDE.md's 017 summary says
"B5 duckdb classification via structured code/extended_code (probe-pinned)".
What the probe actually PINS today (tests/error_codes.rs) is the opposite
fact: the structured channel is DEGENERATE — `ffi::Error` carries
`code: ErrorCode::Unknown` because the DuckDB C API reports no error
category — so classification is MESSAGE-PREFIX keyed (`"IO Error"`,
`"Constraint Error"`), and the probe exists to fail loudly if duckdb-rs ever
starts populating structured categories ("move classification onto them").
This inventory is authoritative on that point.

---

## 1. CONFIG VOCABULARY (frozen)

### 1.1 There is NO config document in gen 1

Gen 1 predates the sdk: no `config::Document`, no from_yaml/from_json/
from_value on the connector, no config_schema anywhere (the ConnectorSpec
carries NO schema — unlike file, which attaches one to both halves). The
configuration surface is a BUILDER API plus the facade's `DestSpec::Duckdb`
YAML arm (§1.3). The v2 crate must derive a destination config document
(sdk `config::Document`, parse-then-validate) that covers exactly this
vocabulary.

### 1.2 Builder API (src/dest/mod.rs)

- `DuckDb::open(path: impl Into<PathBuf>) -> Result<Self, DestinationError>`
  (mod.rs:65) — opens/creates the database file via `Connection::open`,
  mapped through `classify` (a locked or I/O-pressured file is TRANSIENT so
  the engine retries; §2.1, and see docket S5).
- `.options(DestinationOptions) -> Result<Self, _>` (mod.rs:78) — sqlcore's
  vocabulary (§1.4), `options.validate()` at construction, errors fatal.
- `.memory_limit(&str)` (mod.rs:87) — sugar for `.setting("memory_limit", …)`;
  caps DuckDB's buffer/cache (default is a fraction of SYSTEM RAM, which
  dominates pipeline RSS).
- `.setting(key, value)` (mod.rs:122) / `.extension(name)` (mod.rs:137) —
  both via `declare_setup` (mod.rs:97): the key/name must be a BARE
  identifier (`[A-Za-z0-9_]`, nonempty) or typed refusal (§2.2); the
  statement is applied EAGERLY on the builder connection (a bad key/value
  errors HERE) and RECORDED for replay. Value escaping: `SET {key}='{value}'`
  with `'` doubled (`value.replace('\'', "''")`); extensions: `LOAD {name}`.
- Session-setup REPLAY invariant (mod.rs:43-48, 148-156): `try_clone` opens
  a NEW DuckDB session inheriting neither session-scoped SETs nor LOADs, so
  `clone_conn` replays every recorded setup statement on every cloned
  connection — pinned live by tests/probes.rs
  `g3_settings_and_extensions_passthrough` (query_string runs on a CLONED
  connection; passing proves the replay-per-session fix, 013 review
  finding 4).
- ONE shared database instance (mod.rs:36-41): `Arc<Mutex<Connection>>`;
  sessions and probes clone from it. "Two independent `Connection::open`s on
  the same FILE are two database instances — the second cannot see the
  first's un-checkpointed catalog." (Standing limitation; docket S9.)
- `Debug for DuckDb` = `debug_struct("DuckDb").finish_non_exhaustive()`
  (never prints the path or connection).
- doc(hidden) inspection helpers, CONSUMED CROSS-CRATE (mod.rs:161-184):
  `count_rows(table)` — `SELECT count(*) FROM {quote(table)}` (fatal on
  error); `query_string(sql)` — first column of the first row as String.
  Consumers: this crate's suites, the engine's crash_sweep count oracle
  (`dest.count_rows("s")`), conformance TableProbe, file's e2e. The v2
  shell MUST keep an inspection hook (030 solved this as
  `destination::testhook`).
- doc(hidden) `is_constraint_violation(&duckdb::Error) -> bool` (mod.rs:213)
  — public only for the classification tests.
- doc(hidden) `mod sqlgen` (mod.rs:232-258) — the ensure-pin seam:
  `ensure_table_sql(schema, previous) -> Vec<String>`,
  `ensure_merge_sql(options, schema, mode) -> Result<Vec<(String,
  Option<Vec<String>>)>, ValidateError>` (`EnsureStatement` = statement +
  unique-index key columns). Consumed by tests/golden_ensure_sql.rs.

### 1.3 Facade YAML vocabulary (`DestSpec::Duckdb`, crates/rdlt/src/pipeline_spec.rs:132-150)

`DestSpec` is `#[serde(rename_all = "snake_case", deny_unknown_fields)]`,
feature-gated `#[cfg(feature = "duckdb")]`:

```yaml
destination:
  duckdb:
    path: out.duckdb          # PathBuf, required; created if absent
    memory_limit: "4GB"       # Option<String>, passed through
    merge_strategy: upsert    # Option<MergeStrategy> — the sqlcore enum, shared with postgres
    tables: {t: {...}}        # Option<BTreeMap<String, TableOptions>> — sqlcore TableOptions
    extensions: [httpfs]      # Option<Vec<String>> — LOAD passthrough (dlt parity)
    settings: {threads: "4"}  # Option<BTreeMap<String, String>> — raw SET passthrough
```

Build order (pipeline_spec.rs:390-425): open → extensions → settings →
memory_limit → options (only when merge_strategy or tables present;
constructed as a struct literal `DestinationOptions { merge_strategy: *…,
tables: tables.clone().unwrap_or_default() }` then `.options()` which
validates). Error contexts: `opening duckdb: {e}` (400),
`duckdb memory_limit: {e}` (414), `destination options: {e}` (~424);
extension/setting errors pass through as `e.to_string()`.
Facade module: crates/rdlt/src/lib.rs:49-50
`pub use rdlt_connector_duckdb as duckdb;` under `pub mod connector`
(canonical path `rdlt::connector::duckdb::dest::DuckDb`). Feature
(crates/rdlt/Cargo.toml:17): `duckdb = ["dep:rdlt-connector-duckdb"]`;
optional dep at :36.

### 1.4 Destination options (sqlcore's — RE-EXPORTED, spellings frozen there)

dest/mod.rs:31-34 re-exports `rdlt_connector_sqlcore::{AbsentPolicy,
DedupSort, DestinationOptions, MergeStrategy, Scd2Options, SortOrder,
TableOptions}` — the facade and CLI consume these AS `dest::…` paths.
Vocabulary (crates/rdlt-connector-sqlcore/src/options.rs; all structs
`deny_unknown_fields`, enums `rename_all = "snake_case"`):

```yaml
merge_strategy: delete_insert|upsert|scd2   # Option; None = delete_insert default,
                                            # and the distinction is LOAD-BEARING (explicit
                                            # strategy under append/replace = typed error)
tables:
  <table>:
    merge_strategy: …                       # per-table override
    hard_delete: <column>                   # bool cols compare = TRUE, others IS NOT NULL;
                                            # not valid with scd2
    dedup_sort: {column: <col>, order: asc|desc}   # order REQUIRED
    merge_scope: [<col>, …]                 # keyed structured only; not valid with scd2
                                            # unless absent: retire
    scd2:
      valid_from: _rdlt_valid_from          # defaults
      valid_to: _rdlt_valid_to
      absent: keep|retire                   # default keep
      active_record_timestamp: <rfc3339>    # Option; open-marker instead of NULL
      boundary_timestamp: <rfc3339>         # Option; caller-supplied boundary
```

Entry point `DestinationOptions::from_value(serde_json::Value) ->
Result<Self, String>` (parse then validate; timestamps must parse RFC3339
WITH offset — "quoted at generation — never raw user text in a statement").
The 031 rewrite does NOT re-derive this vocabulary — it is sqlcore's, shared
verbatim with postgres; the crate's own suites pin the SHARED wording by
containment (§5).

---

## 2. FROZEN MESSAGE SPELLINGS, CLASSIFICATION, CRASH POINTS

### 2.1 Classification rulebook (src/dest/mod.rs:191-215)

DuckDB exposes NO structured error category (probe-pinned, §headline), so
BOTH classification keys are stable message prefixes:

- `classify(e: duckdb::Error)` (mod.rs:198-205):
  `DuckDBFailure(_, Some(msg))` with `msg.starts_with("IO Error")` →
  `DestinationError::transient(e.to_string())` (file locks — another process
  holding the database — and disk pressure heal on retry); EVERYTHING else →
  fatal. Applied at: `Connection::open`, appender create/append/flush,
  transaction begin/commit, every planned-step execution, staged_nonempty.
- `is_constraint_violation(e)` (mod.rs:213-215):
  `msg.starts_with("Constraint Error")` — the duplicate-merge-key diagnosis
  key, checked on the LIBRARY error BEFORE wrapping so a message that merely
  mentions violations (a table name, a quoted value) can never be
  misdiagnosed (commit.rs:329-346; pinned by tests/classification.rs).
- `fatal(e)` = `DestinationError::fatal(e.to_string())` — the default
  everywhere else. No RateLimited anywhere.
- Probes pinning both facts: tests/error_codes.rs
  `structured_channel_carries_no_category` (ErrorCode::Unknown — "duckdb-rs
  now populates ErrorCode — move classification onto it") and
  `constraint_violation_message_prefix_is_stable` ("DuckDB reworded its
  constraint message — update the classifier prefix in dest/commit.rs").
- INCONSISTENCY (docket S2): the commit-path receipt probes
  (`receipt_exists_sql` commit.rs:392-398, `load_committed_sql`
  commit.rs:402-409) and `read_state`'s error arm (commit.rs:502) map
  through `fatal`, not `classify` — a transient IO Error there aborts the
  run.

### 2.2 Crate-owned message spellings (exact)

- Bare-identifier refusal (mod.rs:105-108), noun ∈ {setting, extension},
  plural ∈ {keys, names}:
  `duckdb {noun} `{name}`: {plural} must be bare identifiers \ ([A-Za-z0-9_]) — refusing to interpolate`
- `connection poisoned` (mod.rs:111, mod.rs:149, commit.rs:52 — the Mutex
  lock-failure arm, fatal)
- `duckdb setting `{key}`: {e}` (mod.rs:221)
- `duckdb extension `{name}`: {e}` (mod.rs:224)
- `internal: merge arm planned for non-merge table `{table}`` (commit.rs:248-250)
- `injected crash at duck.append` (commit.rs:360) /
  `injected crash at duck.tx.commit` (commit.rs:471)

### 2.3 Shared sqlcore spellings this crate emits (frozen in sqlcore, pinned here by containment)

- Duplicate-key diagnosis (sqlcore names.rs:127-135), emitted only when
  `unique_index.is_some() && is_constraint_violation`:
  `table `{table}`: cannot create the unique index the upsert strategy \ requires — existing rows duplicate the merge key ({cols, ", "-joined}); deduplicate the \ table or use delete_insert: {cause}`
- Validator wording pinned by this crate's cells (identical to postgres —
  "one shared validator serves both"): `upsert strategy requires a KEYED` +
  `shredded streams use delete_insert`; `merge_strategy requires the merge
  write mode`; `dedup_sort requires a KEYED`; `merge_scope column `{c}` is
  not a column`; `part of the merge key`; `merge_scope replacement` +
  `SINGLE commit unit` (split-feed / single-unit violation); `tables.{t}.
  merge_scope` + `absent: retire` (scd2+scope under keep);
  `boundary_timestamp` + `not a` (garbage timestamp).

### 2.4 Crash points — IDs, arming spelling, placement

Registry (mod.rs:262-264, `#[cfg(feature = "failpoints")] #[doc(hidden)]`):
`FAIL_POINTS: &[&str] = &["duck.append", "duck.tx.commit"]` — "coarse by
design — DuckDB's own transaction is one atomic step". Arming spelling:
`crash_point!` (imported `rdlt_connector::core::crash_point` in commit.rs);
exactly TWO directly-armed sites:

- `duck.append` — commit.rs:358-361, TOP of `write()`, before the stage
  append.
- `duck.tx.commit` — commit.rs:468-473, inside `commit()` after every
  planned step, before `tx.commit()`, GUARDED `if !replayed` — "a replay
  unit only truncated stages and carried no receipt/state edge (never
  instrumented), so the crash point stays confined to the fresh path".

Registry-vs-sources pin: tests/sweep.rs:180-187
`the_registry_matches_the_sources` runs
`rdlt_testkit::assert_registry_matches_sources(src, &[dest::FAIL_POINTS])`
(one registry; the union is itself). Scanner census: crates/rdlt-testkit/
tests/cases/test_scanner_selfcheck.rs:27 `("rdlt-connector-duckdb", 2)` —
two DISTINCT directly-armed names. The v2 crate must keep this row correct.
The ENGINE's registry cross-pin: crates/rdlt-engine/tests/crash_sweep.rs
`sweep_covers_entire_registry` (~line 355) lists `duck.append` and
`duck.tx.commit` byte-exact in its expected vector.

---

## 3. CORE SEMANTICS — THE PROTOCOL AND THE SQLCORE RELATIONSHIP

### 3.1 Capabilities + spec (mod.rs:337-352)

`ConnectorSpec::new("duckdb", env!("CARGO_PKG_VERSION"))` — NO config
schema attached (gap; docket S11). Capabilities: `merge(true)`,
`structs(true)`, `scalar_lists(true)`, `json_type(true)` (native JSON —
proven by probe + round-trip, tests/{probes,json}.rs), `decimal(true)`,
`ident_rules(IdentRules::default())`.

### 3.2 Type lowering (`sql_type`, mod.rs:292-320) — struct-native

| LogicalType | target | stage (`is_stage`) |
|---|---|---|
| Bool | `BOOLEAN` | same |
| Int64 | `BIGINT` | same |
| Float64 | `DOUBLE` | same |
| Decimal{p,s} | `DECIMAL({p},{s})` | same |
| Json | `JSON` | `VARCHAR` (appender writes Utf8; publish `INSERT…SELECT` applies DuckDB's implicit VARCHAR→JSON cast, which VALIDATES the document — probe-verified) |
| Utf8, Uuid | `VARCHAR` (Uuid as text "for portability with the hex `_rdlt_id` convention") | same |
| Binary | `BLOB` | same |
| TimestampTz | `TIMESTAMP WITH TIME ZONE` | same |
| TimestampNaive | `TIMESTAMP` | same |
| Date / Time | `DATE` / `TIME` | same |
| Struct{fields} | `STRUCT({quoted name} {type}, …)` recursive | same recursion (stage flag propagates) |
| ScalarList{item} | `{scalar type}[]` | same |

`create_table_sql(name, schema, temp)` (mod.rs:322-334):
`CREATE {TEMP }TABLE IF NOT EXISTS {quote(name)} ({cols})` — the stage leg
is `TEMP` and carries stage-shape types.

### 3.3 Naming + quoting

- `quote(ident)` (mod.rs:266-271) = `rdlt_connector_sqlcore::quote_identifier`
  — double-quote, embedded `"` doubled; THE one injection-safety rule (the
  DuckDialect `quote` hook defaults to the same fn).
- `stage_name(table)` (mod.rs:273-280) =
  `{names::STAGE_PREFIX}{ident_hash(table.as_str(), 16)}` =
  `_rdlt_stage_{16-hex}`. NOTE: this is NOT sqlcore's pipeline-scoped
  `names::stage_table(pipeline, table)` — safe ONLY because stages are TEMP
  (session-scoped); record the invariant (docket S10).
- Meta tables (sqlcore names.rs): `_rdlt_state`, `_rdlt_commits`. sqlcore's
  `_rdlt_cleared` (CLEARED_TABLE) is NOT used here — it exists for
  DirectToTarget destinations; DuckDB is Staged (§3.6). `ARRIVAL_COL`
  (`__rdlt_arrival`) also unused — the dialect's arrival order is `rowid`.
- Index names (sqlcore): `rdlt_ix_{16-hex}` (plain) / `rdlt_ux_{16-hex}`
  (unique), hash of `{table}:{col,col}`; DDL built by sqlcore
  `names::create_index_sql(unique, table, columns, quote)`.

### 3.4 Ensure — two phases, RENDERED separately from execution

Phase 1 `table_ddl_stmts(schema, previous)` (commit.rs:105-145) drives
sqlcore `ensure::schema_steps(schema, &WriteMode::Append,
FullLoadPublish::Staged, previous)` — Append+Staged deliberately, because
EVERY write mode here publishes through a stage, so BOTH legs always exist
(differs from postgres; golden-pinned). Emission:
- `EnsureStep::Table{leg}` → `create_table_sql` (target plain, stage TEMP);
- `EnsureStep::Column` →
  `ALTER TABLE {q(leg)} ADD COLUMN IF NOT EXISTS {q(col)} {sql_type}`;
- `EnsureStep::Widen` →
  `ALTER TABLE {q(leg)} ALTER COLUMN {q(col)} SET DATA TYPE {sql_type}` —
  NO USING clause ("DuckDB's ALTER … SET DATA TYPE migrates existing rows";
  differs from postgres, golden-pinned).
- `previous` is the schema THIS SESSION last ensured, "never the live
  catalog — that is what makes the widen a within-run rule" (docket S3).
  Column order: target leg's columns first, then stage's (golden-pinned).

Phase 2 `merge_ensure_stmts(options, schema, mode)` (commit.rs:150-196)
drives sqlcore `ensure::merge_steps` (option validation lives there;
non-merge modes validate and return NOTHING to execute):
- `EnsureStep::Validity(From)` → `ALTER TABLE {q(table)} ADD COLUMN IF NOT
  EXISTS {q(valid_from)} TIMESTAMPTZ DEFAULT now()`; `Validity(To)` → same
  with `TIMESTAMPTZ` — TARGET only, NO `NOT NULL` ("DuckDB rejects ADD
  COLUMN with a NOT NULL constraint. The insert arm always supplies the
  boundary value, so the constraint was belt only; DEFAULT now() still
  backfills pre-existing rows on a table adopting scd2"; golden-pinned).
- `EnsureStep::Index(spec)`: when unique, FIRST
  `DROP INDEX IF EXISTS {q(legacy_unique_index_name)}` — the legacy shim
  (commit.rs:61-68): pre-unique-prefix databases named unique indexes with
  the plain `rdlt_ix_` formula; the old name is dropped before creating the
  `rdlt_ux_` one so such a database "doesn't carry two identical unique ART
  indexes forever". PERSISTED-FORMAT migration — v2 must carry it. Then
  sqlcore `create_index_sql`; `unique_index = spec.unique.then_some(columns)`
  routes the duplicate-key diagnosis (§2.3).

`ensure_table` (commit.rs:307-351) executes phase 1, then phase 2 per
statement with the constraint-violation diagnosis wrap, then records
`(schema, mode)` in the session's `tables` map (BTreeMap keyed by
TableName — a re-ensure OVERWRITES; docket S1).

### 3.5 Write path (commit.rs:353-373)

`write(table, batch)`: `duck.append` crash point; `conn.appender(&stage)` →
`append_record_batch(batch)` → EXPLICIT `appender.flush()` ("Appender drop
swallows errors; flush explicitly so failures surface as DestinationError
instead of silently losing staged rows"). All three classify (staging I/O
is environmental). NOTE: there is NO write-before-ensure typed refusal in
this crate — an un-ensured table surfaces as the appender's own
missing-table error (the sdk session choreography adds the typed refusal in
v2; record the wording change). The appender is POSITIONAL (docket S4).

### 3.6 Commit — ONE DuckDB transaction, planner-owned (commit.rs:375-482)

Staging model (commit.rs doc, lines 4-9): writes land in TEMP tables on the
session's connection; "Temp tables die with the connection, so a fresh
`open` tears down any orphaned stage for free". `open()` (mod.rs:354-373)
clones a connection (own temp-table catalog — "a dead session's staged temp
tables are unreachable") and ensures the meta tables, exact DDL:

```sql
CREATE TABLE IF NOT EXISTS _rdlt_state (pipeline VARCHAR PRIMARY KEY, doc VARCHAR);
CREATE TABLE IF NOT EXISTS _rdlt_commits (
    load_id VARCHAR, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));
```

`commit(meta)` sequence, all inside `conn.transaction()`:
1. Idempotence probe: sqlcore `unit::receipt_exists_sql(|_| "?")` =
   `SELECT count(*) FROM _rdlt_commits WHERE load_id = ? AND commit_seq = ?`
   → `replayed`.
2. Durable Replace guard: `unit::load_committed_sql` =
   `SELECT count(*) FROM _rdlt_commits WHERE load_id = ?` →
   `load_committed_before` ("a crash-recovery session — fresh memory, same
   load — must never re-truncate rows an earlier commit already published";
   pinned by tests/recovery.rs).
3. Up-front full-feed stage probes: for each of sqlcore
   `staged_probe_targets(&tables, &options)`, `unit::stage_nonempty_sql` =
   `SELECT EXISTS (SELECT 1 FROM {quoted stage})` (classify) →
   `staged_nonempty_set` ("matches the former lazy per-table check because
   nothing writes a stage before the steps that READ it").
4. `plan_commit(&tables, &options, &CommitContext { replayed,
   load_committed_before, single_unit_done, staged_nonempty,
   full_load_publish: FullLoadPublish::Staged, cleared_targets: &empty })`
   — THE PLANNER OWNS EVERY DECISION AND THE ORDERING. `Staged` is a
   RECORDED DEFERRAL, not an oversight (commit.rs:436-441): DirectToTarget
   "needs the writes and the clear inside one transaction the session holds
   open across `write` calls; this session appends through an Appender
   opened per write instead… the emitted program is byte-identical to
   before this option existed". `cleared_targets` unused on the staged path.
   `debug_assert_eq!(unit::replay_disposition(Staged),
   ReplayDisposition::RunScript)` — a replayed unit RUNS the script (which
   for a replay is stage truncation and nothing else — that reclaims
   redelivered rows; "they reached no reader, so there is nothing to roll
   back"; the inverse choice belongs to direct-publish destinations).
5. `execute_step` per planned `Step` (commit.rs:217-303), each classify:
   - `ClearTarget{table}` → `DuckDialect.clear_table(quote(table))` =
     `DELETE FROM {table}` (temp-table stages reject TRUNCATE; one spelling
     clears both stage and Replace target).
   - `InsertSelect{table}` → sqlcore `insert_select_sql(target,
     column_list(schema), stage)` — publishes are ALWAYS BY NAME
     (column_list = quoted, comma-joined session schema; pinned live by
     conformance.rs `cross_run_column_drift_publishes_by_name`).
   - `ScopeReplace{table, scope}` → sqlcore
     `scope_replace_sql(&DuckDialect, target, stage, scope)`.
   - `MergeArm{table, arm}` → sqlcore `build_merge_plan(&DuckDialect,
     options, table, schema, key, target, stage, cols, root, root_stage,
     root_schema)` then `render_arm(&plan, arm)` executed statement by
     statement (root = `root_of(&tables, table)` — child tables merge
     against the ROOT's stage for identity). Non-merge table receiving a
     MergeArm = the internal error of §2.2.
   - `TruncateStage{table}` → `DELETE FROM {quoted stage}`.
   - `UpsertState` → `INSERT OR REPLACE INTO _rdlt_state VALUES (?, ?)`
     with `(pipeline, serde_json::to_string(&meta.state))` — state persists
     in the SAME transaction as the data.
   - `InsertReceipt` → `INSERT INTO _rdlt_commits VALUES (?, ?)` with
     `(load_id, commit_seq as i64)` — a duplicate receipt is a genuine
     constraint violation = "the idempotence-anomaly signal — fail loudly".
6. `duck.tx.commit` (fresh path only) → `tx.commit()` (classify) → the
   session extends `single_unit_done` with `script.marks` — applied ONLY
   after the transaction committed; the planner re-emits marks on replay so
   a crash-recovery replay RE-MARKS the single-unit discipline (pinned by
   tests/recovery.rs `replay_re_marks_single_unit_discipline`).

`read_state(pipeline)` (commit.rs:484-509):
`SELECT doc FROM _rdlt_state WHERE pipeline = ?`;
`QueryReturnedNoRows` → Ok(None); parse via serde_json. Keyed by the raw
pipeline string (no hash scope — unlike file; no collision filter needed).

### 3.7 The dialect seam (src/dest/dialect.rs — 24 lines, ONLY two overrides)

`DuckDialect` implements sqlcore `MergeDialect` keeping EVERY default hook
(every capability probe passed): default `quote` (shared quote_identifier),
default `tx_timestamp` = `now()` (TRANSACTION-stable in DuckDB —
probe-pinned, the scd2 boundary rule), default `dedup_subquery` =
`(SELECT DISTINCT ON ({identity}) * FROM {stage} ORDER BY {identity},
{sort_prefix}{arrival} DESC)`, default `flag_set`/`flag_unset`
(`IS TRUE` family — NULL-safe), default `incoming_ref`/`upsert_stmt`
(`INSERT … ON CONFLICT (cols) DO UPDATE SET c = EXCLUDED.c` against the
auto-ensured unique ART index — probe-pinned), default `materialize_dedup`
= None (inline). The two overrides:
- `arrival_order() -> "rowid"` — temp stages have no arrival column; rowid
  reflects append order (deterministic last-wins tie-breaker; behavior
  pinned by probes + refinements).
- `clear_table(table) -> "DELETE FROM {table}"`.

Probes backing the defaults (tests/probes.rs, each names its dependent arm):
DISTINCT ON survivor shape incl. NULLS LAST + rowid tie-break; ON CONFLICT
against a plain CREATE UNIQUE INDEX (not a declared constraint); now()
transaction-stability; `IS DISTINCT FROM` NULL semantics (scd2 change
detection); bundled JSON extension (native JSON + json_extract, no
network); correlated `UPDATE … FROM` + `NOT EXISTS` anti-join (scd2 arms).

---

## 4. THE LIBRARY BOUNDARY

ONE library: `duckdb` (duckdb-rs), workspace-pinned
`{ version = "1", features = ["bundled", "appender-arrow"] }`
(root Cargo.toml:91). The workspace arrow pin is COUPLED to it
(Cargo.toml:52-55): "arrow-rs: single workspace-wide major… Pinned to the
major that duckdb-rs links — connectors receive `RecordBatch` through the
rdlt-connector re-export, so version identity across the workspace is a
correctness requirement" (arrow 58.3 today).

Where library types cross today (gen 1 has no single boundary MODULE —
017's "wrapped at ONE boundary" is honored in spirit, not layout):
- `duckdb::Connection` — mod.rs (open/try_clone/execute_batch/query_row),
  commit.rs (transaction/appender/execute/query_row).
- `duckdb::Error` — `classify`/`is_constraint_violation`/`fatal` arms;
  `DuckDBFailure(ffi, Option<String>)` destructured; `QueryReturnedNoRows`
  matched in read_state; `duckdb::ffi::ErrorCode` in the probe only.
- `duckdb::Transaction`, `duckdb::params!` — commit.rs.
- `Appender::append_record_batch` (the `appender-arrow` feature) — THE
  ingestion edge: Arrow RecordBatch straight into the temp stage.
- Tests use `duckdb::Connection` directly (probes, error_codes,
  classification precondition).
Nothing library-typed escapes the crate's public surface (public API deals
in DestinationError/DestinationOptions only; `is_constraint_violation`
takes `&duckdb::Error` but is doc(hidden) for the crate's own tests).
For v2 under the 027 one-dependency rule: sdk + duckdb + sqlcore (sqlcore
is THE recorded exception, same as postgres/snowflake); SPI via the sdk's
`spi` re-export; boundary module discipline per 028's client.rs precedent.

---

## 5. TESTS CENSUS (the parity target)

51 tests under default features across 11 binaries; +2 in `sweep`
(`#![cfg(feature = "failpoints")]`). NO unit tests in src/. Shared driver
tests/common/mod.rs (126 lines): `StructuredSource` — keyed STRUCTURED
Arrow feed, one checkpoint per unit, `.at(base)` cursor offset for
incremental reruns, resume-honoring read (E6); helpers batch/i64s/strs/
bools/one_unit.

| binary | tests | covers |
|---|---|---|
| classification | 2 | violation-wording-in-other-errors not misdiagnosed (needle `violate` in a catalog error; classifier says NO, genuine constraint YES); unopenable file (missing parent dir) is TRANSIENT not fatal |
| conformance | 5 | testkit `verify_destination` + `assert_conformant` over a real file (DuckProbe on count_rows); e2e nested sync through the engine (STRUCT dot-syntax query, `users__tags` lineage join on `_rdlt_parent_id`/`_rdlt_id`/`_rdlt_pos`); incremental second run = 0 new rows, no duplicates; in-batch merge dedup last-wins (review finding 7); cross-run column drift publishes BY NAME (review finding 4) |
| differential | 6 | THE CROSS-DESTINATION ORACLE (013): identical feeds through postgres (live container) and DuckDB must agree on canonical rows AND typed-error classes — delete_insert redelivery; upsert+hard_delete+dedup; scd2 history shape; merge_scope + NULL-scope; rejection-class identity (append+explicit strategy); scd2 scoped retirement. Container-gated skip-not-fail (`runtime_available()`, eprintln `SKIP: no container runtime — differential cell not run`). Canonicalizer: NULL → `∅`, bools → true/false, `\u{1}`/`\u{2}` join on the duck side; error-class key = text after ``table ` `` |
| error_codes | 2 | the two classification probes (§headline) |
| golden_ensure_sql | 9 | ensure DDL pinned AS DATA via sqlgen (no connection): both legs always created (stage TEMP) even without merge; columns target-then-stage; widen = SET DATA TYPE, never USING; unchanged column never widened; scd2 validity columns carry NO NOT NULL (+ exact `TIMESTAMPTZ DEFAULT now()`/`TIMESTAMPTZ` tails); unique index preceded by legacy-name DROP (drop < create; only create carries key columns); default strategy = ONE plain supporting index, no drop; non-merge emits nothing but still validates; merge-only options under Append refused naming the table. These are this crate's half of sqlcore's net ("the four golden-SQL files in the postgres and duckdb crates" — sqlcore tests/integration.rs) |
| json | 2 | json_type capability is REAL: declared column type is `JSON` in information_schema (not VARCHAR), json_extract works directly; merge composes with native JSON (redelivered key replaces document) |
| probes | 7 | the six dialect-feasibility probes (§3.7) + `g3_settings_and_extensions_passthrough` (replay-per-session, bare-identifier typed refusals incl. `threads='1'; DROP TABLE x; --`, unknown-setting error names the key) |
| recovery | 2 | durable Replace truncate-once guard across crash-recovery sessions (regression pin for the file destination's confirmed data loss — "DuckDB carried the same latent pattern"); replay RE-MARKS single-unit discipline (D3 branch; second unit for a scoped table typed `SINGLE commit unit` + `merge_scope`) |
| refinements | 5 | dedup_sort ordered survivors (values beat NULL, all-NULL arrival last-wins); merge_scope wholesale scope replacement (NULL is not a scope); split-feed typed on the SECOND unit; typed-rejection matrix (keyless dedup / ghost scope column / key-constant dedup — shared validator wording); non-bool hard_delete deletes on IS NOT NULL |
| strategies | 11 | delete_insert replaces matched keys; upsert in-place + hard_delete compose; upsert on shredded stream typed; explicit strategy under append typed (+ unconfigured default never rejects); scd2 close/open + unchanged-key no-churn; scd2 ONE boundary instant per unit (50 keys, count(DISTINCT _rdlt_valid_from) = 1); scd2 absent:retire full feed; upsert over pre-existing duplicates typed naming columns; scd2 SCOPED retirement (absent retires only in delivered scopes); scd2 merge_scope requires retire (parse-time); active_record_timestamp + boundary_timestamp override (+ injection-shaped timestamp refused at parse) |
| sweep (failpoints) | 2 | strategy-arm crash sweep: 4 strategies (delete_insert, upsert+hard_delete+dedup, scd2_retire, merge_scope+dedup) × 2 points × 3 actions (`return`, `panic`, `1*off->return`), TWO armed runs (crash during recovery itself) then recovery + convergence to `1:k1-new,2:k2,3:k3` (scd2: active-only view + no history churn on unchanged keys); ARMED-FIRE PIN over the full (strategy × point) matrix — ONE test fn deliberately (fail points are process-global); + the registry-vs-sources scan |

Recorded flake (benches/flakes.log:6, 2026-07-27):
`differential::differential_upsert_with_hard_delete_and_dedup` ×1 —
environment class, never re-rolled.

The differential suite deliberately lives in THIS crate, not the facade:
"the facade's empty default features would have silently skipped it (plan
R7 amendment, recorded)" (differential.rs:21-23).

---

## 6. CONSUMERS — exact locations

- Workspace: root Cargo.toml:11 member; :45
  `rdlt-connector-duckdb = { path = …, version = "0.3.0" }`; :91 the
  duckdb-rs pin; :52-55 the arrow-major coupling comment.
- Facade `crates/rdlt/src/lib.rs`:49-50
  `#[cfg(feature = "duckdb")] pub use rdlt_connector_duckdb as duckdb;`
  (inside `pub mod connector`; doc :47 names `connector::duckdb::DuckDb`).
- Facade features `crates/rdlt/Cargo.toml`:17
  `duckdb = ["dep:rdlt-connector-duckdb"]`; :36 optional dep; default
  features EMPTY (:15).
- `crates/rdlt/src/pipeline_spec.rs`: 132-150 `DestSpec::Duckdb` variant;
  378-386 cfg_attr dead-code gates name the feature; 390-425 build arm
  (§1.3 error contexts). Types referenced BY PATH:
  `crate::connector::duckdb::dest::{MergeStrategy, TableOptions, DuckDb,
  DestinationOptions}` — the v2 module layout must keep `dest::` exposing
  all four (or the facade arm is edited at swap, as 029 did with Shell::new).
- `crates/rdlt-cli`: Cargo.toml:21 facade features incl. `"duckdb"`;
  src/main.rs:357 `duckdb_options_pass_through_the_yaml` (full YAML block
  incl. extensions/settings/tables); :237 `shared_parity_specs_all_parse`
  over `benches/parity_specs.yaml` (first doc = `pipeline: parity-duckdb`
  with memory_limit/merge_strategy/extensions/settings — "a new destination
  kind is added HERE first"); :522-556 `pipeline_spec_forms_parse` duckdb
  arms (`duckdb: {path: out.db}`, `{path: out.db, memory_limit: "1GB"}`).
- `crates/rdlt-engine`: Cargo.toml:50-52 dev-dep with `failpoints`;
  tests/crash_sweep.rs:185-207 `sweep_duckdb_destination` (Append/Replace/
  Merge × ENGINE_POINTS+FAIL_POINTS, constructs
  `DuckDb::open(dir.join("out.duckdb"))`, oracle `dest.count_rows("s")`);
  :315-364 `sweep_duckdb_keyed_structured_merge` (keyed Arrow merge,
  TOTAL_ROWS exactly-once pin, armed-fire per point); :340-370
  `sweep_covers_entire_registry` (file+duckdb registries vs the literal
  expected list). The engine sweep OWNS the cross-mode point coverage; the
  crate's own sweep binary owns the strategy-arm × action matrix.
- `crates/rdlt-connector-file`: Cargo.toml:43 dev-dep;
  tests/e2e.rs:32-57 `jsonl_to_duckdb_lands_exact_totals_and_resumes`
  (three `DuckDb::open(&db)` constructions at :40/:51/:57 — file's e2e
  oracle counts through count_rows).
- `crates/rdlt-testkit`: tests/cases/test_scanner_selfcheck.rs:27
  `("rdlt-connector-duckdb", 2)` census row.
- Makefile:121 (TARGET=sweep):
  `cargo nextest run -p rdlt-connector-duckdb --features failpoints -E 'binary(sweep)'`;
  the whole-workspace nextest run covers the other 10 binaries. No nextest
  group membership (.config/nextest.toml: iceberg-live only).
- Bench/perf: benches/cold-start/cold.yaml (`duckdb: {path:
  @WORK@/cold.duckdb}`) + benches/check-cold-start.sh — THE ≤40 ms
  cold-start gate runs file → duckdb through the CLI;
  benches/parity_specs.yaml (above); examples/jsonl_to_duckdb.rs (raw-bytes
  perf example; its doc header cites `benches/run-e2e.sh`, retired in 018 —
  stale, docket S12). No current benches/cells/ cell uses duckdb (the 018
  matrix rebuild retired jsonl-duckdb-200k / pg-wide-duckdb-1m; RESULTS.md
  history only). benches/GOVERNANCE.md:49 cites the sweep command as the
  baseline-before example.
- NOT consumers: rdlt-connector-postgres (this crate dev-deps IT for the
  differential oracle), fuzz/ (no duckdb targets).

---

## 7. DEPENDENCIES (crates/rdlt-connector-duckdb/Cargo.toml, all workspace-pinned)

[dependencies] `rdlt-connector` (no features), `rdlt-connector-sqlcore`,
`async-trait`, `duckdb` (workspace: `version = "1"`, features
`["bundled", "appender-arrow"]`), `serde_json`, `tokio`.
[dev-dependencies] `arrow-array`, `arrow-schema`, `rdlt-connector-postgres`
(default-features = false, features `["destination", "fixtures"]` — the 013
differential oracle), `tokio-postgres`, `testcontainers-modules`,
`rdlt-testkit`, `rdlt-engine`, `tempfile`, `bytes`, `arrow`, `tokio`.
[features] `failpoints = ["rdlt-connector/failpoints"]`. [lints] workspace.
Package: description "rdlt DuckDB destination: Arrow ingestion,
struct-native lowering, staged atomic commits"; keywords
elt/duckdb/arrow/olap/sql; categories database; README.md documents the
full YAML/builder vocabulary (the option tables + "There is deliberately no
`read_only` option — a destination writes").

NOTE for the rewrite: under the 027 one-dependency rule the v2 crate
depends on `rdlt-connector-sdk` (+ sqlcore, the recorded exception, +
duckdb itself); failpoints/schema forwarded by the sdk. Gen 1 predates the
sdk entirely — direct `Destination`/`LoadSession` impls, session
choreography (write-before-ensure refusal, existing_receipt→replay→publish)
partly hand-implemented in commit(), partly ABSENT (§3.5).

---

## 8. SUSPICIOUS ITEMS (candidate inherited defects — review-loop input, NOT fixed)

S1. **Two streams resolving to one physical table are not refused** — the
    session's `tables: BTreeMap<TableName, (TableSchema, WriteMode)>`
    (commit.rs:38) silently OVERWRITES on re-ensure (commit.rs:349), and
    `stage_name` keys on the table name alone, so two streams whose
    normalized names collide share ONE stage and one (schema, mode) —
    last-ensure-wins, interleaved appends, no gate. 029 found the analogous
    shared-table case to be SILENT CORRUPTION and refused it at config AND
    ensure; 030's docket carries the same item (its §9-S1).
S2. **Classification asymmetry inside commit** — the receipt probe
    (commit.rs:398), the load-committed probe (commit.rs:409) and
    `read_state`'s error arm (commit.rs:502) map errors through `fatal`,
    while every neighboring statement classifies. A transient `IO Error`
    (locked file) at exactly those reads aborts the run instead of riding
    the retry budget.
S3. **Cross-run widening gap** — `previous` is session memory only
    (commit.rs:105-109): a run whose type widened SINCE the table was
    created sees `previous = None`, emits only no-op `CREATE IF NOT
    EXISTS`/`ADD COLUMN IF NOT EXISTS`, and never widens the live column;
    the mismatch surfaces (or silently casts) at the appender/INSERT
    instead of as a planned `SET DATA TYPE`. 028's rewrite answered the
    analogous problem with a read-before-write catalog image (catalog.rs).
S4. **The stage append is POSITIONAL** — `append_record_batch` matches the
    temp stage by column position, not name; correctness rests on the
    engine invariant that a batch's column order equals the last-ensured
    schema's. Publishes are by-name (pinned), but nothing pins or checks
    the write-side ordering assumption; a batch reordered mid-run would
    land values in wrong stage columns without error (types permitting).
S5. **Every "IO Error" is transient, including deterministic ones** — a
    missing parent directory classifies TRANSIENT (pinned deliberately by
    classification.rs `unopenable_file_is_transient_not_fatal`) and retries
    through the engine's whole backoff budget; the message prefix cannot
    distinguish a lock (heals) from a path that never will. Carry as
    deliberate or refine in v2 — either way the disposition must be
    recorded, not accidental.
S6. **No write-before-ensure typed refusal** (§3.5) — the sdk choreography
    adds one in v2; the ERROR TEXT will change from duckdb's missing-table
    appender error to the sdk's typed message. Frozen-surface decision
    needed (029/030 both let the sdk's refusal stand; record the same).
S7. **`commit_seq as i64` / `count(*)` as u64 casts** — commit.rs:297/395
    cast u64→i64 silently (theoretical wrap at i64::MAX); the receipt probe
    reads count(*) into u64 via duckdb's conversion. Harmless today; the
    rewrite should make the narrowing deliberate.
S8. **Poisoned-mutex message names no operation** — `connection poisoned`
    (three sites) is the whole message; after any panic while holding the
    lock, every later call fails with no hint of what poisoned it.
S9. **Nothing refuses a second `DuckDb::open` on the same file in-process**
    — mod.rs:37-38 documents two opens = two database instances (the second
    cannot see un-checkpointed catalog), but the API cannot detect it;
    sequential re-open works (strategies.rs does it), concurrent instances
    are undefined-by-document. Standing record at minimum (028/029
    coexistence-collision analogue).
S10. **`stage_name` is not pipeline-scoped** (§3.3) — safe solely because
    stages are TEMP (session-scoped). A v2 that ever changed staging to
    real tables (e.g. for DirectToTarget) would collide across pipelines;
    the invariant deserves a pinned comment/test, not folklore.
S11. **No config_schema on the ConnectorSpec** — `ConnectorSpec::new(
    "duckdb", …)` bare (mod.rs:339), unlike file (both halves). The sdk
    config::Document gives v2 a schema; attach it.
S12. **Stale example doc** — examples/jsonl_to_duckdb.rs:1 cites
    `benches/run-e2e.sh`, retired in 018 (file absent). Cosmetic.
S13. **`count_rows` counts EVERYTHING** — scd2 history rows and all
    user rows under the (quoted) name; fine for the current oracles (the
    sweep filters actives itself) but it is the conformance probe, so a v2
    behavior change would shift what conformance certifies. Keep or scope
    deliberately.

Standing (documented, deliberate — carry as records, not defects):
`FullLoadPublish::Staged` deferral — DirectToTarget "not available here
without a separate redesign", emitted program byte-identical to
pre-option (commit.rs:436-441); widen is a WITHIN-RUN rule by design
(golden-pinned; S3 is about the consequence, the rule itself is recorded);
the legacy `rdlt_ix_`→`rdlt_ux_` unique-index DROP shim is PERSISTED-FORMAT
migration and must survive the rewrite (user databases carry the old name);
message-prefix classification is forced by the library (probe-pinned with
named escape hatches); unbounded `_rdlt_commits` receipt retention (same
rationale as file §4.1 — the SPI commit contract is unconditional);
`rowid`-as-arrival assumes DuckDB append order in temp stages (probe-backed
today, re-probe on duckdb-rs major bumps).
