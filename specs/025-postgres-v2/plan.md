# rdlt-connector-postgres-v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for
> tracking. Update checkboxes as tasks complete — this file is the durable
> record across context windows.

**Goal:** Build a brand-new crate `crates/rdlt-connector-postgres-v2` from
scratch to the six-seam design, with every identifier — types, functions,
parameters, fields, modules — named from first principles, full functional
parity with `rdlt-connector-postgres` proven by a ported test suite, and the
workspace gate green.

**Architecture:** Greenfield crate built bottom-up (tls → session → types →
source → destination → cdc), each layer landing with its tests in the same
task. The old crate is read for *behavioral contracts only* (Appendix A) and
stays untouched and in the workspace; nothing links the new crate into the
facade — swapping it in later is the owner's call. Persisted formats, wire
formats, YAML config vocabulary, and engine-visible behavior are frozen
(Appendix B); every Rust identifier is free and MUST be re-derived under the
naming rules (below).

**Tech Stack:** Same dependency set as the old crate (tokio-postgres, rustls,
arrow 58.3, rdlt-connector SPI, rdlt-connector-sqlcore) — the dependency list
may not grow beyond the old crate's.

## STATUS — COMPLETE (2026-08-01)

DONE. Every task 0-11 executed. Final state on branch `postgres-v2`:
- crate `rdlt-connector-postgres-v2` complete: 79 lib tests, 148/148
  integration (zero skips, containers live), 14/14 armed sweep cells
  (source/destination/cdc, exactly-once at every crash point, registries
  scanner-verified), memory_bound heavy claim green, doctests green.
- Golden SQL byte-identical to generation 1 (DDL, ensure, unit literals).
- iai parity: v2 FASTER — decode 20,547,225 vs 20,994,190 instructions
  (−2.1%), encode 28,597,743 vs 30,047,979 (−4.8%).
- clippy --all-targets --all-features −D warnings: zero. rustdoc −D
  warnings: zero. cargo fmt clean. Naming audit clean (src + tests).
- Makefile wired: v2 sweeps in TARGET=sweep, v2 memory_bound in
  TARGET=deep.
- FULL GATE TWICE CLEAN, untouched runs: 1194/1194 workspace tests
  (2 named instrument skips), all sweeps, semver clean, perf gate all
  benches within tolerance / 0 regressed, cold start 22.8 / 24.0 ms
  (bar ≤ 40). Exit 0 both runs.
- WALL-CLOCK A/B vs generation 1 (tests/perf_ab.rs, release profile, one
  warmup pair + 5 interleaved pairs per cell, fresh schema/pipeline per
  run): append 1M rows gen1 median 721 ms vs gen2 706 ms (−2.1%; samples
  gen1 638–743, gen2 651–786 — overlapping, parity-or-better); merge 250k
  gen1 491 ms vs gen2 485 ms (−1.3%; both arms ~477–552 — a wash, expected:
  the merge cell is ~71% server-side). Direction agrees with the iai
  instruction counts. NO regression.
- Old crate UNTOUCHED and still fully gated. v2 NOT wired into the facade
  (D1); merge + facade swap + publishing name are the owner's calls.

## Decision record

- **D1 — new crate, old untouched.** Package `rdlt-connector-postgres-v2`,
  path `crates/rdlt-connector-postgres-v2`, registered as a workspace member,
  `publish = false` until the owner decides its fate. NOT wired into the
  `rdlt` facade. The old crate keeps running the existing gate lines.
- **D2 — branch `postgres-v2`.** Done = full gate twice clean with the new
  crate's suites wired in; merge is the owner's call.
- **D3 — no copying.** Fresh code against Appendix A contracts. Constants that
  ARE the contract (SQLSTATE class lists, the TLS-refusal needle, alert sets,
  driver param list, GUC pins, SQL text) necessarily reappear; prose and
  structure never.
- **D4 — behavior parity, API freedom.** The new crate's Rust API is
  redesigned (naming rules below). What the ENGINE and the OPERATOR observe is
  frozen: Appendix B.
- **D5 — gate wiring is in scope.** 024 made the gate refuse unknown test
  binaries: every new test binary must be invoked or exempted BY NAME
  (Makefile sweep lines + the reachability enumeration). Task 10.
- **D6 — hot paths keep their measured algorithms** (single-pass COPY decode,
  borrowed-view encode, bounded buffers). Task 11 measures old vs new with
  side-by-side iai benches before the gate rules.

## Naming rules (the user's directive — apply to EVERY identifier)

1. **The module path is part of the name.** No type repeats its module's
   noun: `tls::Policy` not `TlsPolicy`, `source::Config` not
   `PostgresSourceConfig`, `cursor::Tracker` not `CursorTracker`. Call sites
   spell the path (`tls::Policy`) — the crate has no root re-exports, so the
   path IS the canonical spelling.
2. **No ad-hoc truncations.** `connection` not `conn`, `statement` not
   `stmt`, `table` not `tbl`, `context` not `ctx`, `buffer` not `buf`.
   Established domain acronyms are words: SQL, TLS, CDC, DDL, WAL, LSN, COPY,
   GUC (CamelCase as `Sql`, `Tls`, …). Rust-idiomatic short forms that std
   itself uses stay: `config`, `len`, `id`, `iter`.
3. **Functions are verbs** (`parse`, `establish`, `reflect`, `render`,
   `plan`); predicates read as questions (`is_transient`, `wants_encryption`,
   `covers_all_columns`); no `get_` prefixes; conversions follow std
   (`from_*`, `into_*`, `as_*`, `to_*`).
4. **Parameters are named for their role,** not their type:
   `tls_override` not `block`, `connection_string` not `s` or `conn`.
5. **One name per concept, crate-wide.** "schema" = the Postgres namespace,
   always; the SPI's `TableSchema` is only ever referred to as a table's
   *shape* in prose and `table_schema` in code. "stream" = an engine stream;
   "table" = a Postgres relation. The rename ledger (Appendix C) is the
   authority; extend it whenever a new concept is coined.
6. **Booleans read as assertions** (`replica_identity_covers_all_columns`,
   `transaction_open`), never bare adjectives (`full`, `open`).
7. **Errors are named by what failed** (`EstablishError`, `ParseError`), not
   by the layer that noticed.

## Global Constraints

- Workspace denies `unsafe_code`; workspace lints apply; `cargo fmt` every touched crate.
- Tests via `cargo nextest run` (doc-tests via `cargo test --doc`); gates with `env -u RUSTUP_TOOLCHAIN`, launched from repo root, Makefile untouched during a run.
- House layout: pure-TOC `mod.rs`, code under its noun, tests = `integration.rs` + `cases/test_<noun>.rs`; no `name.rs` beside `name/`.
- Comments self-contained; no citation IDs; typed error taxonomy end to end.
- Every task ends: `cargo check -p rdlt-connector-postgres-v2 --all-targets --all-features` clean, that task's tests green, one commit ending with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Container suites need `systemctl --user start podman.socket`; the first full run after a rebuild may hit the recorded container-burst flake — rerun once before diagnosing.

## The crate

```
crates/rdlt-connector-postgres-v2/
├── Cargo.toml            # features: default=["source","destination"], failpoints, fixtures
├── README.md
├── benches/iai.rs        # decode/encode hot-path benches, NOT gate-wired (Task 11 compares by hand)
├── src/
│   ├── lib.rs            # front page + module TOC; no root re-exports
│   ├── tls/              # public policy vocabulary (facade-independent)
│   │   ├── mod.rs        #   pub Policy, Mode, ConfigError; pub(crate) build seam
│   │   ├── policy.rs     #   Policy/Mode + resolution + credential shape rules
│   │   ├── client_config.rs # rustls ClientConfig construction
│   │   └── verify.rs     #   the quarantined weaker-mode verifiers
│   ├── session/          # pub(crate): connection string → prepared live connection
│   │   ├── mod.rs
│   │   ├── connection_string.rs  # both libpq spellings, TLS trio extraction, strict escapes
│   │   ├── establish.rs  #   Profile (Plain | CdcControl), Connection, establish()
│   │   └── classify.rs   #   detail rendering + the two opposite-polarity SQLSTATE rules + TLS failure taxonomy
│   ├── types/            # pub(crate): THE Postgres type rulebook
│   │   ├── mod.rs        #   Kind (closed enum), Column (name + kind)
│   │   ├── map.rs        #   catalog type + hint → Kind (+ `rdlt::lossy` warns)
│   │   ├── binary.rs     #   COPY BINARY → Arrow (single-pass, bounded)
│   │   ├── text.rs       #   text form → value (CDC tuples, watermarks)
│   │   ├── literal.rs    #   value → SQL literal (resume predicates)
│   │   └── encode.rs     #   Arrow → COPY BINARY (destination feature)
│   ├── source/
│   │   ├── mod.rs
│   │   ├── connector.rs  #   struct Postgres + impl Source (thin dispatch)
│   │   ├── config/       #   vocabulary.rs (serde nouns incl. TypeHint) + validate.rs (entry points)
│   │   ├── plan.rs       #   THE stream validation gate; streams() and read() share its output
│   │   ├── reflect.rs    #   catalog reflection + query describe
│   │   ├── sql.rs        #   SELECT/COPY text + the boundary matrix
│   │   ├── copy.rs       #   the COPY read loop (crash hook only under failpoints)
│   │   ├── cursor/       #   watermark.rs, tracker.rs, prepare.rs
│   │   ├── cdc/          #   runtime.rs (typestate), slot.rs, read.rs, tail.rs, ack.rs, apply.rs, pgoutput.rs
│   │   ├── errors.rs     #   Phase-tagged source errors + establish/driver adapters
│   │   └── fail_points.rs
│   ├── destination/
│   │   ├── mod.rs
│   │   ├── connector.rs  #   struct Postgres + impl Destination; open()
│   │   ├── config.rs     #   builder + from_yaml/from_json/from_value + config_schema()
│   │   ├── catalog.rs    #   ensured tables + DDL rendering (sqlcore ensure planner consumer)
│   │   ├── unit.rs       #   the commit-unit transaction + cleared-target set
│   │   ├── executor.rs   #   sqlcore Step execution over (Connection, Catalog, Unit)
│   │   ├── load.rs       #   impl LoadSession — coordinates the three
│   │   ├── dialect.rs    #   sqlcore MergeDialect (three overrides, defaults golden-pinned)
│   │   ├── errors.rs     #   Phase-tagged destination errors (parity with source)
│   │   └── fail_points.rs
│   ├── testsupport/      # ONE doc-hidden seam for pins/benches/fuzz across the test-binary boundary
│   │   ├── mod.rs, source.rs, destination.rs, session.rs, data.rs
│   └── fixtures.rs       # feature "fixtures": PostgresContainer, CdcContainer
└── tests/
    ├── integration.rs + cases/…          # ported suites, renamed to the new API
    ├── source_crash_sweep.rs             # named gate binaries (Task 10 wires them)
    ├── destination_crash_sweep.rs
    ├── cdc_crash_sweep.rs
    └── memory_bound.rs
```

---

### Task 1: Scaffold

**Files:** `crates/rdlt-connector-postgres-v2/{Cargo.toml,README.md,src/lib.rs}`, root `Cargo.toml` (workspace member)

- [x] **Step 1:** Cargo.toml: copy the OLD crate's dependency set exactly (minus nothing, plus nothing), `publish = false`, features `default=["source","destination"]`, `failpoints=["rdlt-connector/failpoints"]`, `fixtures=[dep:rdlt-testkit, dep:testcontainers-modules]`, workspace lints.
- [x] **Step 2:** lib.rs: front-page doc (v2 statement + module map + naming rules synopsis) with the empty module tree declared; register in workspace `members`.
- [x] **Step 3:** `cargo check -p rdlt-connector-postgres-v2 --all-features` clean; commit `feat(postgres-v2): scaffold — the crate exists`.

### Task 2: `tls/`

**Files:** `src/tls/{mod.rs,policy.rs,client_config.rs,verify.rs}`
**Interfaces:** `pub struct Policy { mode: Mode, root_cert, client_cert, client_key: Option<PemSource> }` (serde shape byte-compatible with old `TlsPolicy` — same field names, same `snake_case` mode strings, `deny_unknown_fields`); `pub enum Mode { Disable, Prefer, Require, VerifyCa, VerifyFull }`; `pub enum ConfigError` (all seven old variants, fresh prose); `pub(crate) fn resolve(connection_sslmode, tls_override) -> Result<Policy, ConfigError>`; `pub(crate) fn validate_credentials(&Policy)`; `pub(crate) fn build_client_config(&Policy) -> Result<Option<rustls::ClientConfig>, ConfigError>`.
**Contract:** Appendix A §Session (policy rules half). Never-weaken sslmode resolution; both-or-neither credentials; never-with-plaintext; labels never leak inline PEM material; verify_ca = chain-only verifier; require/prefer = accept-any.

- [x] Write fresh with inline unit tests (resolution matrix, credential shapes, verifier selection, PEM label hygiene); green; commit `feat(postgres-v2): tls policy vocabulary`.

### Task 3: `session/`

**Files:** `src/session/{mod.rs,connection_string.rs,establish.rs,classify.rs}`
**Interfaces:**
```rust
pub(crate) struct Parsed { pub driver: tokio_postgres::Config, pub tls: tls::Policy }
pub(crate) fn parse(connection_string: &str, tls_override: Option<&tls::Policy>) -> Result<Parsed, tls::ConfigError>;
pub(crate) enum Profile { Plain, CdcControl }   // CdcControl pins datestyle=ISO + bytea_output=hex after connect
pub(crate) struct Connection { … }              // live client + detached io driver; Deref<Target=tokio_postgres::Client> documented as deliberate
pub(crate) async fn establish(connection_string: &str, tls_override: Option<&tls::Policy>, profile: Profile) -> Result<Connection, EstablishError>;
pub(crate) enum EstablishError { Config(tls::ConfigError), Connect(ConnectError) }  // is_transient() is THE retryability rule
pub(crate) struct ConnectError { pub failure: TlsFailure, pub detail: String, pub transient: bool }
pub(crate) enum TlsFailure { TrustAnchor, Chain, Hostname, ServerRefusedTls, ClientCert, Other }
// classify.rs: detail(&tokio_postgres::Error) -> String;  is_transient_connect_sqlstate; is_permanent_statement_sqlstate
```
**Contract:** Appendix A §Session. TLS trio extraction from both libpq forms; strict percent-escapes; unsupported params rejected by name with hints; `sslrootcert=system`; verify-* strength ordering; `application_name=rdlt` default; the pinned `"server does not support TLS"` needle (with its pin test); rustls alert classification incl. the TLS-1.2 handshake_failure forms; 28000+certificate = ClientCert; connect polarity 08/53/57/40 transient, statement polarity 22/23/42 permanent.

- [x] Write fresh; unit tests inline (needle pin, alert classification, parse matrix ported from old `test_connstring.rs` cases with new spellings); commit `feat(postgres-v2): session — one path from string to prepared connection`.

### Task 4: `types/` — map, binary, text, literal

**Files:** `src/types/{mod.rs,map.rs,binary.rs,text.rs,literal.rs}`
**Interfaces:** `pub(crate) enum Kind { … }` (closed; full set derived from old `type_map.rs` in Step 1 and recorded in Appendix A §Types); `pub(crate) struct Column { name: String, kind: Kind }`; `map::resolve(catalog_row, hint) -> Kind`; `binary::Decoder` (COPY BINARY → RecordBatch); `text::parse(kind, text) -> value`; `literal::render(kind, value) -> String`. Exhaustive `match` over Kind in every face — no `_` arms.
**Contract:** Appendix A §Types. UUID server forms; numeric stays in the string domain; timestamps µs; lossy warns once per column per read; bounded decode memory.

- [x] **Step 1:** Derive the Kind set + per-face behavior tables from old `type_map.rs`/`copy_decode.rs`/`cdc/values.rs`/`cursor.rs`; record in Appendix A §Types.
- [x] **Step 2:** Write fresh, porting old inline test CASES (values/expectations) with new naming; add a proptest: `text::parse ∘ literal::render` round-trips per Kind.
- [x] **Step 3:** Green; commit `feat(postgres-v2): types/ — one rulebook, four faces`.

### Task 5: `types/encode.rs`

**Interfaces:** `pub(crate) struct Encoder` — Arrow → COPY BINARY, borrowed column views, 64 KiB flush.
**Contract:** decoder-as-oracle round-trip test per Kind; numeric string-domain (the u128 lesson).

- [x] Write fresh + round-trip suite; commit `feat(postgres-v2): encode — the decoder is its oracle`.

### Task 6: `source/` (non-CDC) + its suites

**Files:** `src/source/{mod.rs,connector.rs,plan.rs,reflect.rs,sql.rs,copy.rs,errors.rs,fail_points.rs}`, `src/source/config/{mod.rs,vocabulary.rs,validate.rs}`, `src/source/cursor/{mod.rs,watermark.rs,tracker.rs,prepare.rs}`; tests `tests/integration.rs` + `cases/{common.rs,test_config.rs,test_config_schema.rs,test_connection_string.rs,test_reflect.rs,test_native_types.rs,test_incremental.rs,test_query_streams.rs,test_option_edges.rs,test_source_conformance.rs,test_copy_wire_pin.rs,test_tls_matrix.rs}` (ported from old, rewritten to the new API)
**Interfaces:**
```rust
pub struct Postgres { … }                       // source::Postgres — the connector
impl Postgres { pub fn from_yaml/from_json/from_value/new(config: Config) }
pub struct Config { pub connection: String, pub schema: String, pub tables: Option<Vec<TableConfig>>, pub queries: Vec<QueryConfig>, pub tls: Option<tls::Policy>, pub cdc: Option<CdcConfig>, … }
// serde: field spellings FROZEN to the old YAML vocabulary (conn, schema, tables, queries, tls, cdc, …)
// NOTE: Rust field `connection` ↔ YAML `conn` via #[serde(rename)] — the document is frozen, the identifier is not.
pub enum ConfigError { … }   pub fn config_schema() -> serde_json::Value;
pub(crate) plan::streams(...) -> Vec<plan::Stream>   // the ONE validation gate
```
**Contract:** Appendix A §Source. Always-structured streams; validation facts once with one error text each; cursorless never checkpoints; ChannelClosed = done; the boundary matrix; watermark-never-lowered; tracker dedup rules; query streams wrapped one way for describe AND read.

- [x] **Step 1:** Contract completion from old source files → Appendix A §Source.
- [x] **Step 2:** config/ + reflect + sql + plan (fresh; port test cases).
- [x] **Step 3:** cursor/ + copy + connector; wire `Source` impl.
- [x] **Step 4:** Port the listed suites (new API spellings, same assertions); container suites green.
- [x] **Step 5:** Commit `feat(postgres-v2): the source — every fact validated once`.

### Task 7: `destination/` + its suites

**Files:** `src/destination/*` per the tree; tests `cases/{test_dest_conformance.rs→test_destination_conformance.rs,test_dest_recovery.rs→test_destination_recovery.rs,test_golden_sql.rs,test_golden_ensure_sql.rs,test_golden_unit_sql.rs,test_merge_strategies.rs,test_merge_refinements.rs,test_scd2.rs,test_unit_isolation.rs,test_direct_publish.rs,test_differential.rs,test_destination_config.rs (new)}`
**Interfaces:** `destination::Postgres` builder (`new(connection_string)`, `.schema(…)` — YAML keeps `dataset` spelling via serde rename, `.tls(…)`, `.options(…)`), `destination::Config` with `from_yaml/from_json/from_value` + `config_schema()` (freezes the facade field set `{conn, dataset, tls, merge_strategy, tables}`); `Catalog`/`Unit`/`executor` per the tree; sqlcore vocabulary re-exported under its bare names (family rule).
**Contract:** Appendix A §Destination. Golden SQL BYTE-IDENTICAL to the old pins (copy the old goldens' expected strings into the ported pin tests unchanged — they are contract, not code).

- [x] **Step 1:** Contract completion (every literal SQL string, the capabilities values, the reclamation scoping, replay rules) → Appendix A §Destination.
- [x] **Step 2:** catalog + unit + executor + dialect + errors (fresh, unit-tested).
- [x] **Step 3:** load.rs + connector.rs + config.rs; wire `Destination` impl.
- [x] **Step 4:** Port suites; golden pins byte-identical or STOP and fix the code (never the pin).
- [x] **Step 5:** Commit `feat(postgres-v2): the destination — catalog, unit, executor`.

### Task 8: `cdc/` + its suites

**Files:** `src/source/cdc/{mod.rs,runtime.rs,slot.rs,read.rs,tail.rs,ack.rs,apply.rs,pgoutput.rs}`; tests `cases/{cdc_rig.rs,test_cdc_cycle.rs,test_cdc_slot.rs,test_cdc_identity.rs,test_cdc_recovery.rs}`
**Interfaces:** typestate runtime (`Control` established with `Profile::CdcControl`; `Snapshot` holds the repeatable-read transaction; no `Option<Arc<_>> + expect()`); `ack.rs` owns run-completion ack + lag; `slot::ensure` decomposed into named checks; pgoutput parser self-contained with its fuzz entry.
**Contract:** Appendix A §CDC — every 009-review invariant listed there.

- [x] **Step 1:** Contract completion from the eight old files → §CDC.
- [x] **Step 2:** pgoutput + apply (+ types/text consumers) fresh; unit tests.
- [x] **Step 3:** slot + runtime + read + tail + ack; wire into `source::Postgres`.
- [x] **Step 4:** Port the four CDC suites + rig; green with containers.
- [x] **Step 5:** Commit `feat(postgres-v2): cdc — the runtime is a typestate`.

### Task 9: crash sweeps, memory bound, testsupport, fixtures, benches

**Files:** `src/testsupport/*`, `src/fixtures.rs`, `tests/{source_crash_sweep.rs,destination_crash_sweep.rs,cdc_crash_sweep.rs,memory_bound.rs}`, `benches/iai.rs`
**Interfaces:** fail-point registries (same crash-point ID strings as the old crate — they name real crash sites; the sweeps port over); `fixtures::{PostgresContainer, CdcContainer}` (one shared `start` core — the old verbatim duplication does not return); testsupport carries the pin/bench/fuzz seams (one convention).
**Contract:** every registry passes `rdlt_testkit::crash::assert_registry_matches_sources` against THIS crate's tree; sweeps prove exactly-once per point × action; memory_bound reproduces the bounded-snapshot guarantee.

- [x] Write + port; sweeps green under `--features failpoints`; commit `feat(postgres-v2): the nets — sweeps, memory bound, one test seam`.

### Task 10: gate wiring + naming audit + docs

- [x] **Step 1:** Wire the new binaries into the gate per 024's discipline: Makefile sweep/test lines + the by-name enumeration (find the mechanism: `grep -rn "crash_sweep\|memory_bound" Makefile crates/rdlt-testkit/`); every new binary invoked or exempted BY NAME.
- [x] **Step 2:** Naming audit against the rules: `grep -rnE "\b(conn|stmt|tbl|cfg|ctx|buf)\b" crates/rdlt-connector-postgres-v2/src/` → zero hits (serde renames excepted); ledger (Appendix C) complete; every public item documented.
- [x] **Step 3:** README (self-contained, old crate's coverage) + lib.rs front page with a RUNNING doctest (yaml → validated source; contradiction refused at parse time).
- [x] **Step 4:** `cargo clippy -p rdlt-connector-postgres-v2 --all-targets --all-features -- -D warnings`; fmt; `RUSTDOCFLAGS="-D warnings" cargo doc -p rdlt-connector-postgres-v2 --no-deps --all-features`; `cd fuzz && cargo check` if fuzz targets were added.
- [x] **Step 5:** Commit `docs(postgres-v2): the front page + the gate knows every binary`.

### Task 11: parity measurement + the gate, twice

- [x] **Step 1:** Hot-path parity: run `benches/iai.rs` (new) vs old `iai_pg` equivalents; instruction counts within noise of the old crate's, or fix before proceeding.
- [x] **Step 2:** `env -u RUSTUP_TOOLCHAIN cargo test --doc -p rdlt-connector-postgres-v2`.
- [x] **Step 3:** Full gate from repo root, untouched while running: `env -u RUSTUP_TOOLCHAIN make check`; wait on the log's completion marker.
- [x] **Step 4:** Second untouched run; both clean (old crate's 964 + new crate's suites, sweeps, semver, benches, cold start) → done.
- [x] **Step 5:** Update memory; report. Merge + facade swap remain the owner's calls.

## Appendix A — Behavioral contracts

Seeded from the survey reports; each build task's Step 1 completes its section
from the old source BEFORE fresh code is written.

**§Session** — parse gate: TLS trio (`sslrootcert`/`sslcert`/`sslkey`) +
`sslmode=verify-ca|verify-full` extracted from BOTH libpq spellings (URL query
+ key=value with quote-aware scan); strict percent-escapes (malformed = typed
error naming param and value); driver rejection names the offending key when
it is outside tokio-postgres 0.7's accepted set (with per-param hints:
sslpassword/gssencmode/requiressl/sslcrl/service); `sslrootcert=system` = the
platform store (conflicts with an explicit root_cert); trio merge:
conn-value + absent block field fills, agreeing duplicates pass, disagreement
typed; verify-* from the conn string may be kept or STRENGTHENED by the block,
never weakened (strength order disable<prefer<require<verify_ca<verify_full);
credentials both-or-neither, never with plaintext, checked across sources;
`application_name=rdlt` unless set. Resolution without a block: driver sslmode
→ mode (Disable/Require map, else Prefer). Contradictions: explicit disable vs
demanding block; explicit require vs disable block; prefer composes with
everything (incl. a block that only sets root_cert). Connect: verifying modes
force driver ssl_mode Require (never plaintext fallback); Disable forces
driver Disable; ONE generic connect path for plaintext + rustls; driver task
detached, its terminal error logged (`tracing::warn`) — it names WHY later
statements fail. Classification: rustls errors reached through io::Error
get_ref (source() skips it); UnknownIssuer→TrustAnchor,
NotValidForName*→Hostname, other cert errors→Chain; alerts
CertificateRequired/BadCertificate/UnknownCA/CertificateUnknown/AccessDenied/
HandshakeFailure→ClientCert; pinned needle "server does not support TLS"→
ServerRefusedTls (pin test); 28000 + "certificate" in message→ClientCert;
else Other with connect-polarity transience. TLS-verification failures never
transient. detail(): db errors render message + SQLSTATE + COPY `where_`
context; non-db render the whole source chain. GUC profiles: CdcControl =
`SET datestyle = 'ISO'; SET bytea_output = 'hex'`.

**§Types** — Kind set (v2 names; old `Decode` in parens): Bool, Int16(Int2),
Int32(Int4), Int64(Int8), Float32(Float4), Float64(Float8),
Decimal{precision,scale}, Text(Utf8), Jsonb(JsonbText), Uuid(UuidText),
Bytea, TimestampTz/TimestampNaive(Timestamp{tz}), Date, Time. Arrow: ints →
Int64, floats → Float64, Decimal → Decimal128(p,s), Text/Jsonb/Uuid → Utf8,
Bytea → Binary, timestamps → Timestamp(µs, Some("UTC")/None), Date → Date32,
Time → Time64(µs). Mapping (old `map_type`): policy shapes FIRST —
typcategory 'A' or typtype c/r/m → CastJsonbText [lossy]; typtype 'e' →
CastText [lossy]; then by OID (16 bool, 21/23/20 ints cursor-capable, 700/701
floats, 1700 numeric → Decimal iff 1≤p≤38 ∧ 0≤s≤p else CastText [lossy],
25/1043/1042/19 text cursor-capable, 17 bytea, 1184/1114 timestamps
cursor-capable, 1082 date, 1083 time, 2950 uuid cursor-capable, 114 json,
3802 jsonb); fallback CastText [lossy]. Cursor-capable: ints, constrained
numeric, text family, uuid, timestamps, date, time — NOT bool/floats/bytea/
json(b). numeric typmod: packed = typmod−4, precision = (packed>>16)&0xFFFF,
scale = ((packed&0x7FF)^1024)−1024 (PG15 negative scales). Hint table
(CLOSED; `apply_hint`): utf8 = universal (keeps CastJsonbText shape, else
CastText, cursor-capable, lossy); int64←text; float64←text/int/numeric
(numeric lossy); decimal(p,s)←text/int/float/numeric (float lossy; 1≤p≤38,
s≤p); bool←text/int; timestamp_tz←text/timestamp/date (timestamp lossy);
timestamp_naive←text/timestamptz/date (timestamptz lossy); date←text/
timestamps (timestamps lossy); time←text; uuid←text; json←text (casts
jsonb); binary←text (casts bytea); hints apply only to plain base scalars
(typtype 'b', category ≠ 'A'). Binary wire (frozen): 19-byte header
`PGCOPY\n\xff\r\n\0` + flags(4) + extension len(4); per tuple i16 field
count (−1 = trailer) then per field i32 length (−1 = NULL) + bytes; errors:
bad signature, drift (field count ≠ plan), NULL on NOT NULL, data after
trailer, truncated stream; per-kind: bool 1 byte; ints/floats fixed-width
BE; numeric NBASE-10000 {ndigits,weight,sign(0x0000/0x4000; 0xC000 NaN and
0xD000/0xF000 ±Inf = typed errors),dscale,digits<10000} → i128 rescaled to
declared scale exactly (excess wire fraction = drift error, checked
arithmetic throughout); jsonb version byte must be 1, stripped; uuid 16
bytes → canonical lowercase hyphenated; timestamps/dates rebase from PG
epoch (µs 946_684_800_000_000 / days 10_957) with ±infinity (i64/i32
MAX/MIN) saturating, never NULL; per-row ranges Vec deliberately LOCAL
(hoisting measured +2.9%). Text forms (frozen): bool t/f; float via Rust
parse (accepts NaN/±Infinity spellings); decimal plain literal, excess
fraction refused, NaN refused; bytea `\x` hex; timestamptz
`%Y-%m-%d %H:%M:%S%.f%#z` (+00/+05:30/+0530), naive without zone, both
saturate ±infinity; date `%Y-%m-%d` (±infinity saturates); time
`%H:%M:%S%.f`. Config-literal (lenient) timestamp parse: RFC3339 first,
then space/T naive forms; time accepts bare `%H:%M`; uuid lowercased on
intake. Scalar SQL literals (injection-safe, typed): `{v}::int8`,
`'{decimal_text}'::numeric`, `'{escaped}'::text`, `'{escaped}'::uuid`,
`(TIMESTAMPTZ 'epoch' + {µs}::int8 * INTERVAL '1 microsecond')`, naive/date/
time same epoch-anchored integer arithmetic — never float round-trips;
decimal_text pads to scale with sign preserved. `rdlt::lossy` warn once per
column per read; encoder (Task 5) borrowed views + 64 KiB flush; decoder is
the encoder's oracle.

**§Source** — streams always `.with_structured()`; validation facts, once
each: cursor column selected + cursor-capable post-hints; lag requires
inclusive boundary + defined sql_delta + a primary key; CDC replica-identity
keys survive column selection; query-name collisions with reflected tables.
Reflection once per run (OnceCell), query streams described through the same
cache; `SELECT * FROM (sql) AS q` is the ONE wrapping for describe AND read.
Cursorless (snapshot) streams never checkpoint; intermediate checkpoints after
each pushed batch on cursored streams; ChannelClosed = cancellation = Ok.
Boundary matrix (inclusive/exclusive) and watermark-never-lowered exactly as
old `sql.rs`/`cursor.rs` record them; tracker dedup: values beat NULL, ties
keep arrival last-wins (survivor drives every merge strategy); conn parse
failure Fatal, network connect Transient. pg_inherits discovery filter with
explicit-listing override.

**§Destination** — capabilities: merge true, structs false, scalar_lists
false, json_type true, decimal true, ident max_len 63. open(): create schema
if missing → `SET search_path` → create `_rdlt_state`/`_rdlt_commits`/
`_rdlt_cleared` (sqlcore names) → TRUNCATE stage tables matching THIS
pipeline's hash prefix only. Unit: `BEGIN ISOLATION LEVEL READ COMMITTED`,
`SET LOCAL work_mem = '64MB'`, literal COMMIT/ROLLBACK (borrow reason);
rollback on EVERY mutating-path error (25P02 poisoning). Writes: stage for
merge tables, target directly otherwise; Replace clear via sqlcore
`prepare_target` at most once per (load, target) with the durable
`_rdlt_cleared` record in the same transaction. Commit: load_id equality
check; lazy unit open; receipt probe; staged-emptiness probes
(`staged_probe_targets`); `plan_commit` script executed step-by-step;
`ClearTarget`/`InsertSelect` unreachable → fatal "internal:"; replay =
rollback + apply `script.marks` + return prior receipt (sqlcore
`replay_disposition(DirectToTarget) == DiscardUnit`); 23505 on merge-identity
index → sqlcore `duplicate_merge_key_diagnosis`; ensure-before-validate phase
order FROZEN. DDL: stage tables UNLOGGED with `__rdlt_arrival bigint
GENERATED BY DEFAULT AS IDENTITY (CACHE 32)`; widens spell `USING x::type`;
NOT NULL on target only. Dialect: exactly three overrides (arrival_order,
clear_table=TRUNCATE, materialize_dedup=CREATE TEMP TABLE … ON COMMIT DROP);
defaults ARE this destination's text (golden-pinned). Statement
classification: 22/23/42 permanent, else transient.

**§CDC** — slot-first snapshot; ONE repeatable-read view for all CDC tables;
pre-existing slot: snapshot cursor = WAL position read BEFORE the RR BEGIN
(visibility horizon; confirmed_flush would wedge recovery on
TRUNCATE/TOAST-without-old-image); ack trails one run behind — floors =
destination-committed `since` values + fresh snapshot points; failed runs ack
nothing; control connection GUC-pinned (Profile::CdcControl), Arc-shared so a
backpressured stream never stalls others, dropped on any error (retries need
fresh clients); preflight: replica identity d→PK (else typed), i→identity
index columns (dropped index = typed, never empty key), f→declared override
or PK (TOAST substitution possible), nothing→typed; declared primary_key
disagreeing with identity columns = typed (except under FULL); flag-column
collision = typed; empty key = defense-in-depth assert. TOAST
substitute-under-FULL / typed without; PK-change = delete+insert; chunked
tail with per-chunk targets + checkpoint-probe cancellation (quiet tails
observe engine cancel); retention warn @256MiB on `rdlt::cdc`; lag_bytes
reporting; CdcCursor = distinct JSON shape `{cdc_lsn}` (misrouted state fails
typed); slot lifecycle errors distinguished: WAL-retention overrun,
concurrent-consumer PID, invalidation, recreated-slot WAL gap; TRUNCATE in
feed = defer-to-commit typed error; publication existence/coverage checks.

## Appendix B — Frozen surfaces (engine/operator-visible; the parity bar)

1. YAML vocabularies: the source config document (field spellings incl.
   `conn`), `DestinationOptions` (sqlcore, untouched), the facade destination
   field set `{conn, dataset, tls, merge_strategy, tables}`, the `tls:` block
   (mode strings, PEM fields).
2. Persisted data: cursor-state JSON, CdcCursor JSON `{cdc_lsn}`, `_rdlt_*`
   table names + `__rdlt_arrival`, receipt/state semantics.
3. Wire: COPY BINARY both directions; pgoutput v1 accepted grammar; the exact
   SQL text of the golden pins; DDL text.
4. SPI behavior: everything in Appendix A; capabilities values; conformance
   suites pass; crash-point exactly-once guarantees.
5. NOT frozen: every Rust identifier, module path, test name, and doc string
   of the new crate; the old crate (untouched, still shipping the gate).

## Appendix C — Rename ledger (old → v2; extend as coined)

| Old | v2 | Rule |
|---|---|---|
| crate `rdlt-connector-postgres` | `rdlt-connector-postgres-v2` | D1 |
| `source::PostgresSource` | `source::Postgres` | 1 |
| `source::PostgresConfig` | `source::Config` (field `connection`, serde-renamed `conn`) | 1, 2 |
| `source::HintType` | `source::config::TypeHint` | 1 |
| `dest::Postgres` | `destination::Postgres` (`new`, not `connect` — it does not connect) | 2, 3 |
| module `dest` | `destination` | 2 |
| `tls::TlsPolicy` / `TlsMode` / `TlsConfigError` | `tls::Policy` / `tls::Mode` / `tls::ConfigError` | 1 |
| `tls::parse_conn` / `ParsedConn` | `session::parse` / `session::Parsed` | 1, 2 |
| `tls::connect` + `ConnectResult` | `session::establish` + `session::EstablishError` | 3, 7 |
| `driver_error::detail` | `session::classify::detail` | seam move |
| `FieldPlan` (in copy_decode) | `types::Column` | 1, seam move |
| `MappedType`/`Decode` | `types::Kind` | 5 |
| `CopyDecoder` | `types::binary::Decoder` | 1 |
| `copy_pump::pump_copy` | `source::copy::stream` (verb) | 3 |
| `cursor::Tracker::new(8 args)` | builder/spec struct | 4 |
| `PgSession` (dest) | `destination::load::Load` (the LoadSession impl) | 1 |
| `TableIdentity.full` | `covers_all_columns` | 6 |
| `PgFixture` / `CdcPgFixture` | `fixtures::PostgresContainer` / `fixtures::CdcContainer` | 1, 2 |
| `testhook` (×2) + `dest/sqlgen` shim + tls doc-hidden block | `testsupport::{source,destination,session}` | one seam |
| test binaries `crash_sweep`/`dest_crash_sweep` | `source_crash_sweep`/`destination_crash_sweep` | 2 |
