# Research — Feature 032: rdlt-connector-oracle

Branch: `032-oracle` · Date: 2026-08-02
Sections are appended in order; R1 below, R2 by a separate research pass.

## R1 — Driver survey

Registry and source facts gathered 2026-08-02 (crates.io API with UA, docs.rs,
GitHub API, repo sources). Constitution requirement: dependencies resolvable at
plan time with registry facts — every claim below carries its source.

### Candidates at a glance

| | `oracle-rs` 0.1.7 | `oracle` 0.6.3 (kubo/rust-oracle) | `sibyl` 0.7.1 |
|---|---|---|---|
| Implementation | Pure Rust TNS wire protocol | ODPI-C binding (C) | OCI binding (C) |
| Runtime system dep | **None** | Oracle Client/Instant Client ≥ 11.2 | Oracle OCI client |
| Async | Native tokio | Sync only | Both (tokio/actix/async-std) |
| License | MIT OR Apache-2.0 | UPL-1.0 OR Apache-2.0 | MIT |
| First release / latest | 2025-12-15 / 2026-03-24 | 2017-10-31 / 2025-01-02 | 2019-06-10 / 2026-06-26 |
| Downloads (all-time) | 8,265 | 2,344,673 | 56,837 |
| MSRV | 1.70 | 1.60 (README says 1.68) | unstated |
| Structured ORA codes | Yes — `Error::OracleError { code: u32, .. }` | Yes — `DbError` carries code | not confirmed |
| Arrow integration | None | None | None |

Sources: crates.io `/api/v1/crates/{oracle-rs,oracle,sibyl}` (versions,
licenses, MSRV, dates, downloads); repos linked per row below.

### 1. `oracle-rs` 0.1.7 — pure Rust, the owner's suggestion

- **Crate**: <https://crates.io/crates/oracle-rs> · docs:
  <https://docs.rs/oracle-rs/0.1.7> · repo: <https://github.com/stiang/oracle-rs>
  (24 stars, 16 forks, created 2025-12-15). License MIT OR Apache-2.0
  (crates.io registry; repo LICENSE reads Apache-2.0). MSRV 1.70 (registry
  `rust_version`) — workspace pins 1.96.0, fine.
- **Async**: tokio-native throughout (`tokio ^1` with `net,io-util,time,rt,sync`;
  `#[tokio::main]` in every example). No sync API. Companion pool crate
  `deadpool-oracle` exists (README).
- **Connection API** (docs.rs `connection`/`config` modules):
  `Config::new(host, port, service_name, user, password)` builder;
  `Connection::connect_with_config(config).await`. TLS/TCPS via
  `.with_tls()` (system roots, rustls) and `.with_wallet(path, passphrase)`
  (Oracle wallet); `TlsMode`/`TlsConfig` for advanced cases. DRCP via
  `.with_drcp()`. TLS stack is **rustls 0.23 + tokio-rustls 0.26 +
  webpki-roots ^0.26** — no native-tls, no openssl (registry dep list).
- **Query API** (docs.rs `statement`/`row`/`cursor` modules): prepared
  `Statement`s with named (`:name`) and positional (`:1`) binds; `query()` /
  `execute()`; `BatchBuilder`/`execute_batch()` for array DML; statement cache;
  scrollable cursors (`fetch_first/last/absolute/relative`); REF CURSOR and
  implicit results modules exist. **Fetch model**: `query()` returns a
  `QueryResult { rows: Vec<Row>, has_more_rows, cursor_id, .. }` buffering one
  prefetch batch (default 100 rows); continuation is `fetch_more(cursor_id, ..)`.
  There is NO iterator/Stream façade — incremental fetch is manual, and it is
  where the open defects live (see risks).
- **Type mapping** (README table): NUMBER → `i8..i64`, `f32`/`f64`, or
  `String`; DATE/TIMESTAMP → `chrono::NaiveDateTime`; **TIMESTAMP WITH TIME
  ZONE → `chrono::DateTime<FixedOffset>`**; CLOB/NCLOB → `String` (auto-fetch)
  or streamed via `.get_lob()`/`LobLocator`; BLOB → `Vec<u8>`; RAW →
  `Vec<u8>`; BOOLEAN (23c) → `bool`; JSON (21c) → `serde_json::Value`; VECTOR
  (23ai) → `Vec<f32|f64|i8>`. **NUMBER fidelity**: no `rust_decimal`/
  `bigdecimal` integration (neither is in the dep tree) — full-precision
  NUMBER(38) survives only through the `String` mapping; `f64` is lossy above
  15–17 significant digits. Not yet supported (README roadmap): LONG/LONG RAW,
  XMLType, AQ, OCI arrays, sharding, XA.
- **Errors** (`src/error.rs`): ~30-variant `Error` enum with server errors
  STRUCTURED — `Error::OracleError { code: u32, message: String }` (the
  ORA-NNNNN number as `u32`), plus distinct `ServerError`, protocol variants
  (`UnexpectedPacketType`, `BufferUnderflow`, …), `Io(io::Error)`, connection
  variants. `is_no_data_found()` matches code 1403. This is exactly the shape
  the classification rulebook needs — match on `code`, never on rendered text.
- **Server versions**: README claims "Oracle Database 12c Release 1 (12.1) or
  later". **The claim is falsified by the issue tracker for pre-20c** (see
  risks). The repo's own test fixture is `gvenzl/oracle-free:slim`
  (`tests/oracle/docker-compose.yml`) — i.e. **Oracle 23 Free, the same image
  family we would use, IS the driver's primary tested target**. CI
  (`.github/workflows/rust.yml`) runs `cargo build && cargo test` on
  ubuntu-latest with no database — only unit tests gate; integration tests
  need the compose fixture and are not CI-enforced.
- **Maturity signals (measured, not vibes)**: last push 2026-03-23; last
  release 0.1.7 on 2026-03-24. Since then, **12 open issues/PRs (June–July
  2026) with zero maintainer response**, including community PRs that fix
  serious protocol defects: #8 (prefetch pagination), #10 (19c support), #12
  (cancellation poisoning), #14 (END_OF_RESPONSE / multi-packet reads), #16
  (large-SDU framing), #17 (pre-21c error-info over-read). Maintenance is
  stalled at ~4 months as of this survey.
- **Correctness defects on record** (issue tracker, load-bearing for a source):
  - **#8 — silent truncation**: a SELECT larger than the prefetch (100 rows)
    returns the first batch with `has_more_rows = false`; callers treat a
    truncated result as complete, and calling `fetch_more` anyway desyncs the
    connection (`early eof`). Reproduced on XE 21c against the 0.1.x release
    line; the fix is an **unmerged PR**. This is silent data loss on the
    connector's core workload and MUST be probe-verified against Oracle 23
    Free at T001 before any design freezes.
  - **#9/#10/#13/#17 — pre-23c broken**: every query against 12.2 fails with
    buffer underflow (#13: `parse_error_info_with_rowcount` unconditionally
    skips two 20c-only fields); on 19c `fetch_more()` draws a break MARKER and
    a fatal protocol abort (#9). The "12.1+" README claim does not hold in
    0.1.7; realistically only 21c+ (possibly only 23c) works.
  - **#11/#12 — not cancellation-safe**: dropping a `query()` future mid-round-
    trip (tokio timeout/`select!`) leaves the stream mid-frame; the next op on
    that connection hangs. Directly relevant to the engine's ControlFlow
    cancellation — the connector must never share/reuse a connection across a
    cancelled call (drop the connection, not just the future).

### 2. `oracle` 0.6.3 (kubo/rust-oracle) — the ODPI-C incumbent

- **Crate**: <https://crates.io/crates/oracle> · repo:
  <https://github.com/kubo/rust-oracle>. UPL-1.0 OR Apache-2.0. 2.3M
  downloads, maintained since 2017; 0.6.3 released 2025-01-02. MSRV 1.60
  (registry). Mature type support (chrono behind a feature), iterator-based
  `query()`/`query_as()` row fetching, structured `DbError` (ORA code
  available), NUMBER precision via string/decimal paths — functionally the
  most complete and battle-tested option.
- **The disqualifier — runtime C dependency**: ODPI-C is vendored/compiled (C
  compiler at build), and **Oracle Client libraries ≥ 11.2 (e.g. Instant
  Client) must be installed on every machine that runs the binary** (README:
  ODPI-C installation guide). For this workspace that means: the `dev` toolbox,
  every CI runner, every user of the embeddable `rdlt` library and the dist
  binary would need Oracle's proprietary Instant Client (unredistributable-by-
  default licensing, ~100MB+, LD_LIBRARY_PATH management). rdlt's vision is a
  SMALL embeddable engine; bundled duckdb is the recorded sole heavy-build
  exception, and it is self-contained — an external proprietary .so at
  RUNTIME is a different and worse class of dependency. Sync-only is a
  secondary mismatch (the sdk feeds are async; blocking calls would need
  spawn_blocking plumbing).

### 3. `sibyl` 0.7.1 — one row

- <https://crates.io/crates/sibyl> · <https://github.com/quietboil/sibyl>.
  MIT; blocking AND nonblocking (tokio/actix/async-std); actively maintained
  (0.7.1 on 2026-06-26; 43 stars). But it is **OCI-based** — same Oracle
  client runtime requirement as kubo's crate, same disqualifier. Its async is
  a wrapper over OCI's nonblocking mode, not a wire-protocol implementation.
  No advantage over `oracle` that outweighs the shared runtime dep; not
  pursued further.

No other viable candidates: crates.io has no second pure-Rust TNS
implementation with releases (searches for oracle TNS/wire drivers surface
only the three above plus abandoned stubs).

### Workspace compatibility — what `oracle-rs = "0.1"` adds

`cargo add oracle-rs --dry-run -p rdlt-connector-sdk` resolves cleanly against
crates.io ("Adding oracle-rs v0.1.7"). Dep tree (registry list for 0.1.7)
against the current Cargo.lock:

- **Already in the lock at compatible versions** (no new majors): tokio 1.53,
  rustls 0.23.42 (`std,tls12` features), tokio-rustls 0.26.4, chrono 0.4.45,
  indexmap 2.14, bytes, serde/serde_json, async-trait, hex, tracing, sha2
  0.10, hmac 0.12, md-5 0.10, aes 0.8, cbc 0.1, pbkdf2 0.12, rand 0.8.
- **New crates**: hostname 0.4, rustls-pemfile 2, rustls-pki-types 1 (tiny,
  pure Rust), pkcs8 0.10 (`encryption,pem`).
- **Duplicate-major additions**: webpki-roots 0.26 beside the lock's 1.0.9;
  thiserror 1 beside the lock's 2.0.19. Both coexist silently; cosmetic lock
  weight only.
- **No C dependencies anywhere in the tree** — crypto is RustCrypto
  (aes/cbc/hmac/pbkdf2/sha*), TLS is rustls. No native-tls/openssl. No
  `unsafe`-requiring FFI, consistent with the workspace's deny(unsafe_code).

### Recommendation

**Take `oracle-rs` 0.1.7, eyes open.** It is the only candidate compatible
with this workspace's constraints: pure Rust (no system libs, no proprietary
runtime — the ODPI-C/OCI route breaks the embeddable-engine story and every
toolbox/CI/dist environment), tokio-native (matches the sdk), rustls-only TLS
on crates already in the lock, and a structured `OracleError { code: u32 }`
that feeds the classification rulebook without string matching. Its own test
fixture is `gvenzl/oracle-free` — Oracle 23 Free, our intended container
image, is the driver's best-supported target.

But 0.1.x means 0.1.x, and the plan must carry the risks as recorded items:

1. **Silent result-set truncation past the prefetch (#8) — the top risk.**
   Reproduced on 21c in the released line, fix unmerged. A source connector's
   first live probe (T001-style, against `gvenzl/oracle-free`) must be a
   >100-row SELECT with a rowcount assertion; if 0.1.7 truncates on 23c too,
   the options are (a) a rev-pinned fork carrying #8/#12/#14 (023 set the
   fork-by-rev precedent AND recorded it as a constitution violation with
   exits — same treatment here), or (b) connector-side pagination via
   ORDER BY + FETCH NEXT keyset windows, sidestepping `fetch_more` entirely.
   Decide on probe evidence, not hope.
2. **Stalled maintenance.** Zero maintainer activity since 2026-03-24 with 12
   community issues/PRs pending (June–July 2026). If adoption proceeds, assume
   we own our pinned rev; upstreaming is upside, not a plan dependency.
3. **Not cancellation-safe (#11).** A dropped in-flight future desyncs the
   connection. The connector must treat any cancelled/timed-out call as fatal
   to the connection object (drop and reconnect, never reuse), and the crash/
   cancellation sweep must pin that discipline.
4. (Secondary) **Server-version envelope is 21c/23c in practice**, not the
   claimed 12.1+ — pre-20c fails on every query (#13), 19c cannot fetch
   incrementally (#9). Document supported servers honestly as 21c+ verified on
   23 Free only. **NUMBER fidelity**: full-precision NUMBER must ride the
   `String` mapping (no decimal-crate integration) and be converted to Arrow
   decimal on our side; `f64` is only safe ≤ 15 significant digits.

**Oracle 23 Free confirmed supported**: yes, with the strongest evidence
available short of our own probe — it is the driver's own integration-test
image (`gvenzl/oracle-free:slim`, `tests/oracle/docker-compose.yml`), and the
23c-era protocol paths are the ones the 0.1.x line was written against. Our
own live probe remains mandatory before design freeze (risk 1).

## R2 — Container fixture facts

### The image: gvenzl/oracle-free (Oracle Database Free 23ai)

The community-standard image, the Rust testcontainers ecosystem's own
choice, and (per R1) the driver's own integration-test image. Facts
verified against the repo (github.com/gvenzl/oci-oracle-free) and
Docker Hub on 2026-08-02:

- **Tag scheme** `[version][-slim][-faststart]` — e.g. `23`, `23-slim`,
  `23-faststart`, `23-slim-faststart`, and exact versions `23.26.2`,
  `23.26.1` (23.26.0 deprecated; older unsupported). Also `latest`,
  `slim`, `full` floating tags.
- **Flavors**: *full* = everything; *regular* = full minus Java/JVM,
  Workspace Manager, Multimedia, XDK, OPatch, JDBC/UCP, cluster bits;
  *slim* = regular minus Oracle Text, Spatial/Locator, Multilingual
  Engine, R, RMAN, ASM libs, OLAP (ImageDetails.md). Nothing slim
  removes matters to a plain-SQL source connector.
- **Sizes** (Docker Hub, compressed, amd64): `23.26.2` 1.19 GB,
  `23.26.2-slim` 0.85 GB, `23.26.2-faststart` 1.66 GB,
  `23.26.2-slim-faststart` 1.33 GB. All in the band the gate already
  pays for RUSTFS/Polaris/postgres pulls.
- **faststart** = the datafiles ship pre-expanded inside the image
  (bigger pull, no first-boot decompression). In testcontainers every
  run IS a first boot, so faststart is the right trade for the gate:
  non-faststart first start is ~1–3 min (decompression + open);
  slim-faststart typically comes ready in ~15–40 s on this class of
  machine, with slower machines known to exceed 60 s (the upstream
  testcontainers-rs module test sets a 75 s startup timeout for
  exactly this reason).
- **Env vars**: `ORACLE_PASSWORD` (SYS/SYSTEM; mandatory unless
  `ORACLE_RANDOM_PASSWORD`), `APP_USER` + `APP_USER_PASSWORD` (creates
  an ordinary schema user in the PDB — what the fixture should connect
  as), `ORACLE_DATABASE` (extra PDBs; not needed). `_FILE` variants
  exist for secrets.
- **Readiness**: the container logs `DATABASE IS READY TO USE!` (with
  the exclamation mark) once the DB is open; the image also ships a
  `healthcheck.sh` for HEALTHCHECK-based waits. Listener port **1521**;
  default PDB service name **FREEPDB1** — connect string
  `//host:port/FREEPDB1`.
- **License**: the gvenzl build scripts are **Apache-2.0** (not MIT);
  the database inside is Oracle Database Free under the **Oracle Free
  Use Terms and Conditions** — free for dev/test/prod with built-in
  caps (2 CPU threads, 2 GB RAM, 12 GB user data), no license key.
  Fine for a test fixture.
- **Alternative** (one line): `container-registry.oracle.com/database/free`
  is Oracle's official image — larger, no slim/faststart variants, and
  some pulls require registry login; no reason to prefer it over gvenzl.

### testcontainers-modules: an `oracle` module EXISTS at our pin — and should not be used

Workspace pins (root `Cargo.toml` lines 120–121 / `Cargo.lock`):
`testcontainers = "0.23"` (0.23.3 locked), `testcontainers-modules =
{ version = "0.11", features = ["postgres"] }` (0.11.6 locked).
0.11.6 has an **`oracle`** feature flag; module path
`testcontainers_modules::oracle::free`. Its API (read from source,
testcontainers-rs-modules-community `src/oracle/free.rs`):

- `pub struct Oracle` (`Default`, no knobs), `impl Image`:
  image `gvenzl/oracle-free`, tag **`23-slim-faststart`** (hardcoded,
  FLOATING), wait `WaitFor::message_on_stdout("DATABASE IS READY TO USE!")`,
  exposes `ContainerPort::Tcp(1521)` (`FREE_PORT` const), env
  `ORACLE_PASSWORD=testsys`, `APP_USER=test`, `APP_USER_PASSWORD=test`.
  User/password/tag are fixed; only generic `ImageExt` overrides exist.

**Recommendation: use the `GenericImage` pattern (the RUSTFS precedent,
`crates/rdlt-connector-file/tests/cases/s3.rs`), not the module.**
Three reasons: (1) the module's tag is floating `23-slim-faststart`,
against the house pin rule (s3.rs pins `1.0.0-beta.11` with the "a pin,
not a preference" comment; 029 pinned Polaris by digest); (2) the house
fixture needs `rdlt_testkit::gate::runtime_available()` skip-not-fail,
the `RECLAIM_LABEL`, and a post-log real-query readiness probe — none
of which the module gives; (3) it saves ~10 lines. Proposed shape:

```text
GenericImage::new("docker.io/gvenzl/oracle-free", "23.26.2-slim-faststart")
    .with_exposed_port(1521.tcp())
    .with_wait_for(WaitFor::message_on_stdout("DATABASE IS READY TO USE!"))
    .with_env_var("ORACLE_PASSWORD", ...)
    .with_env_var("APP_USER", ...)
    .with_env_var("APP_USER_PASSWORD", ...)
    .with_label(rdlt_testkit::gate::RECLAIM_LABEL, "1")
    .with_startup_timeout(Duration::from_secs(120))  // log line can pass 60 s on slow machines
```

then, RUSTFS-style ("ready means a signed request comes back
answered"), poll a real `SELECT 1 FROM DUAL` as APP_USER against
`//127.0.0.1:{port}/FREEPDB1` before handing the fixture out — the log
line says the CDB opened, not that the listener has registered
FREEPDB1; a connect in the gap yields ORA-12514, which is a transient
readiness signal here. Pin `23.26.2-slim-faststart` as an exact-tag
const; the multi-arch manifest digest, should the house want
digest-pinning like Polaris, is
`sha256:0489e0c1f20b2ca632075653c66f284234689ccff62c9a39809d9a5b3e7c1642`
(Docker Hub, 2026-08-02).

### Gate posture

- **Pull cost**: 1.33 GB compressed — same order as the existing
  RUSTFS/postgres/Polaris pulls; one-time per machine, cached after.
- **Startup latency**: the slowest fixture in the gate by a wide margin
  (15–40 s typical, budget 120 s). The house posture already covers it:
  container cells are **skip-not-fail** (`runtime_available()` → `None`
  + a visible `SKIP:` line), the gate is container-optional, and the
  boot should be paid ONCE — keep the Oracle live cells in one
  integration binary sharing one container, not one container per test.
- **Arch**: gvenzl publishes multi-arch (amd64 + arm64) manifests for
  the 23ai Free tags — this machine is x86_64 Linux, no issue; arm64 is
  also covered should the fixture ever run there (unlike the old
  amd64-only XE images).
- Rootless podman: plain mapped port like every other house fixture;
  nothing Oracle-specific. Free's built-in 2 GB RAM cap keeps the
  container well inside the machine.

## R3 — Oracle source semantics

Facts against Oracle Database 23 documentation (SQL Language Reference,
Concepts, Error Messages); LogicalType vocabulary from
`crates/rdlt-core/src/types.rs` (`Bool, Int64, Float64,
Decimal{precision≤38, scale}, Utf8, Binary, TimestampTz,
TimestampNaive, Date, Time, Uuid, Json`; `DECIMAL_MAX_PRECISION = 38`).

### 1. Type system → LogicalType

| Oracle type | Facts | Mapping | Disposition |
|---|---|---|---|
| `NUMBER(p,s)`, 0 ≤ s ≤ p | precision 1–38, scale −84..127 | `Decimal{p,s}` (Oracle's max p = our max 38, exact fit) | lossless; ride the driver's `String` mapping (R1: no decimal-crate integration) and parse our side |
| `NUMBER(p,s)`, s < 0 or s > p | negative scale rounds to tens/hundreds; `Decimal{u8,u8}` cannot carry s<0 | rescale into `Decimal` where representable, else `Utf8` | **decision needed** — documented-lossy text fallback is the house pattern (pg `text_policy`) |
| bare `NUMBER` / `NUMBER(*)` / `FLOAT(b)` | floating scale, up to 38 significant digits; ints and 1e-40 legal in one column | no fixed `Decimal` exists | **decision needed**: `Utf8` canonical-render (lossless, cursor-incapable) vs a fixed `Decimal{38,s}` (lossy). The pg precedent (`documented_lossy`, visible on `rdlt::lossy`) favors Utf8; `f64` is only safe ≤ 15 significant digits |
| `NUMBER(p)` integer, p ≤ 18 | fits i64 | `Int64` | lossless |
| `BINARY_FLOAT` / `BINARY_DOUBLE` | IEEE 754 single/double; support NaN, ±Inf | `Float64` (BINARY_FLOAT widens) | value-lossless; NaN/±Inf must survive through Arrow |
| `VARCHAR2` / `NVARCHAR2` / `CHAR` / `NCHAR` | VARCHAR2 ≤ 4000 B (32767 under `MAX_STRING_SIZE=EXTENDED`); CHAR blank-padded | `Utf8` | lossless (CHAR padding preserved, never trimmed silently) |
| `DATE` | **carries time**: century..second, 7 bytes, NO fractional seconds, no TZ | **`TimestampNaive`, NOT `Date`** | the classic trap; a `Date` mapping silently drops time-of-day |
| `TIMESTAMP(f)` | fractional 0–9 digits (default 6), no TZ | `TimestampNaive` | f in 7..9 → truncation to the SPI precision = documented-lossy warning |
| `TIMESTAMP WITH TIME ZONE` | preserves per-value offset/region | `TimestampTz` | the instant survives; the original offset/region identity does not — documented-lossy note |
| `TIMESTAMP WITH LOCAL TIME ZONE` | stored normalized to DB TZ, returned in SESSION TZ | `TimestampTz`, session pinned `ALTER SESSION SET TIME_ZONE='UTC'` at connect | without the pin the value depends on client env — pin it |
| `CLOB` / `NCLOB` | LOB locator, up to TBs | `Utf8` | size-unbounded; see §5 for the fetch-cost trap |
| `BLOB` / `RAW(n)` | bytes (RAW ≤ 2000/32767 B) | `Binary` | lossless |
| `LONG` / `LONG RAW` | deprecated since 8i, one per table, awkward wire fetch; R1: NOT supported by oracle-rs | **refusal** (typed, plan-time) | forced by the driver; document it |
| `BOOLEAN` | native SQL boolean **new in 23ai** | `Bool` | driver maps it (R1); older servers never present it |
| `JSON` | native binary (OSON) type since 21c | `Json` (driver yields `serde_json::Value` per R1, or `JSON_SERIALIZE` server-side) | lossless as text |
| `ROWID` / `UROWID` | physical row address; changes on row movement/export/import | `Utf8` if projected | never a persisted key — see §3; discovery may exclude by default |
| `INTERVAL YEAR TO MONTH` / `DAY TO SECOND` | no LogicalType exists | `Utf8` canonical render | documented-lossy text policy |
| `XMLTYPE`, object types, `VARRAY`, nested table, `BFILE`, `ANYDATA`, `VECTOR` | outside the relational lattice (VECTOR the driver maps, the lattice doesn't) | **refusal** (typed, per-column, plan-time) | pg's total-map-with-fallback works for textables; for these, refusal is cleaner than inventing renderings |

### 2. Identifiers

- Unquoted identifiers fold **UPPERCASE** (stored uppercase in the data
  dictionary; matching case-insensitive); quoted identifiers preserve
  case exactly and may contain reserved words. Max 128 bytes (12.2+).
- Exact mirror-image of postgres (folds lowercase), same shape as
  Snowflake — and the house precedent already exists: **022 pinned
  "identifiers quoted-UPPERCASE"** for the Snowflake destination
  (specs/022-snowflake-dest/plan.md). Adopt the same IdentRules story:
  a bare config name is uppercased then always emitted quoted
  (`"EMPLOYEES"`); a name the user quotes is taken verbatim. Never emit
  unquoted SQL — quote-after-fold makes `employees`, `EMPLOYEES`, and a
  genuinely lowercase `"employees"` all unambiguous.
- Discovery reads `ALL_TABLES`/`ALL_TAB_COLUMNS`, which store the
  folded form; a quoted-created `"MyTable"` appears there exactly as
  `MyTable` — round-trips cleanly under quote-always.

### 3. Incremental / cursor options

- **Ordinary cursor columns** — the primary design, the pg rulebook
  transplanted (`crates/rdlt-connector-postgres/src/source/plan.rs`
  validates the cursor column is selected AND `cursor_capable` in the
  type map, `types/map.rs`): monotone watermark, `WHERE col > :last
  ORDER BY col`. Cursor-capable set for Oracle: `Int64`-mapped
  integers, `Decimal`-mapped `NUMBER(p,s)`, `DATE`,
  `TIMESTAMP[/TZ/LTZ]`. Everything below is optional extra, not the
  spine.
- **`ORA_ROWSCN`** — record the trap and keep it out of v1. Verified in
  the 23 SQL Language Reference: (1) with the default
  `NOROWDEPENDENCIES` it is **block-level** — every row in a block
  reports the block's newest SCN, so it moves for rows that did not
  change; row-level fidelity requires the table CREATED with
  `ROWDEPENDENCIES` (+6 bytes/row, cannot be ALTERed on later); (2)
  even then it is a **conservative upper bound** — "a value less than
  [the commit SCN] would never be returned, [but] any value greater
  than or equal to [it] could be" — and it is unsupported in Flashback
  Query, on external tables, and through views. Blind to deletes. If
  ever offered, the plan gate must check
  `ALL_TABLES.DEPENDENCIES = 'ENABLED'` and refuse otherwise.
- **SCN / flashback consistency**: `SELECT ... AS OF SCN n` reads at a
  chosen SCN — the natural way to snapshot many tables at ONE instant
  (read `current_scn` once, query every table AS OF it). Bounded by
  undo retention: past the window, ORA-01555 (snapshot too old) /
  ORA-08181 (invalid SCN) — recoverable only by RE-snapshotting, so
  classification must not retry the same SCN forever. Reading the
  current SCN needs a grant (`V$DATABASE.CURRENT_SCN` or
  `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`) an APP_USER lacks by
  default — a real deployment constraint for the config docs.
- **Repeatable-read equivalent**: Oracle's default is statement-level
  consistency (each statement its own SCN). Transaction-level
  consistency at a single SCN exists two ways: `SET TRANSACTION READ
  ONLY` (no DML, any user, no grant needed) and SERIALIZABLE isolation
  (snapshot + writes, ORA-08177 on conflict). For a multi-statement
  source snapshot `SET TRANSACTION READ ONLY` is the fit — AS-OF-SCN
  semantics without the grant problem — but the snapshot dies with the
  session and long reads can still hit ORA-01555.

### 4. Error classification seed (ORA-NNNNN is structured)

R1 confirmed the driver surfaces the number as
`Error::OracleError { code: u32, .. }` — classify on the code, never on
message text (constitution Principle V; 029's lesson says pin the
EXTRACTION with a live wrong-password cell).

Transient (retry with backoff):
- ORA-12170 TNS connect timeout
- ORA-12541 TNS no listener
- ORA-12514 listener knows no such service — also the normal state
  DURING instance startup (doubly transient for the fixture window)
- ORA-03113 end-of-file on communication channel
- ORA-03114 not connected (connection lost mid-run)
- ORA-01033 initialization or shutdown in progress
- ORA-01034 Oracle not available
- ORA-00018 max sessions / ORA-00020 max processes exceeded
- ORA-04021 timeout waiting to lock object
- ORA-01555 snapshot too old — transient for a fresh statement, but
  PERMANENT for a pinned AS-OF-SCN snapshot: the retry must
  re-snapshot, never re-issue the same SCN

Fatal (config/auth/SQL — never retried):
- ORA-01017 invalid username/password
- ORA-28000 / ORA-28001 account locked / password expired
- ORA-01031 insufficient privileges
- ORA-00942 table or view does not exist
- ORA-00904 invalid identifier (column)
- ORA-00933 / ORA-00936 SQL syntax family (our SQL generation bug —
  loudly fatal)
- ORA-12154 cannot resolve connect identifier (config typo, not the
  network)

### 5. Fetch / streaming

- **Array fetch is the throughput lever.** The driver's model (R1):
  `query()` returns one prefetch batch (default 100 rows) and
  continuation is manual `fetch_more(cursor_id, ..)` — with open defect
  #8 making >100-row results SILENTLY TRUNCATE in 0.1.7. Whatever the
  probe decides (fork-with-fix vs connector-side keyset pagination via
  `ORDER BY key FETCH NEXT :n ROWS ONLY`), the batch size should be
  configurable and default near the SPI batch size (1k–10k), not 100 —
  at 100 rows/round-trip a 1M-row table pays 10,000 round trips.
- **Statement cache**: OCI-family sessions cache prepared statements
  (oracle-rs has one per R1). A source runs a handful of distinct
  statements per stream — defaults suffice; one design sentence, not a
  knob.
- **Result-set consistency while streaming**: an open cursor reads
  under statement-level consistency — the whole result is AS OF the
  statement's start SCN no matter how long the fetch takes (consistent,
  but a long fetch consumes undo → ORA-01555 risk on huge slow reads;
  bigger array fetches shorten wall time and are the mitigation).
- **LOB columns**: fetched as locators by default — one extra round
  trip per LOB per row unless fetched inline/as-string up to a size cap
  (oracle-rs auto-fetches CLOB→String with `.get_lob()` streaming as
  the alternative, R1). A CLOB/BLOB-bearing stream must set the inline
  policy deliberately or throughput craters.

### R2/R3 sources

- github.com/gvenzl/oci-oracle-free (README + ImageDetails.md); Docker
  Hub gvenzl/oracle-free tag API (sizes/digests, 2026-08-02)
- testcontainers-rs-modules-community `src/oracle/free.rs` @ main;
  docs.rs testcontainers-modules 0.11.6 feature list
- Oracle Database 23: SQL Language Reference (ORA_ROWSCN pseudocolumn;
  Data Types), Concepts (Data Concurrency and Consistency), Error
  Messages; Oracle Free Use Terms and Conditions (resource caps)
- House precedents: `crates/rdlt-connector-file/tests/cases/s3.rs`
  (GenericImage fixture, pin comment, readiness probe, RECLAIM_LABEL),
  `crates/rdlt-core/src/types.rs` (LogicalType, DECIMAL_MAX_PRECISION),
  `crates/rdlt-connector-postgres/src/types/map.rs` +
  `src/source/plan.rs` (cursor-capable rulebook, documented-lossy
  policy), `specs/022-snowflake-dest/plan.md` (quoted-UPPERCASE),
  `specs/029-iceberg-v2/plan.md` (digest pinning; code-extraction
  classification lesson)
