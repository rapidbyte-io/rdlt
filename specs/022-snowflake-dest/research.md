# Phase 0 Research: Snowflake Destination Connector

All spec-level unknowns resolved with registry facts and a live probe against
the qual account (2026-07-28). The qual account's identity is deliberately
absent from this document — credentials follow the local convention recorded
in D8. Probe transcripts ran from a scratchpad; every probe object was
created in a dedicated scratch schema and dropped at the end.

## D1 — Driver: `snowflake-connector-rs`, adopted (owner decision), with the one gap verified in source

**Decision**: adopt `snowflake-connector-rs 1.1.0` (estie-inc) as the
session/statement layer, wrapped at one boundary exactly as duckdb-rs is.
The owner chose off-the-shelf over the hand-rolled client this survey
initially leaned to; the survey facts below are what that choice buys and
what it defers.

**Fitness, verified against source at upstream HEAD `4b3905247335`
(2026-07-19), not just the docs surface**:

- **Structured error codes are exposed**: `Error::snowflake_code()` plus a
  typed `ErrorKind` (Auth / Network / Server / SessionExpired / Timeout /
  Cancelled / Protocol / Decode) — Principle V classification works,
  including the duplicate-merge-key diagnosis by code 100090.
- **Key-pair auth built in**, including encrypted PKCS#8
  (`KeyPairConfig::from_encrypted_pem`) — matching the qual key's shape.
- **Persistent sessions** — `BEGIN`/`COMMIT` across `query()` calls are real
  transactions in one Snowflake session (stronger than the SQL API's
  single-request bundling the probe used).
- **`SessionConfig`** carries warehouse/database/schema/role and session
  parameters; rich bind support for batched INSERT.
- Maintenance: released 2026-07-19; no arrow dependency (the single arrow 58
  tree stands untouched).

**The verified gap**: no PUT / internal-stage upload, and no escape hatch to
reach it — `ApiContext`, session tokens, and the statement wire types are
all `pub(crate)`, and the response parser deliberately skips unknown keys,
which is exactly where a PUT response carries its `uploadInfo`/encryption
material. Internal-stage ingestion is therefore NOT reachable through this
crate today. Consequence: the ingestion design in D6 is re-cut around that
fact, and the internal-stage door is deferred with a named upstream trigger
(PUT support or a raw-response API landing upstream — an issue is filed as
part of this feature; contributing the implementation is a recorded option).
A sidecar session stack for PUT alone is rejected: it would duplicate login,
renewal, and error mapping beside the crate's own, the two-stacks shape.

**Costs accepted with eyes open**:

- `reqwest ^0.13` — **measured at T001 (addendum A1) and smaller than this
  entry first claimed**: reqwest 0.13 is ALREADY in the workspace tree via
  opendal ← iceberg, so the lock gains no reqwest at all; the true cost is
  that a `snowflake`-on / `iceberg`-off build gets the 0.13 line from
  snowflake instead. Build cost only, feature-gated, recorded.
- The alternatives stand as recorded: `snowflake-api` 0.14 rejected on the
  arrow ^57-vs-58 major conflict; ODBC/ADBC rejected on packaging/FFI
  grounds; the hand-rolled session client remains the DESIGNED FALLBACK if
  the crate proves unfit at T001 (login shapes, renewal under long loads,
  bind limits) — escalated, never improvised.

## D2 — Auth: the full unattended vocabulary (owner-expanded scope), key-pair proven live

The full chain was proven against the qual account with nothing but openssl
and the SQL API: fingerprint = base64(SHA256(DER(public key))), JWT
`iss = ACCOUNT.USER.SHA256:<fp>`, `sub = ACCOUNT.USER`, RS256, one-hour
expiry; header `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT`.
`SELECT CURRENT_VERSION(), …` returned (Snowflake 10.26.101) with the user's
DEFAULT role, warehouse, and database applied server-side. In the
implementation this chain belongs to the adopted crate
(`KeyPairConfig::from_pem` / `from_encrypted_pem`); the probe proves the
account-side configuration and the key itself are good.

**Scope expansion (owner decision, 2026-07-28)**: v1 ships every
unattended method, not key-pair alone. Crate support verified in source:
`AuthConfig::password` (README documents Snowflake's MFA enforcement on
password sign-ins — the caveat ships in OUR docs too, with an optional MFA
passcode field), `AuthConfig::oauth` (caller-supplied access token; refresh
is the caller's concern), `AuthConfig::key_pair`. **PATs** ride the
password channel in Snowflake's drivers — T001 probes that assumption
through the crate before the config commits to it. External-browser SSO
stays typed-unsupported (interactive; experimental in the crate; wrong for
an embedded engine). Live credentials for the three
new paths are provisioned by the owner ON REQUEST, at the point each leg
is built — the PAT at T001's probe, the password test user (a TYPE that
permits passwords — Snowflake refuses them on TYPE=SERVICE) and the OAuth
security integration at the auth-matrix cells (T030). Each live leg gates
on ITS OWN credential presence and skips-not-fails independently; nothing
blocks on a credential that has not arrived.

Facts that shape the design:

- The qual key is an ENCRYPTED PKCS#8 (`BEGIN ENCRYPTED PRIVATE KEY`); the
  config must accept encrypted keys with a passphrase source. Convention:
  passphrase file beside the key (D8). Both encrypted and unencrypted keys
  are supported; the passphrase is Secret-wrapped like the key.
- User defaults exist server-side (role/warehouse/database), so those config
  fields are OPTIONAL with server defaults honored — but the destination
  always names its database+schema explicitly in SQL (three-part names), so
  a changed user default cannot silently retarget a pipeline.

## D3 — Identifier policy: uppercase, always-quoted

Probe: unquoted `events` lands in the catalog as `EVENTS`; quoted `"events"`
lands as `events`; **both coexist as distinct tables**. Policy decided:

- Emit every rdlt-owned identifier as `"<NORMALIZED_UPPER>"` — the engine's
  normalized name uppercased, always quoted. Quoted-uppercase is identical
  to what an unquoted user query resolves to, so users keep writing
  `select * from events` while rdlt's emission is deterministic and safe for
  reserved words and specials.
- Catalog reads (DESCRIBE/INFORMATION_SCHEMA) compare against the uppercased
  form exactly; no case-insensitive fuzz.
- `_rdlt_`-prefixed persisted identities keep their sqlcore constants as the
  single source, uppercased at the emission boundary only — the persisted
  NAME CONTRACT (lowercase constants) is unchanged; Snowflake's catalog holds
  their uppercase image.

## D4 — Merge dialect: MERGE INTO + QUALIFY, duplicate diagnosis by code 100090

Probed live:

- `MERGE INTO … USING (SELECT … FROM stage QUALIFY ROW_NUMBER() OVER
  (PARTITION BY key ORDER BY arrival DESC) = 1) …` delivers last-wins dedup
  exactly (arrival 2 beat arrival 1; verified row values).
- A MERGE whose source carries duplicate rows for one target key fails with
  **structured error code 100090** ("Duplicate row detected during DML
  action") — the classifiable analogue of postgres's 23505 for the
  duplicate-merge-key diagnosis. Classification is by code, never message
  text (Principle V).
- No enforced unique constraints exist, so the arbiter-index model does not
  transfer: key identity is delivered by the MERGE construction itself, and
  the informational PRIMARY KEY is still declared (metadata parity for
  downstream tools) without being relied on.
- `__rdlt_arrival` on stage tables: no BIGSERIAL/identity dependency —
  arrival is assigned at INSERT time from a sequence or monotonic expression;
  exact mechanism decided in design with the same "orders rows within one
  stage table" contract, and the D-37 lesson (sequence cost is measurable)
  applied from day one.

## D5 — Transactions and DDL auto-commit, proven

Probed live in multi-statement batches:

- Pure-DML `BEGIN; INSERT; ROLLBACK` rolls back (count stayed 0).
- Pure-DML `BEGIN; INSERT; INSERT; COMMIT` is atomic (both rows or none).
- **`BEGIN; INSERT; CREATE TABLE …; ROLLBACK` leaves the INSERT COMMITTED**
  — DDL auto-committed the open transaction. The spec's edge case is a
  proven fact.

Commit-protocol consequence: the atomic unit (publish + receipts + state) is
pure DML inside one explicit transaction; ALL DDL (ensure, stage creation)
runs strictly before the unit opens. A debug assertion in the session guards
the invariant: no DDL statement may be issued while a unit transaction is
open.

## D6 — Ingestion: batched INSERT as the universal path; external-stage COPY as the bulk option; internal stage deferred on an upstream trigger

Probe facts that frame the decision:

- `PUT` is refused by the SQL API (code 391911) and is unreachable through
  the adopted crate (D1) — internal-stage upload is off the table for v1.
- The qual account is AWS-backed (`AWS_EU_CENTRAL_1`). A LOCAL RUSTFS bucket
  is unreachable from the SaaS side, so any stage-based live leg needs a
  REAL cloud bucket.
- Server-side bulk generation (100k rows) ran in ~2 s; client-shipped INSERT
  throughput over WAN is unknown and is measured, not assumed.

**Decision — two shipped paths, one deferral**:

1. **Batched INSERT (universal default)**: multi-row inserts through the
   crate's bind/statement machinery inside the DML unit. Works for every
   user with zero extra infrastructure; fully live-testable on the qual
   account; batch size is measured on the qual account (rows × bytes knee),
   not guessed.
2. **External-stage COPY INTO (the bulk option)**: for users with a cloud
   bucket — the workspace's file-family machinery writes parquet parts to
   the USER'S bucket via `object_store` (existing dependency), then
   `COPY INTO` from an external stage/location executes as plain SQL through
   the crate, with per-COPY loaded-rowcount verification. This is dlt's
   external-staging shape. The live leg is gated on an EXTENDED credential
   convention (optional `RDLT_SNOWFLAKE_STAGE_BUCKET` + storage credentials);
   absent a bucket, the leg records UNPERFORMED — never silently green.
**The AWS native-stage probe (2026-07-28): the external-stage COPY path is
PROVEN END-TO-END** against a real eu-west-2 bucket, cross-region from the
eu-central-1 account, key-credentialed (no storage integration needed):

- client SigV4 PUT → `CREATE STAGE URL='s3://…' CREDENTIALS=(…)` →
  `LIST @stage` sees the client-written object → CSV `COPY INTO` loads it
  (result carries `LOADED, rows_parsed, rows_loaded` — the per-COPY
  rowcount-verification data SD6 depends on) → row values verified.
- **Parquet both directions**: `COPY INTO @stage … TYPE=PARQUET` unloads
  1000 rows (proving the credentials' write breadth), and
  `COPY INTO table FROM @stage … TYPE=PARQUET MATCH_BY_COLUMN_NAME` reloads
  exactly 1000 with min/max ids intact. `MATCH_BY_COLUMN_NAME=
  CASE_INSENSITIVE` is noted for the design: parquet column names written
  lowercase by the arrow writer will match the quoted-upper catalog columns.
- Cleanup verified both sides: `REMOVE @stage`, schema dropped, client LIST
  shows KeyCount 0.

The bulk path therefore has a LIVE leg from day one; arrow-written-parquet
compatibility (as opposed to Snowflake-written) is the one remaining T001
check on this path.

**s3-COMPATIBLE endpoints (recorded negative, probed)**: a non-AWS
S3-compatible bucket was also probed as a candidate target. Client-side it
behaves (TLS, virtual-host style, SigV4 PUT/DELETE all fine), but
Snowflake-side `CREATE STAGE … URL='s3compat://…'` fails with structured
code 001075 "Endpoint not allowed": S3-compatible endpoints require a
per-account allowlist that only Snowflake Support can enable, and probing
`SHOW PARAMETERS … IN ACCOUNT` confirmed no self-service parameter exists.
REJECTED as the qual target on that turnaround; the qual stage target is
the AWS bucket above. For USERS on s3-compatible storage this is a
documented prerequisite (their endpoint must be allowlisted by Snowflake
Support), not an rdlt defect — it goes in the README caveats.

3. **Internal-stage PUT: DEFERRED**, named trigger = upstream
   `snowflake-connector-rs` gaining PUT support or a raw-response escape
   hatch (issue filed as part of this feature; contributing the
   implementation upstream is the recorded route to closing it). This is the
   bucket-free bulk path and remains the parity gap vs dlt's default —
   recorded in the parity matrix, not buried.

The INSERT-vs-COPY crossover and both paths' numbers land in the close-out;
the recorded 1M session runs on whichever paths the session's credentials
allow and says which ran.

## D7 — sqlcore triggers: both TAKEN

- **ensure_table choreography extraction**: the third SQL destination exists
  now; the shared ensure choreography (table legs, column ensure, index
  ensure) moves into sqlcore with the postgres/duckdb executors' emitted SQL
  and golden pins byte-identical. Snowflake's ensure is DESCRIBE-once by
  construction (the round-trip-economy requirement), and the extraction
  makes that shape available to the other two as a later, separately
  measured change — this feature does not alter their statement flow.
- **session-protocol extraction**: the commit/receipt/state choreography
  shared by postgres and duckdb is extracted into sqlcore as the planner's
  companion (execute-side skeleton with dialect seams), with both existing
  destinations' behavior and pins byte-identical. Snowflake becomes its
  third consumer with DML-transaction and DDL-outside-unit constraints
  parameterized. If, during design, the extraction proves to endanger pin
  byte-identity, the fallback is recorded: extract only the shapes all three
  share today, defer the rest with a named trigger — never a silent partial.

## D8 — Live-leg convention and hermetic fixture

**Credential convention** (recorded here; values never committed):
environment first, config-dir fallback —

- `RDLT_SNOWFLAKE_ACCOUNT`, `RDLT_SNOWFLAKE_USER`,
  `RDLT_SNOWFLAKE_PRIVATE_KEY_PATH`, `RDLT_SNOWFLAKE_KEY_PASSPHRASE_PATH`
  (or `…_PASSPHRASE` inline), `RDLT_SNOWFLAKE_DATABASE`,
  `RDLT_SNOWFLAKE_WAREHOUSE`, `RDLT_SNOWFLAKE_ROLE`;
- fallback dir `~/.config/rdlt/snowflake/`: `rdlt_qual_key.p8`,
  `rdlt_qual_key.pub`, `passphrase` (0600); per-method additions:
  `RDLT_SNOWFLAKE_PAT` (or `pat` file), `RDLT_SNOWFLAKE_PASSWORD` +
  `RDLT_SNOWFLAKE_PASSWORD_USER` (or `password.env` file),
  `RDLT_SNOWFLAKE_OAUTH_TOKEN` (or `oauth-token` file) — each new auth
  path's live leg gates on its own entry;
- OPTIONAL, for the external-stage live leg only: `stage.env` beside the
  key (0600) or the equivalent environment variables —
  `RDLT_SNOWFLAKE_STAGE_ENDPOINT`, `RDLT_SNOWFLAKE_STAGE_BUCKET`,
  `RDLT_SNOWFLAKE_STAGE_ACCESS_KEY`, `RDLT_SNOWFLAKE_STAGE_SECRET_KEY` —
  naming a bucket both the runner and the account can reach. The qual
  target is a real AWS bucket, and the full COPY path against it is
  already proven (D6); credentials absent → the leg records UNPERFORMED
  with reason.

Live tests resolve the convention through one testkit-style probe
(`snowflake_available()` returning `Option`), skip-not-fail with a stated
reason — the container posture with credential presence in place of runtime
presence. Live legs run in a dedicated database schema namespace per test
run with teardown, mirroring the container fixtures' isolation.

**fakesnow** (registry: 0.11.11, `server` extra = starlette+uvicorn, Python
≥3.10) explicitly serves non-Python clients over HTTP with a DuckDB backend.
Adopted as a T001 FIDELITY PROBE only: if its session protocol, MERGE +
QUALIFY transpilation, and multi-statement transactions hold up under the
hand-rolled client, it becomes the hermetic container-free leg for
protocol-level tests (venv pattern like pyiceberg); if not, it is rejected
with the probe transcript recorded. Either way the qual account remains the
leg of record, and no gate depends on fakesnow fidelity.

## D11 — Deployment completeness (owner-expanded scope)

Three config-only capabilities that make the connector deployable in real
estates, each defaulting to today's behavior when absent:

- **Transient tables**: `table_type: transient | permanent` (default
  permanent) — `CREATE [TRANSIENT] TABLE` at ensure; the fail-safe cost
  lever; live-verifiable (SHOW TABLES reports kind). Applies to
  destination tables AND the `_rdlt_` bookkeeping tables consistently.
- **Session parameters + query tag**: a validated string map applied at
  session open via the crate's `with_session_parameters`, plus
  `query_tag` (Snowflake QUERY_TAG) so every rdlt statement is
  attributable in QUERY_HISTORY — which the statement-economy live check
  already reads. Live-verifiable.
- **Host override**: optional `host` replacing the derived
  `<account>.snowflakecomputing.com` for PrivateLink-style deployments,
  wired through the crate's `EndpointConfig`. Mock-verified only — no
  PrivateLink test environment exists; recorded UNPERFORMED live.

## D9 — Type mapping (closed, with the plan-time decisions made)

| engine logical | Snowflake | notes |
|---|---|---|
| Bool | BOOLEAN | |
| Int8 | NUMBER(19,0) | i64 range fits |
| Float8 | FLOAT | |
| Text | VARCHAR | |
| Bytea | BINARY | |
| TimestampTz | TIMESTAMP_TZ | zone-carrying |
| TimestampNaive | TIMESTAMP_NTZ | |
| Date | DATE | |
| Time | TIME | |
| Decimal(p,s) | NUMBER(p,s) | p ≤ 38 enforced; over-precision refused typed at write |
| Json | VARIANT | staged as parquet string + PARSE_JSON at COPY, or direct — decided by the COPY-path design; oversized (>16MB) refused typed |
| Uuid | VARCHAR(36) | **decided**: no native UUID type; canonical text form, documented |

Additive drift: `ALTER TABLE … ADD COLUMN` (nullable). Narrowing/incompatible
drift: engine policy verdicts, typed errors, unchanged.

## D10 — Performance posture

- The recorded session runs the bench-shaped 1M×12 dataset through
  pg→snowflake on the qual account; wall, rows/s, statement counts, and the
  chosen file/batch sizing are recorded in the close-out. UNBARRED (018
  governance: a bar requires a recorded floor on infrastructure the harness
  controls; WAN variance is not that).
- Statement economy is a correctness-adjacent SC: steady-state loads issue
  zero schema-mutation statements; counts instrumented in tests (the mock
  transport counts statements) and verified once live.
- Every optimization beyond the shipped defaults follows
  measure-then-take-only-if-it-wins; the D-13/D-21 reversal precedent and
  020's four declined-with-numbers entries are the standing null hypothesis.

---

## T001 addenda — implementation-time probes (2026-07-28)

Run against the live qual account through a throwaway workspace member
(deleted after; every probe object dropped, every staged object removed).
These are the verdicts the plan owed, and two of them CORRECT plan-time
claims.

### A1 — lock impact: smaller than recorded, and NO new reqwest (corrects D1)

Adding `snowflake-connector-rs = "1.1"` to a workspace member takes the
lock from **629 to 647 crates (+18)**, all of them the RustCrypto stack the
encrypted-PKCS#8 key path needs: `rsa`, `pkcs1`, `pkcs5`, `pkcs8`,
`pem-rfc7468`, `pbkdf2`, `scrypt`, `salsa20`, `cbc`, `block-padding`,
`num-bigint-dig`, `signature`, `spin`, plus the crate and its derive.

**The recorded reqwest cost is WRONG and is corrected here.** D1 said the
crate brings "a second reqwest major"; the lock says otherwise —
**reqwest 0.13.4 was ALREADY in the tree** (opendal ← iceberg-storage-opendal
← the iceberg destination), so the workspace-level cost is **zero new
reqwest**. The narrower true statement: in a build with `snowflake` enabled
and `iceberg` disabled, snowflake is what pulls the 0.13 line into an
otherwise-0.12 build. In this workspace's own lock, nothing changes.
Compiles clean in-workspace.

### A2 — the crate's session IS a session (the commit protocol's premise)

The plan-time probe used the SQL API, which bundles statements per request
and therefore could NOT answer whether the crate's `query()` calls share a
Snowflake session. Probed directly:

| property | result |
|---|---|
| key-pair auth through the crate (encrypted PKCS#8 + passphrase) | **OK** — Snowflake 10.26.101 |
| `SELECT CURRENT_SESSION()` twice on one `Session` | **identical** (`1475398575812618`) — one real session |
| `BEGIN` / `INSERT` / `ROLLBACK` as three separate `query()` calls | **count = 0** — transactions genuinely span calls |
| same, with a `CREATE TABLE` before the `ROLLBACK` | **count = 1** — DDL auto-committed the open transaction |

The first three make SD3's pure-DML unit implementable on this crate; the
fourth re-proves the auto-commit hazard through the crate itself, so the
DDL-refusal guard is justified by evidence twice over, on two different
transports.

**A false alarm worth recording.** The first attempt reported the probe
table "does not exist" and looked like a session-scoping failure. It was
`090106: This session does not have a current schema` — the qual database
has **no `PUBLIC` schema**, so `SessionConfig::with_schema("PUBLIC")`
established nothing. Fully-qualified three-part names worked immediately.
That is an accidental validation of D2's decision to always name
database+schema explicitly rather than lean on session context, and it is
why the ensure path must never assume a `PUBLIC` schema exists. (A `PUBLIC`
schema was subsequently created on the qual account for convenience; the
connector must not depend on one.)

### A3 — PAT rides the PASSWORD channel, confirmed (the `pat` arm commits)

| channel | result |
|---|---|
| `AuthConfig::password(pat)` | **OK** — authenticated, `CURRENT_USER()` = the qual user |
| `AuthConfig::oauth(pat)` | **rejected**, `kind = Auth` |

The assumption the `pat` config arm rested on is now a measurement: PATs
authenticate through the password channel and NOT through the OAuth one.
The config arm ships, and its implementation maps `pat` → password.

### A4 — error taxonomy inputs are real, and secrets do not leak

A deliberately invalid password produced `kind = Auth` with **no Snowflake
code** (auth failures are pre-SQL, so code-based classification must not be
the ONLY discriminator — `ErrorKind` carries auth), and the rendered error
was checked programmatically for the secret substring: **not present**.
SD2's "distinguishable by shape" and the grep-proof requirement both have
live evidence.

### A5 — arrow-written parquet loads through COPY (the last bulk-path unknown)

A parquet file written by the WORKSPACE's own arrow writer (lowercase
columns `id`, `v`; 744 bytes; a NULL in the middle) was uploaded to the
qual stage bucket and loaded into a quoted-upper table
`"EVENTS"("ID","V")`:

```
COPY INTO "EVENTS" FROM @stage/t001.parquet
  FILE_FORMAT=(TYPE=PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
→ LOADED, rows_parsed=3, rows_loaded=3, errors=0
→ (1,'a'), (2,NULL), (3,'c')
```

Arrow's lowercase names bind to the quoted-upper catalog columns, NULLs
survive, and the COPY result carries the `rows_loaded` figure SD6's
verification depends on. **The external-stage bulk path is now proven
end-to-end with our own writer**, not merely with Snowflake-written files.

### A6 — session parameters and QUERY_TAG work; the host override is PROVEN, not mock-only

`SessionConfig::with_session_parameter("QUERY_TAG", …)` applies at login and
is visible via `SHOW PARAMETERS LIKE 'QUERY_TAG' IN SESSION`. Separately,
`EndpointConfig::custom_base_url` — the same seam FR-019's PrivateLink host
override uses — was exercised by pointing the crate at a local HTTP server
and completing a **successful login through it**. D11 recorded the host
override as mock-verified only; it now has a real integration proof of the
seam, with only PrivateLink-specific behaviour still unverifiable here.

### A7 — fakesnow: REJECTED, with the incompatibility pinned on both sides

fakesnow 0.11.11 server mode was installed and probed through the crate.
Two gaps, in order of discovery:

1. **Login payload**: fakesnow reads `data.SESSION_PARAMETERS`
   unconditionally, while the crate omits the key when no session
   parameters are set (`skip_serializing_if`). Satisfiable — setting one
   parameter bridges it, and login then succeeds.
2. **Result format — fatal**: fakesnow hardcodes
   `"queryResultFormat": "arrow"` in every success response. The crate is
   **JSON-only by design**: it rejects any other format, and ships a unit
   test asserting exactly `"unsupported result format: arrow"`. Neither
   side is configurable from our position.

fakesnow executes the SQL correctly (its DuckDB backend answered DDL, DML,
`BEGIN`/`ROLLBACK`, and `MERGE … QUALIFY` — 200 OK on every route), so the
rejection is about the transport envelope, not semantics. **No hermetic
protocol leg is adopted**; the mock executor seam (T007) covers
protocol-shaped tests and the qual account remains the leg of record.
Re-trigger: fakesnow honouring a JSON result format, or the crate gaining
arrow support — either alone closes it.

### A8 — credential convention, resolved and extended

Resolution order verified working: environment (`RDLT_SNOWFLAKE_*`) first,
then `~/.config/rdlt/snowflake/` files. Entries now in use: `account`,
`user`, `warehouse`, `database`, `rdlt_qual_key.p8`, `passphrase`, `pat`,
`stage.env`. Every one is 0600 and local-only; none is committed.

## D12 — the two sqlcore extractions are NARROWED, on structural evidence

D7 committed to TAKING both fired triggers and named the fallback: "extract
only the shared shapes and re-record the remainder with a named trigger —
never a silent partial". A four-way structural survey of both destinations
(full reasoning: `extraction-plan.md`) says the fallback is the correct
call for T003 and the ONLY available call for T004. Both are narrowed, both
with triggers, neither silently.

**T004 cannot be a shared execute skeleton — a type-system fact, not taste.**
`DuckDbSession::commit` runs its whole body inside `with_conn(move |conn| …)`:
a SYNCHRONOUS `FnOnce` holding a `MutexGuard<Connection>`, inside which
`conn.transaction()` borrows `&mut Connection` for the guard's lifetime. The
postgres session is an `async fn` over an owned client, and Snowflake's will
be async too. Unifying them needs either an async trait in sqlcore — and
`async-trait` is not a sqlcore dependency, which the shared core's own
contract forbids adding — or a rebuild of DuckDB's concurrency model. Both
are redesigns. **Extracted instead: six pure items** — the receipt-existence
and load-committed probes, the staged-emptiness probe, `roots_of`, a
`load_guard`, and the one item worth the whole task:

**`ReplayDisposition`.** The redelivered-unit outcome is INVERTED between
the two destinations for a structural reason — direct-to-target must roll
back and return, staged must run the truncate program and commit — and
today that invariant exists ONLY as prose in two comments. Typing it is how
Snowflake inherits it correctly instead of rediscovering it as duplicate
rows. `load_guard` is landed but deliberately NOT wired to duckdb, whose
`open` discards the load id: adopting it there is a behaviour ADDITION and
gets its own recorded decision.

**T003 extracts DECISIONS, not SQL.** The third destination does not share
the statement program at all: pg and duckdb both emit blind
`ADD COLUMN IF NOT EXISTS` for every column every time, while Snowflake is
describe-once, emits nothing when nothing is missing, and creates no indexes
at all. So the surface common to three destinations is a desired-state list,
never a statement list. sqlcore gains a pure `ensure` planner returning an
ordered `Vec<EnsureStep>` (Table / Column / Widen / Validity / Index) — the
structural twin of the existing commit-script planner — and every byte of
SQL stays in each destination's own `ddl.rs`.

**No `DdlDialect` trait is added**, and the reason is arithmetic: of the five
hooks such a trait would need, FOUR have no shared body across the three
destinations (create-table, ensure-column, widen, validity-column all differ
in grammar or arity) and the fifth — index DDL — was already extracted in an
earlier feature. A five-hook trait with zero shared defaults is a vtable tax,
not a seam.

**A mechanical fact that makes this safe to do at all**: no test anywhere
pins ensure DDL text (verified by tree-wide search for the emitted literals).
That is also why the edit order below is non-negotiable — **the pins must be
created before anything crosses a crate boundary**: hoist rendering out of
execution WITHIN each destination first, pin the rendered statement vectors
container-free, and only then move the decision logic to sqlcore.

**One live trap this survey caught before it could be written.** Snowflake's
`TRUNCATE TABLE` is DDL and therefore auto-commits the open transaction —
precisely the hazard the pure-DML unit exists to avoid — and the Replace
clear runs INSIDE the unit. `SnowflakeDialect::clear_table` must return
`DELETE FROM`, exactly as the duckdb dialect already does. The existing
dialect seam covers it; no new machinery. Recorded here so the implementation
cannot forget it.

**Trigger for the remainder** (a shared execute skeleton, the DuckDB
load-guard wiring, and any further ensure sharing): a destination whose
session is async AND owns its connection the way postgres does — i.e. the
FOURTH SQL destination, or a rebuild of the duckdb session's concurrency
model, whichever comes first.
