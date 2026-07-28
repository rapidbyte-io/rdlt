# Phase 0 Research: Snowflake Destination Connector

All spec-level unknowns resolved with registry facts and a live probe against
the qual account (2026-07-28). The qual account's identity is deliberately
absent from this document — credentials follow the local convention recorded
in D8. Probe transcripts ran from a scratchpad; every probe object was
created in a dedicated scratch schema and dropped at the end.

## D1 — Driver: hand-rolled thin session-protocol client, at one boundary

**Decision**: implement a THIN Snowflake session-protocol client inside
`rdlt-connector-snowflake` (login, statement execution, token renew, PUT
upload-info, error mapping), over the workspace's existing `reqwest 0.12` +
rustls stack. No Snowflake driver crate is adopted.

**Rationale** — the registry survey disqualified both real candidates:

| candidate | version | facts | verdict |
|---|---|---|---|
| `snowflake-api` | 0.14.0 (2025-10-23) | `arrow-array/ipc/schema ^57` against the workspace's single arrow 58 tree — the recorded design-changing conflict class (016 FR-002 disqualifier); adds reqwest-middleware/retry tree; 9 months stale | **rejected: arrow major conflict** |
| `snowflake-connector-rs` | 1.1.0 (2026-07-19) | actively maintained; no arrow dep; key-pair auth built in; persistent sessions. But: **no PUT / stage-upload API and no raw-response escape hatch** — the performance-defining ingestion path (internal stage + COPY, the dlt-parity path) is unreachable through its public surface; `reqwest ^0.13` adds a second reqwest major wherever the `iceberg` feature is off | **rejected: cannot carry the ingestion path** |
| ODBC (arrow-odbc + Snowflake ODBC) | — | proprietary driver install on every consumer of a crate being prepared for publish | rejected (packaging, presumptive per spec) |
| ADBC (Go driver via FFI) | — | foreign-runtime FFI tree | rejected (the Glue/aws-sdk precedent, presumptive per spec) |

Bolting a second session stack beside `snowflake-connector-rs` for PUT alone
would violate one-boundary wrapping (Principle III). The protocol surface
actually needed is small and is precedented as a hand-roll in this project
(the pgoutput parser in 009, the OAuth/pagination client in 014): a login
request, a statement-execution request, token renewal, and the PUT
upload-info response. `snowflake-api`'s implementation serves as a
feasibility reference for the PUT flow (read, not depended on).

**JWT**: `jsonwebtoken` (RS256) rides the `ring` already present in the lock
via rustls; the key is parsed once and Secret-wrapped. Alternative
(`rsa`+`pkcs8`+`sha2` pure-RustCrypto) recorded as fallback if `jsonwebtoken`'s
key-format handling disappoints at T001.

**Fallback (designed, not improvised)**: if T001 falsifies the protocol
survey — login or PUT upload-info materially deeper than surveyed — v1
narrows to the SQL API v2 (`/api/v2/statements`, proven live in this
research) with batched-INSERT ingestion, recorded as a typed narrowing, and
the internal-stage door is re-recorded with what T001 learned. The switch is
an escalation to the owner, not a silent substitution.

## D2 — Auth: key-pair JWT, proven live

The full chain was proven against the qual account with nothing but openssl
and the SQL API: fingerprint = base64(SHA256(DER(public key))), JWT
`iss = ACCOUNT.USER.SHA256:<fp>`, `sub = ACCOUNT.USER`, RS256, one-hour
expiry; header `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT`.
`SELECT CURRENT_VERSION(), …` returned (Snowflake 10.26.101) with the user's
DEFAULT role, warehouse, and database applied server-side.

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

## D6 — Ingestion: internal stage + COPY INTO as the design target, measured before shipped

Probe facts that frame the decision:

- `PUT` is refused by the SQL API with structured code 391911 — internal
  stage upload REQUIRES the session protocol (PUT statement returns vended
  upload credentials; the client uploads directly to cloud storage and, on
  AWS, encrypts files client-side per the returned encryption material).
- The qual account is **AWS-backed** (`AWS_EU_CENTRAL_1`), so the PUT upload
  half is: parse upload-info → AES-encrypt staged file → S3 PUT with vended
  creds → `COPY INTO` from the internal stage. `aes` and `sha2` are already
  in the workspace lock; `cbc` is a small pure-Rust addition.
- The workspace's parquet writer (file family) produces the staged files;
  `object_store` (already a dependency) performs the vended-credential S3
  upload. A LOCAL RUSTFS bucket is unreachable from the SaaS side, so
  external-stage cells cannot be tested live without a real cloud bucket —
  internal stages are the only bucket-free, live-testable path.
- Server-side bulk generation (100k rows via GENERATOR) ran in ~2 s; the
  client-shipped INSERT path's real throughput over WAN is unknown and is
  measured at implementation time, not assumed.

**Decision**: v1 ships internal-stage PUT + `COPY INTO` (parquet) as the
bulk path — it is the dlt-parity path and the only live-testable bucket-free
one — with batched INSERT as the small-load/fallback path. The crossover
threshold between INSERT and stage+COPY is MEASURED on the qual account
(rows and bytes), not guessed; both paths' numbers land in the close-out.
COPY result rowcounts are verified against staged counts (the rowcount
tripwire from the spec's edge cases).

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
  `rdlt_qual_key.pub`, `passphrase` (0600).

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
