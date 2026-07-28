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

- `reqwest ^0.13` — a second reqwest major wherever the `iceberg` feature is
  off. Same shape as the recorded reqwest 0.12/0.13 double tree (feature-
  gated, build cost only); recorded, not hidden.
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
an embedded engine). The owner provisions live credentials for all three
new paths on the qual account (PAT via Snowsight; a password-capable test
user — note Snowflake refuses passwords on TYPE=SERVICE users; an OAuth
security integration); each live leg gates on ITS OWN credential presence
and skips-not-fails independently.

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
