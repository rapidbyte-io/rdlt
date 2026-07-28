# Data Model: Snowflake Destination Connector

Entities, their fields, validation rules, and state transitions. Persisted
identities reuse the sqlcore constants; nothing here introduces a new
persisted format.

## SnowflakeConfig

The closed destination config (deserialized from the CLI block or built
programmatically; schemars-generated schema; `#[non_exhaustive]` enums).

| field | type | rules |
|---|---|---|
| `account` | String | required; the account identifier as it appears in the host (`<acct>.snowflakecomputing.com`); validated non-empty, no dots/scheme |
| `user` | String | required; login name |
| `auth` | AuthMethod | required; exactly one of the closed vocabulary below |
| — `key_pair` | `{ private_key: Secret, key_passphrase: Option<Secret> }` | PEM text or `path:`-resolved; encrypted PKCS#8 accepted, passphrase required iff encrypted |
| — `password` | `{ password: Secret, mfa_passcode: Option<Secret> }` | ships with the documented Snowflake caveats (MFA enforcement; TYPE=SERVICE users refuse passwords) |
| — `oauth` | `{ token: Secret }` | caller-supplied access token; refresh is the caller's concern |
| — `pat` | `{ token: Secret }` | programmatic access token; rides the password channel (T001-probed) |
| `role` | Option<String> | optional; server default honored when absent |
| `warehouse` | Option<String> | optional; server default honored when absent; loads FAIL typed if neither is set server-side |
| `database` | String | required — three-part naming is always explicit so a changed server default cannot retarget a pipeline |
| `schema` | String | required; same reason (the engine's `dataset` vocabulary maps here) |
| `options` | DestOptions | the shared sqlcore vocabulary (strategy, hard_delete, dedup_sort, merge_scope, scd2 fields) — validation identical to the other SQL destinations |
| `table_type` | enum, default `permanent` | `transient` opts out of fail-safe; applied to destination AND `_rdlt_` tables consistently |
| `session_parameters` | Option<map<String,String>> | validated passthrough applied at session open |
| `query_tag` | Option<String> | QUERY_TAG for attribution in QUERY_HISTORY |
| `host` | Option<String> | PrivateLink-style override of the derived hostname; mock-verified |

Validation is eager and typed, naming the field. Unknown fields are errors.
Secret fields are grep-proofed (Debug/serialize/error paths).

## Boundary (the one wrapped library)

| item | notes |
|---|---|
| wrapped library | `snowflake-connector-rs 1.1.0` — Client/Session construction, the full `AuthConfig` mapping (key_pair / password+passcode / oauth; PAT via the password channel), `EndpointConfig` host override, session parameters, statement execution, binds |
| session lifecycle | crate-managed (persistent session; renewal internal to the crate); a session-expiry error mid-unit classifies Transient and the unit retries per driver policy |
| statement seam | one internal executor trait over the crate's `Session` — the mock transport for statement-count and retry-class tests plugs here; the DDL-inside-unit refusal is asserted here |
| error translation | `Error::snowflake_code()` + `ErrorKind` → SPI taxonomy at this boundary only: Auth, Permission, Transient (Network/SessionExpired/Timeout/throttle codes), Fatal (SQL/schema/oversized; code 100090 carries the duplicate-merge-key diagnosis shape) |
| not reachable | internal-stage PUT (source-verified gap) — no sidecar stack; deferred on the upstream trigger |

## SnowflakeSession (LoadSession impl)

| state | transition |
|---|---|
| `Opened` | after login + context statements; reads state doc; replays receipt check |
| `Ensured(tables)` | describe-once per table; DDL (create/add-column/stage create) all here, strictly BEFORE any unit; quoted-upper identifiers |
| `Staged(parts)` | INSERT path: batches buffered per measured batch size. COPY path (bucket configured): parquet parts written via file-family writer to the user bucket under a pipeline-scoped prefix |
| `Unit(open txn)` | pure DML: `COPY INTO` stage-table verification counts, merge/publish statements, receipt insert, state update — one explicit transaction |
| `Committed(receipt)` | COMMIT observed; staged parts removed (post-commit cleanup is idempotent, crash-safe) |
| `Replayed` | (load, seq) already receipted → publish nothing, return prior receipt |

Crash points: `sf.stage.write` (after part/batch staging begins), `sf.unit.publish`
(inside the unit before COMMIT), `sf.receipt.visible` (after COMMIT before
cleanup). Each swept with armed-fire pins.

## Stage & artifact identity

| item | rule |
|---|---|
| external stage identity | pipeline-scoped prefix in the user bucket, from sqlcore naming constants; the COPY statement references it as an external location/stage |
| part name | `part-<load>-<seq>-<index>.parquet` — the file-family shape, giving ownership-precise cleanup |
| cleanup | on open: remove any parts belonging to THIS pipeline whose (load, seq) is not the live one; never touches foreign files |
| COPY verification | rows loaded per COPY result must equal staged part rowcounts; mismatch fails the unit typed |

## Merge planning (sqlcore)

| item | rule |
|---|---|
| dialect | `SnowflakeDialect` implements the MergeDialect seam: dedup via `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY __rdlt_arrival DESC) = 1` inside the USING subquery; strategies as `MERGE INTO` / delete+insert / scd2 statement sets |
| arrival | assigned at stage-load time (monotonic per stage table); mechanism chosen in design with its cost measured (the sequence-cache lesson applies) |
| duplicate diagnosis | structured code 100090 mapped to the shared duplicate-merge-key diagnosis (same remedies text as the other destinations) |
| golden pins | every emitted statement byte-pinned; pg/duckdb pins byte-identical before/after the extractions |

## State & receipts

`_rdlt_state` / `_rdlt_commits` tables per the sqlcore constants (uppercase
image in the catalog), same document shapes as the other SQL destinations —
no format change. Written only inside the DML unit transaction.

## Type mapping (enforced by ddl.rs + stage writer)

The closed table from research D9; over-limit values (precision > 38,
VARIANT > 16MB) are refused typed at write time with column and value-shape
named. Additive drift = `ALTER TABLE … ADD COLUMN` (nullable) — emitted only
when DESCRIBE shows the column genuinely absent.
