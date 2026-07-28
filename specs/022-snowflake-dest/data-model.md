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
| `private_key` | Secret | required; PEM text or `path:`-resolved file; encrypted PKCS#8 accepted |
| `key_passphrase` | Secret (optional) | required iff the key is encrypted; inline or `path:`-resolved (the qual convention uses a 0600 file beside the key) |
| `role` | Option<String> | optional; server default honored when absent |
| `warehouse` | Option<String> | optional; server default honored when absent; loads FAIL typed if neither is set server-side |
| `database` | String | required — three-part naming is always explicit so a changed server default cannot retarget a pipeline |
| `schema` | String | required; same reason (the engine's `dataset` vocabulary maps here) |
| `options` | DestOptions | the shared sqlcore vocabulary (strategy, hard_delete, dedup_sort, merge_scope, scd2 fields) — validation identical to the other SQL destinations |

Validation is eager and typed, naming the field. Unknown fields are errors.
Secret fields are grep-proofed (Debug/serialize/error paths).

## Client (the one boundary)

| item | notes |
|---|---|
| `Client` | owns transport (workspace reqwest 0.12 + rustls), base URL derived from `account`, session token + renew token, JWT re-issue |
| session token lifecycle | `Fresh → Active(token) → Renewing → Active' → Closed`; renewal is transparent to callers; a renew failure mid-unit is Transient |
| `execute(stmts, txn_scope)` | single or multi-statement; the DDL-inside-unit invariant is asserted HERE (a unit-scoped executor refuses DDL statement kinds) |
| `put_upload_info(stage, path)` | parses the PUT response: cloud location, vended credentials, encryption material (AWS); no library type escapes |
| `SnowflakeError` | structured code + classified SPI taxonomy at this boundary only: Auth, Permission, Transient (network/renew/resume/throttle), Fatal (SQL, schema, oversized, 100090 duplicate-diagnosis carries its own shape for the merge path) |

## SnowflakeSession (LoadSession impl)

| state | transition |
|---|---|
| `Opened` | after login + context statements; reads state doc; replays receipt check |
| `Ensured(tables)` | describe-once per table; DDL (create/add-column/stage create) all here, strictly BEFORE any unit; quoted-upper identifiers |
| `Staged(parts)` | parquet parts written via file-family writer, encrypted per upload material, PUT to the internal named stage (pipeline-scoped stage name from sqlcore constants) |
| `Unit(open txn)` | pure DML: `COPY INTO` stage-table verification counts, merge/publish statements, receipt insert, state update — one explicit transaction |
| `Committed(receipt)` | COMMIT observed; staged parts removed (post-commit cleanup is idempotent, crash-safe) |
| `Replayed` | (load, seq) already receipted → publish nothing, return prior receipt |

Crash points: `sf.stage.put` (after part upload begins), `sf.unit.publish`
(inside the unit before COMMIT), `sf.receipt.visible` (after COMMIT before
cleanup). Each swept with armed-fire pins.

## Stage & artifact identity

| item | rule |
|---|---|
| stage name | pipeline-scoped, from sqlcore naming constants (uppercased at emission); one named internal stage per pipeline |
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
