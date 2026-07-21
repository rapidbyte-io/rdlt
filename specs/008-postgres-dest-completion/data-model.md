# Data Model: Postgres Destination Completion

All additions are destination-local. Zero engine/connector entities
change; `_rdlt_state` and `_rdlt_commits` formats are untouched.

## PgDestOptions (new, `dest/config.rs`)

| Field | Type | Default | Rules |
|---|---|---|---|
| `merge_strategy` | delete_insert \| upsert \| scd2 | delete_insert | destination-wide default; per-table overridable |
| `hard_delete` | Option<String> (column name) | None | per-table only in `tables`; not valid with scd2 |
| `tables` | map<table, PgTableOptions> | empty | per-table overrides |

## PgTableOptions

| Field | Type | Default | Rules |
|---|---|---|---|
| `merge_strategy` | as above | inherit | scd2/upsert require a keyed table (typed ensure-time error otherwise) |
| `hard_delete` | Option<String> | None | column must exist in the stream schema; bool → `= TRUE`, other → `IS NOT NULL`; not valid with scd2 |
| `scd2` | Option<Scd2Options> | None | only with `merge_strategy: scd2` |

## Scd2Options

| Field | Type | Default | Rules |
|---|---|---|---|
| `valid_from` | String | `_rdlt_valid_from` | collision with schema columns = typed error at ensure |
| `valid_to` | String | `_rdlt_valid_to` | same |
| `absent` | keep \| retire | keep | `retire` = full-feed semantics (active keys not in the stage retire at the boundary) |

Entry points: builder `Postgres::options(...)`, serde
(`from_value`/CLI TOML `[destination.postgres]`), schemars derive; every
validation error names the offending field/column.

## Native type mapping (ddl.rs / encode.rs)

| Logical column type | Created column | Wire encoding (binary COPY) |
|---|---|---|
| Decimal { p, s } | `NUMERIC(p,s)` | NumericWire: base-10000 digit groups from i128+scale (mirror of source decoder) |
| Json | `JSONB` | JsonbWire: version byte 1 + UTF-8 |
| Uuid | `UUID` | UuidWire: 16 bytes parsed from canonical text |
| (all existing) | unchanged | unchanged |
| nullable=false | column gains `NOT NULL` | CREATE-time only (additive rule) |

Capabilities flip: `decimal: true`, `json_type: true` (engine lowering
then passes Decimal128/Json through — verified capability-driven).

Pre-008 text columns: never retyped; per-column fallback to the text
encoding + ONE `rdlt::lossy` warn naming table, column, and the
documented migration paths.

## Indexes (ddl.rs)

| Trigger | Index | Name | Failure |
|---|---|---|---|
| merge (delete-insert), shredded root | btree on `_rdlt_id` | `rdlt_ix_<table>_<hash>` | — |
| merge, shredded child | btree on `_rdlt_root_id` | same scheme | — |
| merge (delete-insert), keyed structured | btree on key columns | same scheme | — |
| upsert strategy | UNIQUE on key columns | `rdlt_ux_<table>_<hash>` | 23505 during create = typed error naming key columns (pre-existing duplicates) |
| scd2 | btree on (key…, valid_to) | same scheme | — |

All `IF NOT EXISTS`, deterministic names ⇒ idempotent across sessions.

## SCD2 table shape

Target table = stream columns + `valid_from TIMESTAMPTZ NOT NULL` +
`valid_to TIMESTAMPTZ NULL` (configured names). Active version:
`valid_to IS NULL`. Invariants (conformance-pinned): per key, validity
ranges non-overlapping and contiguous by boundary; exactly one active
version; unchanged staged rows produce NO new version; boundary per
(load_id, commit_seq) minted once — redelivery returns the recorded
receipt and re-executes nothing (existing D3).

## Error rendering (commit.rs `describe()`)

Every `tokio_postgres::Error` surfaced from the destination:
`as_db_error()` → "message (SQLSTATE xxxxx)"; else source-chain walk;
transient classification heuristic (08/53/57/40 + io-shaped) unchanged.
