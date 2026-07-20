# Data Model: Postgres SQL Source Connector

**Feature**: 005-postgres-source | **Date**: 2026-07-20

Runtime entities are internal to the new crate (no `rdlt-core` type
changes). The durable entities are the config document, the per-stream
cursor state, and the benchmark records.

## 1. PostgresSourceConfig (declarative document — contract: contracts/source-config.md)

| Field | Type | Rules |
|---|---|---|
| `conn` | string (required) | libpq-style URL; TLS per `sslmode`; validated at open |
| `schema` | string, default `public` | reflection scope |
| `include_views` | bool, default false | adds relkind v/m to discovery |
| `tables` | list of TableConfig, or absent | absent ⇒ discover all in schema |
| `batch_target_bytes` | int, default 8 MiB | decoder batch cut (R4) |
| `batch_max_rows` | int, default 65 536 | secondary cut |
| `retry` | `{max_attempts: 3, base_ms: 250}` | connect/boundary retries only (R6) |

**TableConfig**: `name` (required; schema-qualified names rejected —
`schema` field owns that), optional `cursor` (CursorConfig), optional
`primary_key` override (list), optional `included_columns` /
`excluded_columns` (mutually exclusive).

**CursorConfig**: `column` (required, must exist in reflected schema),
`initial_value` (optional, typed literal), `boundary` = `closed`
(default) | `open`, `direction` = `max` (default) | `min`,
`end_value` (optional), `nulls` = `exclude` (default) | `include`.

Validation: unknown fields rejected (`deny_unknown_fields`, house
style); unknown table/column names are typed config errors at open,
never silent skips.

## 2. ReflectedTable (runtime, per run)

Column order, name, type OID + typmod (→ `LogicalType` via the
type-mapping contract), NOT NULL, PK columns; captured once per run
(R3). It is the authority for the published `StreamSpec` schema
(`structured: true`, `cursor_field` from config) and for cursor-column
validation. Drift between reflect and read surfaces as a typed error
or schema-policy application (spec US4-AS4) — never misaligned columns.

## 3. Cursor state (persisted via engine `Cursor`, JSON)

```json
{ "watermark": <typed scalar>, "boundary_keys": ["<pk-or-rowhash>", …] }
```

- `watermark`: last committed cursor value (typed rendering defined in
  the type-mapping contract so it round-trips losslessly through JSON).
- `boundary_keys`: keys of rows whose cursor == watermark (PK values,
  else canonical row hash); bounded to boundary rows only; used to
  dedup closed-boundary re-fetch (R5). Empty under `boundary: open`.

**Transitions**: advances only via `Checkpoint` after covered rows are
pushed, persisted only on destination commit (engine clause E6);
monotonicity guard — a candidate ≤ stored watermark never overwrites it
(regressing-clock rule). Crash at any point ⇒ next run resumes from the
last committed value (003 guarantee).

## 4. Record batches (runtime)

`PushPayload::Arrow(RecordBatch)` on the structured path; schema check
by the engine, shredder bypassed. Batch shape bounded by
`batch_target_bytes`/`batch_max_rows`; nullability from reflection.
JSONB/array/composite columns are `Json`-typed columns (escape hatch),
NOT shredded (R2).

## 5. Benchmark records (durable)

Two new matrix rows in `benches/RESULTS.md` (postgres→DuckDB,
postgres→Postgres), each: baseline-first same-session pair, dataset
identity (seed row count + content hash), explicit **Gated?** status,
bar set measurement-first with a version-policy entry linking evidence
(004 data-model §1/§4 formats apply unchanged). New gated iai baseline
`pg_copy_decode_10k` in `benches/perf-baselines.json`, recorded at
feature close in a commit naming this feature (P5-compliant new entry).
Evidence artifacts live in `specs/005-postgres-source/evidence/`
following the 004 environment-header rule.

## Validation rules (cross-entity)

- Every stream published by `streams()` has a reflected schema; every
  cursor column named in config exists in that schema with a mappable
  type — checked at open (fail fast, typed).
- A pushed batch's schema equals the published schema for that stream
  (engine-enforced on the structured path).
- Cursor JSON must round-trip: `decode(encode(state)) == state` for
  every supported cursor type (property-tested).
- No benchmark row without: same-session pair, dataset identity,
  gated/scoreboard status, measurement-first bar derivation (004
  protocol inherited).
