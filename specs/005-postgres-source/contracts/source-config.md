# Contract: Postgres Source Declarative Configuration

**Feature**: 005-postgres-source | **Date**: 2026-07-20

ONE document shape, three entry points, shared validation:
`from_yaml(&str)` (human/CLI files), `from_json(&str)` (JSON text), and
`from_value(serde_json::Value)` — the EMBEDDER entry point: a platform
(rapidbyte) holding connector configs as JSON documents validated
against the connector's declared config schema (`ConnectorSpec`) passes
the Value directly, no string round-trip. The CLI picks YAML or JSON by
file extension (`.json`). Same contract style as the REST source:
unknown fields are errors everywhere.

```yaml
# required
conn: "postgresql://user:pass@host:5432/db"  # TLS: see 006 contracts/tls-policy.md (full sslmode matrix)

# optional, defaults shown
schema: public
include_views: false
batch_target_bytes: 8388608     # 8 MiB
batch_max_rows: 65536
# NOTE: there is deliberately no retry configuration — retry policy is
# engine-owned (SPI clauses S3/E5). The source classifies errors as
# Transient (engine retries with backoff) or Fatal.

# absent => discover ALL tables in `schema`
tables:
  - name: orders                # bare name; `schema` owns qualification
    cursor:
      column: updated_at        # must exist on the table (validated at open)
      initial_value: "2026-01-01T00:00:00Z"   # optional, typed literal
      boundary: closed          # closed (>=, default, deduped) | open (>)
      direction: max            # max (default) | min
      end_value: null           # optional upper bound
      nulls: exclude            # exclude (default) | include
    primary_key: [id]           # optional override of reflected PK
    included_columns: []        # mutually exclusive with excluded_columns
  - name: customers             # snapshot-only stream (no cursor)
```

## Validation rules (typed config errors at open — never silent)

1. `conn` must parse (parse failure = Fatal config error); connection
   failures classify as Transient — the ENGINE retries with backoff
   (clauses S3/E5), the source never loops.
2. `tables[].name` must exist in the reflected `schema` (with
   `include_views` honored); schema-qualified names in `name` are
   rejected.
3. `cursor.column` must exist on the table and map to a cursor-capable
   `LogicalType` (Int64, Decimal, Utf8, TimestampTz, TimestampNaive,
   Date, Time, Uuid — per the type-mapping contract).
4. `included_columns`/`excluded_columns` are mutually exclusive; names
   must exist; a selection leaving zero data columns is an error.
5. `initial_value`/`end_value` must parse as the cursor column's type.
6. Unknown fields anywhere → error (`deny_unknown_fields`).

## Semantics bound by this contract

- One stream per selected table, named by the bare table name.
- Streams publish `structured: true` schemas from reflection; the
  engine's structured path (schema mapping + policies) applies.
- `boundary: closed` re-fetches watermark-equal rows and dedups them
  via `boundary_keys` (PK, else canonical row hash) — exactly-once
  delivery at the boundary. `boundary: open` skips dedup and is only
  safe for strictly monotonic cursors (documented).
- Watermark advances only on committed loads and never regresses.
- Incremental reads are cursor-ordered and checkpoint mid-stream on
  cursor-value completion — a retried/resumed read continues from the
  last committed mid-table checkpoint, never the table start.
- Failures classify per SPI S3 (Transient/Fatal); the engine owns all
  retry loops, and resume-from-committed-cursor (E6/S1) makes retried
  reads double-apply-safe.
