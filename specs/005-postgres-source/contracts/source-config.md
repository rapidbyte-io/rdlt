# Contract: Postgres Source Declarative Configuration

**Feature**: 005-postgres-source | **Date**: 2026-07-20

The YAML document accepted by `PostgresSource::from_yaml` (and via the
CLI's `[source.postgres] config = <path>`). Same contract style as the
REST source: one document a platform can render and validate; unknown
fields are errors.

```yaml
# required
conn: "postgresql://user:pass@host:5432/db?sslmode=require"

# optional, defaults shown
schema: public
include_views: false
batch_target_bytes: 8388608     # 8 MiB
batch_max_rows: 65536
retry:
  max_attempts: 3
  base_ms: 250

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

1. `conn` must parse; connection failure after the bounded retry policy
   is a typed connect error.
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
- Mid-stream failures are never auto-retried; connect/table-boundary
  retries follow `retry`.
