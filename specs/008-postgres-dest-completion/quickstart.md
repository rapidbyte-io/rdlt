# Quickstart: Postgres Destination Completion

## Native types, automatically

Nothing to configure — decimals now land as `numeric(p,s)`, JSON as
`jsonb`, UUIDs as `uuid`, and required columns as `NOT NULL`:

```sql
-- downstream, after a sync:
SELECT SUM(amount) FROM raw.orders;          -- exact numeric math
SELECT doc->>'city' FROM raw.events;         -- native JSON paths
SELECT * FROM raw.users WHERE id = 'a0eebc99-…'::uuid;
```

Tables created by earlier versions keep their text columns (never
silently retyped); a `rdlt::lossy` warning names the column and the
migration paths.

## Upsert merge

```rust
let dest = Postgres::connect(conn)
    .dataset("raw")
    .options(PgDestOptions {
        merge_strategy: MergeStrategy::Upsert,   // matched keys update in place
        ..Default::default()
    });
```

```toml
# CLI
[destination.postgres]
conn = "…"
dataset = "raw"
merge_strategy = "upsert"
```

The unique index on the merge key is created automatically; a table
that already violates uniqueness fails with an error naming the key
columns.

## CDC-shaped hard deletes

```toml
[destination.postgres.tables.orders]
hard_delete = "is_deleted"    # flagged rows delete instead of upserting
```

## SCD2 history

```toml
[destination.postgres.tables.customers]
merge_strategy = "scd2"
[destination.postgres.tables.customers.scd2]
absent = "keep"               # or "retire" for full feeds
# valid_from / valid_to column names configurable; defaults shown:
# valid_from = "_rdlt_valid_from", valid_to = "_rdlt_valid_to"
```

```sql
-- as-of query:
SELECT * FROM raw.customers
WHERE id = 42
  AND _rdlt_valid_from <= '2026-06-01'
  AND COALESCE(_rdlt_valid_to, 'infinity') > '2026-06-01';
```

## Verify

```bash
cargo nextest run -p rdlt-postgres          # incl. type-fidelity, strategies, scd2
make check                                  # full gate; bars within tolerance
benches/run-pg.sh                           # + merge-heavy / index scoreboard cells
```
