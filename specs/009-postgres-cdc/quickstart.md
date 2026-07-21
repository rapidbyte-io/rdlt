# Quickstart: Postgres CDC

## One-time database setup

```sql
-- server: wal_level = logical (restart required on self-managed;
-- managed providers expose a setting)
ALTER TABLE orders REPLICA IDENTITY DEFAULT;   -- fine with a PK
-- tables with TOAST-able columns you update:
ALTER TABLE documents REPLICA IDENTITY FULL;
```

## The pipeline

```yaml
# source yaml
conn: "postgresql://etl@db.internal/app?sslmode=verify-full&sslrootcert=/etc/ca.pem"
cdc:
  slot: orders_mirror
  publication: orders_pub
  create_if_missing: true
tables:
  - name: orders
  - name: customers
```

```toml
# pipeline TOML — the recommended composition (validation warns if absent)
write_mode = { merge = { key = ["id"] } }
[destination.postgres]
conn = "…"
dataset = "mirror"
merge_strategy = "upsert"
[destination.postgres.tables.orders]
hard_delete = "_rdlt_deleted"
[destination.postgres.tables.customers]
hard_delete = "_rdlt_deleted"
```

Run 1 snapshots every CDC table under one consistent view and starts
the change feed. Every later run catches up — inserts, updates, and
DELETES (rows actually disappear at the destination). Schedule it, or
set `mode: tail` to keep applying continuously until cancelled.

## Operations

- Every completed run emits replication lag (`lag_bytes`, plus
  `lag_seconds` when the server has `track_commit_timestamp = on`) as a
  structured event on the `rdlt::cdc` tracing target — embedders
  subscribe, no log-scraping (same seam as `rdlt::lossy`).
- rdlt never drops your slot or publication. If the server discarded
  the slot's backlog (WAL retention), the typed error says exactly
  that and prescribes a fresh snapshot.
- A table without a usable replica identity fails loudly at enable
  time, naming the table and the fix.

## Verify

```bash
cargo nextest run -p rdlt-postgres -E 'binary(cdc)'   # equality cycle + ops matrix
cargo nextest run -p rdlt-postgres --features rdlt-postgres/failpoints -E 'binary(cdc_crash_sweep)'
benches/run-cdc.sh                                     # scoreboard cells
```
