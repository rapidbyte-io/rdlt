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

```yaml
# pipeline.yaml — the recommended composition (validation warns if absent)
write_mode: {merge: {key: [id]}}
destination:
  postgres:
    conn: "…"
    dataset: mirror
    merge_strategy: upsert
    tables:
      orders: {hard_delete: _rdlt_deleted}
      customers: {hard_delete: _rdlt_deleted}
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
- The slot's acknowledged position advances once per run, to positions
  the destination durably committed — so it trails one run behind. A
  LONG-LIVED tail therefore accumulates WAL retention for its whole
  life (a warning fires on the `rdlt::cdc` target past 256 MiB): cycle
  tail runs periodically, or use catch-up mode on a cron for durable
  long-lived pipelines — every completed run reclaims retention.
- `TRUNCATE` on a published table is a typed error (truncation does not
  replicate as row deletes). Recovery: reset the stream's pipeline
  state AND re-initialize the destination table — the fresh snapshot
  starts past the truncation.
- A table without a usable replica identity fails loudly at enable
  time, naming the table and the fix.

## Verify

```bash
cargo nextest run -p rdlt-postgres -E 'binary(cdc)'   # equality cycle + ops matrix
cargo nextest run -p rdlt-postgres --features rdlt-postgres/failpoints -E 'binary(cdc_crash_sweep)'
benches/run-cdc.sh                                     # scoreboard cells
```
