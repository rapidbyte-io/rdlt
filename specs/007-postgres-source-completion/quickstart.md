# Quickstart: Postgres Source Completion (pre-CDC)

## Connect to a cert-authenticated database (mTLS)

```yaml
conn: "postgresql://etl@db.internal/app"
tls:
  mode: verify_full
  root_cert: /etc/rdlt/ca.pem
  client_cert: /etc/rdlt/client.pem
  client_key: /etc/rdlt/client.key
```

Or paste the libpq URL you already have — it just works:

```yaml
conn: "postgresql://etl@db.internal/app?sslmode=verify-full&sslrootcert=/etc/rdlt/ca.pem&sslcert=/etc/rdlt/client.pem&sslkey=/etc/rdlt/client.key"
```

The destination takes the same policy (`Postgres::tls(...)` / CLI TOML
`tls = {...}`).

## Capture late-arriving rows (cursor lag)

```yaml
tables:
  - name: orders
    cursor:
      column: updated_at
      lag: "5m"          # re-scan 5 minutes behind the watermark each run
```

Pair with Merge write mode for exact totals (the table's primary key
drives dedup — feature 006 keyed merge):

```toml
# pipeline TOML
write_mode = { merge = { key = ["id"] } }
```

Under Append, rows inside the window re-deliver each run (documented
at-least-once). Lag requires a closed boundary and a primary key.

## Fail on NULL cursors; inclusive backfill windows

```yaml
cursor:
  column: updated_at
  nulls: error           # NULL cursor value = typed failure (data contract)
```

```yaml
cursor:
  column: id
  initial_value: "1"
  end_value: "1000"
  end_bound: inclusive   # load [1, 1000], boundary row included
```

## Operational bits

- Every rdlt connection shows `application_name = rdlt` in
  `pg_stat_activity` (override via the standard conn-string param).
- Discovery excludes partition AND classic-INHERITS children (rows
  arrive once, via the parent). List a child explicitly under
  `tables:` to read it as its own stream. Foreign tables are not
  discovered.

## Verify

```bash
cargo nextest run -p rdlt-postgres            # incl. mTLS matrix, lag, discovery cells
make check                                    # full gate; perf unchanged (SC-008)
```
