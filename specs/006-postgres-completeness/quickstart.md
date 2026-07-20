# Quickstart: Postgres Completeness — Parity + TLS

**Feature**: 006-postgres-completeness | **Date**: 2026-07-20

## TLS to a managed/production Postgres (once implemented)

```yaml
conn: "postgresql://app:secret@db.internal:5432/prod"
tls:
  mode: verify_full            # the production recommendation
  root_cert: /etc/rdlt/ca.pem  # omit to use the platform trust store
tables:
  - name: orders
    cursor: { column: updated_at }
```

`sslmode=require` in the conn string alone also works (encrypt, no
identity check — libpq semantics, documented loudly). The Postgres
DESTINATION takes the same `tls` options in the CLI TOML.

## Hints + query streams

```yaml
tables:
  - name: events
    type_hints:
      raw_ts: timestamp_tz     # text column carrying ISO timestamps
      big_id: utf8             # deliberate textual landing
queries:
  - name: order_totals
    sql: "SELECT o.id, o.updated_at, sum(i.amount) AS total FROM orders o JOIN order_items i ON i.order_id=o.id GROUP BY 1,2"
    cursor: { column: updated_at }
    primary_key: [id]
```

## Upserts

```toml
# CLI pipeline spec
write_mode = { merge = { key = ["id"] } }   # keyed structured merge —
# accepted when the stream declares a key and the destination declares
# the merge capability (DuckDB, Postgres); parquet keeps its rejection.
```

## Verify

```bash
cargo nextest run -p rdlt-pg-tls -p rdlt-source-postgres        # + TLS matrix (container)
make test TARGET=sweep                                          # sweeps incl. Merge mode, armed-fire pins
cargo test --doc --workspace
# schemas: each source's ConnectorSpec now carries config_schema —
# round-trip tests validate documented examples against it.
```
