# Quickstart: DuckDB Destination Completeness

## Use the options on DuckDB (the point of the feature)

```yaml
destination:
  duckdb:
    path: analytics.duckdb
    merge_strategy: upsert          # delete_insert | upsert | scd2
    tables:
      events: {hard_delete: _deleted, dedup_sort: [seq]}
      daily:  {merge_key: [day]}
```

Same vocabulary, same validation, same typed errors as postgres —
documented once in the README destination-options reference.

## Verify

```bash
cargo nextest run -p rdlt-connector-sqlcore          # shapes + validation
cargo nextest run -p rdlt-connector-duckdb           # strategy cells + probes
cargo nextest run -p rdlt-connector-postgres         # MUST be untouched-green
cargo nextest run -p rdlt -E 'test(differential)'    # cross-dest oracle (container)
cargo nextest run -p rdlt-connector-duckdb --features failpoints  # sweeps
cargo llvm-cov nextest -p rdlt-connector-duckdb      # coverage floor ≥80%
```

Golden-SQL pins (SM4): `cargo nextest run -p rdlt-connector-postgres
-E 'test(golden_sql)'` — byte-identical before/after the extraction.

## Benchmarks (scoreboard, 012 harness)

```bash
TARGET='duckdb-strategy-*' make bench
```

## The rules

`contracts/shared-merge-core.md` (SM1–SM8): one core, dialects own SQL
text only, typed capability gaps over approximations, postgres provably
unchanged, differential equivalence, 011-standard verification.
