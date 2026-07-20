# Quickstart: Postgres SQL Source Connector

**Feature**: 005-postgres-source | **Date**: 2026-07-20

## Run a snapshot locally (once implemented)

```bash
# 1. a postgres to read from (host podman via distrobox-host-exec on the ref machine)
podman run -d --name pg-src -e POSTGRES_PASSWORD=rdlt -p 5432:5432 postgres:16
psql postgresql://postgres:rdlt@127.0.0.1:5432/postgres \
  -c "CREATE TABLE t(id int8 primary key, name text, ts timestamptz default now());
      INSERT INTO t SELECT i, 'row-'||i, now() FROM generate_series(1,1000) i;"

# 2. source config (contracts/source-config.md)
cat > /tmp/pg.yaml <<'YAML'
conn: "postgresql://postgres:rdlt@127.0.0.1:5432/postgres"
tables:
  - name: t
    cursor: { column: ts }
YAML

# 3. pipeline spec + run
cat > /tmp/pg.toml <<'TOML'
pipeline = "pg-demo"
workdir = "/tmp/.rdlt-pg"
[source.postgres]
config = "/tmp/pg.yaml"
[destination.duckdb]
path = "/tmp/pg.duckdb"
TOML
cargo run --release -p rdlt-cli -- run /tmp/pg.toml
# re-run → incremental: only rows with ts past the committed watermark
```

## Verify

```bash
duckdb /tmp/pg.duckdb -c "SELECT count(*) FROM t;"
cargo nextest run -p rdlt-source-postgres            # conformance + incremental + differential
make test TARGET=sweep                               # crash sweep incl. new pg fail points
```

## Benchmarks (US3 — baseline FIRST, 004 protocol)

```bash
benches/baseline/seed_pg.sh          # deterministic datasets, identity printed + recorded
# dlt side first (pinned; pyarrow backend = gated baseline, others scoreboard):
podman run --rm --network=host rdlt-baseline pipeline_pg_duckdb.py <conn> pyarrow
podman run --rm --network=host rdlt-baseline pipeline_pg_duckdb.py <conn> sqlalchemy
# then rdlt cells; bars are set from measurement via version-policy entries.
```

Records land in `benches/RESULTS.md` (+ evidence in
`specs/005-postgres-source/evidence/`, 004 header rule).
