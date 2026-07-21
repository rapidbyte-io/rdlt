#!/usr/bin/env bash
# Feature 005 US3: Postgres-source cells — BASELINE FIRST, then rdlt, same
# server, same seeded datasets (benches/baseline/seed_pg.sql; identity hashes
# printed at seed time — record them in the evidence artifact).
#
# Cells (5 runs each, medians recorded):
#   pg_wide  -> DuckDB   : dlt pyarrow backend GATED baseline;
#                          sqlalchemy + connectorx = scoreboard rows.
#   pg_wide  -> Postgres : same discipline.
#   pg_jsonb -> DuckDB   : scoreboard context (Json escape-hatch path).
#
# Needs: podman (or docker), a release rdlt CLI, quiet machine (004 P2).
set -euo pipefail
cd "$(dirname "$0")"
RUNS="${RUNS:-5}"
ENGINE=$(command -v podman || command -v docker)
RDLT="../target/release/rdlt"
DATA=$(mktemp -d)

PG_NAME=rdlt-bench-pg-src
"$ENGINE" rm -f "$PG_NAME" >/dev/null 2>&1 || true
"$ENGINE" run -d --name "$PG_NAME" -e POSTGRES_PASSWORD=postgres -p 5439:5432 postgres:16 >/dev/null
for i in $(seq 1 30); do "$ENGINE" exec "$PG_NAME" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
trap '"$ENGINE" rm -f "$PG_NAME" >/dev/null' EXIT

echo "== seeding datasets (identity hashes below go into the evidence record) =="
"$ENGINE" exec -i "$PG_NAME" psql -q -U postgres < baseline/seed_pg.sql

CONN_HOST="postgresql://postgres:postgres@127.0.0.1:5439/postgres"
CONN_LOCAL="postgresql://postgres:postgres@127.0.0.1:5439/postgres"

drop_dest_schemas() {
  "$ENGINE" exec "$PG_NAME" psql -q -U postgres -c \
    "DO \$\$ DECLARE s text; BEGIN FOR s IN SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'raw%' LOOP EXECUTE 'DROP SCHEMA ' || quote_ident(s) || ' CASCADE'; END LOOP; END \$\$;" >/dev/null
}

echo "== baseline: dlt sql_database -> duckdb (pyarrow GATED; others scoreboard) =="
"$ENGINE" build -q -t rdlt-baseline baseline/
for backend in pyarrow sqlalchemy connectorx; do
  for i in $(seq 1 "$RUNS"); do
    "$ENGINE" run --rm --network=host rdlt-baseline pipeline_pg_duckdb.py "$CONN_LOCAL" "$backend" pg_wide
  done
done
for i in $(seq 1 "$RUNS"); do
  "$ENGINE" run --rm --network=host rdlt-baseline pipeline_pg_duckdb.py "$CONN_LOCAL" pyarrow pg_jsonb
done

echo "== baseline: dlt sql_database -> postgres (pyarrow GATED; sqlalchemy scoreboard) =="
for backend in pyarrow sqlalchemy; do
  for i in $(seq 1 "$RUNS"); do
    drop_dest_schemas
    "$ENGINE" run --rm --network=host rdlt-baseline pipeline_pg_pg.py "$CONN_LOCAL" "$CONN_LOCAL" "$backend" pg_wide
  done
done

echo "== rdlt: pg_wide -> duckdb, pg_jsonb -> duckdb, pg_wide -> postgres =="
cat > "$DATA/wide.yaml" <<YAML
conn: "$CONN_HOST"
tables:
  - name: pg_wide
YAML
cat > "$DATA/jsonb.yaml" <<YAML
conn: "$CONN_HOST"
tables:
  - name: pg_jsonb
YAML
cat > "$DATA/wide-duck.yaml" <<YAML
pipeline: bench-pg-duck
workdir: $DATA/.rdlt-wd
source:
  postgres: {config: $DATA/wide.yaml}
destination:
  duckdb: {path: $DATA/wide.duckdb}
YAML
cat > "$DATA/jsonb-duck.yaml" <<YAML
pipeline: bench-pgj-duck
workdir: $DATA/.rdlt-jd
source:
  postgres: {config: $DATA/jsonb.yaml}
destination:
  duckdb: {path: $DATA/jsonb.duckdb}
YAML
cat > "$DATA/wide-pg.yaml" <<YAML
pipeline: bench-pg-pg
workdir: $DATA/.rdlt-wp
source:
  postgres: {config: $DATA/wide.yaml}
destination:
  postgres:
    conn: "host=127.0.0.1 port=5439 user=postgres password=postgres dbname=postgres"
    dataset: raw_rdlt
YAML
for i in $(seq 1 "$RUNS"); do
  rm -rf "$DATA/.rdlt-wd" "$DATA/wide.duckdb"
  /usr/bin/time -v "$RDLT" run "$DATA/wide-duck.yaml" 2>&1 >/dev/null | grep -E 'Elapsed|Maximum resident'
done
for i in $(seq 1 "$RUNS"); do
  rm -rf "$DATA/.rdlt-jd" "$DATA/jsonb.duckdb"
  /usr/bin/time -v "$RDLT" run "$DATA/jsonb-duck.yaml" 2>&1 >/dev/null | grep -E 'Elapsed|Maximum resident'
done
for i in $(seq 1 "$RUNS"); do
  drop_dest_schemas
  rm -rf "$DATA/.rdlt-wp"
  /usr/bin/time -v "$RDLT" run "$DATA/wide-pg.yaml" 2>&1 >/dev/null | grep -E 'Elapsed|Maximum resident'
done
echo "medians go to benches/RESULTS.md; identity + raw runs to specs/005-postgres-source/evidence/"
