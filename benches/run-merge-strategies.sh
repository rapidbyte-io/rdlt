#!/usr/bin/env bash
# Feature 008 T014 (FR-013): merge-heavy e2e — delete-insert vs upsert on a
# 1M-row pg→pg pipeline where load 2 re-delivers everything with 50% of rows
# changed. Timed statistic: load 2 wall time, 5 runs per strategy, medians.
# Scoreboard only; the gated pg→pg bars are untouched.
set -euo pipefail
cd "$(dirname "$0")"
RUNS="${RUNS:-5}"
ENGINE=$(command -v podman || command -v docker)
RDLT="../target/release/rdlt"
NAME=rdlt-bench-strat
"$ENGINE" rm -f "$NAME" >/dev/null 2>&1 || true
"$ENGINE" run -d --name "$NAME" -e POSTGRES_PASSWORD=postgres -p 5441:5432 postgres:16 >/dev/null
trap '"$ENGINE" rm -f "$NAME" >/dev/null' EXIT
for i in $(seq 1 30); do "$ENGINE" exec "$NAME" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
"$ENGINE" exec -i "$NAME" psql -q -U postgres < baseline/seed_pg.sql
CONN="postgresql://postgres:postgres@127.0.0.1:5441/postgres"
DATA=$(mktemp -d)

cat > "$DATA/src.yaml" <<YAML
conn: "$CONN"
tables:
  - name: pg_wide
YAML

make_toml() { # $1 strategy
  cat > "$DATA/$1.toml" <<TOML
pipeline = "strat-$1"
workdir = "$DATA/.rdlt-$1"
write_mode = { merge = { key = ["id"] } }
[source.postgres]
config = "$DATA/src.yaml"
[destination.postgres]
conn = "host=127.0.0.1 port=5441 user=postgres password=postgres dbname=postgres"
dataset = "strat_$1"
merge_strategy = "$1"
TOML
}
make_toml delete_insert
make_toml upsert

for strategy in delete_insert upsert; do
  echo "== $strategy: load-2 wall time ($RUNS runs) =="
  for i in $(seq 1 "$RUNS"); do
    "$ENGINE" exec "$NAME" psql -q -U postgres -c \
      "DROP SCHEMA IF EXISTS strat_$strategy CASCADE" >/dev/null
    rm -rf "$DATA/.rdlt-$strategy"
    "$RDLT" run "$DATA/$strategy.toml" >/dev/null       # load 1 (untimed)
    "$ENGINE" exec "$NAME" psql -q -U postgres -c \
      "UPDATE pg_wide SET name = 'upd-$i-' || id WHERE id % 2 = 0" >/dev/null
    /usr/bin/time -f "%e" "$RDLT" run "$DATA/$strategy.toml" 2>&1 >/dev/null | tail -1
  done
done
