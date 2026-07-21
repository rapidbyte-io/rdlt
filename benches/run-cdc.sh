#!/usr/bin/env bash
# Feature 009 T014 (R11/SC-005): CDC scoreboard cells — no gate.
#   1) change-apply throughput: 1M-row pg_wide snapshot, then a 500k-change
#      backlog (400k updates, 50k deletes, 50k inserts); timed statistic is
#      the catch-up run's wall time. Fresh slot/publication/mirror per run.
#   2) catch-up latency: quiet-to-caught-up on a small delta (1k updates),
#      warm slot, timed per run.
# 5 runs each, medians recorded in RESULTS.md. Recommended composition
# (merge+upsert+hard_delete) via the CLI — the shipped path, not a harness.
set -euo pipefail
cd "$(dirname "$0")"
RUNS="${RUNS:-5}"
ENGINE=$(command -v podman || command -v docker)
RDLT="../target/release/rdlt"
[ -x "$RDLT" ] || { echo "build first: cargo build --release -p rdlt-cli" >&2; exit 1; }
NAME=rdlt-bench-cdc
PORT=5442
"$ENGINE" rm -f "$NAME" >/dev/null 2>&1 || true
"$ENGINE" run -d --name "$NAME" -e POSTGRES_PASSWORD=postgres -p $PORT:5432 \
  postgres:16 -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8 >/dev/null
trap '"$ENGINE" rm -f "$NAME" >/dev/null' EXIT
for i in $(seq 1 30); do "$ENGINE" exec "$NAME" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
CONN="postgresql://postgres:postgres@127.0.0.1:$PORT/postgres"
DATA=$(mktemp -d)
PSQL() { "$ENGINE" exec "$NAME" psql -q -U postgres -c "$1" >/dev/null; }

make_pipeline() { # $1 tag
  cat > "$DATA/src-$1.yaml" <<YAML
conn: "$CONN"
cdc:
  slot: cdc_$1
  publication: cdc_${1}_pub
  create_if_missing: true
tables:
  - name: pg_wide
YAML
  cat > "$DATA/$1.yaml" <<YAML
pipeline: cdc-$1
workdir: $DATA/.rdlt-$1
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: $DATA/src-$1.yaml}
destination:
  postgres:
    conn: "host=127.0.0.1 port=$PORT user=postgres password=postgres dbname=postgres"
    dataset: cdc_$1
    merge_strategy: upsert
    tables:
      pg_wide: {hard_delete: _rdlt_deleted}
YAML
}
make_pipeline tp
make_pipeline lat

echo "== change-apply throughput: 1M snapshot + ~500k-change catch-up ($RUNS runs) =="
for i in $(seq 1 "$RUNS"); do
  PSQL "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'cdc_tp'"
  PSQL "DROP PUBLICATION IF EXISTS cdc_tp_pub"
  PSQL "DROP SCHEMA IF EXISTS cdc_tp CASCADE"
  rm -rf "$DATA/.rdlt-tp"
  "$ENGINE" exec -i "$NAME" psql -q -U postgres < baseline/seed_pg.sql >/dev/null
  "$RDLT" run "$DATA/tp.yaml" >/dev/null 2>&1                     # snapshot (untimed)
  PSQL "UPDATE pg_wide SET name = 'upd-' || id, small = small + 1 WHERE id % 10 < 4"
  PSQL "DELETE FROM pg_wide WHERE id % 20 = 19"
  PSQL "INSERT INTO pg_wide SELECT id + 1000000, small, big, ratio, amount,
        'new-' || id, code, active, created_at, birthday, token, note
        FROM pg_wide WHERE id % 20 = 7 AND id <= 1000000"
  /usr/bin/time -f "%e" "$RDLT" run "$DATA/tp.yaml" 2>&1 >/dev/null | tail -1
done

echo "== catch-up latency: quiet-to-caught-up on a 1k-update delta ($RUNS runs) =="
PSQL "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'cdc_lat'"
PSQL "DROP PUBLICATION IF EXISTS cdc_lat_pub"
PSQL "DROP SCHEMA IF EXISTS cdc_lat CASCADE"
rm -rf "$DATA/.rdlt-lat"
"$ENGINE" exec -i "$NAME" psql -q -U postgres < baseline/seed_pg.sql >/dev/null
"$RDLT" run "$DATA/lat.yaml" >/dev/null 2>&1                      # snapshot (untimed)
# Settle to steady state: the peek walks WAL from the slot's CONFIRMED
# position, which trails one run behind (ack design) — the first runs after
# a snapshot re-walk the engine's own 1M-row mirror writes (same database).
# Steady state is reached once acks pass that backlog.
for i in 1 2 3 4 5; do "$RDLT" run "$DATA/lat.yaml" >/dev/null 2>&1; done
for i in $(seq 1 "$RUNS"); do
  PSQL "UPDATE pg_wide SET small = small + 1 WHERE id <= 1000"
  /usr/bin/time -f "%e" "$RDLT" run "$DATA/lat.yaml" 2>&1 >/dev/null | tail -1
done
