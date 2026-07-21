#!/usr/bin/env bash
# Feature 008 T009 (FR-009/SC-005): the unindexed-vs-indexed merge DELETE,
# measured at the SQL level on the exact statement shape the delete-insert
# strategy emits. 1M-row target, 500k-key stage; 5 runs each inside
# BEGIN/ROLLBACK so every run sees identical data. Scoreboard, not a gate.
set -euo pipefail
ENGINE=$(command -v podman || command -v docker)
NAME=rdlt-bench-ixmerge
"$ENGINE" rm -f "$NAME" >/dev/null 2>&1 || true
"$ENGINE" run -d --name "$NAME" -e POSTGRES_PASSWORD=postgres postgres:16 >/dev/null
trap '"$ENGINE" rm -f "$NAME" >/dev/null' EXIT
for i in $(seq 1 30); do "$ENGINE" exec "$NAME" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

psql() { "$ENGINE" exec -i "$NAME" psql -qX -U postgres "$@"; }

echo "== seeding 10M-row target; deltas: 5k keys (incremental) and 5M keys (half-table) =="
psql <<'SQL'
CREATE TABLE target (id BIGINT, v TEXT);
INSERT INTO target SELECT i, 'row-'||i FROM generate_series(1, 10000000) i;
CREATE TABLE stage_small (id BIGINT);
INSERT INTO stage_small SELECT i FROM generate_series(1, 10000000, 2000) i; -- 5k keys
CREATE TABLE stage_half (id BIGINT);
INSERT INTO stage_half SELECT i FROM generate_series(1, 10000000, 2) i;     -- 5M keys
ANALYZE;
SQL

run_delete() { # $1 = stage table
  psql -t <<SQL | grep -oP 'Execution Time: \K[0-9.]+'
BEGIN;
EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON)
DELETE FROM target WHERE (id) IN (SELECT id FROM $1);
ROLLBACK;
SQL
}

echo "== INCREMENTAL regime (5k-key delta vs 10M rows) =="
echo "-- unindexed (5 runs, ms):"
for i in 1 2 3 4 5; do run_delete stage_small; done
psql -c 'CREATE INDEX rdlt_ix_bench ON target (id); ANALYZE target;'
echo "-- indexed (5 runs, ms):"
for i in 1 2 3 4 5; do run_delete stage_small; done

echo "== HALF-TABLE regime (5M-key delta — planner may ignore the index) =="
echo "-- indexed present (5 runs, ms):"
for i in 1 2 3 4 5; do run_delete stage_half; done
psql -c 'DROP INDEX rdlt_ix_bench;'
echo "-- unindexed (5 runs, ms):"
for i in 1 2 3 4 5; do run_delete stage_half; done
