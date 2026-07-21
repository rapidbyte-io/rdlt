#!/usr/bin/env bash
# Feature 010 T008 (FR-011/SC-007): the two refinement shapes, measured at
# the SQL level on the exact statements the strategies emit. 5 runs each
# inside BEGIN/ROLLBACK so every run sees identical data. Scoreboard, no
# gate.
#   1) scope-replace: DELETE by a 100k-row scope on a 10M-row scoped
#      target (indexed on the scope column, as a scoped deployment would)
#      vs the identity-only delete of the SAME 100k keys.
#   2) ordered dedup: DISTINCT ON with the dedup_sort key vs plain
#      last-wins over a 2x-duplicated 1M-row stage.
set -euo pipefail
ENGINE=$(command -v podman || command -v docker)
NAME=rdlt-bench-refine
"$ENGINE" rm -f "$NAME" >/dev/null 2>&1 || true
"$ENGINE" run -d --name "$NAME" -e POSTGRES_PASSWORD=postgres postgres:16 >/dev/null
trap '"$ENGINE" rm -f "$NAME" >/dev/null' EXIT
for i in $(seq 1 30); do "$ENGINE" exec "$NAME" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

psql() { "$ENGINE" exec -i "$NAME" psql -qX -U postgres "$@"; }

echo "== seeding: 10M-row scoped target (100 days x 100k), 2x-duplicated 1M-row stage =="
psql <<'SQL'
CREATE TABLE target (id BIGINT, day BIGINT, v TEXT);
INSERT INTO target SELECT i, i % 100, 'row-'||i FROM generate_series(1, 10000000) i;
-- mirrors the rdlt_ix_ index ensure_table auto-provisions for merge_key
CREATE INDEX rdlt_ix_target_day ON target (day);
CREATE INDEX rdlt_ix_target_id ON target (id);
CREATE TABLE stage_scope (id BIGINT, day BIGINT);
INSERT INTO stage_scope SELECT i, 7 FROM generate_series(7, 10000000, 100) i; -- day 7: 100k rows
CREATE TABLE stage_dupes (id BIGINT, seq BIGINT, arrival BIGSERIAL);
INSERT INTO stage_dupes (id, seq)
SELECT i % 1000000, i % 2 + 1 FROM generate_series(1, 2000000) i;             -- 1M ids x 2
ANALYZE;
SQL

timed() { # stdin = SQL statement to EXPLAIN ANALYZE inside BEGIN/ROLLBACK
  { echo "BEGIN;"; echo "EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON)"; cat; echo "ROLLBACK;"; } \
    | psql -t | grep -oP 'Execution Time: \K[0-9.]+'
}

echo "== scope-replace: 100k-row scope out of 10M (5 runs, ms) =="
for i in 1 2 3 4 5; do
  timed <<'SQL'
DELETE FROM target WHERE (day) IN (SELECT day FROM stage_scope WHERE day IS NOT NULL);
SQL
done
echo "-- identity-only delete of the same 100k keys (5 runs, ms):"
for i in 1 2 3 4 5; do
  timed <<'SQL'
DELETE FROM target WHERE (id) IN (SELECT id FROM stage_scope);
SQL
done

echo "== ordered dedup over a 2x-duplicated 1M-row stage (5 runs, ms) =="
echo "-- dedup_sort (seq DESC NULLS LAST, arrival tie-break):"
for i in 1 2 3 4 5; do
  timed <<'SQL'
SELECT count(*) FROM (SELECT DISTINCT ON (id) * FROM stage_dupes
                      ORDER BY id, seq DESC NULLS LAST, arrival DESC) d;
SQL
done
echo "-- last-wins (arrival only):"
for i in 1 2 3 4 5; do
  timed <<'SQL'
SELECT count(*) FROM (SELECT DISTINCT ON (id) * FROM stage_dupes
                      ORDER BY id, arrival DESC) d;
SQL
done
