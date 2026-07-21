#!/bin/sh
# CDC change-apply cell prepare (carried verbatim from run-cdc.sh): fresh
# slot/publication/dataset, reseed the 1M-row table, UNTIMED snapshot via the
# same rendered spec, then the ~500k-change backlog (400k updates / 50k
# deletes / 50k inserts). The counted run that follows is the timed catch-up.
# Usage: cdc_prepare.sh <container> <cli> <spec> <benches-dir> <tag>
set -eu
NAME=$1 CLI=$2 SPEC=$3 BENCHES=$4 TAG=$5
ENGINE=$(command -v podman || command -v docker)
P() { "$ENGINE" exec "$NAME" psql -qX -U postgres -c "$1" >/dev/null; }
P "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'cdc_${TAG}'" || true
P "DROP PUBLICATION IF EXISTS cdc_${TAG}_pub" || true
P "DROP SCHEMA IF EXISTS cdc_${TAG} CASCADE"
"$ENGINE" exec -i "$NAME" psql -q -U postgres < "$BENCHES/fixtures/seed_pg.sql" >/dev/null
"$CLI" run "$SPEC" >/dev/null 2>&1     # snapshot (untimed)
P "UPDATE pg_wide SET name = 'upd-' || id, small = small + 1 WHERE id % 10 < 4"
P "DELETE FROM pg_wide WHERE id % 20 = 19"
P "INSERT INTO pg_wide SELECT id + 1000000, small, big, ratio, amount,
   'new-' || id, code, active, created_at, birthday, token, note
   FROM pg_wide WHERE id % 20 = 7 AND id <= 1000000"
