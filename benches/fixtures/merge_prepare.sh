#!/bin/sh
# Strategy-comparison cell prepare (carried verbatim from
# run-merge-strategies.sh): fresh dataset schema, UNTIMED load 1 via the same
# rendered spec, then update 50% of source rows — the counted run that
# follows is the timed full-redelivery load 2.
# Usage: merge_prepare.sh <container> <cli> <spec> <dataset>
set -eu
NAME=$1 CLI=$2 SPEC=$3 DATASET=$4
ENGINE=$(command -v podman || command -v docker)
"$ENGINE" exec "$NAME" psql -qX -U postgres -c "DROP SCHEMA IF EXISTS $DATASET CASCADE" >/dev/null
"$CLI" run "$SPEC" >/dev/null          # load 1 (untimed)
"$ENGINE" exec "$NAME" psql -qX -U postgres -c \
  "UPDATE pg_wide SET name = 'upd-' || id WHERE id % 2 = 0" >/dev/null
