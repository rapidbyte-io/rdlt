#!/bin/sh
# Strategy-comparison cell prepare (the run-merge-strategies.sh protocol):
# fresh dataset schema, UNTIMED load 1 via the same rendered spec, then
# update 50% of source rows — the counted run that follows is the timed
# full-redelivery load 2. RUN must be per-run unique ({{run}}): a constant
# suffix makes runs 2..N re-write identical values, silently measuring a
# 0%-changed workload (review finding 1).
# Usage: merge_prepare.sh <container> <cli> <spec> <dataset> <run>
set -eu
NAME=$1 CLI=$2 SPEC=$3 DATASET=$4 RUN=$5
ENGINE=$(command -v podman || command -v docker)
"$ENGINE" exec "$NAME" psql -qX -U postgres -c "DROP SCHEMA IF EXISTS $DATASET CASCADE" >/dev/null
"$CLI" run "$SPEC" >/dev/null          # load 1 (untimed)
"$ENGINE" exec "$NAME" psql -qX -U postgres -c \
  "UPDATE pg_wide SET name = 'upd-$RUN-' || id WHERE id % 2 = 0" >/dev/null
