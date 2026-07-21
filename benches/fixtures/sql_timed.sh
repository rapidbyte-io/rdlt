#!/bin/sh
# SQL-level cell runner: EXPLAIN ANALYZE inside BEGIN/ROLLBACK, printing the
# server-side Execution Time in ms as the last line (cell timing =
# "stdout_ms" — the exact statistic the 008/010 scoreboards recorded; wall
# clock would add client/exec overhead that isn't the measured claim).
# Usage: sql_timed.sh <container> <statement.sql>
set -eu
NAME=$1 SQL=$2
ENGINE=$(command -v podman || command -v docker)
{ echo "BEGIN;"; echo "EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON)"; cat "$SQL"; echo "ROLLBACK;"; } \
  | "$ENGINE" exec -i "$NAME" psql -qXt -U postgres \
  | grep -oP 'Execution Time: \K[0-9.]+'
