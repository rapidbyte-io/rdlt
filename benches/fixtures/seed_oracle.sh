#!/bin/sh
# Seed the Oracle bench fixture (032 cell oracle-to-pg-200k).
#
#   sh seed_oracle.sh <container> <host_port> <user> <password> <rows> <identity_out>
#
# Invoked as the `oracle` fixture's generate_sh, i.e. AFTER the harness has
# started the container and its own readiness probe (a TCP connect on the host
# port) has passed. That probe means nothing here: Oracle's listener accepts
# TCP long before FREEPDB1 registers with it, and a connect in that gap answers
# ORA-12514. So the REAL readiness wait lives here, in two phases, mirroring
# the connector's own fixture (crates/rdlt-connector-oracle/tests/cases/
# common.rs::await_service):
#
#   1. the image's "DATABASE IS READY TO USE!" line in the container log — the
#      CDB opened and gvenzl's setup scripts (which create the app user) ran;
#   2. THEN a real query as the app user through the listener, polled until it
#      answers. Only phase 2 proves the service is registered.
#
# It then creates EVENTS and seeds it with <rows> rows in ONE deterministic
# CONNECT BY insert, and writes the recorded dataset identity to
# <identity_out> (the file the fixture blake3-hashes).
#
# <rows> = 0 creates the table and seeds nothing — the shape bench-setup.sh's
# Airbyte leg needs, where `discover` reads metadata and never reads a row.
#
# Everything runs through `podman exec` into the container, so no Oracle client
# is needed on the host. Progress goes to stderr; the only stdout is whatever
# sqlplus is told to print.
set -eu

NAME=${1:?container name}
HOST_PORT=${2:?host port}
APP_USER=${3:?app user}
APP_PASS=${4:?app password}
ROWS=${5:?row count}
IDENTITY=${6:?identity output path}

SERVICE=FREEPDB1
# Budgets. slim-faststart is typically ready in 15-40 s; a loaded machine (or a
# cold page cache after a reboot) takes far longer, and an unbounded wait reads
# as a hung session rather than a failure.
LOG_TIMEOUT=${ORACLE_LOG_TIMEOUT:-600}
QUERY_TIMEOUT=${ORACLE_QUERY_TIMEOUT:-300}

log() { printf '[seed_oracle] %s\n' "$*" >&2; }
die() { printf '[seed_oracle] FATAL: %s\n' "$*" >&2; exit 1; }

ENGINE=$(command -v podman || command -v docker) ||
  die "no container engine (podman or docker) on PATH"

# The published port is the one the pipeline spec and the competitor arms dial.
# A registry/script disagreement about it would otherwise surface much later as
# a connection refusal with no hint at the cause.
mapped=$("$ENGINE" port "$NAME" 1521/tcp 2>/dev/null | head -n1) ||
  die "container $NAME is not running"
case "$mapped" in
  *:"$HOST_PORT") ;;
  "") die "container $NAME publishes no port for 1521/tcp" ;;
  *) die "container $NAME maps 1521/tcp to '$mapped', not the expected host port $HOST_PORT" ;;
esac

# --- phase 1: the image's readiness line ----------------------------------
log "waiting for the container log's readiness line (<= ${LOG_TIMEOUT}s)..."
waited=0
until "$ENGINE" logs "$NAME" 2>&1 | grep -q 'DATABASE IS READY TO USE!'; do
  [ "$waited" -lt "$LOG_TIMEOUT" ] ||
    die "$NAME never printed 'DATABASE IS READY TO USE!' within ${LOG_TIMEOUT}s \
(inspect it with \`$ENGINE logs $NAME\`)"
  sleep 2
  waited=$((waited + 2))
done
log "readiness line seen after ${waited}s"

# --- phase 2: a real query through the listener ---------------------------
# sqlplus exits 0 on a failed STATEMENT unless told otherwise, so every script
# below opens with WHENEVER SQLERROR EXIT SQL.SQLCODE; `-L` makes a refused
# login one attempt rather than an interactive prompt against a closed stdin.
sqlp() {
  "$ENGINE" exec -i "$NAME" sqlplus -S -L \
    "$APP_USER/$APP_PASS@//localhost:1521/$SERVICE"
}

log "polling a live query as $APP_USER@$SERVICE (<= ${QUERY_TIMEOUT}s)..."
waited=0
until printf 'WHENEVER SQLERROR EXIT SQL.SQLCODE\nSELECT 1 FROM DUAL;\nEXIT\n' |
      sqlp >/dev/null 2>&1; do
  [ "$waited" -lt "$QUERY_TIMEOUT" ] || {
    last=$(printf 'WHENEVER SQLERROR EXIT SQL.SQLCODE\nSELECT 1 FROM DUAL;\nEXIT\n' |
             sqlp 2>&1 || true)
    die "$SERVICE never answered a query within ${QUERY_TIMEOUT}s; last: $last"
  }
  sleep 2
  waited=$((waited + 2))
done
log "service answered after ${waited}s"

# --- the schema + the seed ------------------------------------------------
# Column-for-column the Oracle mirror of seed_pg.sql's `events`, so the two
# source cells differ in the SOURCE, not in the payload:
#   ACTIVE is NUMBER(1), not 23ai BOOLEAN — readable by the older Oracle
#   versions the connector also supports, and by Airbyte's source-oracle, which
#   predates 23ai BOOLEAN. It costs a source-read benchmark nothing.
#   TOKEN is VARCHAR2(36) holding hex — Oracle has no UUID type.
#   RATIO is BINARY_DOUBLE — the faithful mirror of pg's float8.
#   It was NUMBER(18,6) while the connector rode the pure-Rust driver, whose
#   row decoder had no arm for BINARY_DOUBLE or BINARY_FLOAT and handed back
#   `String::from_utf8_lossy` of the raw IEEE bytes: silent corruption that
#   surfaced only because Postgres refused the embedded NUL. The connector now
#   rides `oracle` (ODPI-C), which decodes both correctly (plan.md T005), so
#   the faithful type is restored.
#   There is deliberately NO LOB column: LOBs are read through a separate
#   per-value locator path whose cost is a different measurement, and mixing it
#   in would make this cell's number uninterpretable.
# Deterministic (no DBMS_RANDOM, no SYSDATE), so every session seeds a
# byte-identical table and the recorded identity below is stable.
log "creating EVENTS..."
sqlp <<'SQL' || die "creating EVENTS failed"
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET FEEDBACK OFF
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE EVENTS PURGE';
EXCEPTION
  WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
CREATE TABLE EVENTS (
  ID         NUMBER(19)               NOT NULL PRIMARY KEY,
  SMALL      NUMBER(10)               NOT NULL,
  BIG        NUMBER(19)               NOT NULL,
  RATIO         BINARY_DOUBLE             NOT NULL,
  AMOUNT     NUMBER(12,4)             NOT NULL,
  NAME       VARCHAR2(64)             NOT NULL,
  CODE       VARCHAR2(16)             NOT NULL,
  ACTIVE     NUMBER(1)                NOT NULL,
  CREATED_AT TIMESTAMP WITH TIME ZONE NOT NULL,
  BIRTHDAY   DATE                     NOT NULL,
  TOKEN      VARCHAR2(36)             NOT NULL,
  NOTE       VARCHAR2(32)
);
EXIT
SQL

# The insert is a SEPARATE script, skipped entirely at ROWS=0, because
# `CONNECT BY LEVEL <= 0` does NOT return zero rows — the hierarchy's root is
# always produced, so it returns ONE. Measured: the first cut ran the insert
# unconditionally and the schema-only path (bench-setup.sh's Airbyte discover
# leg) quietly seeded a single row while the identity file said `rows=0`.
if [ "$ROWS" -gt 0 ]; then
log "seeding $ROWS rows..."
sqlp <<SQL || die "seeding EVENTS failed"
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET FEEDBACK OFF
-- Pinned so the identity digest below cannot move with the session's locale
-- (the concatenations there convert NUMBERs with the session's separators).
ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,';
INSERT INTO EVENTS
SELECT LEVEL,
       MOD(LEVEL, 100000),
       LEVEL * 2654435761,
       LEVEL / 3,
       MOD(LEVEL, 99999999) / 10000,
       'user-' || LEVEL,
       'C-' || LPAD(TO_CHAR(MOD(LEVEL, 65536)), 6, '0'),
       CASE WHEN MOD(LEVEL, 3) = 0 THEN 1 ELSE 0 END,
       -- '+00:00', NOT 'UTC': a named region is stored on the wire as a REGION
       -- id, and the connector refuses those (measured here — the first seed
       -- used 'UTC' and every read failed with "Named timezone regions are not
       -- supported"). The pg mirror stores the same instants as timestamptz,
       -- so an explicit zero offset is the faithful shape as well as the
       -- readable one.
       FROM_TZ(TIMESTAMP '2026-01-01 00:00:00' + NUMTODSINTERVAL(MOD(LEVEL, 86400), 'SECOND'), '+00:00'),
       DATE '1970-01-01' + MOD(LEVEL, 20000),
       LOWER(RAWTOHEX(STANDARD_HASH('token-' || LEVEL, 'MD5'))),
       CASE WHEN MOD(LEVEL, 10) = 0 THEN NULL ELSE 'note-' || MOD(LEVEL, 1000) END
FROM DUAL CONNECT BY LEVEL <= $ROWS;
COMMIT;
EXIT
SQL
fi

# --- the recorded dataset identity ----------------------------------------
# Read back from the TABLE, never echoed from the argument: an identity that
# repeats what it was asked to seed cannot detect the seed going wrong, which
# is the one job it has. Order-independent (a SUM over per-row hashes), so it
# pins the CONTENT rather than a scan order Oracle is free to change. Plain
# `key=value` lines: the fixture hashes the FILE, and a human reading the
# artifact's fingerprint wants to see what moved when it changes.
log "recording the dataset identity to $IDENTITY..."
sqlp <<'SQL' > "$IDENTITY" || die "computing the dataset identity failed"
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMSPOOL ON LINESIZE 200
ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,';
SELECT 'rows=' || TO_CHAR(COUNT(*)) FROM EVENTS;
SELECT 'content_hash=' ||
       TO_CHAR(NVL(SUM(ORA_HASH(ID || '|' || NAME || '|' ||
                                TO_CHAR(AMOUNT, 'FM99999999990.0000') || '|' ||
                                NVL(NOTE, '~'))), 0))
FROM EVENTS;
EXIT
SQL

log "seeded: $(tr '\n' ' ' < "$IDENTITY")"
