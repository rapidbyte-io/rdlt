#!/usr/bin/env bash
# One-shot competitor setup for the e2e matrix (`TARGET=setup make bench`).
#
# Leg 1 — dlt: build the pinned baseline image (idempotent, cached).
# Leg 2 — Airbyte: create/refresh the bench connections. Airbyte's discover
#   must READ the source schemas, so this leg brings up the three fixture
#   containers with the harness's own seeds, runs the module's setup.py
#   against them, and always tears them down. Skipped with guidance (exit 0)
#   when no abctl cluster is reachable — the bench then runs 2-way and records
#   the Airbyte arms as Missing{reason}, never silently.
#
#   The Oracle throwaway is seeded with ZERO rows: discover reads metadata
#   only, and Oracle's boot is the slowest thing here without also paying for
#   200k rows nobody reads.
#
# Prerequisites for the Airbyte leg (one-time, see
# benches/competitors/airbyte/README.md): `abctl local install` on rootless
# podman; setup.py itself re-enforces the two cluster deltas (ingress-nginx
# scaled to 0, node pids-limit raised).
set -euo pipefail
cd "$(dirname "$0")/.."

ENGINE=$(command -v podman || command -v docker)
KC="${KUBECONFIG:-$HOME/.airbyte/abctl/abctl.kubeconfig}"
# No hardcoded fallback path: a guessed binary location fails later and
# further from the cause than a clear message here does.
KUBECTL=$(command -v kubectl || true)

echo "== dlt: building rdlt-baseline image =="
"$ENGINE" build -q -t rdlt-baseline benches/competitors/dlt/

if [ ! -f "$KC" ] || ! KUBECONFIG="$KC" "$KUBECTL" get ns airbyte-abctl >/dev/null 2>&1; then
  echo "== airbyte: SKIPPED — no reachable abctl cluster =="
  echo "   The matrix will run 2-way (Airbyte arms record Missing{reason})."
  echo "   To enable: benches/competitors/airbyte/README.md, then re-run this."
  exit 0
fi

echo "== airbyte: seeding throwaway fixtures + creating connections =="
for port in 5439 19110 15210; do
  if command -v ss >/dev/null && ss -tln | grep -q ":$port "; then
    echo "port $port is already in use — a bench session or stale fixture is" \
         "running; stop it first (podman ps)" >&2
    exit 1
  fi
done

ROWS=$(mktemp /tmp/rdlt-bench-setup-rows.XXXXXX.jsonl)
IDENTITY=$(mktemp /tmp/rdlt-bench-setup-oracle.XXXXXX.txt)
cleanup() {
  "$ENGINE" rm -f rdlt-bench-pg rdlt-bench-rustfs rdlt-bench-oracle \
    >/dev/null 2>&1 || true
  rm -f "$ROWS" "$IDENTITY"
}
trap cleanup EXIT

"$ENGINE" run -d --name rdlt-bench-pg -p 5439:5432 \
  -e POSTGRES_PASSWORD=postgres postgres:16 >/dev/null
"$ENGINE" run -d --name rdlt-bench-rustfs -p 19110:9000 \
  -e RUSTFS_ACCESS_KEY=rdlt-bench -e RUSTFS_SECRET_KEY=rdlt-bench-secret \
  docker.io/rustfs/rustfs:1.0.0-beta.11 >/dev/null
# Started detached here so its (slow) boot overlaps the postgres and S3 seeds
# below; seed_oracle.sh does its own readiness wait, so nothing races.
"$ENGINE" run -d --name rdlt-bench-oracle -p 15210:1521 \
  -e ORACLE_PASSWORD=rdlt-bench-sys -e APP_USER=RDLT \
  -e APP_USER_PASSWORD=rdlt-bench \
  docker.io/gvenzl/oracle-free:23.26.2-slim-faststart >/dev/null

# Bounded: an unbounded `until` loop against a container that will never
# become ready hangs the setup with no output at all, which reads as a slow
# machine rather than a failure. 120s is far beyond a healthy postgres start.
PG_READY_TIMEOUT=120
waited=0
until "$ENGINE" exec rdlt-bench-pg pg_isready -U postgres >/dev/null 2>&1; do
  if [ "$waited" -ge "$PG_READY_TIMEOUT" ]; then
    echo "bench-setup: postgres (rdlt-bench-pg) did not become ready within \
${PG_READY_TIMEOUT}s — inspect it with \`$ENGINE logs rdlt-bench-pg\`" >&2
    exit 1
  fi
  sleep 1
  waited=$((waited + 1))
done
"$ENGINE" exec -i rdlt-bench-pg psql -q -U postgres -f - \
  < benches/fixtures/seed_pg.sql >/dev/null
python3 benches/fixtures/gen_jsonl.py 200000 "$ROWS"
python3 benches/fixtures/seed_s3.py http://127.0.0.1:19110 \
  rdlt-bench rdlt-bench-secret raw landed/rows.jsonl "$ROWS"
python3 benches/fixtures/s3_bucket.py http://127.0.0.1:19110 \
  rdlt-bench rdlt-bench-secret lake
# Schema only (0 rows): discover reads metadata. The script owns the Oracle
# readiness wait — the listener accepts TCP long before FREEPDB1 registers.
sh benches/fixtures/seed_oracle.sh rdlt-bench-oracle 15210 RDLT rdlt-bench 0 \
  "$IDENTITY"

python3 benches/competitors/airbyte/setup.py

echo "== setup complete — run the matrix with: TARGET=e2e make bench =="
