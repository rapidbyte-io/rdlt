#!/usr/bin/env bash
# One-shot competitor setup for the e2e matrix (`TARGET=setup make bench`).
#
# Leg 1 — dlt: build the pinned baseline image (idempotent, cached).
# Leg 2 — Airbyte: create/refresh the oracle-to-pg-200k connection. This leg
#   provisions a zero-row RDLT.EVENTS schema in Oracle for source discovery and
#   an empty Postgres dest_airbyte database, runs setup.py, and tears both
#   services down. Skipped with guidance (exit 0) when no abctl cluster is
#   reachable — the bench then runs 2-way and records the Airbyte arm as
#   Missing{reason}, never silently.
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
cd "$(dirname "$0")/../.."

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
for port in 5439 15210; do
  if command -v ss >/dev/null && ss -tln | grep -q ":$port "; then
    echo "port $port is already in use — a bench session or stale fixture is" \
         "running; stop it first (podman ps)" >&2
    exit 1
  fi
done

IDENTITY=$(mktemp /tmp/rdlt-bench-setup-oracle.XXXXXX.txt)
cleanup() {
  "$ENGINE" rm -f rdlt-bench-pg rdlt-bench-oracle \
    >/dev/null 2>&1 || true
  rm -f "$IDENTITY"
}
trap cleanup EXIT

"$ENGINE" run -d --name rdlt-bench-pg -p 5439:5432 \
  -e POSTGRES_PASSWORD=postgres postgres:16 >/dev/null
# Oracle starts beside Postgres so its slower boot overlaps the destination
# readiness check. seed_oracle.sh performs its own readiness wait.
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
"$ENGINE" exec rdlt-bench-pg createdb -U postgres dest_airbyte
# Schema only (0 rows): discover reads metadata. The script owns the Oracle
# readiness wait — the listener accepts TCP long before FREEPDB1 registers.
sh benches/harness/fixtures/seed_oracle.sh rdlt-bench-oracle 15210 RDLT rdlt-bench 0 \
  "$IDENTITY"

python3 benches/competitors/airbyte/setup.py

echo "== setup complete — run the matrix with: TARGET=e2e make bench =="
