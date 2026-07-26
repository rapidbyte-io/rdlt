#!/usr/bin/env sh
# Cold-start embeddability check (instruments track). Relocated from the retired
# `cold-start` benchmark cell (archive commit 40841ab): the recorded hyperfine
# protocol, now a standalone gate rather than a matrix row. Asserts the release
# binary starts a one-row file -> duckdb pipeline in <= 40 ms median (floor
# 23.6 ms x 1.5). Exits non-zero on breach.
#
# QUIET MACHINE REQUIRED: startup latency is dominated by page-cache and
# scheduler state; a loaded machine inflates the median. Run it like the rest
# of the instruments track — nothing else competing for the CPU.
#
# This script builds nothing: it expects target/release/rdlt to exist already
# (run `make release`). hyperfine and python3 are prerequisites.
set -eu

BENCHES_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$BENCHES_DIR/.." && pwd)
# Honour CARGO_TARGET_DIR: a contributor who redirects cargo's output (a
# shared target dir, a faster disk) otherwise gets "release binary missing"
# immediately after a successful `make release`.
CLI="${CARGO_TARGET_DIR:-$REPO_ROOT/target}/release/rdlt"
BAR_MS=40

if [ ! -x "$CLI" ]; then
    echo "cold-start: release binary missing at $CLI — run \`make release\` first" >&2
    exit 1
fi
if ! command -v hyperfine >/dev/null 2>&1; then
    echo "cold-start: hyperfine not installed (instruments-track prerequisite)" >&2
    exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
    echo "cold-start: python3 not installed (needed to read the hyperfine export)" >&2
    exit 1
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# One-row source dataset + its file-source config.
printf '{"id":1,"name":"cold"}\n' >"$WORK/one-row.jsonl"
cat >"$WORK/files.yaml" <<YAML
streams:
  - name: cold
    format: jsonl
    path: "$WORK/one-row.jsonl"
YAML

# Render the standalone spec for this run's paths.
SPEC="$WORK/cold.yaml"
sed -e "s#@FILES@#$WORK/files.yaml#g" -e "s#@WORK@#$WORK#g" \
    "$BENCHES_DIR/cold-start/cold.yaml" >"$SPEC"

EXPORT="$WORK/hyperfine.json"
# warmups 3, runs 20, fresh workdir+db per run (the recorded protocol); -N skips
# the intermediate shell so we time the binary, not sh.
hyperfine -N \
    --warmup 3 --runs 20 \
    --prepare "rm -rf $WORK/.rdlt-cold $WORK/cold.duckdb" \
    --export-json "$EXPORT" \
    "$CLI run $SPEC" >&2

python3 - "$EXPORT" "$BAR_MS" <<'PY'
import json, sys
export, bar_ms = sys.argv[1], float(sys.argv[2])
with open(export) as f:
    median_ms = json.load(f)["results"][0]["median"] * 1000.0
print(f"cold-start: median {median_ms:.1f} ms (bar <= {bar_ms:.0f} ms absolute)")
if median_ms > bar_ms:
    print(f"cold-start: BREACH — {median_ms:.1f} ms > {bar_ms:.0f} ms", file=sys.stderr)
    sys.exit(1)
PY
