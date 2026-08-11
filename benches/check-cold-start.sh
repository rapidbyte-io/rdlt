#!/usr/bin/env sh
# Cold-start embeddability check (instruments track). Relocated from the retired
# `cold-start` benchmark cell (archive commit 40841ab): the recorded hyperfine
# protocol, now a standalone gate rather than a matrix row. Asserts the release
# binary starts a one-row file -> duckdb pipeline in <= 40 ms median. Exits
# non-zero on breach.
#
# RE-DERIVED 2026-08-11 (the D1 swap, 043): the CLI now SPAWNS its connectors,
# so the measured pipeline covers the engine start PLUS two connector
# spawn+handshakes (file source, duckdb destination) and every batch crossing
# the connector protocol — end-to-end including spawn is what "cold start"
# means from here on. The 40 ms bar is UNCHANGED and deliberately so: the
# embeddability claim survives the architecture. Measured on the swapped
# tree, quiet machine (loadavg 0.87): 27.1 ms median (mean 27.2 ms +/- 0.7 ms,
# range 25.8-28.8 ms, 20 runs) — inside the band the in-process protocol
# recorded, consistent with 041's 1.81 ms spawn->handshake measurement
# (two spawns are ~3.6 ms).
#
# QUIET MACHINE REQUIRED: startup latency is dominated by page-cache and
# scheduler state; a loaded machine inflates the median. Run it like the rest
# of the instruments track — nothing else competing for the CPU.
#
# This script builds nothing: it expects target/release/rdlt AND the file and
# duckdb connector bins to exist already (run `make release` and
# `make connector-bins`). hyperfine and python3 are prerequisites.
set -eu

BENCHES_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$BENCHES_DIR/.." && pwd)
# Honour CARGO_TARGET_DIR: a contributor who redirects cargo's output (a
# shared target dir, a faster disk) otherwise gets "release binary missing"
# immediately after a successful `make release`.
RELEASE_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}/release"
CLI="$RELEASE_DIR/rdlt"
BAR_MS=40

if [ ! -x "$CLI" ]; then
    echo "cold-start: release binary missing at $CLI — run \`make release\` first" >&2
    exit 1
fi
# The pipeline's rich `file:`/`duckdb:` spellings resolve their connector
# bins off PATH, so point discovery at the same release dir the CLI came
# from — the measurement must spawn the shipped shape, never whatever
# happens to be installed.
for bin in rdlt-connector-file rdlt-connector-duckdb; do
    if [ ! -x "$RELEASE_DIR/$bin" ]; then
        echo "cold-start: $bin missing at $RELEASE_DIR/$bin — run \`make connector-bins\` first" >&2
        exit 1
    fi
done
PATH="$RELEASE_DIR:$PATH"
export PATH
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
