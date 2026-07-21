#!/usr/bin/env bash
# End-to-end comparison harness: BASELINE FIRST, then rdlt, same dataset.
# Usage: ./run-e2e.sh [rows]   (default 200,000)
# Needs: podman (or docker), python3, and a release build of the rdlt CLI:
#   cargo build --release -p rdlt-cli --features rest,duckdb,file,parquet
set -euo pipefail
cd "$(dirname "$0")"
ROWS="${1:-200000}"
ENGINE=$(command -v podman || command -v docker)
RDLT="../target/release/rdlt"
DATA=$(mktemp -d)

echo "generating $ROWS nested jsonl rows into $DATA/rows.jsonl"
python3 - "$ROWS" "$DATA/rows.jsonl" <<'PY'
import json, sys
n, path = int(sys.argv[1]), sys.argv[2]
with open(path, "w") as f:
    for i in range(n):
        f.write(json.dumps({"id": i, "name": f"user-{i}", "score": i * 0.5,
                            "profile": {"city": "NYC", "zip": 10001 + i % 100},
                            "tags": [{"label": "a"}, {"label": "b"}]}) + "\n")
PY

echo "== baseline 1: dlt jsonl -> duckdb (measured FIRST) =="
"$ENGINE" build -q -t rdlt-baseline baseline/
"$ENGINE" run --rm -v "$DATA":/data:z rdlt-baseline pipeline_jsonl_duckdb.py /data/rows.jsonl | tee baseline-result.json

echo "== rdlt: jsonl -> duckdb (bundled file source via CLI) =="
cat > "$DATA/jsonl-files.yaml" <<YAML
streams:
  - name: events
    format: jsonl
    path: "$DATA/rows.jsonl"
YAML
cat > "$DATA/jsonl.yaml" <<YAML
pipeline: bench-jsonl
workdir: $DATA/.rdlt-jsonl
source:
  file: {config: $DATA/jsonl-files.yaml}
destination:
  duckdb: {path: $DATA/out.duckdb}
YAML
/usr/bin/time -v "$RDLT" run "$DATA/jsonl.yaml" --report "$DATA/jsonl-report.json" 2>&1 | grep -E 'Elapsed|Maximum resident'

echo "== rdlt: re-encode dataset as parquet (rdlt itself is the generator) =="
cat > "$DATA/to-parquet.yaml" <<YAML
pipeline: bench-genpq
workdir: $DATA/.rdlt-genpq
source:
  file: {config: $DATA/jsonl-files.yaml}
destination:
  parquet: {path: $DATA/pq}
YAML
"$RDLT" run "$DATA/to-parquet.yaml" >/dev/null

echo "== baseline 2: dlt parquet -> parquet / duckdb (arrow-native fast path) =="
"$ENGINE" run --rm -v "$DATA/pq":/data/pq:ro,z rdlt-baseline pipeline_parquet.py '/data/pq/events/*.parquet' parquet
"$ENGINE" run --rm -v "$DATA/pq":/data/pq:ro,z rdlt-baseline pipeline_parquet.py '/data/pq/events/*.parquet' duckdb

echo "== rdlt: parquet -> parquet and parquet -> duckdb (Arrow passthrough) =="
cat > "$DATA/pq-files.yaml" <<YAML
streams:
  - name: events
    format: parquet
    path: "$DATA/pq/events/*.parquet"
YAML
cat > "$DATA/pq-to-pq.yaml" <<YAML
pipeline: bench-pq-pq
workdir: $DATA/.rdlt-pq-pq
source:
  file: {config: $DATA/pq-files.yaml}
destination:
  parquet: {path: $DATA/pq-out}
YAML
cat > "$DATA/pq-to-duck.yaml" <<YAML
pipeline: bench-pq-duck
workdir: $DATA/.rdlt-pq-duck
source:
  file: {config: $DATA/pq-files.yaml}
destination:
  duckdb: {path: $DATA/pq-out.duckdb}
YAML
/usr/bin/time -v "$RDLT" run "$DATA/pq-to-pq.yaml" --report "$DATA/pq-pq-report.json" 2>&1 | grep -E 'Elapsed|Maximum resident'
/usr/bin/time -v "$RDLT" run "$DATA/pq-to-duck.yaml" --report "$DATA/pq-duck-report.json" 2>&1 | grep -E 'Elapsed|Maximum resident'

echo "== cold start (one-row pipeline) =="
# GATED protocol (feature 004, contracts/measurement-protocol.md P3): the
# gated statistic is the MEDIAN of >= 20 hyperfine runs after >= 3 warmups,
# warm FS cache, fresh workdir+db per run, on the reference machine. Bar:
# <= 40 ms ABSOLUTE (derivation: specs/004-close-perf-misses/evidence/
# resolution-cold-start.md). The dlt cold-start comparison below is a
# SCOREBOARD number only — it can never change the gated verdict.
printf '{"id":1,"name":"one"}\n' > "$DATA/one.jsonl"
cat > "$DATA/cold-files.yaml" <<YAML
streams:
  - name: events
    format: jsonl
    path: "$DATA/one.jsonl"
YAML
cat > "$DATA/cold.yaml" <<YAML
pipeline: cold
workdir: $DATA/.rdlt-cold
source:
  file: {config: $DATA/cold-files.yaml}
destination:
  duckdb: {path: $DATA/cold.duckdb}
YAML
if command -v hyperfine >/dev/null; then
  hyperfine -N --warmup 3 --runs 20 \
    --prepare "rm -rf $DATA/.rdlt-cold $DATA/cold.duckdb" \
    "$RDLT run $DATA/cold.yaml"
else
  echo "hyperfine missing — falling back to /usr/bin/time (10 ms quantized, NOT the gated protocol)"
  for i in 1 2 3 4 5; do
    rm -rf "$DATA/.rdlt-cold" "$DATA/cold.duckdb"
    /usr/bin/time -f "rdlt cold: %e s" "$RDLT" run "$DATA/cold.yaml" >/dev/null
  done
fi
for i in 1 2 3 4 5; do
  "$ENGINE" run --rm rdlt-baseline cold_start.py 2>/dev/null | tail -1
done
echo "(REST->Postgres cell: separate recipe — start crates/rdlt-source-rest/examples/mock_api"
echo " + a postgres container, then benches/baseline/pipeline_rest_pg.py vs the CLI; see RESULTS.md)"

echo "reports in $DATA/*-report.json (elapsed_ms is the self-timed number)"
