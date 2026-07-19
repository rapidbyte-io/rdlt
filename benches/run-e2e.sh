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
cat > "$DATA/jsonl.toml" <<TOML
pipeline = "bench-jsonl"
workdir = "$DATA/.rdlt-jsonl"
[source.file]
config = "$DATA/jsonl-files.yaml"
[destination.duckdb]
path = "$DATA/out.duckdb"
TOML
/usr/bin/time -v "$RDLT" run "$DATA/jsonl.toml" --report "$DATA/jsonl-report.json" 2>&1 | grep -E 'Elapsed|Maximum resident'

echo "== rdlt: re-encode dataset as parquet (rdlt itself is the generator) =="
cat > "$DATA/to-parquet.toml" <<TOML
pipeline = "bench-genpq"
workdir = "$DATA/.rdlt-genpq"
[source.file]
config = "$DATA/jsonl-files.yaml"
[destination.parquet]
path = "$DATA/pq"
TOML
"$RDLT" run "$DATA/to-parquet.toml" >/dev/null

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
cat > "$DATA/pq-to-pq.toml" <<TOML
pipeline = "bench-pq-pq"
workdir = "$DATA/.rdlt-pq-pq"
[source.file]
config = "$DATA/pq-files.yaml"
[destination.parquet]
path = "$DATA/pq-out"
TOML
cat > "$DATA/pq-to-duck.toml" <<TOML
pipeline = "bench-pq-duck"
workdir = "$DATA/.rdlt-pq-duck"
[source.file]
config = "$DATA/pq-files.yaml"
[destination.duckdb]
path = "$DATA/pq-out.duckdb"
TOML
/usr/bin/time -v "$RDLT" run "$DATA/pq-to-pq.toml" --report "$DATA/pq-pq-report.json" 2>&1 | grep -E 'Elapsed|Maximum resident'
/usr/bin/time -v "$RDLT" run "$DATA/pq-to-duck.toml" --report "$DATA/pq-duck-report.json" 2>&1 | grep -E 'Elapsed|Maximum resident'

echo "reports in $DATA/*-report.json (elapsed_ms is the self-timed number)"
