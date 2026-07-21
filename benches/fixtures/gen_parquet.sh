#!/bin/sh
# Parquet dataset for the passthrough cells: rdlt itself re-encodes the
# flagship jsonl (the dataset-generator role it has had since feature 002).
# Usage: gen_parquet.sh <release-cli> <data-dir> <benches-dir>
set -eu
CLI=$1 DATA=$2 BENCHES=$3
python3 "$BENCHES/fixtures/gen_jsonl.py" 200000 "$DATA/rows.jsonl"
cat > "$DATA/files.yaml" <<YAML
streams:
  - name: events
    format: jsonl
    path: "$DATA/rows.jsonl"
YAML
cat > "$DATA/to-parquet.yaml" <<YAML
pipeline: bench-genpq
workdir: $DATA/.rdlt-genpq
source:
  file: {config: $DATA/files.yaml}
destination:
  parquet: {path: $DATA/pq}
YAML
"$CLI" run "$DATA/to-parquet.yaml" >/dev/null
cat > "$DATA/pq-files.yaml" <<YAML
streams:
  - name: events
    format: parquet
    path: "$DATA/pq/events/*.parquet"
YAML
