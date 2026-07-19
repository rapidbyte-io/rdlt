#!/usr/bin/env bash
# End-to-end comparison harness: BASELINE FIRST, then rdlt, same dataset.
# Usage: ./run-e2e.sh [rows]   (default 1,000,000)
set -euo pipefail
cd "$(dirname "$0")"
ROWS="${1:-1000000}"
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
echo "== baseline (pinned dlt in container; measured FIRST) =="
docker build -q -t rdlt-baseline baseline/
docker run --rm -v "$DATA":/data rdlt-baseline /data/rows.jsonl | tee baseline-result.json
echo "== rdlt =="
echo "(jsonl file source lands post-v1; use the criterion microbench + CLI REST run for now:"
echo "   cargo bench -p rdlt-engine --bench shred )"
