"""The flagship nested-record dataset (identical shape since feature 002).

Usage: gen_jsonl.py <rows> <out.jsonl>
"""
import json
import sys

n, path = int(sys.argv[1]), sys.argv[2]
with open(path, "w") as f:
    for i in range(n):
        f.write(json.dumps({"id": i, "name": f"user-{i}", "score": i * 0.5,
                            "profile": {"city": "NYC", "zip": 10001 + i % 100},
                            "tags": [{"label": "a"}, {"label": "b"}]}) + "\n")
