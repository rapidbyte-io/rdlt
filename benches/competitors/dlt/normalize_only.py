"""Baseline: dlt's NORMALIZE stage alone (feature 003 R15/T015 — the ≥20× cell).

Extract is performed first (untimed); only `pipeline.normalize()` — dlt's
analog of rdlt's shred stage (relational decomposition, lineage, typing) — is
timed. No destination I/O on either side of the comparison.

Usage: normalize_only.py <rows.jsonl>
Emits JSON: rows, normalize_seconds, rows_per_s, peak_rss_kb.
"""

import json
import resource
import sys
import time

import dlt

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/data/rows.jsonl"

    @dlt.resource(name="events")
    def events():
        with open(path) as f:
            for line in f:
                yield json.loads(line)

    pipe = dlt.pipeline(pipeline_name="baseline_normalize_only",
                        destination="duckdb", dataset_name="raw")
    pipe.extract(events)          # staged to disk, NOT timed

    started = time.monotonic()
    pipe.normalize()              # THE measured stage
    elapsed = time.monotonic() - started

    with open(path) as f:
        source_rows = sum(1 for _ in f)
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": source_rows, "normalize_seconds": elapsed,
                      "seconds": elapsed,  # harness convention: the self-timed statistic
                      "rows_per_s": source_rows / elapsed, "peak_rss_kb": peak_kb}))
