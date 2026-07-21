"""Baseline: dlt cold start (feature 003 R28 — the ≤1/20th cell).

One-row pipeline to DuckDB. The timed window starts BEFORE `import dlt`: the
rdlt number it is compared against includes full process startup (binary load),
so the baseline's import cost belongs in its number too — that is the
user-perceived latency of a tiny sync. Interpreter boot itself (~30ms) is still
excluded; stated in RESULTS.md.

Emits JSON: import_seconds, pipeline_seconds, seconds (total).
"""

import json
import time

if __name__ == "__main__":
    t0 = time.monotonic()
    import dlt
    t1 = time.monotonic()
    pipe = dlt.pipeline(pipeline_name="baseline_cold_start",
                        destination="duckdb", dataset_name="raw", dev_mode=True)
    pipe.run([{"id": 1, "name": "one"}], table_name="events")
    t2 = time.monotonic()
    print(json.dumps({"import_seconds": t1 - t0, "pipeline_seconds": t2 - t1,
                      "seconds": t2 - t0}))
