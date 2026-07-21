"""Baseline: parquet -> (parquet | duckdb) via pinned dlt, fed pyarrow tables
(dlt's arrow-native fast path — its fastest ingestion route, compared honestly).

Usage: pipeline_parquet.py <input_dir_glob> <parquet|duckdb>
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb.
"""

import glob
import json
import resource
import sys
import time

import dlt
import pyarrow.parquet as pq

if __name__ == "__main__":
    pattern = sys.argv[1] if len(sys.argv) > 1 else "/data/pq-dataset/events/*.parquet"
    target = sys.argv[2] if len(sys.argv) > 2 else "parquet"

    files = sorted(glob.glob(pattern))
    started = time.monotonic()
    tables = [pq.read_table(f) for f in files]
    rows = sum(t.num_rows for t in tables)

    if target == "duckdb":
        pipe = dlt.pipeline(pipeline_name="baseline_pq_duck", destination="duckdb",
                            dataset_name="raw")
        pipe.run(tables, table_name="events")
    else:
        pipe = dlt.pipeline(
            pipeline_name="baseline_pq_pq",
            destination=dlt.destinations.filesystem(bucket_url="file:///tmp/dlt-pq-out"),
            dataset_name="raw",
        )
        pipe.run(tables, table_name="events", loader_file_format="parquet")

    elapsed = time.monotonic() - started
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": rows, "seconds": elapsed,
                      "rows_per_s": rows / elapsed, "peak_rss_kb": peak_kb}))
