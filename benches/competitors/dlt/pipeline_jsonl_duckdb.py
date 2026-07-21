"""Baseline: jsonl -> DuckDB via pinned dlt. Emits rows/s + peak RSS on stdout."""
import json, resource, sys, time
import dlt

def rows(path):
    with open(path) as f:
        for line in f:
            yield json.loads(line)

if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "/data/rows.jsonl"
    started = time.monotonic()
    pipe = dlt.pipeline(pipeline_name="baseline", destination="duckdb", dataset_name="raw")
    info = pipe.run(rows(src), table_name="events")
    elapsed = time.monotonic() - started
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    count = sum(1 for _ in open(src))
    print(json.dumps({"rows": count, "seconds": elapsed,
                      "rows_per_s": count / elapsed, "peak_rss_kb": peak_kb}))
