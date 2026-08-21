"""Baseline: nested JSONL on S3 -> Parquet on S3 via pinned dlt's filesystem
source + filesystem destination (018 e2e cell s3jsonl-to-s3parquet-200k). Reads
the RUSTFS `raw` bucket and writes parquet under lake/<dest_prefix>, full
replace. RUSTFS is S3-compatible with a custom endpoint, so both the source
reader (s3fs) and the destination are pointed at it via dlt's filesystem
credentials env config.

Usage: pipeline_s3jsonl_s3parquet.py <endpoint> <access> <secret> \
           <src_bucket> <src_glob> <dest_bucket> <dest_prefix>
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb.
"""

import json
import os
import resource
import sys
import time

endpoint, access, secret, src_bucket, src_glob, dest_bucket, dest_prefix = sys.argv[1:8]

# s3fs / fsspec + dlt filesystem credentials for a custom (non-AWS) endpoint.
os.environ["AWS_ACCESS_KEY_ID"] = access
os.environ["AWS_SECRET_ACCESS_KEY"] = secret
os.environ["AWS_ALLOW_HTTP"] = "true"
for scope in ("SOURCES__FILESYSTEM__CREDENTIALS", "DESTINATION__FILESYSTEM__CREDENTIALS"):
    os.environ[f"{scope}__AWS_ACCESS_KEY_ID"] = access
    os.environ[f"{scope}__AWS_SECRET_ACCESS_KEY"] = secret
    os.environ[f"{scope}__ENDPOINT_URL"] = endpoint

import dlt
from dlt.sources.filesystem import filesystem, read_jsonl


def loaded_rows(pipe, table):
    counts = pipe.last_trace.last_normalize_info.row_counts
    if table in counts:
        return counts[table]
    return sum(v for k, v in counts.items() if not k.startswith("_dlt"))


if __name__ == "__main__":
    reader = (
        filesystem(bucket_url=f"s3://{src_bucket}", file_glob=src_glob) | read_jsonl()
    )
    reader.apply_hints(table_name="events")

    started = time.monotonic()
    pipe = dlt.pipeline(
        pipeline_name="bench_s3jsonl_s3parquet",
        destination=dlt.destinations.filesystem(
            bucket_url=f"s3://{dest_bucket}/{dest_prefix}"
        ),
        dataset_name="bench",
        dev_mode=False,
    )
    pipe.run(reader, loader_file_format="parquet", write_disposition="replace")
    elapsed = time.monotonic() - started

    rows = loaded_rows(pipe, "events")
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": rows, "seconds": elapsed,
                      "rows_per_s": rows / elapsed if elapsed else 0,
                      "peak_rss_kb": peak_kb}))
