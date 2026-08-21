"""Baseline: nested JSONL on S3 -> Postgres via pinned dlt's filesystem source
+ postgres destination (018 e2e cell s3jsonl-to-pg-200k). Reads the RUSTFS `raw`
bucket and writes the `bench` dataset (schema) of the per-product destination
database dest_dlt, full replace. dlt shreds nested `tags` into child tables the
same way rdlt does.

Usage: pipeline_s3jsonl_pg.py <endpoint> <access> <secret> \
           <src_bucket> <src_glob> <dest_pg_conn_url>
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb.
"""

import json
import os
import resource
import sys
import time

endpoint, access, secret, src_bucket, src_glob, dest = sys.argv[1:7]

# s3fs / fsspec + dlt filesystem source credentials for a custom endpoint.
os.environ["AWS_ACCESS_KEY_ID"] = access
os.environ["AWS_SECRET_ACCESS_KEY"] = secret
os.environ["AWS_ALLOW_HTTP"] = "true"
os.environ["SOURCES__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = access
os.environ["SOURCES__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = secret
os.environ["SOURCES__FILESYSTEM__CREDENTIALS__ENDPOINT_URL"] = endpoint

import dlt
from dlt.sources.filesystem import filesystem, read_jsonl

if __name__ == "__main__":
    reader = (
        filesystem(bucket_url=f"s3://{src_bucket}", file_glob=src_glob) | read_jsonl()
    )
    reader.apply_hints(table_name="events")

    started = time.monotonic()
    pipe = dlt.pipeline(
        pipeline_name="bench_s3jsonl_pg",
        destination=dlt.destinations.postgres(dest),
        dataset_name="bench",
        dev_mode=False,
    )
    pipe.run(reader, write_disposition="replace")
    elapsed = time.monotonic() - started

    with pipe.sql_client() as client:
        rows = client.execute_sql("SELECT count(*) FROM bench.events")[0][0]
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": rows, "seconds": elapsed,
                      "rows_per_s": rows / elapsed if elapsed else 0,
                      "peak_rss_kb": peak_kb}))
