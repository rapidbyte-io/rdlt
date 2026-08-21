"""Baseline: Postgres table -> Parquet on S3 via pinned dlt's sql_database
source + filesystem destination (018 e2e cell pg-to-s3parquet-1m). Extracts with
connectorx (variant `dlt`) or pyarrow (variant `dlt-pyarrow`) and writes parquet
under lake/<dest_prefix> on the RUSTFS store, full replace.

Usage: pipeline_pg_s3parquet.py <src_pg_conn_url> <backend> <table> \
           <endpoint> <access> <secret> <dest_bucket> <dest_prefix>
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb, backend, table.
"""

import json
import os
import resource
import sys
import time

src, backend, table, endpoint, access, secret, dest_bucket, dest_prefix = sys.argv[1:9]

# dlt filesystem destination credentials for the custom (non-AWS) endpoint.
os.environ["AWS_ACCESS_KEY_ID"] = access
os.environ["AWS_SECRET_ACCESS_KEY"] = secret
os.environ["AWS_ALLOW_HTTP"] = "true"
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = access
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = secret
os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL"] = endpoint

import dlt
from dlt.sources.sql_database import sql_database

if __name__ == "__main__":
    source = sql_database(credentials=src, table_names=[table], backend=backend)

    started = time.monotonic()
    pipe = dlt.pipeline(
        pipeline_name=f"bench_pg_s3parquet_{backend}",
        destination=dlt.destinations.filesystem(
            bucket_url=f"s3://{dest_bucket}/{dest_prefix}"
        ),
        dataset_name="bench",
        dev_mode=False,
    )
    pipe.run(source, loader_file_format="parquet", write_disposition="replace")
    elapsed = time.monotonic() - started

    counts = pipe.last_trace.last_normalize_info.row_counts
    rows = counts.get(table, sum(v for k, v in counts.items() if not k.startswith("_dlt")))
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": rows, "seconds": elapsed,
                      "rows_per_s": rows / elapsed if elapsed else 0,
                      "peak_rss_kb": peak_kb, "backend": backend, "table": table}))
