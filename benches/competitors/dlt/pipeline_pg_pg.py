"""Baseline: Postgres table -> Postgres via pinned dlt's sql_database source
(feature 005 US3). Same protocol as pipeline_pg_duckdb.py; destination is a
separate dataset (schema) on the same server.

Usage: pipeline_pg_pg.py <src_pg_conn_url> <dest_pg_conn_url> <backend> [table]
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb, backend, table.
"""

import json
import resource
import sys
import time

import dlt
from dlt.sources.sql_database import sql_database

if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    backend = sys.argv[3] if len(sys.argv) > 3 else "pyarrow"
    table = sys.argv[4] if len(sys.argv) > 4 else "pg_wide"

    source = sql_database(
        credentials=src,
        table_names=[table],
        backend=backend,
    )

    started = time.monotonic()
    pipe = dlt.pipeline(
        pipeline_name=f"baseline_pg_pg_{backend}_{table}",
        destination=dlt.destinations.postgres(dest),
        dataset_name="raw_dlt",
        dev_mode=False,
    )
    pipe.run(source)
    elapsed = time.monotonic() - started

    with pipe.sql_client() as client:
        rows = client.execute_sql(f"SELECT count(*) FROM raw_dlt.{table}")[0][0]
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": rows, "seconds": elapsed,
                      "rows_per_s": rows / elapsed, "peak_rss_kb": peak_kb,
                      "backend": backend, "table": table}))
