"""Baseline: Postgres table -> DuckDB via pinned dlt's sql_database source
(feature 005 US3). Self-timed in-process like every other baseline row.

Backend is a parameter: pyarrow (dlt's fastest documented pure-dlt config —
the GATED baseline), sqlalchemy (dlt default — scoreboard), connectorx
(Rust reader — scoreboard context).

Usage: pipeline_pg_duckdb.py <pg_conn_url> <backend> [table]
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb, backend, table.
"""

import json
import resource
import sys
import time

import dlt
from dlt.sources.sql_database import sql_database

if __name__ == "__main__":
    conn = sys.argv[1]
    backend = sys.argv[2] if len(sys.argv) > 2 else "pyarrow"
    table = sys.argv[3] if len(sys.argv) > 3 else "pg_wide"

    source = sql_database(
        credentials=conn,
        table_names=[table],
        backend=backend,
    )

    started = time.monotonic()
    pipe = dlt.pipeline(
        pipeline_name=f"baseline_pg_duckdb_{backend}_{table}",
        destination="duckdb",
        dataset_name="raw",
        dev_mode=False,
    )
    pipe.run(source)
    elapsed = time.monotonic() - started

    with pipe.sql_client() as client:
        rows = client.execute_sql(f"SELECT count(*) FROM raw.{table}")[0][0]
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": rows, "seconds": elapsed,
                      "rows_per_s": rows / elapsed, "peak_rss_kb": peak_kb,
                      "backend": backend, "table": table}))
