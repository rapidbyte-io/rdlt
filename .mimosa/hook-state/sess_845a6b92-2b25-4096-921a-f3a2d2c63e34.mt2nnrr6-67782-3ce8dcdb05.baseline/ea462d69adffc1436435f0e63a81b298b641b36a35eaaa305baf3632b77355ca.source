"""Baseline: Postgres -> Postgres keep-in-sync by id via pinned dlt's
sql_database source + postgres destination (018 e2e cell pg-to-pg-dedup-1m,
research D-08). Two loads into ONE destination table `events` in the `bench`
dataset of dest_dlt, deduped by id (write_disposition="merge"):

  LOAD 1 (untimed setup) — the initial 1M rows from <load1_table> (events).
  LOAD 2 (measured)      — full re-delivery of the 50%-changed <load2_table>
                           (events_v2), merged by id over LOAD 1's table.

Only LOAD 2 is timed, matching the rdlt arm (whose LOAD 1 runs via the cell's
prepare_sh). backend is connectorx (variant `dlt`) or pyarrow (`dlt-pyarrow`).

Usage: pipeline_pg_pg_dedup.py <src_conn_url> <dest_conn_url> <backend> \
           <load1_table> <load2_table>
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb, backend.
"""

import json
import resource
import sys
import time

import dlt
from dlt.sources.sql_database import sql_database

DEST_TABLE = "events"


def deliver(pipe, src, backend, table):
    """Merge `table` into the single dest table `events`, deduped by id."""
    source = sql_database(credentials=src, table_names=[table], backend=backend)
    source.resources[table].apply_hints(table_name=DEST_TABLE, primary_key="id")
    pipe.run(source, write_disposition="merge")


if __name__ == "__main__":
    src = sys.argv[1]
    dest = sys.argv[2]
    backend = sys.argv[3] if len(sys.argv) > 3 else "connectorx"
    load1_table = sys.argv[4] if len(sys.argv) > 4 else "events"
    load2_table = sys.argv[5] if len(sys.argv) > 5 else "events_v2"

    pipe = dlt.pipeline(
        pipeline_name=f"bench_pg_pg_dedup_{backend}",
        destination=dlt.destinations.postgres(dest),
        dataset_name="bench",
        dev_mode=False,
    )

    # LOAD 1 — untimed setup.
    deliver(pipe, src, backend, load1_table)

    # LOAD 2 — the measured re-delivery.
    started = time.monotonic()
    deliver(pipe, src, backend, load2_table)
    elapsed = time.monotonic() - started

    with pipe.sql_client() as client:
        rows = client.execute_sql(f"SELECT count(*) FROM bench.{DEST_TABLE}")[0][0]
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": rows, "seconds": elapsed,
                      "rows_per_s": rows / elapsed if elapsed else 0,
                      "peak_rss_kb": peak_kb, "backend": backend}))
