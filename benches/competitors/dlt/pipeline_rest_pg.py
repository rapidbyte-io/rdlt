"""Baseline: mock REST API -> Postgres via pinned dlt's rest_api source (R29).

The mock API (crates/rdlt-source-rest/examples/mock_api.rs) must be running on
the host; Postgres reachable via the connection env vars below. Self-timed
in-process like every other baseline row.

Usage: pipeline_rest_pg.py <api_base_url> <postgres_url>
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb.
"""

import json
import resource
import sys
import time

import dlt
from dlt.sources.rest_api import rest_api_source

if __name__ == "__main__":
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://host.containers.internal:8642"
    pg_url = sys.argv[2] if len(sys.argv) > 2 else "postgresql://postgres:rdlt@localhost:5432/postgres"

    source = rest_api_source({
        "client": {"base_url": base_url},
        "resources": [
            {
                "name": "events",
                "endpoint": {
                    "path": "events",
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "total_path": None,  # stop on empty page
                    },
                },
            }
        ],
    })

    started = time.monotonic()
    pipe = dlt.pipeline(
        pipeline_name="baseline_rest_pg",
        destination=dlt.destinations.postgres(pg_url),
        dataset_name="raw",
    )
    info = pipe.run(source)
    elapsed = time.monotonic() - started

    with pipe.sql_client() as client:
        rows = client.execute_sql("SELECT count(*) FROM raw.events")[0][0]
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": rows, "seconds": elapsed,
                      "rows_per_s": rows / elapsed, "peak_rss_kb": peak_kb}))
