"""Baseline: Oracle table -> Postgres via pinned dlt's sql_database source
(032 e2e cell oracle-to-pg-200k). Full replace into the `bench` dataset
(schema) of the per-product destination database dest_dlt.

THIN MODE, deliberately: python-oracledb runs thin by default — no Oracle
Instant Client, no Oracle client libraries — and SQLAlchemy's
`oracle+oracledb://` dialect selects that DBAPI. connectorx is NOT offered on
this cell the way it is on the pg cells: ConnectorX's Oracle source is the
`oracle` crate over ODPI-C, which dlopen's libclntsh from Oracle Instant
Client at run time. Instant Client is not pip-installable and carries Oracle's
OTN license, so dlt's pg-cell headline configuration does not transfer here.
That exclusion is recorded in the cell note and in RESULTS.md Caveats rather
than left as a quiet handicap.

Usage: pipeline_oracle_pg.py <sqlalchemy_oracle_url> <dest_pg_url> [backend]
                             [table] [schema]
Emits JSON: rows, seconds, rows_per_s, peak_rss_kb, backend, table, dest_table.
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
    table = sys.argv[4] if len(sys.argv) > 4 else "EVENTS"
    schema = sys.argv[5] if len(sys.argv) > 5 else "RDLT"

    # Oracle folds unquoted identifiers UPPERCASE; SQLAlchemy's oracle dialect
    # then presents an ALL-UPPERCASE name lowercased ("case insensitive") and
    # re-uppercases it on the way out. So reflection wants `events`, not
    # `EVENTS` — measured: the cell's first live run failed with
    # `Could not reflect: requested table(s) not available … schema 'RDLT':
    # (EVENTS)` while `inspect(engine).get_table_names()` answered ['events'].
    # The cell's args stay spelled in ORACLE's case, matching the fixture and
    # the rdlt arm; this is where the dialect's rule is applied, so no caller
    # has to know it. A mixed-case name is left alone — that is a genuinely
    # quoted Oracle identifier and lowercasing it would break the lookup.
    denormalize = lambda n: n.lower() if n.isupper() else n   # noqa: E731
    source = sql_database(credentials=src, schema=denormalize(schema),
                          table_names=[denormalize(table)], backend=backend)

    started = time.monotonic()
    pipe = dlt.pipeline(
        pipeline_name=f"bench_oracle_pg_{backend}",
        destination=dlt.destinations.postgres(dest),
        dataset_name="bench",
        dev_mode=False,
    )
    pipe.run(source, write_disposition="replace")
    elapsed = time.monotonic() - started

    landed = pipe.default_schema.data_table_names()
    dest_table = landed[0] if landed else table.lower()
    with pipe.sql_client() as client:
        rows = client.execute_sql(f"SELECT count(*) FROM bench.{dest_table}")[0][0]
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"rows": rows, "seconds": elapsed,
                      "rows_per_s": rows / elapsed, "peak_rss_kb": peak_kb,
                      "backend": backend, "table": table,
                      "dest_table": dest_table}))
