#!/usr/bin/env python3
"""Per-cell Airbyte sync driver (feature 018, T022). The harness invokes:

    <module>/.venv/bin/python driver.py <cell-id> [expected-rows]   # cwd=module

for each [[cell.competitor]] entry, AFTER resetting the cell's fixtures. It
drives ONE pre-created connection (from state.json, written by setup.py) to a
terminal sync, verifies the landed rowcount in the destination, and prints ONE
JSON summary line as the LAST line of stdout — the harness's driver contract:

    {"seconds": <driver trigger->terminal wall, THE headline>,
     "rows": <rowsSynced>,
     "peak_rss_kb": <kind-node cgroup memory.peak — WHOLE-CLUSTER, not a
                     per-process RSS>,
     "extra": {"sync_s": <API job `duration`, the platform job wall>}}

All progress goes to stderr so stdout's last line is the JSON. A non-zero exit
(prerequisite not ready, sync failed, rowcount mismatch) makes the harness
record this arm as Missing{reason} — a loud skip, never a fabricated number.

The headline `seconds` is the driver's OWN wall clock (trigger -> first
terminal poll), continuous with how the spike measured it; the API's ISO-8601
`duration` is carried as labeled context in extra.sync_s (spike 03).
"""

import json
import sys
import time

import airbyte_api as ab


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def fail(reason):
    # Non-zero exit + reason on stderr => harness records Missing{reason}.
    log(f"MISSING: {reason}")
    sys.exit(1)


def clean_destination(cell):
    """Guarantee a clean destination before the timed run. The harness fixture
    reset recreates the postgres dest schemas (covers pg cells), but does NOT
    touch the lake output prefix, so S3-dest cells are cleaned here (spike 05).
    Postgres cells are verified empty defensively."""
    v = cell["verify"]
    if v["kind"] == "s3":
        n = ab.s3_delete_prefix(v["endpoint"], v["key"], v["secret"],
                                v["bucket"], v["prefix"])
        log(f"cleaned {n} object(s) under {v['bucket']}/{v['prefix']}")
    else:
        existing = ab.pg_count(v["container"], v["db"], v["schema"], v["table"])
        if existing:
            log(f"warning: dest {v['schema']}.{v['table']} not empty "
                f"({existing} rows) after fixture reset")


def verify_rows(cell, rows_synced, expected):
    v = cell["verify"]
    if v["kind"] == "pg":
        got = ab.pg_count(v["container"], v["db"], v["schema"], v["table"])
        if got != expected:
            fail(f"destination rowcount {got} != expected {expected} "
                 f"({v['schema']}.{v['table']})")
        log(f"verified pg dest {v['schema']}.{v['table']} = {got} rows")
    else:
        # Parquet on S3: a stdlib parquet row re-count is impractical, so the
        # check is Airbyte's emitted rowsSynced == expected AND >=1 object
        # present under the prefix (documented in README).
        keys = ab.s3_list_keys(v["endpoint"], v["key"], v["secret"],
                               v["bucket"], v["prefix"])
        if not keys:
            fail(f"no objects under {v['bucket']}/{v['prefix']} after sync")
        if rows_synced != expected:
            fail(f"rowsSynced {rows_synced} != expected {expected} "
                 f"(s3 dest, {len(keys)} objects)")
        log(f"verified s3 dest {v['bucket']}/{v['prefix']}: "
            f"{len(keys)} object(s), rowsSynced={rows_synced}")


def main():
    if len(sys.argv) < 2:
        fail("usage: driver.py <cell-id> [expected-rows]")
    cell_id = sys.argv[1]

    try:
        state = ab.load_state()
    except Exception as exc:
        fail(f"state.json unreadable ({exc}) — run setup.py first")
    cell = state.get("cells", {}).get(cell_id)
    if not cell:
        fail(f"cell `{cell_id}` not in state.json — run setup.py")
    if cell.get("status") != "ready":
        fail(f"cell `{cell_id}` not ready in setup: {cell.get('error', cell.get('status'))}")

    expected = int(sys.argv[2]) if len(sys.argv) > 2 else cell["verify"]["expected"]
    conn = cell["connectionId"]

    ab.ensure_port_forward()
    token = ab.mint_token()

    # Clean-dest invariant + (dedup only) wipe connection state so the timed
    # run re-reads the full dataset. Both untimed.
    clean_destination(cell)
    if cell.get("reset_state_before_run"):
        log("wiping connection state (reset job, untimed)...")
        job_id, token = ab.trigger_job(token, conn, "reset")
        rj = ab.poll_job(token, job_id, cap_s=600)
        if rj.get("status") != "succeeded":
            fail(f"pre-run reset did not succeed: {rj.get('status')}")
        clean_destination(cell)  # reset may have re-created an empty dest

    # Timed sync: driver wall = trigger -> terminal.
    log(f"[{cell_id}] triggering sync on connection {conn} (expected {expected} rows)")
    started = time.monotonic()
    job_id, token = ab.trigger_job(token, conn, "sync")
    job = ab.poll_job(token, job_id, cap_s=3600)
    wall_s = time.monotonic() - started

    if job.get("status") != "succeeded":
        fail(f"sync job {job_id} terminal={job.get('status')}: {json.dumps(job)}")

    rows_synced = int(job.get("rowsSynced") or 0)
    sync_s = ab.iso8601_seconds(job.get("duration"))
    log(f"[{cell_id}] succeeded: driver_wall={wall_s:.1f}s "
        f"api_duration={sync_s}s rowsSynced={rows_synced}")

    verify_rows(cell, rows_synced, expected)

    peak_rss_kb = ab.node_memory_peak_kb()
    summary = {
        "seconds": wall_s,
        "rows": rows_synced,
        "peak_rss_kb": peak_rss_kb,
        "extra": {"sync_s": sync_s},
    }
    print(json.dumps(summary))   # LAST stdout line — the harness contract


if __name__ == "__main__":
    main()
