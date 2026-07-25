#!/usr/bin/env python3
"""One-time, idempotent setup for the Airbyte competitor arm (feature 018,
T022). Run MANUALLY once per machine before a bench session:

    python3 benches/competitors/airbyte/setup.py

It (1) re-applies the two runtime deltas the spike recorded (ingress scaled to
0; node pids-limit raised), (2) creates one Airbyte source/destination set and
a connection per e2e cell against the bench fixture endpoints, discovering each
source's catalog BEFORE the connection create (mandatory — else the public API
NPEs, spike 03), and (3) writes benches/competitors/airbyte/state.json (the
connection ids + per-cell destination-verify recipe the driver reads).
state.json is gitignored.

Fixtures: it targets the same endpoints the bench fixtures publish — postgres
on :5439 (databases `src` + `dest_airbyte`, user postgres) and RUSTFS on :19110
(buckets `raw` + `lake`). Connector PODS reach them at 169.254.1.2 (pasta host
mapping); this host-side script reaches them at 127.0.0.1. If the fixtures are
not up when setup runs, bring them up first (the harness owns them) — setup
does not seed 1M rows itself; discover needs only the schema. For a standalone
smoke run, seed a tiny schema-identical postgres on :5439 (see README) and pass
AB_* overrides if your throwaway differs.

Each cell is built independently; a connector that fails (e.g. an S3 image that
will not pull) records its reason in state.json and the driver reports that arm
as Missing — the other arms still run. Re-running setup is safe: it deletes any
prior `rb-ab-*` resources first, then recreates.
"""

import os
import sys
import time

import airbyte_api as ab

# --- fixture coordinates (overridable for standalone smoke runs) ----------

POD_HOST = os.environ.get("AB_POD_HOST", ab.POD_HOST_IP)   # pods -> host
PG_PORT = os.environ.get("AB_PG_PORT", "5439")
PG_USER = os.environ.get("AB_PG_USER", "postgres")
PG_PASS = os.environ.get("AB_PG_PASS", "postgres")
SRC_DB = os.environ.get("AB_SRC_DB", "src")
DEST_DB = os.environ.get("AB_DEST_DB", "dest_airbyte")
S3_ENDPOINT = os.environ.get("AB_S3_ENDPOINT", f"http://{POD_HOST}:19110")
S3_KEY = os.environ.get("AB_S3_KEY", "rdlt-bench")
S3_SECRET = os.environ.get("AB_S3_SECRET", "rdlt-bench-secret")
S3_RAW_BUCKET = os.environ.get("AB_S3_RAW", "raw")
S3_LAKE_BUCKET = os.environ.get("AB_S3_LAKE", "lake")
# The pg fixture container name (driver counts rows via `podman exec` into it).
PG_CONTAINER = os.environ.get("AB_PG_CONTAINER", "rdlt-bench-pg")
# Host-side view of the endpoints (this script + the driver's verification).
PG_HOST_LOCAL = os.environ.get("AB_PG_HOST_LOCAL", "127.0.0.1")
S3_ENDPOINT_LOCAL = os.environ.get("AB_S3_ENDPOINT_LOCAL", "http://127.0.0.1:19110")

NAME_PREFIX = "rb-ab-"


# --- connector config builders (field names pinned from the live specs) ---

def pg_source_config(db):
    return {
        "sourceType": "postgres", "host": POD_HOST, "port": int(PG_PORT),
        "database": db, "username": PG_USER, "password": PG_PASS,
        "schemas": ["public"], "ssl_mode": {"mode": "disable"},
        "replication_method": {"method": "Standard"},
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
    }


def pg_destination_config(db):
    return {
        "destinationType": "postgres", "host": POD_HOST, "port": int(PG_PORT),
        "database": db, "username": PG_USER, "password": PG_PASS,
        "schema": "public", "ssl_mode": {"mode": "disable"},
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
    }


def s3_source_config(stream_name, glob):
    return {
        "sourceType": "s3", "bucket": S3_RAW_BUCKET, "endpoint": S3_ENDPOINT,
        "region_name": "us-east-1",
        "aws_access_key_id": S3_KEY, "aws_secret_access_key": S3_SECRET,
        "streams": [{
            "name": stream_name, "globs": [glob],
            "format": {"filetype": "jsonl"},
            "validation_policy": "Emit Record",
        }],
    }


def s3_destination_config(path):
    return {
        "destinationType": "s3", "s3_bucket_name": S3_LAKE_BUCKET,
        "s3_bucket_path": path, "s3_bucket_region": "us-east-1",
        "s3_endpoint": S3_ENDPOINT, "access_key_id": S3_KEY,
        "secret_access_key": S3_SECRET,
        "format": {"format_type": "Parquet"},
    }


# --- the five cells, declaratively ---------------------------------------
# stream = the source stream Airbyte selects; sync_mode per regime; verify =
# how the driver independently checks the landed rowcount.

def pg_verify(table, expected):
    return {"kind": "pg", "container": PG_CONTAINER, "db": DEST_DB,
            "schema": "public", "table": table, "expected": expected}


def s3_verify(path, expected):
    return {"kind": "s3", "endpoint": S3_ENDPOINT_LOCAL, "key": S3_KEY,
            "secret": S3_SECRET, "bucket": S3_LAKE_BUCKET,
            "prefix": path + "/", "expected": expected}


CELLS = [
    {
        "id": "pg-to-pg-1m",
        "source": ("pg", SRC_DB), "destination": ("pg", DEST_DB),
        "stream": "events", "sync_mode": "full_refresh_overwrite",
        "verify": pg_verify("events", 1_000_000),
    },
    {
        "id": "pg-to-s3parquet-1m",
        "source": ("pg", SRC_DB),
        "destination": ("s3", "airbyte/pg-to-s3parquet-1m"),
        "stream": "events", "sync_mode": "full_refresh_overwrite",
        "verify": s3_verify("airbyte/pg-to-s3parquet-1m", 1_000_000),
    },
    {
        "id": "s3jsonl-to-pg-200k",
        "source": ("s3", ("events", "landed/*.jsonl")),
        "destination": ("pg", DEST_DB),
        "stream": "events", "sync_mode": "full_refresh_overwrite",
        "verify": pg_verify("events", 200_000),
    },
    {
        "id": "s3jsonl-to-s3parquet-200k",
        "source": ("s3", ("events", "landed/*.jsonl")),
        "destination": ("s3", "airbyte/s3jsonl-to-s3parquet-200k"),
        "stream": "events", "sync_mode": "full_refresh_overwrite",
        "verify": s3_verify("airbyte/s3jsonl-to-s3parquet-200k", 200_000),
    },
    {
        # Dedup: full re-delivery deduped by id. Airbyte cannot merge two
        # distinct source tables (events -> events_v2) into one dest table via
        # one connection, so it benches the closest supported shape: a single
        # incremental+dedup stream on events_v2 (PK id, cursor id) with the
        # connection STATE wiped before each timed run, so every run re-reads
        # all 1M rows and dedups by id -> 1M final rows. Same regime words as
        # the cell note ("full-redelivery-plus-dedup"); deviation documented in
        # README. reset_state_before_run drives the driver's per-run wipe.
        "id": "pg-to-pg-dedup-1m",
        "source": ("pg", SRC_DB), "destination": ("pg", DEST_DB),
        "stream": "events_v2", "sync_mode": "incremental_deduped_history",
        "primary_key": [["id"]], "cursor_field": ["id"],
        "reset_state_before_run": True,
        "verify": pg_verify("events_v2", 1_000_000),
    },
]


# --- API plumbing --------------------------------------------------------

def workspace_id(token):
    _, data, token = ab.api("GET", "/api/public/v1/workspaces", token)
    return data["data"][0]["workspaceId"], token


def cleanup(token, ws):
    """Delete every prior rb-ab-* connection, then source, then destination.
    Loops until a listing page has no rb-ab-* rows left, so an accumulation of
    duplicate sources from earlier failed runs is fully cleared (not capped at
    one page)."""
    idk = {"connections": "connectionId", "sources": "sourceId",
           "destinations": "destinationId"}
    for kind in ("connections", "sources", "destinations"):
        while True:
            _, data, token = ab.api(
                "GET", f"/api/public/v1/{kind}?workspaceIds={ws}&limit=100", token)
            ours = [it for it in (data or {}).get("data", [])
                    if it.get("name", "").startswith(NAME_PREFIX)]
            if not ours:
                break
            for item in ours:
                ab.api("DELETE", f"/api/public/v1/{kind}/{item[idk[kind]]}", token)
    return token


def create_source(token, ws, name, config):
    _, data, token = ab.api("POST", "/api/public/v1/sources", token,
                            body={"name": name, "workspaceId": ws,
                                  "configuration": config})
    if not isinstance(data, dict) or "sourceId" not in data:
        raise RuntimeError(f"source create failed: {data}")
    return data["sourceId"], token


def create_destination(token, ws, name, config):
    _, data, token = ab.api("POST", "/api/public/v1/destinations", token,
                            body={"name": name, "workspaceId": ws,
                                  "configuration": config})
    if not isinstance(data, dict) or "destinationId" not in data:
        raise RuntimeError(f"destination create failed: {data}")
    return data["destinationId"], token


def _discover_ok(data):
    return (isinstance(data, dict)
            and (data.get("jobInfo") or {}).get("succeeded")
            and data.get("catalogId"))


def _discover_call(source_id, disable_cache, timeout):
    """One discover_schema call (fresh token, PF ensured). Returns the parsed
    body or None on a transport drop (http() never raises now)."""
    ab.ensure_port_forward()
    token = ab.mint_token()
    _, data = ab.http("POST", "/api/v1/sources/discover_schema", token=token,
                      body={"sourceId": source_id,
                            "disable_cache": disable_cache}, timeout=timeout)
    return data


def discover(token, source_id, cap_s=1800):
    """Cache the source's catalog. The bug the first cut had: it read the LONG
    synchronous discover_schema response every iteration, so when the
    port-forward dropped mid-call (spike 03) the completed result was never
    seen, and it re-fired a fresh discover each loop (pod spam). Fix: FIRE one
    fresh discover, then POLL a cheap `disable_cache=false` read (~0.01s once
    the catalog is cached — verified) that survives PF drops. Re-fire only
    sparingly (stale-failure bust / after a provable drop), never per poll."""
    deadline = time.time() + cap_s
    refire_at = 0.0
    while time.time() < deadline:
        # (Re)fire a fresh discover: immediately (bust any stale cached
        # failure), then at most every 120s. The fast-image path returns here.
        if time.time() >= refire_at:
            fired = _discover_call(source_id, True, timeout=25)
            refire_at = time.time() + 120
            if _discover_ok(fired):
                return fired["catalogId"], token
        # Cheap cache-read poll — short, PF-drop resistant. Once the fired
        # discover has cached server-side, this returns the catalog fast.
        polled = _discover_call(source_id, False, timeout=30)
        if _discover_ok(polled):
            return polled["catalogId"], token
        time.sleep(15)
    raise RuntimeError(f"discover did not succeed within {cap_s}s for {source_id}")


def create_connection(token, ws, name, cell, source_id, destination_id):
    stream = {"name": cell["stream"], "syncMode": cell["sync_mode"]}
    if "primary_key" in cell:
        stream["primaryKey"] = cell["primary_key"]
    if "cursor_field" in cell:
        stream["cursorField"] = cell["cursor_field"]
    _, data, token = ab.api(
        "POST", "/api/public/v1/connections", token,
        body={"name": name, "sourceId": source_id,
              "destinationId": destination_id,
              "configurations": {"streams": [stream]}}, timeout=120)
    if not isinstance(data, dict) or "connectionId" not in data:
        raise RuntimeError(f"connection create failed: {data}")
    return data["connectionId"], token


def build_source(token, ws, cell, cache):
    kind, arg = cell["source"]
    key = ("src", kind, str(arg))
    if key in cache:
        return cache[key], token
    if kind == "pg":
        sid, token = create_source(token, ws, NAME_PREFIX + "src-pg-" + arg,
                                   pg_source_config(arg))
    else:
        stream_name, glob = arg
        sid, token = create_source(
            token, ws, NAME_PREFIX + "src-s3-" + stream_name,
            s3_source_config(stream_name, glob))
    # Cache BEFORE discover so a discover failure does not leak a duplicate
    # source on the next cell — the same source (and its cached catalog) is
    # reused across sibling cells and across a re-run's cleanup.
    cache[key] = sid
    _, token = discover(token, sid)   # cache the catalog (mandatory)
    return sid, token


def build_destination(token, ws, cell, cache):
    kind, arg = cell["destination"]
    key = ("dst", kind, str(arg))
    if key in cache:
        return cache[key], token
    if kind == "pg":
        did, token = create_destination(token, ws, NAME_PREFIX + "dst-pg-" + arg,
                                        pg_destination_config(arg))
    else:
        did, token = create_destination(
            token, ws, NAME_PREFIX + "dst-s3-" + cell["id"],
            s3_destination_config(arg))
    cache[key] = did
    return did, token


def main():
    print("enforcing setup deltas (ingress=0, node pids-limit)...")
    if not ab.cluster_reachable():
        sys.exit("abctl cluster not reachable (kubectl get ns airbyte-abctl "
                 "failed) — run `abctl local install` first.")
    ab.enforce_setup_deltas()
    ab.ensure_port_forward()
    token = ab.mint_token()
    ws, token = workspace_id(token)
    print(f"workspace {ws}; cleaning prior rb-ab-* resources...")
    token = cleanup(token, ws)

    state = {"workspaceId": ws, "apiBase": ab.API_BASE, "cells": {}}
    src_cache, dst_cache = {}, {}
    # AB_CELLS=<comma-separated ids> restricts which cells to build (smoke /
    # incremental setup); default is all five.
    only = {c for c in os.environ.get("AB_CELLS", "").split(",") if c}
    cells = [c for c in CELLS if not only or c["id"] in only]
    for cell in cells:
        cid = cell["id"]
        entry = {"verify": cell["verify"],
                 "reset_state_before_run": cell.get("reset_state_before_run", False)}
        try:
            print(f"[{cid}] source...")
            source_id, token = build_source(token, ws, cell, src_cache)
            print(f"[{cid}] destination...")
            destination_id, token = build_destination(token, ws, cell, dst_cache)
            print(f"[{cid}] connection...")
            conn_id, token = create_connection(
                token, ws, NAME_PREFIX + cid, cell, source_id, destination_id)
            entry.update(sourceId=source_id, destinationId=destination_id,
                         connectionId=conn_id, status="ready")
            print(f"[{cid}] ready: connection {conn_id}")
        except Exception as exc:
            entry.update(status="error", error=str(exc))
            print(f"[{cid}] ERROR: {exc}")
        state["cells"][cid] = entry
        ab.save_state(state)   # persist incrementally

    ready = [c for c, e in state["cells"].items() if e["status"] == "ready"]
    print(f"\nsetup done: {len(ready)}/{len(cells)} cells ready -> {ab.STATE_PATH}")
    print("ready:", ", ".join(ready) or "(none)")
    errs = [c for c, e in state["cells"].items() if e["status"] != "ready"]
    if errs:
        print("NOT ready (driver will report Missing):", ", ".join(errs))


if __name__ == "__main__":
    main()
