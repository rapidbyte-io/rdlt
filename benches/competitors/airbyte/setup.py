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
on :5439 (database `dest_airbyte`, user postgres) and Oracle 23ai Free on
:15210 (service FREEPDB1, schema RDLT). Connector PODS reach them at
169.254.1.2 (pasta host mapping); this host-side script reaches them at
127.0.0.1. If the fixtures are not up when setup runs, bring them up first
(the harness owns them) — setup does not seed rows itself; discover needs
only the schema. Pass AB_* overrides if a standalone throwaway fixture
differs.

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
DEST_DB = os.environ.get("AB_DEST_DB", "dest_airbyte")
ORACLE_PORT = os.environ.get("AB_ORACLE_PORT", "15210")
ORACLE_USER = os.environ.get("AB_ORACLE_USER", "RDLT")
ORACLE_PASS = os.environ.get("AB_ORACLE_PASS", "rdlt-bench")
ORACLE_SERVICE = os.environ.get("AB_ORACLE_SERVICE", "FREEPDB1")
# The pg fixture container name (driver counts rows via `podman exec` into it).
PG_CONTAINER = os.environ.get("AB_PG_CONTAINER", "rdlt-bench-pg")

NAME_PREFIX = "rb-ab-"


# --- connector config builders (field names pinned from the live specs) ---

def pg_destination_config(db):
    return {
        "destinationType": "postgres", "host": POD_HOST, "port": int(PG_PORT),
        "database": db, "username": PG_USER, "password": PG_PASS,
        "schema": "public", "ssl_mode": {"mode": "disable"},
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
    }


def oracle_source_config():
    """airbyte/source-oracle — community/alpha, ELv2, and present in the
    DEFAULT OSS catalog (registryOverrides.oss.enabled), so it needs no
    create_custom registration and this ordinary source create works.

    NOT source-oracle-enterprise, which is a separate paid, license-key-gated
    connector (LogMiner CDC) and out of scope.

    Field names follow the connector's spec.json: `connection_data` is a oneOf
    discriminated by connection_type (service_name | sid); `encryption` is a
    oneOf (unencrypted | client_nne | encrypted_verify_certificate); `schemas`
    is an array that defaults to the upper-cased username. `tunnel_method` is
    the standard JDBC-base shape used by the pg source above — it did not
    render in the fetched spec extraction, so if a create is rejected naming
    it, drop the key.

    The connector's docs claim testing through 21c only, and the fixture is
    23ai. Nothing documents a 23ai failure, but nothing verifies one either:
    if discover or sync fails, main()'s per-cell try/except records the reason
    and driver.py turns it into Missing{reason}. That is the correct outcome —
    substituting a 21c container for this arm alone would give it a DIFFERENT
    source server and break the matrix's same-conditions rule.
    """
    return {
        "sourceType": "oracle",
        "host": POD_HOST, "port": int(ORACLE_PORT),
        "connection_data": {"connection_type": "service_name",
                            "service_name": ORACLE_SERVICE},
        "username": ORACLE_USER, "password": ORACLE_PASS,
        "schemas": [ORACLE_USER],
        "encryption": {"encryption_method": "unencrypted"},
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
    }


# --- the cells, declaratively --------------------------------------------
# stream = the source stream Airbyte selects; sync_mode per regime; verify =
# how the driver independently checks the landed rowcount.

def pg_verify(table, expected):
    return {"kind": "pg", "container": PG_CONTAINER, "db": DEST_DB,
            "schema": "public", "table": table, "expected": expected}


CELLS = [
    {
        # 032: Oracle 23ai Free -> postgres, full replace. The stream name is
        # the Oracle table as the connector discovers it (Oracle folds
        # unquoted names UPPERCASE, so `EVENTS`, in namespace `RDLT`), and the
        # landed destination table is whatever Airbyte's pg destination
        # normalizes that to. BOTH are unverified against a live 23ai — no
        # abctl cluster was reachable when this was wired — so if setup or the
        # first sync disagrees, correct these two strings from the discover
        # output rather than assuming the sync is broken.
        "id": "oracle-to-pg-200k",
        "source": ("oracle", None), "destination": ("pg", DEST_DB),
        "stream": "EVENTS", "sync_mode": "full_refresh_overwrite",
        # VERIFIED LIVE (032): Oracle folds identifiers upper, so the
        # stream is `EVENTS`, and Airbyte's postgres destination
        # PRESERVES that case — `public."EVENTS"`, confirmed by
        # querying the fixture after the first successful sync. This
        # is the correction the comment above anticipated.
        "verify": pg_verify("EVENTS", 200_000),
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
    assert kind == "oracle", f"unknown source kind {kind}"
    sid, token = create_source(token, ws, NAME_PREFIX + "src-oracle",
                               oracle_source_config())
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
    assert kind == "pg", f"unknown destination kind {kind}"
    did, token = create_destination(token, ws, NAME_PREFIX + "dst-pg-" + arg,
                                    pg_destination_config(arg))
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
    # incremental setup); default is every cell in CELLS.
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
