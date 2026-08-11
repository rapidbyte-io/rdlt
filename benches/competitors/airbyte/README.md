# Airbyte competitor module (feature 018)

Airbyte is a competitor arm of the e2e matrix — today on the oracle cell
(`oracle-to-pg-200k`, `benches/cells/oracle.toml`). Unlike dlt — a
self-timed container the harness runs directly — Airbyte is a long-lived
**cluster**, so this module is a `kind = driver` variant: a host-side
`driver.py` drives pre-created connections against an already-running
`abctl` cluster and prints the same summary JSON line as every other arm.

Everything here was pinned by the feasibility probes in
`specs/018-bench-refinement/spike/` — read those first:
`01-runtime.md` (abctl on rootless podman), `02-networking.md`
(pods reach host fixtures at `169.254.1.2`), `03-api-fields.md` (the job API
field pins, the two setup deltas, the driver gotchas), `05-reset.md`
(reset fidelity / the per-run clean-dest recipe).

## Files

- `variants.toml` — the single `airbyte` variant (driver kind, `runs = 3`,
  a light prerequisite gate).
- `airbyte_api.py` — shared, **stdlib-only** helpers (port-forward supervision,
  token minting, API HTTP, S3 SigV4, pg counts via `podman exec`, cluster RSS).
- `setup.py` — one-time, idempotent cluster + connection setup; writes
  `state.json`.
- `driver.py` — per-cell timed sync + destination verification + summary line.
- `state.json` — machine-local connection ids (gitignored; written by setup).

No third-party Python is required. A `.venv/` is optional; without one the
harness runs the driver on system `python3`.

## Prerequisites (once per machine)

1. **Install the cluster** (owner-approved, one-time): `abctl local install
   --low-resource-mode`. See `spike/01-runtime.md`.
2. **Run setup**:

   ```
   TARGET=setup make bench
   ```

   That verb (`benches/bench-setup.sh`) builds the dlt image, brings up the
   fixture containers with the harness seeds, runs this module's
   `setup.py` against them, and tears the fixtures down. To run `setup.py`
   directly instead, the bench postgres (`:5439`, database `dest_airbyte`)
   and Oracle (`:15210`, service FREEPDB1) fixtures must already be up —
   Airbyte's discover reads the source schemas.

   Setup re-applies the two runtime deltas (below), creates a source /
   destination / connection per cell (discovering each source's catalog first
   — mandatory, else the public API NPEs), and writes `state.json`. It is
   safe to re-run (it deletes prior `rb-ab-*` resources first). The FIRST run
   pulls the source connector images inside the cluster and can take several
   minutes per new connector.

### The two setup deltas (enforced by setup.py, `spike/03`)

- **ingress-nginx scaled to 0** — its CrashLooping controller declares
  hostPorts 80/443, and the CNI portmap DNAT then hijacks *all* in-cluster
  `:443` (the apiserver ClusterIP included), so no job pod can launch. It is
  useless under this rootless-podman path anyway (NodePort-only).
- **node pids-limit raised to 32768** — abctl's default kind-node cgroup
  `pids.max` is 2048 and the idle stack already sits near it, so a connector
  job hits `unable to create native thread`. `podman update --pids-limit`
  fixes it live.

Both are idempotent; setup refreshes them every run.

## What these cells measure (fairness policy)

- **Headline `seconds` = job wall**: the driver's own clock from triggering the
  sync (`POST /jobs`) to the job reaching a terminal state. This is
  three-way-comparable as *time to move the data end-to-end*, but note it
  **includes Airbyte's fixed per-job orchestration floor** (~20-50s of
  check + discover + replication-pod spin-up) that dlt/rdlt do not pay. On the
  small cells that floor can dominate; on the 1M cells data transfer dominates
  but the floor is still added. `extra.sync_s` carries the API's own
  `duration` (the platform job wall) as labeled context — it excludes the
  driver's trigger/enqueue latency and reads slightly lower.
- **`peak_rss_kb` is a WHOLE-CLUSTER statistic**: the kind-node container's
  cgroup `memory.peak` (every Airbyte pod runs inside that one node). It is
  **not** comparable to dlt/rdlt per-process `ru_maxrss` and is **never
  barred** — it is context only. The module reports it plainly as what it is.
- **Rowcount verification**: postgres destinations are counted exactly
  (`SELECT count(*)` via `podman exec` into the fixture container).

## Per-run reset & clean-dest recipe (`spike/05`)

The harness resets fixtures before every driver invocation. For postgres
destinations that reset drops and recreates the `dest_airbyte` schemas, so
the destination starts empty (the driver verifies it, defensively).

## Warmup rule

The FIRST sync of a cell on a cold cluster pulls the destination connector
image (minutes). The driver does **not** special-case this — instead, **T023
(the recorded session) runs one untimed warmup sync per cell before the
measured runs**, so all recorded runs are warm-image. Do the same for any ad
hoc measurement: discard the first (cold) run.

## Missing, not fabricated

Any arm whose prerequisite fails (no cluster / no `state.json`), whose setup
did not complete (recorded in `state.json` as `status: error`), or whose sync
fails or lands the wrong rowcount, exits non-zero — the harness records it as
`Missing{reason}`, a loud skip. The rdlt and dlt arms still run.
