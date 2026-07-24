# Spike Summary — Airbyte Feasibility Probes (BR5)

All five probes GO. The 3-way matrix (US4) proceeds as designed —
Airbyte joins as a `driver` competitor kind; no absent-with-reason
fallback needed. Evidence per probe in 01–05; the go/no-go table:

| # | Probe | Decision | Pinned fact the driver relies on |
|---|---|---|---|
| 1 | Runtime (#1 risk) | **GO** (amended) | abctl kind cluster runs on rootless podman, zero system-level installs. API via host `kubectl port-forward svc/…-airbyte-server-svc 8600:8001` (ingress edge dead). TWO cluster remediations required and recorded as setup.py obligations: ingress-nginx scaled to 0 (its hostPorts hijack all in-cluster :443 via unguarded portmap DNAT) and node `--pids-limit` raised to 32768 (default 2048 is exhausted by the idle stack) |
| 2 | Networking (pods → host fixtures) | **GO** | pods address host services as `169.254.1.2` (pasta host map; IP literal — pods can't resolve the node-only special names). Fixture ports: pg 5439, RUSTFS 19110 |
| 3 | Job API fields | **GO** | `jobId` int; `status` ∈ pending/running/succeeded/failed/cancelled/incomplete; `duration` ISO-8601 (PT49S); rows = `rowsSynced` (NOT recordsSynced); bytes = `bytesSynced`. Headline = driver's own trigger→terminal wall; API duration logged as labeled context. Data lands at `<source-namespace>.<stream>` typed (Direct-Load, no raw schema) + 4 `_airbyte_*` metadata columns |
| 4 | Quiet-guard fit | **GO** | idle cluster = 0.21 loadavg / 32 cores — no allowance needed, cluster stays up between arms |
| 5 | Reset fidelity | **GO** | per-run reset = plain `DROP TABLE … CASCADE` (or product-prefix delete for lake dests), verified count==0 before each timed run; re-sync recreates cleanly. Reset job works too but buys nothing in full-refresh |

## Resulting US4 shape

- Airbyte = `driver` competitor kind, runs=3, headline `seconds` =
  driver-measured trigger→terminal wall; `extra.sync_s` = API `duration`
  (platform job wall, labeled context — reads slightly low, excludes
  trigger/enqueue).
- `setup.py` (idempotent, once per cluster): assert abctl cluster
  present else `Missing{reason}`; scale ingress-nginx to 0; raise node
  pids-limit; create source/destination/connection against
  `169.254.1.2` fixtures; ids → gitignored state.json. MUST
  `discover_schema` before connection-create (server 500s otherwise).
- `driver.py`: supervise the port-forward (start-if-absent, restart on
  drop); never hold long synchronous API calls — poll-based job flow
  only; re-mint the short-lived token per poll loop; reset via DROP,
  verify count==0, trigger sync, poll to terminal, verify rowcount.
- Session protocol: the cluster's FIRST job after a fresh install pays
  a multi-minute connector image pull — run one untimed warmup sync
  before recorded arms. Expect a fixed ~20–50 s per-job orchestration
  floor independent of data volume (state it in the cell `note`).
