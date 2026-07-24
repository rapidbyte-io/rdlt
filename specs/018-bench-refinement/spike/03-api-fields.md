# Probe 3 — Job API field names: GO (end-to-end sync verified)

**Question**: pin the exact field names of the Airbyte job API end-to-end
(GET /jobs/{id}) so the bench driver reads status/duration/rows correctly.

**Answer: GO. A full-refresh-overwrite postgres→postgres sync ran to
`succeeded` and landed 5/5 rows. Job JSON pinned below.** Getting here
required TWO runtime remediations that Probe 1's GO never exercised (it
only proved the API server answers, never launched a job pod) — both are
recorded below as `setup.py` obligations.

## Terminal job JSON (verbatim, pinned)

Sync job (jobId 1), `GET /api/public/v1/jobs/1`:
```json
{"jobId":1,"status":"succeeded","jobType":"sync","startTime":"2026-07-24T22:19:10Z","connectionId":"efb53231-c2d6-45f8-95e3-8bd85f9394c3","lastUpdatedAt":"2026-07-24T22:19:59Z","duration":"PT49S","bytesSynced":116,"rowsSynced":5}
```
Trigger response (`POST /api/public/v1/jobs {connectionId, jobType:"sync"}`),
first observed shape:
```json
{"jobId":1,"status":"running","jobType":"sync","startTime":"2026-07-24T22:19:10Z","connectionId":"efb53231-c2d6-45f8-95e3-8bd85f9394c3","duration":"PT0S"}
```
(reset trigger returns `"status":"pending"` first; see spike/05.)

### Field pins for the driver

| need              | field           | type / format                    |
|-------------------|-----------------|----------------------------------|
| job id            | `jobId`         | integer (1, 2, 3…), NOT a UUID   |
| state             | `status`        | see vocabulary below             |
| kind              | `jobType`       | `"sync"` \| `"reset"`           |
| start             | `startTime`     | ISO-8601 UTC `2026-07-24T22:19:10Z` |
| end               | `lastUpdatedAt` | ISO-8601 UTC (present once terminal) |
| platform job wall | `duration`      | ISO-8601 duration `"PT49S"` (== start→lastUpdated) |
| rows              | `rowsSynced`    | integer — **NOT** `recordsSynced` |
| bytes             | `bytesSynced`   | integer                          |

- `status` vocabulary: non-terminal `pending` → `running`; terminal set =
  `succeeded` | `failed` | `cancelled` | `incomplete`. Driver polls until
  status ∈ terminal set (sleep ~15-20s between polls).
- **Headline `seconds`**: the driver should measure its OWN
  trigger→terminal wall (clock at POST /jobs → clock at first terminal
  poll). Record the API's `duration` + `startTime`/`lastUpdatedAt` as
  extra context — `duration` is the platform's internal job wall and
  excludes the driver's trigger/enqueue latency, so it reads a hair lower
  than the driver wall. Recommendation: headline = driver wall; label the
  API `duration` "airbyte platform job wall" context.
- `rowsSynced == 5` confirmed == source rowcount. ✓

## Destination naming convention (pinned — for bench rowcount verify)

Connection created with `namespaceDefinition:"destination"`,
`namespace:"public"` (mirrors source schema), `prefix:""`. Data landed at
**`dst.public.probe`** = `<source-namespace>.<stream-name>`, no prefix.
Full-refresh-overwrite via the postgres **Direct-Load** dest:
- Final typed table written directly; **no** `airbyte_internal` / raw
  schema exists (only `public`). Rowcount verify = `SELECT count(*) FROM
  <namespace>.<stream>`.
- Extra metadata columns alongside the real columns: `_airbyte_raw_id`
  (uuid), `_airbyte_extracted_at` (timestamptz), `_airbyte_meta` (jsonb
  `{"changes":[],"sync_id":N}`), `_airbyte_generation_id` (int). The bench
  counts rows (not columns) and, if it hashes payloads, excludes the four
  `_airbyte_*` columns.

## Timing observed (calibration for the P3 session)

All connector images were ALREADY cached on the node (abctl/prior pull),
so these are **warm-image** walls — a truly cold first pull was not
re-observed here and would add minutes (source-postgres:3.8.1,
destination-postgres, container-orchestrator:2.1.1 layers):
- sync #1 (fresh connection, warm images): driver wall ~50s, API
  `duration` PT49S, 5 rows.
- reset #2: PT22S.
- sync #3 (warm, right after a plain DROP): PT17S, 5 rows.

Takeaway: Airbyte carries a fixed per-job orchestration floor (~15-50s:
check + discover + replication pod spin-up) INDEPENDENT of data volume.
For the tiny probe payload that floor IS the headline. For the real
1M-row cells data transfer will dominate, but the driver should expect a
~20-50s Airbyte fixed overhead added to every job, and a several-minute
image-pull penalty on the very FIRST job of a cold cluster. Run-count
guidance for P3: discard/label the first (cold) run separately; take the
recorded runs from warm state.

## Setup path (pinned, reusable)

API base `http://127.0.0.1:8600` (host port-forward — start-if-absent,
idempotent). Bearer token from `airbyte-auth-secrets` → POST
/api/v1/applications/token. Default workspace
`731c7910-c622-4d90-be6f-55766f76a220`. Fixture: podman postgres:16-alpine
on :5439, db `src` table `public.probe(id int pk, name text)` 5 rows,
empty db `dst`.

Working payloads:
- Source (POST /api/public/v1/sources) — `tunnel_method` REQUIRED (422
  without): `{sourceType:postgres, host:169.254.1.2, port:5439,
  database:src, username:postgres, password:probe, schemas:[public],
  ssl_mode:{mode:disable}, replication_method:{method:Standard},
  tunnel_method:{tunnel_method:NO_TUNNEL}}` → sourceId
  `4ee76390-f1b4-4191-adfb-60d04416cff5`.
- Destination (POST /api/public/v1/destinations) — Direct-Load `schema`
  accepted: `{destinationType:postgres, host:169.254.1.2, port:5439,
  database:dst, username:postgres, password:probe, schema:public,
  ssl_mode:{mode:disable}, tunnel_method:{tunnel_method:NO_TUNNEL}}` →
  destinationId `57044cd7-dfd7-4408-b814-dea6e1f7c3c2`.
- **Discover FIRST** (POST /api/v1/sources/discover_schema
  `{sourceId, disable_cache:true}`) → caches the catalog. MANDATORY before
  connection-create: without a cached catalog, `POST
  /api/public/v1/connections` throws a server-side NullPointerException in
  `SourceServiceImpl.getSourceSchema` (HTTP 500). With it cached → 200.
- Connection (POST /api/public/v1/connections) `{name, sourceId,
  destinationId, configurations:{streams:[{name:probe,
  syncMode:full_refresh_overwrite}]}}` → connectionId
  `efb53231-c2d6-45f8-95e3-8bd85f9394c3`, active.

## Cluster deltas required to make the runtime job-capable

Both surfaced only when actually launching a job pod (Probe 1 never did).
Each MUST be handled by the bench `setup.py`.

### Delta 1 — in-cluster :443 hijacked by dead ingress (RESOLVED)

The CrashLooping `ingress-nginx-controller` (ns `ingress-nginx`) declares
**hostPorts 80/443**; the CNI portmap plugin's prerouting DNAT for a
hostPort matches ANY `daddr` on that port, so every new pod connection to
dport 443 — the apiserver ClusterIP `10.96.0.1:443` included — was
hijacked to the dead ingress pod and RST'd. Job pods use the in-cluster
k8s client, so none could launch. (An earlier hypothesis blamed the
kube-proxy masquerade/hairpin nft rule — red herring; node-vs-pod
asymmetry was conntrack on established flows.) Fix applied: `kubectl -n
ingress-nginx scale deploy/ingress-nginx-controller --replicas=0`;
verified pod→10.96.0.1:443 open. ingress-nginx is unusable under this
rootless-podman path anyway (NodePort-only). `masqueradeAll: true` was
also set first; harmless, left in place.
**setup.py**: ensure `ingress-nginx-controller` stays scaled to 0.

### Delta 2 — node container pids-limit too low (RESOLVED)

After Delta 1: `POST /connections` → HTTP 500 `unable to create native
thread: possibly out of memory or process/resource limits reached`
(airbyte-server JVM; no pod created). Not host starvation (43 GiB free).
The kind **node container**'s cgroup pids limit
(`podman inspect airbyte-abctl-control-plane .HostConfig.PidsLimit`) was
**2048**, already at ~1812 from the idle abctl low-resource stack — no
headroom for a job's threads. Fix applied (live, no recreation):
`podman update --pids-limit 32768 airbyte-abctl-control-plane`.
**setup.py**: raise the node pids-limit once at cluster-up.

## Gotchas the driver must handle

- **Port-forward instability**: the `kubectl port-forward` drops during
  long *synchronous* API calls (a `discover_schema` that pulled returned
  HTTP 000 after 183s when the PF died mid-call). Mitigations: (a) keep
  the PF supervised, restart-if-health-fails; (b) NEVER hold a long
  synchronous call — `discover_schema` and jobs complete server-side
  regardless of the HTTP connection, so fire then poll with SHORT calls.
  The sync flow is already poll-based (POST /jobs returns immediately;
  GET /jobs/{id} is short), so it is unaffected once discover is done.
- **Token lifetime**: access tokens are short-lived (~minutes). Re-mint
  per poll loop (cheap) or on any 401 / HTTP 000.
- **Stale discover workflow**: a failed discover leaves a Completed-with-
  failureReason `ConnectorCommandWorkflow` in Temporal; a fresh
  `disable_cache:true` discover starts a NEW workflow fine (verified
  `discover_276da875…` Running→Completed→catalog cached).

**Decision: GO. Field names pinned; end-to-end sync verified 5/5 rows;
two setup deltas + three driver gotchas recorded.**
