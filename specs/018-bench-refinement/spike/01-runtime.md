# Probe 1 — Runtime (the #1 risk): GO (amended)

**Question**: can abctl's kind cluster run on this podman-based machine
without system-level changes?

**Answer: GO — rootless podman hosts it; two broken edges, both with
clean user-space remedies.** (Amended 2026-07-25: the first GO covered
API access only; actually LAUNCHING a job pod exposed a second defect —
see "Job-pod launch defect" below.)

Evidence (2026-07-24, this machine, all user-space):
- abctl v0.30.4 installed to ~/.local/bin (release tarball).
- `DOCKER_HOST=unix:///run/user/1000/podman/podman.sock abctl local
  install --low-resource-mode` created the kind node
  (`airbyte-abctl-control-plane`) on rootless podman THROUGH the
  distrobox-host-exec shim. Cluster Ready (k8s v1.32.2); ALL Airbyte
  pods healthy (server/worker/temporal/cron/workload-launcher/db,
  bootloader Completed); Airbyte chart 2.1.1 deployed.
- BROKEN EDGE 1 (ingress): the node publish `80/tcp -> 0.0.0.0:8000`
  RSTs every connection — the ingress-nginx controller pod never became
  Ready (CrashLoopBackOff under rootless podman, 89 restarts over 5 h),
  so nothing serves :80 in the node. The
  installer's final health poll of localhost:8000 therefore never
  succeeds (killed after charts deployed; exit 144 recorded).
  `abctl local credentials` polls the same edge and fails — credentials
  come from the cluster secret instead (probe 3 record).
- PROVEN REMEDY (no system changes): the OTHER published edge works —
  apiserver 6443→36013 answers through pasta (curl /version = 200). So:
  host kubectl (installed user-space via mise, 1.36.3) + the abctl
  kubeconfig (~/.airbyte/abctl/abctl.kubeconfig) + `kubectl -n
  airbyte-abctl port-forward svc/airbyte-abctl-airbyte-server-svc
  8600:8001` → http://127.0.0.1:8600/api/v1/health = {"available":true}.

## Job-pod launch defect (found by probe 3, fixed 2026-07-25)

The first GO proved API access but never launched a job pod. Probe 3's
connection-create then failed (HTTP 500, `KubeClientException: Failed
to create pod` from the workload-launcher): every NEW pod connection to
the `kubernetes` apiserver ClusterIP `10.96.0.1:443` was refused, so no
discover/check/sync pod could ever be created.

Root cause (proven by rule counters + conntrack + ruleset): the
CrashLooping ingress-nginx controller pod declares hostPorts 80/443,
and the CNI portmap plugin implements hostPorts as a prerouting DNAT
that matches ANY destination address on those ports (its
local-destination guard exists only on the output hook). Every new
in-cluster connection to dport 443 — the apiserver ClusterIP included —
was hijacked to the dead ingress pod and RST'd. Fingerprint: DNS (53)
and other service ports (7233, 5432) worked; apiserver watches
ESTABLISHED at cluster boot kept working; every new 443 flow died
without ever reaching kube-proxy's rules (its apiserver rule counter
stayed 0). A first hypothesis — kube-proxy `masqueradeAll: false`
preventing the pod→node-IP hairpin — was applied (`masqueradeAll:
true`, kube-proxy restarted) and did NOT clear the refusal; it remains
in effect and is harmless.

FIX (owner-applied, reversible): `kubectl -n ingress-nginx scale
deploy/ingress-nginx-controller --replicas=0` — portmap withdraws the
hostport DNAT rules with the pod; verified pod→10.96.0.1:443 connects
(exit=0). Ingress is dead weight in this environment; all API access is
via the port-forward.

**Decision**: GO. driver.py OWNS the port-forward (start-if-absent,
idempotent) and uses http://127.0.0.1:8600 as the API base. setup.py
MUST ensure the ingress-nginx controller stays scaled to 0 — its
hostPorts poison all in-cluster 443 traffic under rootless podman. No
docker install needed — the owner-approval path was never triggered.
