# Probe 1 — Runtime (the #1 risk): GO

**Question**: can abctl's kind cluster run on this podman-based machine
without system-level changes?

**Answer: GO — rootless podman hosts it; one broken edge with a clean
user-space remedy.**

Evidence (2026-07-24, this machine, all user-space):
- abctl v0.30.4 installed to ~/.local/bin (release tarball).
- `DOCKER_HOST=unix:///run/user/1000/podman/podman.sock abctl local
  install --low-resource-mode` created the kind node
  (`airbyte-abctl-control-plane`) on rootless podman THROUGH the
  distrobox-host-exec shim. Cluster Ready (k8s v1.32.2); ALL Airbyte
  pods healthy (server/worker/temporal/cron/workload-launcher/db,
  bootloader Completed); Airbyte chart 2.1.1 deployed.
- BROKEN EDGE: the node publish `80/tcp -> 0.0.0.0:8000` RSTs every
  connection — nothing binds :80 in the node (ingress-nginx deployed
  NodePort-only; no hostPort binding under this runtime path). The
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

**Decision**: GO. driver.py OWNS the port-forward (start-if-absent,
idempotent) and uses http://127.0.0.1:8600 as the API base. No docker
install needed — the owner-approval path was never triggered.
