# Probe 2 — Networking (pods → host services): GO

**Question**: can connector pods inside the kind cluster reach the
host-published bench fixtures (postgres, RUSTFS)?

**Answer: GO — address form pinned: `169.254.1.2`.**

Evidence (2026-07-24):
- Gateway-IP form FAILS: pod → 10.89.0.1:<port> refused (pasta does not
  expose host services on the bridge gateway).
- The node resolves `host.containers.internal` = `host.docker.internal`
  = **169.254.1.2** (pasta's host mapping). Pods use cluster DNS, so
  connections must use the IP literal.
- Pod (busybox) → 169.254.1.2:18999 (throwaway host 0.0.0.0 HTTP
  listener): OK. Pod → 169.254.1.2:5439 (throwaway podman-published
  postgres:16-alpine, the exact fixture shape): OK.

**Decision**: Airbyte source/destination configs use host
`169.254.1.2` with the fixture ports (pg 5439, RUSTFS 19110).
