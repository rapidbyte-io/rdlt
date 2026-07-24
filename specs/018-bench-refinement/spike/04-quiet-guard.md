# Probe 4 — Quiet-guard fit: GO

**Question**: does the idle kind cluster's load fit under the bench
quiet guard?

**Answer: GO — no allowance needed.**

Evidence (2026-07-24): with the full Airbyte cluster idle-running,
host 1-min loadavg = 0.21 on 32 cores (5-min 0.31). The guard's
threshold operates on load-per-core; an idle cluster contributes noise
well under it.

**Decision**: the cluster may stay up during recorded sessions; no
recorded allowance, no stop-between-arms requirement. (Re-check printed
loadavg in every session artifact as usual.)
