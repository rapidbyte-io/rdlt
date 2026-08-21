# rdlt-bench

rdlt's declarative benchmark harness: cells as data, bars as policy,
results as artifacts. A cell names a fixture set, an rdlt pipeline and
its competitor arms (same seeded sources, rowcount-verified on every
arm); `bars.toml` states which ratios are CONTRACTS rather than
scoreboard; the gate refuses a recording that violates one.

```text
rdlt-bench list                 # the cell matrix
rdlt-bench run --filter 'pg-*'  # measured runs -> benches/results/
rdlt-bench gate                 # evaluate bars.toml against artifacts
rdlt-bench report               # regenerate RESULTS.md tables
```

Governance lives in `benches/GOVERNANCE.md` in the repository: new
cells are scoreboard unless a recorded session grants them a bar, and
bars move only with their measurement attached.
