# rdlt benchmark framework

One declarative harness (`crates/rdlt-bench`) runs the six-cell,
three-way end-to-end matrix (rdlt / dlt / Airbyte, same conditions):
cells are DATA, the protocol is code, results are committed artifacts,
and enforcement exists only as bars set below recorded session floors.
Contract: `specs/018-bench-refinement/contracts/bench-refinement.md`
(BR1–BR8); constitution Principle VIII governs.

## Running it

```
TARGET=setup make bench    # once per machine: dlt image + Airbyte connections
                           # (Airbyte leg skips with guidance when no abctl
                           #  cluster is reachable — the matrix then runs 2-way)
TARGET=e2e make bench      # the recorded matrix (quiet machine!)
TARGET=report make bench   # regenerate RESULTS.md tables from artifacts
TARGET=gate make bench     # evaluate bars.toml against committed artifacts
TARGET=pg-to-pg-1m make bench   # one cell (globs work: TARGET='pg-*')
```

The harness owns fixture lifecycle (create → seed → shared within one
invocation → teardown), resets every destination before every arm's every
run, refuses to measure on a loaded machine (quiet guard; forced runs are
stamped `forced: true`), and verifies destination rowcounts — a mismatch
fails the cell rather than recording a bad number.

## Layout

| Path | What |
|---|---|
| `cells/e2e.toml` | The 018 five — each cell: fixtures, pipeline spec, rowcount verify, per-competitor arms, and a `note` rendered as the matrix caption |
| `cells/oracle.toml` | The 032 sixth cell (Oracle → Postgres). Its own file; the cells dir globs, so it joins the default run either way |
| `cells/pipelines/` | Pipeline-spec YAML templates (`{{conn}}`, `{{data}}`, `{{workdir}}` substituted per run) |
| `fixtures/` | Fixture registry + seeds (pg 1M×12 + 50%-changed twin; RUSTFS raw/lake; Oracle 23ai Free RDLT.EVENTS 200k×12) |
| `competitors/dlt/` | Self-timed container competitor: Dockerfile, `variants.toml` (connectorx headline, pyarrow context), in-container pipelines. Oracle is the one source where connectorx is NOT offered — it needs Oracle Instant Client (see RESULTS.md Caveats) |
| `competitors/airbyte/` | Driver competitor: `setup.py` + `driver.py` over an abctl kind cluster (its README carries the fairness policy and prerequisites) |
| `bench-setup.sh` | The `TARGET=setup` implementation (dlt image + Airbyte connections over throwaway seeded fixtures) |
| `bars.toml` | Enforcement bars — ≤ 1 per cell, each below a recorded session floor, each citing a RESULTS.md policy entry; `rdlt-bench gate` enforces them |
| `results/` | Committed JSON artifacts (format_version 3), one per cell (`raw/` is gitignored) |
| `history.jsonl` | Append-only per-session medians; the Trends section renders from it |
| `RESULTS.md` | Policy log, Caveats, Milestones (narrative) + GENERATED matrix/trends between `rdlt-bench` markers |
| `GOVERNANCE.md` | Coverage/semver/exclusion records |
| `check-cold-start.sh`, `compare-iai.sh`, `perf-baselines.json` | The instruments track (cold-start ≤ 40 ms + instruction-count gate) — separate from the matrix, run by `TARGET=iai make bench` |

## Rules that keep the numbers honest

- **Cell ids are frozen** — they appear in committed artifact filenames and
  history; renaming one orphans its recorded rows. New cells spell
  `<source>-to-<dest>-<size>` (e.g. `pg-to-pg-1m`).
- **Same conditions**: every arm reads the same seeded sources and writes to
  per-product destinations on the same server/store; competitors run their
  fastest honest configuration.
- **Bars are measurement-first**: no bar without a recorded session; cells at
  parity or behind carry no bar — the matrix reports them as they are.
- **Missing is loud**: an arm that cannot run records `Missing{reason}` in
  the artifact; nothing is silently skipped.
