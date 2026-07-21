# rdlt benchmark framework

One declarative harness (feature 012, `crates/rdlt-bench`) runs every
end-to-end cell: cells are DATA, the protocol is code, results are committed
artifacts, and the gated bars are enforced by a command — never by prose.
Contract: `specs/012-bench-harness/contracts/bench-harness.md` (BH1–BH8).

## Layout

| Path | What |
|---|---|
| `cells/*.toml` | The cell matrix — source × destination × workload × mode, `gated` or `scoreboard`. Adding a pairing that reuses existing fixtures touches only TOML. |
| `cells/pipelines/` | Pipeline-spec YAML templates (`{{conn}}`, `{{data}}`, `{{workdir}}` substituted per run) |
| `fixtures/` | Fixture registry + seeds/generators (containers, datasets — identity hashes recorded) |
| `competitors/dlt/` | The pinned dlt baseline: Dockerfile, variant registry (`variants.toml`), in-container pipelines |
| `bars.toml` | The gated bars (+ tolerances + policy pointers). `rdlt-bench gate` enforces them. |
| `results/` | Committed JSON artifacts, one per cell (`raw/` is gitignored) |
| `RESULTS.md` | Narrative (history, policy, honest caveats) + GENERATED tables between `rdlt-bench` markers |
| `compare-iai.sh`, `perf-baselines.json` | The instruction-count gate (iai-callgrind) — a separate instrument, unchanged |

## Run

```bash
make release                                  # gated cells measure the release CLI
podman build -t rdlt-baseline benches/competitors/dlt/   # once per dlt pin

cargo run -p rdlt-bench -- list               # the matrix
cargo run -p rdlt-bench -- run pg-wide-duckdb-1m
TARGET=e2e    make bench                      # the gated set (quiet machine!)
TARGET=matrix make bench                      # everything, incl. scoreboards
TARGET=gate   make bench                      # bars.toml vs committed artifacts
TARGET=report make bench                      # regenerate RESULTS.md tables
```

## Method (unchanged since feature 004; now executable)

- **Baseline first**: competitors run before rdlt, same session, same seeded
  datasets (identity hashes in every artifact fingerprint).
- **Gated = the product**: gated numbers come from the release CLI as a
  subprocess. Library-mode cells add per-stream attribution as scoreboard
  detail only.
- **Quiet machine**: gated runs REFUSE on a loaded machine
  (`RDLT_BENCH_FORCE=1` runs annotated instead).
- **Metrics**: wall (median/p95 over N runs), rows/s + MB/s from the
  RunReport's own accounting, CPU + peak RSS via procfs (rdlt) and cgroup v2
  (dlt container; its wall number stays in-process self-timed — continuity
  with every recorded multiple). CPU/RSS are recorded, not gated.
- **Bars move only with evidence**: every bar in `bars.toml` points at the
  policy record that set it; violations exit nonzero naming cell and value.

Instruction-count (`TARGET=iai make bench`) and the criterion shred micro
(`make bench`) are separate instruments with their own baselines.
