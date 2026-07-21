# Quickstart: Benchmark Framework

## See what exists

```bash
cargo run -p rdlt-bench -- list                 # all cells: coordinates, class, competitors
cargo run -p rdlt-bench -- list --class gated   # the per-change confidence set
```

## Run cells

```bash
make release                                    # gated cells measure the release CLI
cargo run -p rdlt-bench -- run pg-wide-to-duckdb        # one cell
cargo run -p rdlt-bench -- run --class gated            # the gated set (quiet machine!)
cargo run -p rdlt-bench -- run --filter 'pg-*'          # a slice
```

Artifacts land in `benches/results/<cell-id>.json` (committed).
Competitor runs need the dlt image: built once from
`benches/competitors/dlt/` (the harness tells you if it's missing).

## Gate and report

```bash
cargo run -p rdlt-bench -- gate     # bars.toml vs latest artifacts; nonzero on violation
cargo run -p rdlt-bench -- report   # regenerate RESULTS.md tables (narrative untouched)
```

Makefile verbs: `TARGET=e2e make bench` (gated run), `TARGET=matrix
make bench` (full scoreboard), `TARGET=iai make bench` (instruction
gate — unchanged instrument). Cold start stays hyperfine under the
hood; it is declared as a cell so list/gate/report see it.

## The rules

`contracts/bench-harness.md` (BH1–BH8): cells as data, one protocol,
full metric set or explicit null, loud missing baselines, committed
versioned artifacts, wall-median-only gates, generated tables,
continuity or version-policy on migration.
