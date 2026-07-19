# rdlt benchmark harness

Methodology (research.md R12, spec SC-003..005): measure the **pinned Python dlt
baseline first**, same hardware, same datasets, then rdlt — one command each.
Engine-bound cases are labeled; API-bound reality is acknowledged.

## Targets (design doc §8)

| Benchmark | Target vs pinned dlt |
|---|---|
| Nested-JSON shred microbench (rows/s/core) | ≥ 20x |
| End-to-end: jsonl files → DuckDB | ≥ 10x |
| End-to-end: local mock REST API → Postgres (engine-bound) | ≥ 5x |
| Arrow passthrough: parquet → parquet | ≥ 2x |
| Peak RSS (file→DuckDB run) | ≤ 1/5th |
| Cold start to first row loaded | ≤ 1/20th |

## Layout

- `baseline/` — pinned-dlt container (see `baseline/Dockerfile`) + equivalent dlt
  pipelines over the same datasets.
- `run-e2e.sh` — generates datasets, runs baseline then rdlt, writes RESULTS.md rows.
- rdlt microbench: `cargo bench -p rdlt-engine --bench shred` (criterion; per-PR CI).

## Status

The rdlt side runs today (criterion + the CLI). The dlt-baseline container and the
full comparison matrix are **not yet executed** — RESULTS.md carries measured rdlt
numbers only until the baseline run lands. Targets stay unfalsified claims until then;
do not quote comparison multiples before RESULTS.md has both columns.
