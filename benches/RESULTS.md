# Benchmark results

> Baseline-first methodology (research.md R12): pinned dlt measured first, same
> machine, same dataset, then rdlt. No multiple is quoted without both columns.

## Run: 2026-07-19 — jsonl → DuckDB, 200k nested records

- Dataset: 200,000 NDJSON records (39 MB), each with a nested object (`profile`) and a
  2-element list of objects (`tags`) → 600,000 emitted rows (root + child table).
- Baseline: `dlt[duckdb]==1.11.0`, python:3.12-slim container (podman), timing
  self-reported inside the process (excludes container/pip startup).
- rdlt: release build, `crates/rdlt-dest-duckdb/examples/jsonl_to_duckdb`
  (file → `raw_json` slabs → shred → DuckDB appender → transactional commit),
  peak RSS via `/usr/bin/time -v`.

| Metric | pinned dlt 1.11.0 | rdlt | multiple | target (design §8) | status |
|---|---|---|---|---|---|
| Wall time (200k records) | 19.60 s | 1.81 s | **10.8× faster** | ≥ 10× | ✅ met |
| Source records/s | 10,204 | 110,436 | 10.8× | — | — |
| Peak RSS | 1,985 MB | 410 MB | **4.8× less** (1/4.85) | ≤ 1/5th | ⚠️ marginal miss (target is 1/5.0) |

Caveats, stated so the numbers stay honest:
- One run each (not averaged); variance on this machine is low but unmeasured.
- rdlt's number includes file read, shred, child-table split, lineage hashing, DuckDB
  ingestion, and the atomic commit — the full pipeline, not a microbench.
- rdlt's 410 MB peak is dominated by DuckDB's own buffering + the 64 MB channel
  budget; `memory_limit` tuning on the DuckDB side is untried (likely closes the RSS
  gap past 1/5th).
- dlt runs in a container; its CPU-bound normalize work executes at native speed and
  its DuckDB is the same C++ library, so containerization skew is minimal.

## Still pending

| Benchmark | blocker |
|---|---|
| Shred microbench in isolation (≥20× target) | needs a dlt `normalize`-stage-only baseline for a like-for-like cut; engine-side criterion bench exists (`cargo bench -p rdlt-engine --bench shred`) |
| mock REST → Postgres (≥5×) | harness pieces exist (wiremock, Postgres dest); dlt-side pipeline not yet written |
| parquet → parquet passthrough (≥2×) | Arrow passthrough not wired in the engine yet |
| Cold start (≤1/20th) | not yet instrumented separately |

Reproduce: `benches/run-e2e.sh` (dataset gen + baseline container) and the example
binary above.
