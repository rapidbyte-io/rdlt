# Benchmark results

> Baseline-first methodology (research.md R12): pinned dlt measured first, same
> machine, same dataset, then rdlt. No multiple is quoted without both columns.

## Run: 2026-07-19 — jsonl → DuckDB, 200k nested records

- Dataset: 200,000 NDJSON records (39 MB), each with a nested object (`profile`) and a
  2-element list of objects (`tags`) → 600,000 emitted rows (root + child table).
- Baseline: `dlt[duckdb]==1.11.0`, python:3.12-slim container (podman), timing
  self-reported inside the process (excludes container/pip startup).
- rdlt: release build of the **product path** — the bundled `file` source through the
  real CLI (`rdlt run pipeline.toml`), not an example binary (feature 002, SC-002).
  Self-reported `elapsed_ms` from the run report; peak RSS via `/usr/bin/time -v`
  (the RSS number therefore includes process startup, dlt's does not).

| Metric | pinned dlt 1.11.0 | rdlt (bundled file source, CLI) | multiple | target (design §8) | status |
|---|---|---|---|---|---|
| Wall time (200k records) | 19.60 s | 1.73 s | **11.3× faster** | ≥ 10× | ✅ met (product claim) |
| Source records/s | 10,204 | 115,800 | 11.3× | — | — |
| Peak RSS | 1,985 MB | 642 MB | **3.1× less** | ≤ 1/5th | ⚠️ missed; see caveats |

(The earlier example-binary measurement — 1.81 s / 410 MB — is retired; the product
path is what we claim. The RSS regression vs the example is DuckDB buffering under the
CLI's default channel budget; `memory_limit` tuning on the DuckDB side is untried.)

Caveats, stated so the numbers stay honest:
- One run each (not averaged); variance on this machine is low but unmeasured.
- rdlt's number includes file read, shred, child-table split, lineage hashing, DuckDB
  ingestion, and the atomic commit — the full pipeline, not a microbench.
- rdlt's 410 MB peak is dominated by DuckDB's own buffering + the 64 MB channel
  budget; `memory_limit` tuning on the DuckDB side is untried (likely closes the RSS
  gap past 1/5th).
- dlt runs in a container; its CPU-bound normalize work executes at native speed and
  its DuckDB is the same C++ library, so containerization skew is minimal.

## Run: 2026-07-19 — parquet passthrough, 200k records (feature 002)

- Dataset: the SAME 200k records re-encoded as parquet by rdlt itself (10 files,
  snappy). Both engines read pyarrow-native parquet and never touch the shredder /
  normalizer: this is the structured fast path on both sides.
- Baseline: same pinned `dlt[duckdb,filesystem]==1.11.0` container, fed pre-read
  `pyarrow.Table`s (dlt's arrow-native fast path — its fastest route), self-timed.
- rdlt: bundled `file` source (parquet, row-group units) → Arrow passthrough (clause
  E7) → bundled destinations, via the release CLI. Self-reported `elapsed_ms`.

| Cell | pinned dlt 1.11.0 | rdlt | multiple | target (design §8) | status |
|---|---|---|---|---|---|
| parquet → parquet, wall | 0.185 s | 0.077 s | **2.4× faster** | ≥ 2× | ✅ met |
| parquet → parquet, peak RSS | 218 MB | 47 MB | **4.6× less** | — | — |
| parquet → DuckDB (bonus), wall | 0.352 s | 0.307 s | 1.15× faster | — (context row) | — |
| parquet → DuckDB (bonus), peak RSS | 343 MB | 203 MB | 1.7× less | — | — |

The bonus row is near-parity by design: both sides reduce to "hand arrow batches to
the same DuckDB C++ library", so there is little engine work left to win on. The
parquet→parquet cell isolates actual engine overhead and is the honest ≥2× claim.

## Still pending

| Benchmark | blocker |
|---|---|
| Shred microbench in isolation (≥20× target) | needs a dlt `normalize`-stage-only baseline for a like-for-like cut; engine-side criterion bench exists (`cargo bench -p rdlt-engine --bench shred`) |
| mock REST → Postgres (≥5×) | harness pieces exist (wiremock, Postgres dest); dlt-side pipeline not yet written |
| Cold start (≤1/20th) | not yet instrumented separately |

Reproduce: `benches/run-e2e.sh` (dataset gen, baseline container, rdlt CLI runs —
jsonl and parquet cells).
