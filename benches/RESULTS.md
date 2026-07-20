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
| Wall time (200k records) | 19.60 s | 0.92 s | **21.3× faster** | ≥ 10× | ✅ met (product claim) |
| Source records/s | 10,204 | 216,900 | 21.3× | — | — |
| Peak RSS | 1,985 MB | 515 MB | **3.9× less** | ≤ 1/5th | ⚠️ missed; see caveats |

(Re-measured 2026-07-20 after feature 003's hot-path work: tape shredder, hex
encoder, zero-clone builds — was 1.73 s / 642 MB at feature-002 merge.)

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
| parquet → parquet, wall | 0.185 s | 0.078 s | **2.4× faster** | ≥ 2× | ✅ met |
| parquet → parquet, peak RSS | 218 MB | 47 MB | **4.6× less** | — | — |
| parquet → DuckDB (bonus), wall | 0.352 s | 0.317 s | 1.11× faster | — (context row) | — |
| parquet → DuckDB (bonus), peak RSS | 343 MB | 203 MB | 1.7× less | — | — |

The bonus row is near-parity by design: both sides reduce to "hand arrow batches to
the same DuckDB C++ library", so there is little engine work left to win on. The
parquet→parquet cell isolates actual engine overhead and is the honest ≥2× claim.

## Run: 2026-07-20 — remaining design-§8 cells (feature 003)

- Same machine and pinned `dlt==1.11.0` container as every prior row; baseline
  measured first in each cell.

**mock REST → Postgres, 100k records (100 pages, page-number pagination)**
- Both sides hit the same in-memory mock API (pre-rendered pages,
  `crates/rdlt-source-rest/examples/mock_api.rs`) and the same Postgres 16
  container, sequentially. dlt = `rest_api` source; rdlt = bundled REST source
  via the release CLI. 100k source records → 300k rows (root + tags children).

| Metric | pinned dlt | rdlt | multiple | target | status |
|---|---|---|---|---|---|
| Wall time | 7.49 s | 1.37 s | **5.5× faster** | ≥ 5× | ✅ met |
| Peak RSS | 250 MB | 49 MB | **5.1× less** | — | — |

**Shred stage only, 200k nested records (no destination I/O either side)**
- dlt: `pipeline.normalize()` timed alone (extract pre-staged, untimed) —
  `benches/baseline/normalize_only.py`. rdlt: full shred path (parse → shape
  observation → schema resolution → Arrow build) over the same file in 8 MB
  slabs — `cargo run --release -p rdlt-engine --example shred_only`. Median of 3.

| Metric | pinned dlt | rdlt | multiple | target | status |
|---|---|---|---|---|---|
| Stage time | 7.63 s | 0.95 s | **8.1× faster** | ≥ 20× | ❌ missed (honest) |

Re-measured 2026-07-20 after the US3 work (was 4.6×). The profile-driven story:
the tape rewrite alone moved nothing — instruction counts proved the Value trees
were never the bottleneck. The real cost was `RowId::to_hex` formatting via
`write!("{:02x}")` (48% of ALL shred instructions), fixed with a table encoder,
plus per-cell String clones. Shred stage: 1.094 G → 531 M instructions (2.06×),
wall 1.66 s → 0.95 s. The remaining gap to 20× is allocator traffic + blake3 +
arrow building — hash swap measured and REJECTED (blake3 is ~16% of the stage;
the >30% e2e switch bar is unreachable; see design doc §5.4).

**Cold start, one-row pipeline**
- rdlt: release CLI, 10 fresh runs, median, INCLUDING full process startup.
- dlt: in-container, timed from before `import dlt` through load (interpreter
  boot ~30 ms still excluded — generous to the baseline); median of 5; dlt's
  pipeline phase is bimodal (0.53 s / 1.53 s) — median shown.

| Metric | pinned dlt | rdlt | multiple | target | status |
|---|---|---|---|---|---|
| Startup→loaded | 0.527 s | 0.023 s | **22.7× less overhead** | ≤ 1/20th | ✅ met |

## Perf-regression gate (feature 003, G1)

Instruction-count baselines for the hot paths live in
`benches/perf-baselines.json` (iai-callgrind; >3% regression blocks CI —
`TARGET=iai make bench`). Recorded 2026-07-20 post-optimization: shred (tape)
531 M instructions / 10k nested rows; tree reference 549 M; passthrough 601 k;
identity keyed/keyless 20.5 M / 29.3 M.

## Still pending

| Benchmark | blocker |
|---|---|
| Shred-only ≥20× | 8.1× after the feature-003 hot-path work; next levers are allocator traffic and arrow building (documented miss) |
| Flagship RSS ≤1/5th | 515 MB vs 397 MB target after 003; DuckDB `memory_limit` tuning still pending (T024) |

Reproduce: `benches/run-e2e.sh` (dataset gen, baseline container, rdlt CLI runs —
jsonl and parquet cells).
