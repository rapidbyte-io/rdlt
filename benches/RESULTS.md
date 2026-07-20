# Benchmark results

> Baseline-first methodology (research.md R12): pinned dlt measured first, same
> machine, same dataset, then rdlt. No multiple is quoted without both columns.
>
> **Baseline version policy**: the pin tracks the LATEST stable dlt at
> measurement time; a pin bump re-measures every cell before any multiple is
> quoted. Current baseline: **dlt 1.29.0** (bumped from 1.11.0 on 2026-07-20 —
> dlt improved materially between those releases, and the multiples below
> reflect that honestly).

## The matrix — 2026-07-20, dlt 1.29.0, all cells same-session pairs

| Cell | pinned dlt 1.29.0 | rdlt | multiple | target (design §8) | status |
|---|---|---|---|---|---|
| jsonl → DuckDB, 200k nested records (product CLI) | 14.38 s / 1,870 MB | 1.04 s / 348 MB | **13.8× faster** | ≥ 10× | ✅ met |
| — peak RSS of that run | 1,870 MB | 348 MB | **1/5.4** | ≤ 1/5th | ✅ met |
| Shred stage only (dlt `normalize()` vs `shred_only`) | 5.95 s | 0.50 s | **12.0× faster** | ≥ 20× | ❌ missed (honest) |
| mock REST → Postgres, 100k records | 5.50 s / 180 MB | 0.85 s / 29 MB | **6.5× faster** | ≥ 5× | ✅ met |
| Arrow passthrough: parquet → parquet | 0.228 s / 195 MB | 0.083 s / 47 MB | **2.75× faster** | ≥ 2× | ✅ met |
| parquet → DuckDB (bonus context row) | 0.395 s / 335 MB | 0.329 s / 159 MB | 1.2× | — | — |
| Cold start, one-row pipeline | 0.418 s | 0.030 s | **1/14.2 overhead** | ≤ 1/20th | ❌ missed (honest) |

Caveats, stated so the numbers stay honest:

- Datasets/methodology unchanged from the 1.11.0-era rows: 200k nested NDJSON
  (→600k rows with children), the same records re-encoded as parquet by rdlt,
  a 100k-record mock API (100 pages), one-row cold start. Wall times are
  medians (5 runs for flagship/shred/cold-rdlt; dlt in-process self-timing as
  before; cold-start dlt timed from before `import dlt`, interpreter boot
  still excluded — generous to the baseline).
- **Cold start regressed from met (1/22.7 vs dlt 1.11.0) to missed**: dlt's
  startup improved ~21% and lost its old bimodality, while rdlt's one-row run
  measured 30 ms this session. This is the version policy doing its job — the
  miss is recorded, not hidden. rdlt cold start is dominated by DuckDB
  open+catalog work; untried levers noted in the backlog.
- Shred-only moved 8.1× → 12.0× NOT because rdlt changed since that row, but
  because BOTH sides were re-measured cleanly (rdlt's 0.50 s was previously
  contended by a background mutation run; dlt's normalize also improved).
- dlt 1.29.0 vs 1.11.0 on this machine: flagship 19.60→14.38 s, normalize
  7.63→5.95 s, cold 0.527→0.418 s, REST→PG 7.49→5.50 s — credit where due.
- The parquet→DuckDB bonus row remains near-parity by design (both sides
  reduce to the same DuckDB C++ appender); parquet→parquet is the
  engine-bound claim.

## Perf-regression gate (feature 003, G1)

Instruction-count baselines for the hot paths live in
`benches/perf-baselines.json` (iai-callgrind; >3% regression blocks CI;
cross-toolchain comparisons refused — re-record deliberately). Recorded
2026-07-20: shred (tape) 362 M instructions / 10k nested rows; passthrough
602 k; identity keyed/keyless 20.5 M / 29.3 M.

## History

- 2026-07-19 (dlt 1.11.0, feature 002 merge): flagship 11.3×, passthrough
  2.4×, RSS miss at 1/3.1.
- 2026-07-20 (dlt 1.11.0, feature 003 optimizations): flagship 18.6×,
  shred-only 8.1×, REST→PG 5.5×, cold 1/22.7, RSS met at 1/5.6.
- 2026-07-20 (baseline bumped to dlt 1.29.0): the matrix above.

Reproduce: `benches/run-e2e.sh` (jsonl, parquet, cold-start cells) and the
REST→Postgres recipe in RESULTS history / `benches/baseline/pipeline_rest_pg.py`.
