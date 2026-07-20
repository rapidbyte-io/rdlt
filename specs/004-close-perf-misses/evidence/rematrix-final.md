# Evidence: Feature-Close Full-Matrix Re-Measure (T017)

**Date**: 2026-07-20 | **Identity**: per [environment.md](environment.md)
(blashyrkh, Ryzen AI MAX+ 395, kernel 7.0.12-201.fc44, rustc 1.96.0
ac68faa20, hyperfine 1.20.0, podman 5.8.3 host engine via
`distrobox-host-exec` shim; pin FROZEN at dlt 1.29.0; dataset
regenerated via the run-e2e.sh generator, 200,000 lines, sha256
`22bdb31b…9bfb` verified)

No engine code changed during feature 004 — this re-measure closes the
feature on the same tree the close-out measured, per protocol P6.
Baseline-first, same-session pairs, quiet machine, 5 runs per cell
(cold-rdlt: hyperfine 3 warmups + 20 runs, protocol P3). dlt-side
scripts frozen (`benches/baseline/*.py`, unchanged).

## Raw runs (medians bolded into the matrix)

| Cell | Side | Runs | Median |
|---|---|---|---|
| flagship jsonl→DuckDB | dlt | 14.219, 14.192, 14.106, 14.025, 14.007 s (RSS ~1,880 MB each) | **14.11 s / 1,880 MB** |
| | rdlt | 1.07, 1.09, 1.10, 1.09, 1.09 s; RSS 343.9, 342.3, 345.2, 349.7, 351.2 MB | **1.09 s / 345 MB** → 12.9×, RSS 1/5.4 |
| shred-only | dlt normalize | 5.7185, 5.7223, 5.7610, 5.7525, 5.7845 s | **5.752 s** |
| | rdlt shred_only | see [resolution-shred.md](resolution-shred.md) (two 5-run series) | **0.5221 s** → 11.0× |
| REST→Postgres 100k | dlt | 5.444, 5.384, 5.389, 5.426, 5.403 s; RSS 183, 171, 181, 164, 173 MB | **5.40 s / 173 MB** |
| | rdlt | 0.67, 0.76, 0.68, 0.70, 0.74 s; RSS 54, 31, 37, 42, 31 MB | **0.70 s / 37 MB** → 7.7× |
| parquet→parquet | dlt | 0.2077, 0.2097, 0.2047, 0.2119, 0.2131 s | **0.209 s / 263 MB** |
| | rdlt | 0.08, 0.09, 0.08, 0.08, 0.09 s | **0.08 s / 47 MB** → 2.6× |
| parquet→DuckDB (scoreboard) | dlt | 0.3950, 0.3872, 0.3887, 0.3869, 0.3874 s | **0.387 s / 419 MB** |
| | rdlt | 0.37, 0.37, 0.38, 0.38, 0.37 s | **0.37 s / 161 MB** → 1.0× |
| cold start (gated absolute) | rdlt | hyperfine 20 runs: 23.4 ± 1.5 ms (20.2–26.6); dedicated T014 series median 23.57 ms | **23.6 ms ≤ 40 ms bar** ✅ |
| cold start (scoreboard ratio) | dlt | 0.4198, 0.4132, 0.4092, 0.4171, 0.4251 s | **0.417 s** → rdlt 1/17.7 |

## Method notes

- REST→PG: `mock_api` (100k rows / 100 pages, port 8642) on the
  reference machine; postgres:16 container; dlt container run with
  `--network=host` against `127.0.0.1` for both endpoints; `raw*`
  schemas dropped between every run on both sides.
- rdlt walltimes for CLI cells are `/usr/bin/time -v` (10 ms
  quantization — coarse but the same instrument the matrix always used
  for these cells); the gated cold cell uses hyperfine per P3.
- Session movements vs the close-out matrix (all same code, both sides):
  flagship multiple 13.8× → 12.9× (rdlt 1.04→1.09 s), shred 11.6× →
  11.0× (dlt 5.95→5.75 s), REST→PG 6.5× → 7.7× (rdlt 0.85→0.70 s),
  pq→DuckDB 1.2× → 1.0× (rdlt 0.329→0.37 s). This ±2–10% inter-session
  spread on paired cells is why gated bars carry explicit headroom.
- Full raw logs: session scratch (`t017-run-e2e.log`, `t017-5runs.log`,
  `t017-restpg.log`) — medians and per-run values reproduced above in
  full, so the artifact stands alone.
