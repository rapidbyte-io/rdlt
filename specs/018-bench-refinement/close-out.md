# Close-Out: Benchmark Refinement — Three-Way E2E Matrix

One row per contract clause and per phase deliverable; filled as work
completes. Evidence cites tests, commits, sessions, or spike records.

## Archive facts (captured at T001, pre-migration)

- **Archive commit** (every retired cell/fixture/artifact/bar checkout-able
  here): `40841ab` (main at feature start).
- **Retired bars (8, across 7 cells)**: jsonl-duckdb-200k ratio_vs dlt ≥10×
  AND rss_ratio_vs dlt ≤0.2; shred-only-200k ≥10×; rest-pg-100k ≥5×;
  parquet-passthrough ≥2×; pg-wide-duckdb-1m ≥6× vs dlt-pyarrow;
  pg-wide-pg-1m ≥6× vs dlt-pyarrow; cold-start absolute ≤40 ms.
- **Final recorded values (last session, 2026-07-24 gate run)**:
  jsonl-duckdb-200k 13.5× (14869/1105 ms), RSS 1/5.4 (353/1910 MB);
  shred-only-200k 12.0× (5898/490 ms); rest-pg-100k 6.7× (5523/820 ms);
  parquet-passthrough 3.5× (331/93 ms); pg-wide-duckdb-1m 7.8× vs
  dlt-pyarrow (10243/1306 ms); pg-wide-pg-1m 7.6× vs dlt-pyarrow
  (17652/2318 ms); cold-start 24.2 ms; cdc catch-up ~72k changes/s
  (6.96 s / 500k on 1M, 009 session).
- **Cold-start protocol to relocate**: hyperfine, warmups 3, runs 20,
  fresh workdir + db per run (`rm -rf` prepare), one-row pipeline, warm FS
  cache; bar ≤ 40 ms absolute (floor 23.6 ms × 1.5).

## Contract clauses

| Item | Phase | Disposition | Evidence |
|---|---|---|---|
| BR1 one matrix by rule (migration + sweep) | P0 | | |
| BR2 amend-then-delete governance | P0 | | |
| BR3 artifacts versioned (v2 + history feed) | P0+P2 | | |
| BR4 same conditions provable | P2 | | |
| BR5 probes before machinery | P1 | | |
| BR6 driver kind, zero artifact divergence | P3 | | |
| BR7 honest competitor configuration | P2+P3 | | |
| BR8 measurement-first enforcement | P4 | | |

## Phase deliverables

| Item | Phase | Disposition | Evidence |
|---|---|---|---|
| Constitution v1.1.0 (Amendment A) | P0 | | |
| 012 BH amendment (Amendment B) | P0 | | |
| Harness collapse (class/suite/mode, artifact v2, quiet guard) | P0 | | |
| library_mode deletion; parity fixture + CLI pins survive | P0 | | |
| Cold-start → instruments (≤40 ms kept) | P0 | | |
| Migration commit (25 cells / 10 fixtures / artifacts / scripts / 8 bars) | P0 | | |
| RESULTS.md rebuild + GOVERNANCE.md + history plumbing + Milestones seed | P0 | | |
| Spike: 5 probes go/no-go | P1 | | |
| Fixtures pg + rustfs reshape | P2 | | |
| 5 pipelines + 5 cells | P2 | | |
| dlt module slim (+s3fs +connectorx −duckdb; sqlalchemy deleted) | P2 | | |
| First recorded session (rdlt vs dlt, 10 arms verified) | P2 | | |
| Driver kind + variants discovery | P3 | | |
| Airbyte module (setup/driver/variants/README) | P3 | | |
| First 3-way session (15 arms or absent-with-reason) | P3 | | |
| Bars measurement-first (≤1/cell) | P4 | | |
