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
| BR1 one matrix by rule (migration + sweep) | P0 | applied (P0 half) | Migration commit 212edf5: 64 files deleted (25 cells, 10 fixtures, all v1 artifacts, 5 dlt scripts, 8 bars), message cites every final value + archive 40841ab; selftest exempt; vocabulary sweep ZERO hits (verified independently). The 5 new cells land in P2 |
| BR2 amend-then-delete governance | P0 | applied | 631d9bd (constitution v1.1.0 + 012 BH note) PRECEDES 212edf5 — order verified in git log. Ops note: the first amendment attempt committed only Amendment B (scripted-edit assert failure, caught by tree grep); fixed with exact edits + commit amend BEFORE any deletion existed |
| BR3 artifacts versioned (v2 + history feed) | P0+P2 | applied (P0 half) | format_version 2 (class/mode/suite gone; forced + extra added); v1 REJECTED naming 40841ab — pins v1_artifact_is_refused_naming_the_archive_commit + future_format_version_is_refused; history.jsonl appended from main.rs (deliberately not run_cell — selftest must not dirty it) |
| BR4 same conditions provable | P2 | | |
| BR5 probes before machinery | P1 | | |
| BR6 driver kind, zero artifact divergence | P3 | | |
| BR7 honest competitor configuration | P2+P3 | | |
| BR8 measurement-first enforcement | P4 | | |

## Phase deliverables

| Item | Phase | Disposition | Evidence |
|---|---|---|---|
| Constitution v1.1.0 (Amendment A) | P0 | applied | Principle VIII to cells/bars, mechanism strengthened (recorded-session floor); Sync Impact Report in header; SCOREBOARD grep = 0 |
| 012 BH amendment (Amendment B) | P0 | applied | Recorded note appended to specs/012-bench-harness/contracts/bench-harness.md |
| Harness collapse (class/suite/mode, artifact v2, quiet guard) | P0 | applied | Class+Mode enums deleted; subprocess-only; classless quiet guard (wait-5min then refuse; FORCE gives forced:true annotation); gate cross-validation = bar-references-existing-cell; Timing enum KEPT deliberately (carries the live competitor last-line JSON convention — recorded deviation) |
| library_mode deletion; parity fixture + CLI pins survive | P0 | applied | library_mode.rs + bench-side pin deleted; fixture header names rdlt-cli as sole consumer; CLI parity pins 2/2 green (FR-005) |
| Cold-start → instruments (≤40 ms kept) | P0 | applied | benches/check-cold-start.sh (relocated protocol verbatim); wired into TARGET=iai and make check; measured 24.8 ms median at relocation — passes |
| Migration commit (25 cells / 10 fixtures / artifacts / scripts / 8 bars) | P0 | applied | 212edf5 (see BR1 row) |
| RESULTS.md rebuild + GOVERNANCE.md + history plumbing + Milestones seed | P0 | applied | RESULTS.md net 359 lines removed, 78 added (matrix/caveats/trends/milestones; policy entry cites 40841ab; 8 retired claims seeded w/ evidence commit); GOVERNANCE.md carries relocated records verbatim; report regeneration idempotent |
| Spike: 5 probes go/no-go | P1 | | |
| Fixtures pg + rustfs reshape | P2 | | |
| 5 pipelines + 5 cells | P2 | | |
| dlt module slim (+s3fs +connectorx −duckdb; sqlalchemy deleted) | P2 | | |
| First recorded session (rdlt vs dlt, 10 arms verified) | P2 | | |
| Driver kind + variants discovery | P3 | | |
| Airbyte module (setup/driver/variants/README) | P3 | | |
| First 3-way session (15 arms or absent-with-reason) | P3 | | |
| Bars measurement-first (≤1/cell) | P4 | | |
