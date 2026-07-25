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
| BR3 artifacts versioned (v2 + history feed) | P0+P2 | applied | format_version 2 (class/mode/suite gone; forced + extra added); v1 REJECTED naming 40841ab — pins v1_artifact_is_refused_naming_the_archive_commit + future_format_version_is_refused; history.jsonl appended from main.rs (deliberately not run_cell — selftest must not dirty it) |
| BR4 same conditions provable | P2 | applied | First recorded session 2026-07-25: all 5 cells verify PASS (actual==expected rowcounts in every artifact); same seeded sources, per-product destination databases, fingerprint (cpu/kernel/rustc/pins/dataset hashes/loadavg) in each artifact; timing boundaries stated in cell notes |
| BR5 probes before machinery | P1 | applied | 5/5 probes GO with evidence in spike/00–05 BEFORE any driver code exists. The spike caught two runtime defects machinery would have hit blind: ingress-nginx hostPort 443 hijack (portmap DNAT, controller scaled to 0) and node pids-limit exhaustion (2048→32768) — both owner-applied live, recorded as setup.py obligations. Job-API fields pinned from a real sync (rowsSynced, ISO-8601 duration); reset recipe = plain DROP, verified |
| BR6 driver kind, zero artifact divergence | P3 | applied | kind=driver in variants discovery (flat namespace, dup id names both files — pinned by test); driver.py consumes the SAME last-line JSON convention; artifact v2 unchanged except optional extra{} pass-through; all 15 arms of the 3-way session share one artifact schema; prerequisite failure = Missing{reason} — live T024 proof: with state.json absent, pg-to-s3parquet-1m recorded airbyte Missing("prerequisite failed: state.json missing — run setup.py") while dlt/dlt-pyarrow/rdlt all measured and verified 1M; unit test pins the same path. Full workspace gate green after the run (636-suite, containers present; caught+fixed a P0-migration casualty: iceberg tests' polaris_bootstrap.py moved into the crate at crates/rdlt-connector-iceberg/tests/fixtures/) |
| BR7 honest competitor configuration | P2+P3 | applied | connectorx = dlt headline (its fastest pg backend), pyarrow = labeled context; policy entry 2026-07-25 records the accepted consequence: 4.6x compressed, parquet parity 1.0x, dedup 0.9x LOSS recorded as-is. Airbyte half: job wall = headline (orchestration floor stated in Caveats), sync_s labeled context, cluster RSS never barred, matrix shows EVERY pairing least-flattering-first (report change, T023) |
| BR8 measurement-first enforcement | P4 | applied | 3 bars (pg-to-pg-1m >=4x, s3jsonl-to-pg >=40x, s3jsonl-to-s3parquet >=45x vs dlt), each below BOTH recorded sessions' floors, one policy entry citing them; gate PASS 3/3 against the justifying session; parity/loss cells + RSS + Airbyte ratios deliberately unbarred (recorded); Iceberg 3-way cell NOT taken (owner did not elevate; recorded in policy log) |

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
| Spike: 5 probes go/no-go | P1 | applied | All GO → US4 proceeds 3-way (no absent-with-reason needed). Pinned facts: API via supervised port-forward :8600 (ingress dead); pods → host fixtures at 169.254.1.2; headline = driver trigger→terminal wall w/ API duration as labeled context; discover-before-create required; first-job image pull = untimed warmup; ~20–50 s per-job orchestration floor goes in the cell note |
| Fixtures pg + rustfs reshape | P2 | applied | pg 5439 (src + dest_rdlt/dest_dlt/dest_airbyte + events_v2 twin), rustfs beta.11 :19110 raw/lake — both live in the recorded session |
| 5 pipelines + 5 cells | P2 | applied | 6 pipeline specs (dedup = load1 + measured), 5 cells in e2e.toml — all executed live 2026-07-25 |
| dlt module slim (+s3fs +connectorx −duckdb; sqlalchemy deleted) | P2 | applied | 5 scripts, variants dlt + dlt-pyarrow; s3fs endpoint wiring validated live in both s3jsonl cells |
| First recorded session (rdlt vs dlt, 10 arms verified) | P2 | applied | 13 arms (5 rdlt + 5 dlt + 3 dlt-pyarrow context), 5 runs each, all rowcount-verified; artifacts v2 committed; history.jsonl +13 lines; matrix + first Trends render. Live catch: dedup cell spec collided a query-stream name with a reflected table — both loads renamed to query stream events_merged (first live execution of the P2-built spec) |
| Driver kind + variants discovery | P3 | applied | commit 2f3b145 (competitors.rs): VariantKind, discover_variants, driver exec via module venv convention, runs precedence; 4 new tests |
| Airbyte module (setup/driver/variants/README) | P3 | applied | commits 6908e05 + 341a644; stdlib-only; smoke-proven incl. dedup shape; discover fire/poll decoupled after live 0/5 failure (pod-spam + PF-drop swallowing — recorded); setup requires fixtures up (run with harness-shaped seeds) |
| First 3-way session (15 arms or absent-with-reason) | P3 | applied | 2026-07-25: 15/15 arms measured, zero Missing, all rdlt verifies exact; airbyte medians 45.4/45.4/45.4/45.4/60.4 s (floor-dominated, caveat added); artifacts + history committed; matrix renders all pairings |
| Bars measurement-first (≤1/cell) | P4 | applied | see BR8 row; bars.toml carries per-bar floor citations; empty-header narrative replaced by the recorded set |
