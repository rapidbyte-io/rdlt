# Benchmark results

Three-way end-to-end matrix: **rdlt vs dlt vs Airbyte**, same seeded source,
same destination instance, same quiet machine — each cell measured baseline-first
and reported. Every number in the Matrix and Trends sections is generated from
committed artifacts (`TARGET=report make bench`); nothing is quoted without its
competitor column.

**Pin policy**: each competitor variant carries a version pin
(`benches/competitors/*/variants.toml`); a pin bump re-measures every cell
before any multiple is quoted (bump ⇒ re-measure). Coverage, semver, and
classified-exclusion records live in [`GOVERNANCE.md`](GOVERNANCE.md).

**Policy log** (one entry per governance event; newest first):

- **2026-07-25 — the dedup cell was measuring three times its own claim
  (feature 019 US1)**: `pg-to-pg-dedup-1m` declared one query stream and its
  postgres source ALSO discovered every table in the schema, so rdlt moved
  `events` + `events_v2` + the declared `events_merged` — 3,000,000 rows —
  while dlt's script moved 1,000,000. The arms were never comparable. The
  committed artifact recorded both numbers side by side (`rdlt.rows` 3000000,
  `verify.actual_rows` 1000000) and nothing compared them.
  **Superseded values, withdrawn**: rdlt 14.81 s median (±8%) = **0.8× vs
  dlt** in the three-way session, and 14.75 s = 0.9× in the two-way session the
  same day. **Corrected, same machine and fixtures**: 5.00 s (±1%) =
  **2.5× vs dlt**, peak RSS 284 → 143 MB, CPU 5.55 → 1.51 s. The cell was
  never behind; the earlier entry below calling the merge path "an
  optimization target" was chasing an artifact of the cell spec.
  Three things changed so it cannot recur: an empty `tables:` list now means
  "discover no tables" (previously inexpressible — the only spellings were
  "these tables" and "all tables"); every cell declares its full expected table
  set and the harness **fails any run whose delivered set differs**; and the
  artifact `format_version` goes 2 → 3, so pre-check artifacts are refused
  rather than quoted. Re-recording under v3 exposed the same blind spot in two
  more cells — `s3jsonl-to-pg-200k` and `s3jsonl-to-s3parquet-200k` verified
  only `events` while also landing `events__tags` at 400,000 rows — now both
  declared. A `ratio_vs` bar at 2.0× is set for the cell from this session's
  2.5× floor; unlike the three bars below it rests on ONE recorded session, so
  a second session may tighten it. Session: 2026-07-25, five cells, 15/15 arms
  rowcount-verified, quiet guard passed.

- **2026-07-25 — bars return, measurement-first (feature 018 P4)**: three
  bars set from the first recorded three-way session (15/15 arms,
  rowcount-verified), each below its recorded floor: `pg-to-pg-1m` ≥ 4×
  vs dlt (floors 5.3× three-way / 4.6× two-way), `s3jsonl-to-pg-200k`
  ≥ 40× vs dlt (floors 55.3× / 54.9×), `s3jsonl-to-s3parquet-200k` ≥ 45×
  vs dlt (floors 60.1× / 61.1×). Deliberately NOT barred:
  `pg-to-s3parquet-1m` (recorded parity, 1.0×) and `pg-to-pg-dedup-1m`
  (recorded 0.9× — rdlt behind; the matrix reports it until the merge
  path improves and a new session justifies a bar); no RSS bar (one bar
  per cell, and the wall ratio is the flagship claim); no bar ever binds
  an Airbyte ratio (its job wall is floor-dominated context, not an
  engine comparison). A three-way Iceberg cell was considered and NOT
  taken — the owner did not elevate lakehouse scope this feature
  (plan P4); the 016 `iceberg-polaris-200k` evidence remains in the
  archive at `40841ab`.

- **2026-07-25 — dlt baseline = connectorx (first recorded session)**: the
  dlt arm's headline backend is `connectorx` — dlt's fastest supported
  postgres extractor — with `dlt-pyarrow` kept as labeled context, per the
  honest-fastest rule. Consequence, accepted knowingly: multiples compress
  or invert versus the retired pyarrow-baselined bars (pg-to-pg-1m 4.6×
  where the retired bar era showed 7.6×; pg-to-s3parquet-1m is at parity
  1.0×; pg-to-pg-dedup-1m rdlt LOSES at 0.9× — recorded as-is, the merge
  path is an optimization target, not a reporting problem). The s3-jsonl
  cells (54.9×, 61.1×) reflect dlt's filesystem/jsonl reader; the cell
  notes state the regime. Session: 2026-07-25, dlt 1.29.0, five cells,
  every arm rowcount-verified.

- **2026-07-24 — matrix rebuild + 8-bar retirement (feature 018, archive
  commit `40841ab`)**: the benchmark collapsed to five end-to-end cells. The
  gated/scoreboard taxonomy, cell suites, the library/hyperfine run modes, 25
  legacy cells, 10 fixtures, every v1 artifact, and all 8 bars were retired in
  one migration commit; the cold-start check moved to the instruments track
  (`benches/check-cold-start.sh`, ≤ 40 ms). Enforcement returns
  measurement-first (constitution v1.1.0): `bars.toml` is empty until the first
  recorded three-way session sets at most one bar per cell, each below its cited
  session floor with a policy-log entry here. Every retired cell's final value
  is recorded under Milestones below; the full pre-migration matrix and its
  artifacts are checkout-able at `40841ab`.

## Matrix

The five e2e cells, three-way. Every arm is rowcount-verified against the cell's
DECLARED table set — a run that lands a table the cell did not declare fails
before it is recorded, because its timing would cover work the competitor arm
never did.

<!-- rdlt-bench:BEGIN matrix -->
| Cell | rdlt median | vs baseline | Target | Status | rows/s | MB/s | peak RSS |
|---|---|---|---|---|---|---|---|
| pg-to-pg-1m | 744.2 ms (±6%) | **13.7×** (dlt: 10.17 s); 23.1× (dlt-pyarrow: 17.19 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | ≥ 4× | PASS | 1343642 | 255.9 | 104 MB |
| pg-to-s3parquet-1m | 913.8 ms (±5%) | **1.8×** (dlt: 1.67 s); 11.8× (dlt-pyarrow: 10.80 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | — | — | 1094312 | 208.4 | 110 MB |
| s3jsonl-to-pg-200k | 639.8 ms (±3%) | **98.1×** (dlt: 62.76 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | ≥ 40× | PASS | 937773 | 223.5 | 170 MB |
| s3jsonl-to-s3parquet-200k | 848.9 ms (±3%) | **68.9×** (dlt: 58.52 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | ≥ 45× | PASS | 706782 | 168.5 | 207 MB |
| pg-to-pg-dedup-1m | 4.37 s (±2%) | **2.8×** (dlt: 12.45 s); 4.7× (dlt-pyarrow: 20.74 s); airbyte: MISSING (prerequisite failed for `airbyte`: abctl cluster unreachable (kubectl get ns airbyte-abctl)) | ≥ 2× | PASS | 228796 | 43.4 | 97 MB |
| oracle-to-pg-200k | 832.6 ms (±4%) | **4.1×** (dlt: 3.42 s); 54.6× (airbyte: 45.45 s) | — | — | 240205 | 46.9 | 60 MB |

_Generated by `rdlt-bench report` from committed artifacts (recorded 2026-08-01, 2026-08-03; airbyte 2.1.1, dlt 1.29.0)._
<!-- rdlt-bench:END matrix -->

## Caveats

Stated so the numbers stay honest as the matrix fills:

- **Per-product timing boundaries** (what each column measures): rdlt and dlt
  are single-process pipelines timed by the harness wall clock around the
  release CLI / the baseline's own self-timed `seconds` line — the number is
  the pipeline, nothing else. Airbyte's headline `seconds` is the **job wall**
  (orchestration, connector-pod scheduling, and platform overhead included, and
  labeled as such); its attempt time rides `extra.sync_s` as recorded context.
  The three columns are comparable as "how long to move this data with this
  tool as operated", not as isolated engine microbenchmarks.
- **Airbyte's fixed floor**: the first recorded 3-way session measured a
  ~35–45 s per-job orchestration floor (pod scheduling, check + replication
  container spin-up) that dominates its wall at these volumes — four
  full-refresh cells all median ≈45 s regardless of whether 200k or 1M rows
  moved. The Airbyte columns say "the platform's unit of work costs this
  much end to end", not "its connectors stream this slowly"; at much larger
  volumes the floor amortizes. Its `peak_rss_kb` is the whole-cluster cgroup
  high-water (labeled context, never barred). Airbyte arms run 3 times (the
  floor makes 5 runs pure cost); rdlt/dlt run 5.
- **Quiet machine**: every recorded session passes the classless quiet guard
  first (loadavg below 0.25×cores). A forced run on a loaded machine is stamped
  `forced: true` in its artifact — the number is context, not evidence.
- **Dedup cell regime**: the `pg-to-pg-dedup-1m` cell measures LOAD 2 only
  (full re-delivery + dedup by `id`); all three products run the full-redelivery
  regime, so Airbyte's cheaper incremental mode is deliberately not benched
  (no dlt counterpart). The cell's note renders as the matrix caption. Its
  source declares `tables: []` so only the query stream is delivered — without
  that, schema discovery adds every table in `public` on top, which is what the
  superseded 0.8× figure was measuring (see the policy log).
- **Oracle cell — what the driver switch bought** (`oracle-to-pg-200k`, 032):
  this row supersedes an earlier reading of the same cell. rdlt's *first*
  Oracle read paged by ROWID keyset and sized each page so one query reply fit
  ONE 8 KB packet — 14 rows per round trip on this table — because the
  pure-Rust driver could not continue a cursor. That shape was measured at
  **3-7 minutes extrapolated** for 200k rows, and could not complete past ~297
  pages at all (the driver never closed a server cursor). rdlt now reads
  through `oracle`/ODPI-C, streaming ONE cursor per stream into Arrow batches:
  **837.3 ms**. Both ceilings are gone, so the cell is now a fair read-path
  comparison rather than a record of a self-imposed cap. It still carries **no
  bar** — one recorded session is not a basis for one (018 BR8).
- **Oracle cell — read the Airbyte ratio with 018's caveat, not as a
  headline.** 54.6× (45.45 s) is JOB WALL CLOCK including orchestration: a
  Kubernetes pod per check, per discover and per replication attempt, on a
  single-node kind cluster. 018 recorded Airbyte's floor at ~45-60 s across
  every cell regardless of dataset size, and 45.45 s here sits squarely in
  that band — which means this row measures Airbyte's fixed startup cost far
  more than its Oracle read throughput. Its sync is CORRECT (200,000 rows
  verified in the destination) and its own reported figures were
  `driver_wall≈91 s / api_duration≈79 s` on the first attempt. The dlt ratio
  is the informative comparison; the Airbyte one bounds the difference
  between an embedded engine and an orchestrated platform.
- **Oracle cell — dlt's fastest backend is deliberately NOT run**: ConnectorX
  does support Oracle, but through ODPI-C, which dlopen's `libclntsh` from
  Oracle Instant Client at run time. Instant Client is not pip-installable and
  carries Oracle's OTN license, so the pg cells' headline `backend=connectorx`
  does not transfer. dlt's arm here runs python-oracledb **thin** mode
  (verified: no `libclntsh` anywhere in the baseline image). This is a recorded
  handicap, not a hidden one; closing it means adding Instant Client to the
  competitor image and a third arm — an owner decision, not a default.
- **Oracle cell — the Airbyte arm may be a documented absence**:
  `airbyte/source-oracle` is alpha/community (ELv2, in the default OSS catalog,
  so no custom registration) and its docs claim testing only through 21c, while
  the fixture is 23ai. Nothing documents a 23ai failure and nothing verifies
  one. If discover or sync fails, the arm records `Missing{reason}` and the
  matrix runs two-way. Substituting a 21c container for that arm alone is
  refused: it would give one arm a different source server and break the
  same-conditions rule the whole matrix rests on.
- **Cold start** lives on the instruments track, not the matrix: a one-row
  file → duckdb pipeline, ≤ 40 ms absolute (`benches/check-cold-start.sh`,
  run by `TARGET=iai make bench` and therefore `make check`).

## Trends

Generated from `benches/history.jsonl` (one line per cell×variant per recorded
invocation) — the latest two medians per pair and their delta.

<!-- rdlt-bench:BEGIN trends -->
| Cell | Variant | Latest | Previous | Δ |
|---|---|---|---|---|
| oracle-to-pg-200k | airbyte | 45.45 s | — | — |
| oracle-to-pg-200k | dlt | 3.36 s | 3.61 s | -7.0% |
| oracle-to-pg-200k | rdlt | 845.9 ms | 923.5 ms | -8.4% |
| pg-to-pg-1m | airbyte | 60.44 s | 45.37 s | +33.2% |
| pg-to-pg-1m | dlt | 10.17 s | 10.26 s | -0.9% |
| pg-to-pg-1m | dlt-pyarrow | 17.19 s | 17.46 s | -1.6% |
| pg-to-pg-1m | rdlt | 744.2 ms | 749.8 ms | -0.7% |
| pg-to-pg-dedup-1m | airbyte | 45.38 s | 45.39 s | -0.0% |
| pg-to-pg-dedup-1m | dlt | 12.45 s | 12.54 s | -0.7% |
| pg-to-pg-dedup-1m | dlt-pyarrow | 20.74 s | 20.81 s | -0.3% |
| pg-to-pg-dedup-1m | rdlt | 4.37 s | 4.39 s | -0.5% |
| pg-to-s3parquet-1m | airbyte | 45.37 s | 45.39 s | -0.0% |
| pg-to-s3parquet-1m | dlt | 1.67 s | 1.69 s | -0.9% |
| pg-to-s3parquet-1m | dlt-pyarrow | 10.80 s | 10.92 s | -1.1% |
| pg-to-s3parquet-1m | rdlt | 913.8 ms | 937.4 ms | -2.5% |
| s3jsonl-to-pg-200k | airbyte | 45.38 s | 45.37 s | +0.0% |
| s3jsonl-to-pg-200k | dlt | 62.76 s | 65.71 s | -4.5% |
| s3jsonl-to-pg-200k | rdlt | 639.8 ms | 649.9 ms | -1.6% |
| s3jsonl-to-s3parquet-200k | airbyte | 45.38 s | 45.37 s | +0.0% |
| s3jsonl-to-s3parquet-200k | dlt | 58.52 s | 60.29 s | -2.9% |
| s3jsonl-to-s3parquet-200k | rdlt | 848.9 ms | 872.4 ms | -2.7% |
| selftest-protocol | rdlt | 22.0 ms | 21.3 ms | +3.4% |
<!-- rdlt-bench:END trends -->

## Milestones

Claims from the pre-018 matrix, retired by the rebuild and preserved here with
their final recorded values. Evidence for every entry: **archive commit
`40841ab`** (the last session before the migration, checkout-able with its
cells and artifacts).

- **Flagship jsonl → DuckDB (jsonl-duckdb-200k)**: 13.5× vs dlt
  (14869 ms / 1105 ms, 5-run medians), peak RSS 1/5.4 (353 MB / 1910 MB).
  Evidence: commit `40841ab`.
- **Shred-only (shred-only-200k)**: 12.0× vs dlt (5898 ms / 490 ms).
  Evidence: commit `40841ab`.
- **REST → Postgres (rest-pg-100k)**: 6.7× vs dlt (5523 ms / 820 ms).
  Evidence: commit `40841ab`.
- **Parquet passthrough (parquet-passthrough)**: 3.5× vs dlt
  (331 ms / 93 ms). Evidence: commit `40841ab`.
- **Postgres → DuckDB, 1M wide (pg-wide-duckdb-1m)**: 7.8× vs dlt-pyarrow
  (10243 ms / 1306 ms). Evidence: commit `40841ab`.
- **Postgres → Postgres, 1M wide (pg-wide-pg-1m)**: 7.6× vs dlt-pyarrow
  (17652 ms / 2318 ms). Evidence: commit `40841ab`.
- **Cold start (cold-start)**: 24.2 ms median, ≤ 40 ms absolute — relocated
  live to the instruments track. Evidence: commit `40841ab`.
- **Postgres CDC catch-up (cdc-change-apply-500k)**: ≈72k changes/s
  (6.96 s for a 500k-change catch-up on 1M rows). Evidence: commit `40841ab`.
