# Benchmark results

> Baseline-first methodology (research.md R12): pinned dlt measured first, same
> machine, same dataset, then rdlt. No multiple is quoted without both columns.
>
> **Baseline version policy**: the pin tracks the LATEST stable dlt at
> measurement time; a pin bump re-measures every cell before any multiple is
> quoted. Current baseline: **dlt 1.29.0** (bumped from 1.11.0 on 2026-07-20 —
> dlt improved materially between those releases, and the multiples below
> reflect that honestly).
>
> **2026-07-20 — shred-only bar adjusted ≥ 20× → ≥ 10× (owner decision)**:
> measured 11.0× same-session pair (0.522 s vs 5.75 s, 5-run medians,
> protocol P2; 11.0–11.6× observed across this feature's same-day sessions
> — the bar sits one integer below the session floor so ordinary ±2–3%
> jitter cannot flap it). The
> fresh two-lens profile classified five candidates viable with measured
> shares (C5′ identity-pipeline usage 33–40%, C6 column interning ~10%,
> C3 arena layout ~10–11%, C1 structural scan ~10–12%, C2 UTF-8-once ~4%;
> optimistic ceiling estimate ~18–20×), and NONE was A/B-attempted: the
> owner closed the cell at current performance and deferred all candidates
> to the backlog. This entry is the recorded policy event deviating from
> protocol P6's "measured accept/reject per candidate" rule for leaf (b);
> the bar is set from the measured current value, never the unmeasured
> ceiling estimate. Evidence:
> `specs/004-close-perf-misses/evidence/resolution-shred.md` (+
> `profile-shred.md`). Perf-gate baselines untouched (no accepted change).
>
> **2026-07-20 — feature 005: Postgres-source cells added, bars set
> measurement-first**: pg→DuckDB and pg→Postgres (1M-row wide table,
> dataset identity recorded) measured baseline-first against pinned dlt
> 1.29.0's `sql_database` source in its fastest documented configuration
> (pyarrow backend) — NOT its slow default — with sqlalchemy-default and
> connectorx (Rust reader) as scoreboard rows. Gated bars ≥ 6× derive
> from the session's WORST-case run pairs (medians 7.8×/8.9×; worst
> 6.6×/7.1×) so ordinary jitter cannot flap them — the 004 protocol
> applied from birth. New gated iai entry `pg_copy_decode_10k` recorded
> as a NEW baseline (existing entries untouched). Evidence:
> `specs/005-postgres-source/evidence/bench-pg.md`.
>
> **2026-07-20 — cold-start criterion converted ratio → absolute
> (measurement-design fix)**: the gated bar is now an absolute bound on
> reference hardware (see the split rows below); the dlt ratio remains as
> a scoreboard number. Derivation and protocol:
> `specs/004-close-perf-misses/evidence/resolution-cold-start.md`.

## The matrix — 2026-07-20, dlt 1.29.0, all cells same-session pairs

> **Gated vs scoreboard** (feature 004): a `gated` row participates in
> pass/fail decisions and can block; a `scoreboard` row is reported context
> and cannot. Status vocabulary for formerly-missed cells: `resolved (a)` =
> bar met, perf gate re-baselined; `resolved (b)` = bar adjusted with
> committed evidence, recorded in the version policy. Resolution records live
> in `specs/004-close-perf-misses/evidence/`.

| Cell | pinned dlt 1.29.0 | rdlt | multiple | target (design §8) | Gated? | status |
|---|---|---|---|---|---|---|
| jsonl → DuckDB, 200k nested records (product CLI) | 14.11 s / 1,880 MB | 1.09 s / 345 MB | **12.9× faster** | ≥ 10× | gated | ✅ met |
| — peak RSS of that run | 1,880 MB | 345 MB | **1/5.4** | ≤ 1/5th | gated | ✅ met |
| Shred stage only (dlt `normalize()` vs `shred_only`) | 5.75 s | 0.52 s | **11.0× faster** | ≥ 10× (adjusted; was ≥ 20×) | gated | `resolved (b)` — [record](../specs/004-close-perf-misses/evidence/resolution-shred.md) |
| mock REST → Postgres, 100k records | 5.40 s / 173 MB | 0.70 s / 37 MB | **7.7× faster** | ≥ 5× | gated | ✅ met |
| Arrow passthrough: parquet → parquet | 0.209 s / 263 MB | 0.08 s / 47 MB | **2.6× faster** | ≥ 2× | gated | ✅ met |
| parquet → DuckDB (bonus context row) | 0.387 s / 419 MB | 0.37 s / 161 MB | 1.0× | — | scoreboard | — |
| Postgres → DuckDB, 1M-row wide table (feature 005) | 10.19 s (dlt pyarrow backend) | 1.31 s / 444 MB | **7.8× faster** | ≥ 6× (set measurement-first) | gated | ✅ met |
| Postgres → Postgres, 1M-row wide table (feature 005) | 17.02 s (dlt pyarrow backend) | 1.92 s / 138 MB | **8.9× faster** | ≥ 6× (set measurement-first) | gated | ✅ met |
| — same cells vs dlt DEFAULT (sqlalchemy backend) | 57.1 s / 107.1 s | same rdlt | 43.6× / 55.8× | — | scoreboard | — |
| — pg → DuckDB vs dlt connectorx backend (Rust reader) | 2.94 s | 1.31 s | **2.2× faster** | — | scoreboard | — |
| Postgres (jsonb docs) → DuckDB, 200k nested (feature 005) | 4.51 s (pyarrow) | 0.24 s | 18.8× | — | scoreboard | — |
| Cold start, one-row pipeline (absolute) | — | 23.6 ms | — | ≤ 40 ms (abs; protocol P3) | gated | `resolved (a)` — [record](../specs/004-close-perf-misses/evidence/resolution-cold-start.md) |
| Cold start vs dlt (ratio, context only) | 0.417 s | 23.6 ms | **1/17.7 overhead** | — | scoreboard | — |

Caveats, stated so the numbers stay honest:

- Datasets/methodology unchanged from the 1.11.0-era rows: 200k nested NDJSON
  (→600k rows with children), the same records re-encoded as parquet by rdlt,
  a 100k-record mock API (100 pages), one-row cold start. Wall times are
  medians (5 runs for flagship/shred; cold-rdlt is a hyperfine median of 20
  runs after 3 warmups since feature 004; dlt in-process self-timing as
  before; cold-start dlt timed from before `import dlt`, interpreter boot
  still excluded — generous to the baseline).
- **Cold start (resolved 2026-07-20, feature 004)**: the old ratio bar
  regressed from met (1/22.7 vs dlt 1.11.0) to missed purely because dlt's
  startup improved ~21% — rdlt got zero slower. The gated criterion is now
  an absolute bound (≤ 40 ms = 23.6 ms measured floor × 1.5, rounded up to
  5 ms) so a baseline-tool release can never again flip the gated verdict;
  the ratio row above is scoreboard context. The earlier "30 ms" reading
  was `/usr/bin/time` 10 ms quantization of the same ~23 ms reality; the
  gated statistic is now a hyperfine 20-run median (protocol in the
  resolution record and run-e2e.sh cold cell).
- Shred-only moved 8.1× → 12.0× at the 1.29.0 bump NOT because rdlt changed,
  but because BOTH sides were re-measured cleanly (rdlt's 0.50 s was
  previously contended by a background mutation run; dlt's normalize also
  improved). The row above shows the feature-004 final same-session pair
  (0.522 s vs 5.75 s → 11.0×; the close-out 0.50 s / 5.95 s sit inside the
  same ±2–3% session-to-session jitter band, on both sides) and the bar
  adjusted per the policy entry — the candidates profile with remaining
  headroom is committed alongside the resolution record.
- dlt 1.29.0 vs 1.11.0 on this machine: flagship 19.60→14.38 s, normalize
  7.63→5.95 s, cold 0.527→0.418 s, REST→PG 7.49→5.50 s — credit where due.
- The parquet→DuckDB bonus row remains near-parity by design (both sides
  reduce to the same DuckDB C++ appender); parquet→parquet is the
  engine-bound claim.

## Merge-path index scoreboard (feature 008, FR-009 — measured 2026-07-21)

`benches/run-merge-index.sh`, the EXACT DELETE shape the delete-insert
strategy emits, 10M-row target, BEGIN/ROLLBACK isolation, 5-run medians:

| Regime | Unindexed | Indexed (`rdlt_ix_*`) | Effect |
|---|---|---|---|
| Incremental delta (5k keys vs 10M rows) | 583.9 ms | 28.6 ms | **20.4× faster** — the common production merge |
| Half-table delta (5M keys) | 24,209 ms | 23,322 ms | planner ignores the index; no penalty |

Scoreboard entries (no gate): the auto-ensured merge-identity indexes
(feature 008 M5) eliminate the unindexed scan exactly where incremental
merges live, and cost nothing where they don't apply.

Strategy comparison (`benches/run-merge-strategies.sh`, 1M-row pg→pg,
load 2 re-delivers everything with 50% changed, 5-run medians,
2026-07-21): delete-insert 4.84 s vs upsert 4.97 s — statistically
indistinguishable in the full-redelivery regime (both inside the
session jitter band). Upsert's value is SEMANTIC: matched keys update
in place with no delete-visibility window, and it composes with
hard-delete — not raw throughput. Recorded so nobody re-runs this
expecting a speedup that was never the point.

Connector-crate coverage (feature 011, contract PM5; `make coverage` =
`cargo llvm-cov nextest -p rdlt-postgres --features failpoints`,
cargo-llvm-cov 0.8.7; floor: 80% lines; NOT a CI gate):

| Measurement | Lines | Functions | Date |
|---|---|---|---|
| Baseline (before feature-011 cells) | 87.69% / 87.71% (two runs, stable) | 83.17% | 2026-07-21 |
| Final (feature 011 close) | **88.98%** | 84.13%* | 2026-07-21 |

Feature-011 delta: types.rs 76.88% → 91.59% (the hint-matrix cell),
encode.rs → 87.46%, cursor.rs → 85.42%; 13 new behavioral cells + the
R5 fix. Classified exclusions (verified via `--show-missing-lines`,
each a REAL uncovered cluster with a stated reason — contract PM5):

| Cluster | Lines | Reason |
|---|---|---|
| source/mod.rs 82–262 | ~168 | the `testhook` module (bench_wire/bench_decode/fuzz entries) executes only under benches and fuzz targets, outside nextest — instrumentation surface, not product paths |
| source/mod.rs scattered (368–372, 515–517, 566–568, 593–617, 652–658, …) | ~30 | defensive engine-contract guards (e.g. stream-without-reflected-table) unreachable through the engine, plus thin `PostgresSource::from_json/from_value` delegators whose shared validation path is covered at the config layer |
| dest/mod.rs (51–59, 75, 89–91) | 13 | capability/edge helper arms |
| tls_verify.rs (52–59, 116–123) | 16 | verifier trait methods for protocol variants the TLS matrix's handshakes never negotiate (TLS 1.2 signature arms under a TLS 1.3 stack) |

(*function coverage; region 88.??% — see `make coverage` output.)

Merge-refinement cells (`benches/run-merge-refinements.sh`, feature 010,
5-run medians, 2026-07-21; scoreboard, no gate — the new SQL runs only
when the options are declared, so every existing bar is untouched by
construction):

| Cell | Median | Notes |
|---|---|---|
| Scope-replace delete | 1,559.9 ms | 100k-row scope out of a 10M-row scoped target; the scope index the harness creates mirrors the one the destination AUTO-ENSURES for merge_key tables (review F8 — the plan provisions it, the bench does not cheat); the identity-only delete of the SAME keys costs 1,933.5 ms — the scope route is ~19% FASTER |
| Ordered dedup | 278.4 ms | `DISTINCT ON` with `seq DESC NULLS LAST` over a 2×-duplicated 1M-row stage; plain last-wins costs 334.6 ms on the same stage — the extra sort key costs nothing here |

CDC cells (`benches/run-cdc.sh`, feature 009, 5-run medians, 2026-07-21;
scoreboard, no gate — the snapshot itself rides the existing gated COPY
path unchanged):

| Cell | Median | Notes |
|---|---|---|
| Change-apply throughput | 6.96 s | 1M-row `pg_wide`, 500k-change backlog (400k updates / 50k deletes / 50k inserts) → source-equal catch-up ≈ 72k changes/s end-to-end (SQL-peek decode + upsert + hard-delete, pg→pg) |
| Catch-up latency | 0.05 s | quiet-to-caught-up on a 1k-update delta, steady state |

Measurement note (recorded honestly): the peek decodes WAL from the
slot's CONFIRMED position, which trails one run behind by the ack
design — the first runs after a snapshot re-walk the engine's own
mirror writes when source and destination share a database, then
converge to the steady state above (the script settles before timing).

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
- 2026-07-20 (baseline bumped to dlt 1.29.0): flagship 13.8×, shred-only
  12.0× (❌ vs ≥20×), REST→PG 6.5×, passthrough 2.75×, cold 1/14.2
  (❌ vs ≤1/20) — the two honest misses feature 004 resolves.
- 2026-07-20 (feature 004 close): full-matrix re-measure, same pin, no
  engine code changes. Shred bar adjusted ≥20× → ≥10× (owner decision,
  policy entry above); cold-start criterion converted to a gated absolute
  ≤ 40 ms with the dlt ratio demoted to scoreboard.
- 2026-07-20 (feature 005): Postgres source lands — pg→DuckDB 7.8×,
  pg→Postgres 8.9× vs dlt's fastest documented config (43.6×/55.8× vs
  its default; 2.2× vs its connectorx Rust reader), jsonb docs 18.8×;
  bars ≥ 6× measurement-first. The matrix above.
- 2026-07-20 (feature 006 pre-merge verification): full same-session
  paired re-measure, no bar or baseline changes, EVERY gated row met —
  flagship 13.1× (14.91 s / 1,967 MB vs 1.14 s / 350 MB), shred-only
  11.8× (5.87 s vs 0.499 s), REST→PG 6.6× (5.51 s vs 0.84 s, 100k rows
  verified in-destination), passthrough 3.7×, pg→DuckDB 8.1× (10.12 s
  vs 1.25 s), pg→Postgres 8.7× (17.24 s vs 1.98 s), cold 23.8 ms mean
  (21.8–26.5 ms, 20 hyperfine runs) vs ≤ 40 ms, iai gate worst drift
  +0.67% vs 3% tolerance. Scoreboard: jsonb→DuckDB 18.9×, 2.4× vs
  connectorx, RSS 1/5.6. All movement vs the recorded 004/005 numbers
  is inside the documented ±2–10% session jitter band — TLS plumbing,
  hints, query streams, and the keyed-merge engine changes cost nothing
  measurable on the hot paths.

- 2026-07-21 (feature 010): merge refinements (dedup_sort + merge_key)
  land as scoreboard cells — scope-replace 1.56 s for a 100k-row scope
  on 10M rows (faster than the identity route), ordered dedup 278 ms on
  a 2M-row stage; no new gates, existing bars untouched.
- 2026-07-21 (feature 009): Postgres CDC lands — change-apply 6.96 s
  median for a 500k-change catch-up on 1M rows (≈72k changes/s, full
  pg→pg upsert+hard-delete composition), quiet catch-up latency 50 ms
  steady state; scoreboard cells, no new gates; existing gated bars
  untouched (snapshot rides the unchanged COPY path).

Reproduce: `benches/run-e2e.sh` (jsonl, parquet, cold-start cells) and the
REST→Postgres recipe in RESULTS history / `benches/baseline/pipeline_rest_pg.py`.
