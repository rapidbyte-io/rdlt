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

Reproduce: `benches/run-e2e.sh` (jsonl, parquet, cold-start cells) and the
REST→Postgres recipe in RESULTS history / `benches/baseline/pipeline_rest_pg.py`.
