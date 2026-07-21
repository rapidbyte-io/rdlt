# Continuity record — migration re-measure (T016, research R7)

**Session**: 2026-07-21, reference machine (AMD Ryzen AI MAX+ 395, 32
threads, kernel 7.0.12-201.fc44), release CLI, dlt 1.29.0 image, quiet
machine (loadavg guard active), same-session paired, baseline first.
Artifacts: `benches/results/*.json` (committed, fingerprinted).
`rdlt-bench gate`: **all 8 bars met** (output below the table).

The R7 rule: every migrated GATED cell's median must land inside the
documented session-jitter band (±2–10%) of its recorded value, or carry a
diagnosis and (only if accepted) a version-policy re-derivation.
Verdict: **all in-band; zero bar changes; zero policy entries needed.**

## Gated cells

| Cell | Recorded (004/005/006 sessions) | New harness | Delta vs nearest recorded | Verdict |
|---|---|---|---|---|
| jsonl→DuckDB wall | 1.09 s / 1.14 s | 1.091 s | −0.1% vs 1.09 | in-band |
| jsonl→DuckDB ratio | 12.9× / 13.1× | 13.7× (dlt 14.92 s) | +4.6% | in-band |
| jsonl→DuckDB peak RSS | 345 MB vs 1,880 MB = 1/5.4 | 340 MB vs 1,844 MB = **1/5.4** | exact | in-band |
| shred-only wall | 0.522 s / 0.499 s | 0.491 s | −1.6% vs 0.499 | in-band |
| shred-only ratio | 11.0× / 11.8× | 12.1× (dlt 5.93 s) | +2.5% | in-band |
| REST→PG wall | 0.70 s / 0.84 s | 0.772 s | inside the recorded pair | in-band |
| REST→PG ratio | 7.7× / 6.6× | 7.6× (dlt 5.84 s) | −1.3% vs 7.7 | in-band |
| parquet passthrough ratio | 2.6× / 3.7× (006) | 3.7× (89 ms vs 332 ms) | matches 006 | in-band |
| pg→DuckDB wall | 1.31 s / 1.25 s | 1.245 s | −0.4% vs 1.25 | in-band |
| pg→DuckDB ratio | 7.8× / 8.1× | 8.4× (pyarrow 10.44 s) | +3.6% vs 8.1 | in-band |
| pg→PG wall | 1.92 s / 1.98 s | 2.036 s | +2.8% vs 1.98 | in-band |
| pg→PG ratio | 8.9× / 8.7× | 8.5× (pyarrow 17.35 s) | −2.3% vs 8.7 | in-band |
| cold start (absolute) | 23.6 ms / 23.8 ms | 24.2 ms (hyperfine 20 runs) | +1.7% | in-band (bar ≤ 40 ms) |

Scoreboard context re-measured alongside (no continuity requirement, R7
applies to gated only): parquet→DuckDB 370 ms (recorded 370 ms);
pg→DuckDB vs connectorx 2.42× (recorded 2.2×, 006: 2.4×); cdc
change-apply 7.26 s (recorded 6.96 s), catch-up latency 52.7 ms
(recorded ~50 ms); merge-index incremental 583.6→26.8 ms (recorded
583.9→28.6 ms — striking); strategies 5.15 s vs 5.23 s (recorded
4.84 s vs 4.97 s — both +6%, the "indistinguishable" claim holds);
ordered-dedup 288.9 ms vs last-wins 345.2 ms (recorded 278.4/334.6).
One scoreboard observation worth flagging: scope-replace measured
1,951.6 ms vs its recorded 1,559.9 ms while the identity-delete
comparison row stayed put (1,894.4 vs 1,933.5) — this session the two
routes are statistically EQUAL rather than scope being ~19% faster.
10M-row DELETE timings are sensitive to buffer/autovacuum state; no
product claim rests on the 19% (the 010 feature's claim was "scope
route is not slower", which still holds). Recorded, not adjudicated.

## Gate output (verbatim)

```
[PASS] jsonl-duckdb-200k: 13.7x vs dlt (14925 ms / 1091 ms), bar >= 10x (tol 0%)
[PASS] jsonl-duckdb-200k: peak RSS 1/5.4 of dlt (340 MB / 1844 MB), bar <= 1/5 (tol 0%)
[PASS] shred-only-200k: 12.1x vs dlt (5930 ms / 491 ms), bar >= 10x (tol 0%)
[PASS] rest-pg-100k: 7.6x vs dlt (5839 ms / 772 ms), bar >= 5x (tol 0%)
[PASS] parquet-passthrough: 3.7x vs dlt (332 ms / 89 ms), bar >= 2x (tol 0%)
[PASS] pg-wide-duckdb-1m: 8.4x vs dlt-pyarrow (10443 ms / 1245 ms), bar >= 6x (tol 0%)
[PASS] pg-wide-pg-1m: 7.9x vs dlt-pyarrow (17429 ms / 2197 ms), bar >= 6x (tol 0%)
[PASS] cold-start: 24.2 ms, bar <= 40 ms absolute (tol 0%)
gate: all bars met
```

(The pg-wide-pg-1m line above is from the first session; the committed
artifact is the clean re-run — 8.5× / 2036 ms — after the incident-2 fix
below. Both pass the ≥6× bar.)

## Incidents (diagnosed, fixed in the harness — no bar touched)

1. **Quiet-guard refusal mid-session**: the first session aborted at
   rest-pg-100k with loadavg 8.13 > 8.00 — load created by the session
   itself (mock-API release build + container churn in the 1-minute
   average). Fix: gated runs now WAIT for the machine to settle (up to
   5 min, 15 s polls) before refusing; refusal is reserved for load that
   never decays. The guard's protocol role is unchanged.
2. **pg→PG first pass measured +11% with dlt-sqlalchemy MISSING**: the
   competitor loop did not reset destination schemas between baseline
   runs (run-pg.sh dropped them per run), so sqlalchemy inherited
   pyarrow's tables and refused the migration; the polluted session also
   inflated the rdlt wall (2.197 s). Fix: `fixture.reset()` now runs
   before every competitor run, mirroring the rdlt side. The clean
   re-run measured 2.036 s — in-band — and sqlalchemy runs.
3. **Suspected transposition in the OLD hand-recorded sqlalchemy row**:
   the new same-session pair measured sqlalchemy pg→DuckDB 109.0 s and
   pg→PG 58.3 s; the 005 hand-recorded row read 57.1 s / 107.1 s — the
   same two numbers, opposite order. Scoreboard context only (2 runs,
   declared per-competitor in the cell), no bar involved; recorded here
   as a data point for exactly the error class generated tables remove.
   The generated table supersedes the old row.
4. **normalize_only.py output**: gained a `seconds` alias alongside
   `normalize_seconds` (harness convention); the measured window is
   unchanged.

## Method deltas vs the shell harnesses (recorded, none affect verdicts)

- Wall clock is `Instant` around the release-CLI subprocess (previously
  `/usr/bin/time`, 10 ms quantized). dlt walls stay in-process
  self-timed — every recorded multiple keeps its meaning.
- dlt CPU/peak-RSS now come from cgroup v2 read in-container
  (`memory.peak`, `cpu.stat`); when unreachable the baseline's
  self-reported ru_maxrss is used and labeled (that statistic is what
  all pre-012 dlt RSS rows used). This session: cgroup readings
  (flagship dlt 1,844 MB vs ru_maxrss-recorded 1,880 MB — consistent).
- rdlt peak RSS is `VmHWM` (kernel high-water mark) sampled at 50 ms;
  spikes cannot be missed between samples by construction.
