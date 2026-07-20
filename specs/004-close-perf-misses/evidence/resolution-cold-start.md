# Resolution: Cold-Start Cell (T014/T016)

**Date**: 2026-07-20 | **Identity**: per [environment.md](environment.md)
(blashyrkh, Ryzen AI MAX+ 395, kernel 7.0.12-201.fc44, rustc 1.96.0
ac68faa20, hyperfine 1.20.0, one-row pipeline exactly as the
`benches/run-e2e.sh` cold cell)

## Outcome

**(a) closed — criterion redesigned as an absolute bar and met.**

The gated criterion converts from `≤ 1/20 of dlt cold start` (ratio) to
**`≤ 40 ms` absolute on the reference machine**; the dlt ratio is demoted
to a scoreboard number. Measured 23.6 ms passes with 41% headroom. This
is the measurement-design fix the spec prescribes: the 003→004 "miss"
existed only because dlt improved its own startup ~21%; rdlt got zero
slower.

## Final measurement (protocol P3)

`hyperfine -N --warmup 3 --runs 20`, fresh workdir + fresh `.duckdb`
per run (`--prepare`), warm FS cache, quiet machine, release binary at
the current tree:

- **Median: 23.57 ms** (mean 23.9 ± 1.2 ms, range 21.7–26.3 ms, 20 runs)
- Consistent with T003's independent reproduction (22.5 ms ± 0.9,
  [environment.md](environment.md)) and with the instrumented-composition
  bracket ([profile-cold-start.md](profile-cold-start.md)).
- The 003-era "30 ms" record was `/usr/bin/time` 10 ms quantization of
  this same reality (T003 note) — no performance change has occurred.

## Bar derivation (P3)

| Step | Value |
|---|---|
| Irreducible floor (composition profile, 100% of phases classified floor) | 23.6 ms |
| × 1.5 headroom | 35.4 ms |
| Rounded UP to nearest 5 ms | **N = 40 ms** |

Flap check: worst observed run (26.3 ms) sits at 66% of the bar; the
bar cannot flap under its own protocol on the observed spread.

## T015 — reducible-phase decision (negative result, recorded)

No reducible phase was worth taking: DuckDB open-overlap is bounded at
~2–4 ms, thread-pool capping is a micro win with complexity cost, fsync
cost is 55 µs total ([profile-cold-start.md](profile-cold-start.md)).
Under the 2026-07-20 owner decision (no optimization work in this
feature), all are backlog notes; no A/B was run, so
`ab-cold-startup.md` intentionally does not exist.

## SC-003 invariance statement

The gated verdict is now a function of (reference machine, protocol,
rdlt binary) only. Re-pinning ANY dlt version — faster or slower —
changes only the scoreboard ratio row; it cannot change the gated
pass/fail. The bar references no competitor-relative quantity
(data-model validation rule).

Scoreboard context at current numbers: dlt 1.29.0 cold start 0.417 s
(same-session 5-run median at feature close, [rematrix-final.md](rematrix-final.md);
the close-out session recorded 0.418 s) vs rdlt 23.57 ms → rdlt starts
in **1/17.7** of dlt's time (the earlier `1/14.2` used the
10 ms-quantized 30 ms reading; same code, finer instrument).

## Evidence links

- [profile-cold-start.md](profile-cold-start.md) — T013 composition
  (phase table, syscall lens, floor classification).
- [environment.md](environment.md) — identity + T003 reproduction.

## Policy entry reference

`benches/RESULTS.md` → "Baseline version policy" → entry
**2026-07-20 — cold-start criterion converted ratio → absolute**.
Protocol recorded in the `benches/run-e2e.sh` cold cell and in
`specs/004-close-perf-misses/contracts/measurement-protocol.md` P3.
