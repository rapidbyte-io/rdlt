# Simulation throughput

How fast the simulation checks seeds. The local rows were measured on the simulation as this record
leaves it, against `c69b15e1` (the parent below), whose simulation `1f28d921` runs unchanged; the CI
rows marked estimated are scaled by the local ratio from what each row names. `just sim-rate` checks
the reference rate every night against the floor below.

## Method

- **Build.** The shards, the replay, the stress run and the rate check run the `sim` profile
  (optimised, with overflow checks and debug assertions kept) from the archive `just sim-archive`
  builds; the test job runs the dev profile.
- **Seeds side by side.** `for_each_seed` runs as many seeds at once as the cores hold, the
  host's unless `RDLT_SIM_CORES` names a count: one thread a seed, eight a stress seed. Each
  seed's wall time is a line of `target/sim-timings/<sweep>.jsonl`, and the last line is the
  sweep's seeds a second.
- **Local.** Intel Core Ultra X7 358H, performance cores 0-3 (`taskset -c 0-3`), governor
  `powersave`, each run started with the one-minute load under 1.0, median of three runs; the
  parent's medians are of three runs taken the same way in an earlier session. A test's time is
  nextest's `PASS` duration.
- **CI.** GitHub's ubuntu-24.04 runners, four vCPUs; macos-15 for the macOS test job. Job times
  from the jobs API, test times from nextest's `PASS` lines in the job logs.
- **Reference set.** The exactly-once sweep over seeds 0 to 999.

## Figures

| Measure | Value |
|---|---|
| Exactly-once sweep, seeds 0-999, four P-cores | 18.4 seeds/s (54.5 s), against 22.8 on the parent |
| Change sweep, seeds 0-999, four P-cores | 72.5 seeds/s (13.8 s), against 154.3 on the parent |
| Both sweeps together, seeds 0-999 | 68.3 s, 1.36 times the parent's 50.4 s |
| Replay of both oracles, 1 000 seeds, four P-cores | 139.1 s (7.2 seeds/s), `just sim-replay` |
| Replay tests of the test job, dev profile | 83.3 s and 93.8 s, against 54.4 s; the played seeds' replay test 14.4 s |
| Pull request shards | 40 of 2 500 seeds, 5.2-9.3 min of 15 each on CI, estimated from main's run at `6eb517d7`, before played processes (4.9-8.7 min; `d28d458f`'s 25 of 4 000 took up to 11.6 min); no more than about 8 of a run's jobs start together, so the shards took 21 min from the first start to the last end |
| Exactly-once sweep, a pull request's shard, ubuntu-24.04 | 7.0 seeds/s median, 6.3-11.8 over 40 shards, estimated from main's run at `6eb517d7`, before played processes (7.4 median, 6.7-12.5; `d28d458f`'s 25 shards: 7.7 median, 7.3-14.6) |
| Nightly shards | 80 of 12 500 seeds, up to about 49 min of 70, estimated from `d28d458f`'s nightly, whose 50 of 20 000 took 28.9-58.3 min (median 53.6) |
| Coverage run, CI | about 31-39 min of 60, estimated from the parent's nightlies, 22.6 min on `d28d458f` and 28.4 on `9ff145dd` |
| Replay job, CI | about 7.3 min of 30, estimated from the local time; no nightly has run it yet |
| Replay test of the test job, CI | 58-120 s on ubuntu-24.04 (median 80 s), 107-124 s on macos-15, against 54.4 s locally on the parent; the three replay tests need every core |
| Test job, ubuntu-24.04 | 18.2 min of 45 on one run (the parent's took up to 24.4 min) |
| Test job, macos-15 | 23.7 min of 45 on one run (the parent's took up to 27.3 min, and its replay test 107-124 s) |
| Archive extraction per shard | 0.1 s |
| Nightly floor for the reference set | 4 seeds/s, the whole number below half the reference job's rate: 12.0 and 12.2 seeds/s on the parent's nightlies (`9ff145dd`, `d28d458f`), about 9.7-9.9 at this branch's local ratio (18.4 / 22.8), estimated, so half is 4.9 |
