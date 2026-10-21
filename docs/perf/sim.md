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
| Exactly-once sweep, seeds 0-999, four P-cores | 19.5 seeds/s (51.4 s), against 22.8 on the parent |
| Change sweep, seeds 0-999, four P-cores | 79.7 seeds/s (12.6 s), against 154.3 on the parent |
| Both sweeps together, seeds 0-999 | 64.0 s, 1.27 times the parent's 50.4 s |
| Replay of both oracles, 1 000 seeds, four P-cores | 134.3 s (7.4 seeds/s), `just sim-replay` |
| Replay tests of the test job, dev profile | 83.3 s and 93.8 s, against 54.4 s |
| Pull request shards | 40 of 2 500 seeds, about 9.2 min of 15 each, estimated (`d28d458f`'s 25 of 4 000 took up to 11.6 min); no more than about 8 of a run's jobs started together, so the job's wall time is the shards' total over the runners that are free, about 25 min, estimated |
| Exactly-once sweep, a pull request's shard, ubuntu-24.04 | about 6.5 seeds/s median, estimated (`d28d458f`'s 25 shards: 7.7 median, 7.3-14.6) |
| Nightly shards | 80 of 12 500 seeds, about 46 min of 70, estimated (`d28d458f`'s 50 of 20 000 took up to 58.2 min) |
| Coverage run, CI | about 36 min of 60, estimated from the parent's 28.4 min |
| Replay job, CI | about 7 min of 30, estimated from the local time |
| Replay test of the test job, CI | 58-120 s on ubuntu-24.04 (median 80 s), 107-124 s on macos-15, against 54.4 s locally on the parent; the two replay tests need every core |
| Test job, ubuntu-24.04 | 27-29 min of 45, estimated from the parent's 24.4 min and 3-4.5 min more |
| Test job, macos-15 | about 32 min of 45, estimated from the parent's 27.3 min and 4.7 min more |
| Archive extraction per shard | 0.1 s |
| Nightly floor for the reference set | 5 seeds/s, the whole number below half the reference job's rate: 12.0 seeds/s on `d28d458f`, about 10.3 at this branch's local ratio (19.5 / 22.8), estimated |
