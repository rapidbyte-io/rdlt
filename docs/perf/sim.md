# Simulation throughput

How fast the simulation checks seeds. The local rows were measured on the simulation as this record
leaves it, `c3cc0588`, against `bf2b4a5e` (the parent below), which has no churning sources; the CI
rows marked estimated are scaled by the local ratio from what each row names. `just sim-rate`
checks the reference rate every night against the floor below.

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
| Exactly-once sweep, seeds 0-999, four P-cores | 17.2 seeds/s (58.2 s), against 18.1 on the parent |
| Change sweep, seeds 0-999, four P-cores | 73.5 seeds/s (13.6 s), against 71.1 on the parent |
| Both sweeps together, seeds 0-999 | 71.8 s, 1.036 times the parent's 69.4 s |
| Replay of both oracles, 1 000 seeds, four P-cores | 141.4 s (7.1 seeds/s), `just sim-replay` |
| Pull request shards | 40 of 2 500 seeds, 4.9-9.3 min of 15 each on CI, estimated from main's run at `bf2b4a5e` (4.7-9.0 min); no more than about 8 of a run's jobs start together |
| Exactly-once sweep, a pull request's shard, ubuntu-24.04 | about 6.8 seeds/s median, 6.1-11.4 over 40 shards, estimated from the parent's shards (7.0 median, 6.3-11.8) |
| Nightly shards | 80 of 12 500 seeds, up to about 51 min of 70, estimated from `d28d458f`'s nightly, whose 50 of 20 000 took 28.9-58.3 min |
| Coverage run, CI | about 32-40 min of 60, estimated from the parent's nightlies, 22.6 min on `d28d458f` and 28.4 on `9ff145dd` |
| Replay job, CI | about 7.4 min of 30, estimated from the local time and the 3.15 times CI took over local before this record |
| Test job, ubuntu-24.04 | 20.2 min of 45 on main's run at `bf2b4a5e`, plus 3.3 s for the churning seeds' test |
| Test job, macos-15 | 23.3 min of 45 on main's run at `bf2b4a5e`, plus the same 3.3 s |
| Archive extraction per shard | 0.1 s |
| Nightly floor for the reference set | 4 seeds/s, the whole number below half the reference job's rate: 12.0 and 12.2 seeds/s on the parent-era nightlies (`9ff145dd`, `d28d458f`), about 9.0-9.2 at this branch's local ratio (17.2 / 22.8 against the sweep those nightlies ran), estimated, so half is 4.5 |
