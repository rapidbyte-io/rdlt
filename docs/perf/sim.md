# Simulation throughput

How fast the simulation checks seeds. The figures are of the simulation at `096f6e74`, the tip of
the branch that added connectors which break the protocol, against its parent `a92436bf`. The
local rows come from the parent's rounds on an idle machine, scaled by the ratio of the two
measured side by side on a loaded one (below); the CI rows marked estimated are scaled by the
same ratio from what each row names. `just sim-rate` checks the reference rate every night against
the floor below.

## Method

- **Build.** The shards, the replay, the stress run and the rate check run the `sim` profile
  (optimised, with overflow checks and debug assertions kept) from the archive `just sim-archive`
  builds; the test job runs the dev profile.
- **Seeds side by side.** `for_each_seed` runs as many seeds at once as the cores hold, the
  host's unless `RDLT_SIM_CORES` names a count: one thread a seed, eight a stress seed. Each
  seed's wall time is a line of `target/sim-timings/<sweep>.jsonl`, and the last line is the
  sweep's seeds a second.
- **Local.** Intel Core Ultra X7 358H, performance cores 0-3 (`taskset -c 0-3`), governor
  `powersave`, median of three runs. The parent's rates are of runs started with the one-minute
  load under 1.0. The branch was measured side by side with the parent, the two alternating on
  the same cores under a one-minute load of 8.6 to 9.7, and its rates are the parent's scaled by
  the ratio of the medians. A test's time is nextest's `PASS` duration.
- **CI.** GitHub's ubuntu-24.04 runners, four vCPUs; macos-15 for the macOS test job. Job times
  from the jobs API, test times from nextest's `PASS` lines in the job logs.
- **Reference set.** The exactly-once sweep over seeds 0 to 999.

## Figures

| Measure | Value |
|---|---|
| Exactly-once sweep, seeds 0-999, four P-cores | about 17.7 seeds/s (56.5 s), estimated, against 18.25 (54.8 s) on the parent |
| Change sweep, seeds 0-999, four P-cores | about 76.6 seeds/s (13.1 s), estimated, against 74.5 (13.4 s) on the parent |
| Both sweeps together, seeds 0-999 | about 69.6 s, estimated: 1.02 times the parent's 68.2 s (measured side by side 105.7 s against 104.0 s, 1.016) |
| Replay of both oracles, 1 000 seeds, four P-cores | about 139.6 s (7.2 seeds/s), `just sim-replay`: the parent's 139.4 s times the side-by-side ratio 1.001 (185.2 s against 185.1 s) |
| Pull request shards | 40 of 2 500 seeds, 4.9-9.3 min of 15 each on CI, estimated from main's run at `a92436bf` (4.8-9.1 min); no more than about 8 of a run's jobs start together |
| Nightly shards | 80 of 12 500 seeds, 23.1-44.8 min of 70, estimated from the nightly at `c48540dd` (run 39748626499), whose shards took 22.6-43.9 min |
| Coverage run, CI | about 37.4 min of 60, estimated: the nightly at `c48540dd` took 36.7 min, times 1.02 |
| Replay job, CI | about 7.3 min of 30, estimated: the nightly at `c48540dd` took 7.3 min, times 1.00 |
| Test job, ubuntu-24.04 | 26.5 min of 45 on main's run at `a92436bf`, plus 41 s for the broken connectors' tests: about 27.2 |
| Test job, macos-15 | 24.7 min of 45 on main's run at `a92436bf`, plus the same 41 s: about 25.4 |
| Archive extraction per shard | 0.1 s |
| Nightly floor for the reference set | 2 seeds/s, the whole number below half the reference job's rate: 5.91 seeds/s on the nightly at `c48540dd` (run 39748626499, 169.3 s), about 5.7 at this branch's ratio (18.25 / 17.7 on the sweep alone), estimated, so half is 2.9 |
