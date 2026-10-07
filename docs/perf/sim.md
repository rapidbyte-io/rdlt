# Simulation throughput

How fast the simulation checks seeds, measured at `590f7ff3`. `just sim-rate` checks the
reference rate every night against the floor below.

## Method

- **Build.** The shards, the stress run and the rate check run the `sim` profile (optimised, with
  overflow checks and debug assertions kept) from the archive `just sim-archive` builds; the test
  job runs the dev profile.
- **Seeds side by side.** `for_each_seed` runs as many seeds at once as the cores hold, the
  host's unless `RDLT_SIM_CORES` names a count: one thread a seed, eight a stress seed. Each
  seed's wall time is a line of `target/sim-timings/<sweep>.jsonl`, and the last line is the
  sweep's seeds a second.
- **Local.** Intel Core Ultra X7 358H, performance cores 0-3 (`taskset -c 0-3`), governor
  `powersave`, each run started with the one-minute load under 1.0, median of three runs
  interleaved with the parent commit's (two for the coverage run); a test's time is nextest's
  `PASS` duration.
- **CI.** GitHub's ubuntu-24.04 runners, four vCPUs; macos-15 for the macOS test job. Job times
  from the jobs API, test times from nextest's `PASS` lines in the job logs.
- **Reference set.** The exactly-once sweep over seeds 0 to 999.

## Figures

| Measure | Value |
|---|---|
| Exactly-once sweep, seeds 0-399, four P-cores | 33.6 seeds/s (11.91 s), at most 343 MiB resident |
| Exactly-once sweep, seeds 0-399, one P-core | 9.4 seeds/s |
| Per seed, seeds 0-399, one thread | 106 ms mean, 1 567 ms max |
| Network seeds, seeds 0-399 | 25 % of seeds, 49 % of wall time |
| Coverage run, 1 000 seeds, four P-cores | 7.6 min (456 s) |
| Exactly-once sweep, a pull request's shard of 5 000 seeds | TODO (controller, from CI) |
| Test job, Linux / macOS | TODO (controller, from CI) |
| Archive extraction per shard | TODO (controller, from CI) |
| Nightly, ubuntu-24.04 | TODO (controller, from the nightly dispatched on the branch): stress, 200 seeds; coverage, 1 000 seeds; reference set |
| Nightly floor for the reference set | 3 seeds/s until the shards are measured: TODO (controller), half the shards' median |
