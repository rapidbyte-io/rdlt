# Simulation throughput

How fast the simulation checks seeds. The local rows were measured on `b2bda30d` against
`c69b15e1`, the parent of its first commit; the CI rows are `c69b15e1`'s jobs scaled by the local
ratio until a pull request's own run replaces them. `just sim-rate`
checks the reference rate every night against the floor below.

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
| Exactly-once sweep, seeds 0-999, four P-cores | 19.5 seeds/s (51.4 s), against 22.8 on the parent |
| Change sweep, seeds 0-999, four P-cores | 79.7 seeds/s (12.6 s), against 154.3 on the parent |
| Both sweeps together, seeds 0-999 | 64.0 s, 1.27 times the parent's 50.4 s |
| Replay of both oracles, 1 000 seeds, four P-cores | 134.3 s (7.4 seeds/s), `just sim-replay` |
| Replay tests of the test job, dev profile | 83.3 s and 93.8 s, against 54.4 s |
| Hostile networked seeds, seeds 0-999, `9ff145dd` | 13.3 % of seeds, 22.0 % of wall time; the sweep 93 % of the parent's rate, interleaved rounds at a load of 6-9 |
| Network seeds' steps in settling's fixed second, seeds 0-999, `90ce76c2` | 36.1 %, all after the last task ended |
| Pull request shards | 40 of 2 500 seeds, about 9.4 min of 15 (the parent's 20 of 5 000 took up to 14.7 min) |
| Exactly-once sweep, a pull request's shard, ubuntu-24.04 | about 6.6 seeds/s median, estimated (the parent's 20 shards: 7.8 median, 7.3-14.3) |
| Nightly shards | 80 of 12 500 seeds, about 44 min of 70 (the parent's 40 of 25 000 took up to 69 min) |
| Coverage run, CI | about 36 min of 60, estimated from the parent's 28.4 min |
| Replay job, CI | about 7 min of 30, estimated from the local time |
| Test job, Linux | about 2 min more than the parent's 22.7 min, estimated |
| Archive extraction per shard | 0.1 s |
| Nightly floor for the reference set | 3 seeds/s, the whole number below half the shards' estimated median |
