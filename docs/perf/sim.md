# Simulation throughput

How fast the simulation checks seeds. The figures are measured on the simulation as this record
leaves it, against its parent `a92436bf`. The local rows are the parent's rounds, taken with the
one-minute load under 1.0, scaled by the ratio of the two measured side by side on a loaded machine
(below): 1.02 for the sweeps, 1.016 rounded up, and 1.001 for the replay. The CI rows marked
estimated are scaled by the same ratios from the jobs API's times of the run each row names, the
nightly's from the one at `cd2bb077`, which ran the parent's simulation; the pull-request shards'
rows are measured on main's run at `8e212940`, which holds this record's. `just sim-rate` checks
the reference rate every night against the floor below.

## Limits

What the simulation costs is held to these limits, in absolute terms, so what each change adds
is bounded in sum:

- The longest pull-request shard takes at most 12 of its 15 minutes.
- Each nightly shard stays within its 70 minutes.
- The reference set runs at the floor the nightly's `just sim-rate` checks or above it: the whole
  number below half the nightly's reference rate, 2 seeds/s (the floor's row below).
- A change that would take a shard past its limit adds shards in the same change; no change raises
  a shard's time limit, or lowers the floor, to make room for its own work. A change that would
  take the reference rate below the floor makes the simulation cheaper elsewhere in the same
  change.
- Every change that adds simulation work records here, by this record's method, the reference
  set's rate on four performance cores and the longest pull-request shard on CI.

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
  the ratio of the medians. The rounds, in seconds: exactly-once 79.0, 77.0, 77.9 on the parent
  and 78.9, 80.3, 89.8 on the branch; changes 30.0, 26.1, 24.6 and 25.4, 20.7, 25.7; replay 185.1
  and 185.2. The rounds vary more than the 1.6 % between the medians, so the ratio is good to a few
  percent. A test's time is nextest's `PASS` duration.
- **Staleness.** A later change to simulation code outdates these figures. Rows marked estimated
  are replaced by a run's medians when one exists.
- **CI.** GitHub's ubuntu-24.04 runners, four vCPUs; macos-15 for the macOS test job. Job times
  from the jobs API, test times from nextest's `PASS` lines in the job logs.
- **Reference set.** The exactly-once sweep over seeds 0 to 999.

## Figures

| Measure | Value |
|---|---|
| Exactly-once sweep, seeds 0-999, four P-cores | about 17.9 seeds/s (55.9 s), estimated, against 18.25 (54.8 s) on the parent |
| Change sweep, seeds 0-999, four P-cores | about 73.0 seeds/s (13.7 s), estimated, against 74.5 (13.4 s) on the parent |
| Both sweeps together, seeds 0-999 | about 69.6 s, estimated: 1.02 times the parent's 68.2 s (measured side by side 105.7 s against 104.0 s, 1.016) |
| Replay of both oracles, 1 000 seeds, four P-cores | about 139.5 s (7.2 seeds/s), `just sim-replay`: the parent's 139.4 s times the side-by-side ratio 1.001 (185.2 s against 185.1 s) |
| Pull request shards | 40 of 2 500 seeds, 4.8-9.7 min of 15 each on CI, on main's run at `8e212940` (run 39985988144); no more than about 8 of a run's jobs start together |
| Exactly-once sweep, a pull request's shard, ubuntu-24.04 | 6.64 seeds/s median, 5.94-12.73 over 40 shards, on main's run at `8e212940` (2 500 seeds in 196.4-420.7 s, nextest's `PASS` lines) |
| Nightly shards | 80 of 12 500 seeds, 23.3-45.7 min of 70, estimated from the nightly at `cd2bb077` (run 39924989865), whose shards took 22.8-44.8 min, times 1.02 |
| Coverage run, CI | about 38.8 min of 60, estimated: the nightly at `cd2bb077` took 38.1 min, times 1.02 |
| Replay job, CI | about 7.7 min of 30, estimated: the nightly at `cd2bb077` took 7.65 min, times 1.001 |
| Test job, ubuntu-24.04 | 26.5 min of 45 on main's run at `a92436bf` (run 39811772076), plus 41 s for the broken connectors' tests: about 27.2 |
| Test job, macos-15 | 24.8 min of 45 on main's run at `a92436bf` (run 39811772076), plus the same 41 s: about 25.5 |
| Archive extraction per shard | 0.1 s |
| Nightly floor for the reference set | 2 seeds/s, the whole number below half the reference job's rate: 5.92 seeds/s on the nightly at `cd2bb077` (run 39924989865, 169.1 s), estimated at about 5.8 here, divided by 1.02, so half is about 2.9 |

## Commits answered transiently, sent again in place

The simulated destination leaves some tries it answers transiently running, to land later, and the
engine sends such a commit again in place rather than ending its attempt. Measured side by side
with `main` at `ea57c46d` on 2026-10-31, the reference set from each tree's archive, alternating
on CPUs 0-3 (base, branch; branch, base; base, branch) under a one-minute load of 4.05 to 4.14, the
sweep's own four threads among it:

| Measure | `main` | This change |
|---|---|---|
| Exactly-once sweep, seeds 0-999, four P-cores | 21.41 seeds/s (46.70 s; 21.40-21.55) | 22.04 seeds/s (45.37 s; 21.93-22.04) |

The change runs the reference set 2.9 % faster: a transient commit answer no longer costs an
attempt's reopen and re-read. The longest pull-request shard, at most the 9.7 min the table's run
took times the same ratio, stays within its 12; the nightly floor holds.

## Events told and checked

Each run of a seed's phases tells its events to an environment that folds them into the run's
trace and the events oracle's tallies, keeping none. Measured side by side with `main` at
`01cd996b` on 2026-10-30, the reference set from each tree's archive, alternating on CPUs 0-3
(base, branch, three times) under a one-minute load of 1.1 to 4.1, the sweep's own four threads
among it:

| Measure | `main` | This change |
|---|---|---|
| Exactly-once sweep, seeds 0-999, four P-cores | 21.28 seeds/s (46.99 s; 43.95-47.04) | 21.35 seeds/s (46.85 s; 46.39-47.02) |

The medians differ by 0.3 %, less than the rounds tell apart: folding the events costs the
simulation nothing these rounds can see. The longest pull-request shard stays within its 12
minutes, and the nightly floor holds.
