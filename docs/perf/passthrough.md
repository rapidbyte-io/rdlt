# Arrow passthrough

In-process Arrow passthrough is held to an engine overhead of at most 10 % (the spec's bound)
against a bare loop writing the same batches to the same destination. The overhead is a ratio
measured on one machine, so this record keeps the method with the figures.

## Method

- **Batches.** 64 batches of 80 000 rows, 6.9 MB of logical bytes each (the bytes
  their rows reference, which the throughput counts): three 64-bit integers, two floats, two short
  strings, a timestamp, a boolean and a 32-bit integer.
- **Destination.** `ipc_sink` from the `bench` feature, which encodes each staged batch as an
  Arrow IPC stream into a buffer it reuses: the least a real destination does with a batch.
- **Bare loop.** The batches written to the sink's writer one after another, on one thread.
- **Engine.** `Engine::run` with the `Replay` source pushing the same batches, a checkpoint after
  each, one stream of one partition, a 1 GiB memory budget and one commit, at the end. The bench
  splits the cores it is pinned to as a process that builds its own runtime does, through
  `Cores::try_from_host`: half of them, and two at least, its tokio workers, and the rest compute
  threads, one at least (`SystemEnv::try_new`).
- **Pairs.** `passthrough/paired/<W>+<C>`, named by its tokio workers and compute threads, runs
  blocks of bare loop, engine, engine, bare loop in each of its 30 samples, so each
  side runs first as often as last, and takes the ratio of the engine's time to the bare loop's
  in each sample. The overhead is the median of the ratios, less one, with a 95 % percentile
  bootstrap interval: a machine that slows down or speeds up during a run moves both sides of
  every ratio alike. Criterion's time is one engine run.
- **Allocations.** The `allocations` bench, whose global allocator is `stats_alloc`'s counting
  system allocator, runs each side once on the same cores and prints its allocations,
  reallocations and bytes requested, a row and a batch. The timed bench keeps the system
  allocator.
- **Build.** The release profile: fat LTO and one codegen unit.
- **Running it.** `just bench passthrough <cores>`, which pins the cores with `taskset` and
  records the commit, the load average, each core's governor and energy preference, the thread
  counts, the allocations, and `perf stat`'s instructions a cycle and CPUs busy over both sides of
  the pairs, ten seconds of them.
- **Profiles.** `just profiling passthrough` builds the bench with line tables and frame pointers;
  [profiling.md](profiling.md) holds the recipes.

## Results

Commit `f64df77f`, 2026-10-11. Intel Core Ultra X7 358H (4 performance, 8 efficient and 4 low-power
cores) on mains power, platform profile `performance`, governor `powersave`, energy preference
`performance`, turbo on. Five rounds on the performance cores, interleaved with rounds of the
merge-base and with the wide, shredding and served benches, in alternating orders; each invocation
started at a one-minute load average under 1.0 with its cores idle, the load average 0.92 to 0.98
at their starts. Each figure is the median of the five rounds, their range in brackets, and each
round lies within 3 % of the median (1.5 points for a percentage) except the fifth round's
instructions a cycle, 0.63; the interval's ends are the medians of the rounds' ends. Other load on
the machine raised the one-minute load average to 2.66 by the end of the fifth round's run; the
round is kept, and its overhead, 16.0 %, is the top of the range.

The efficient-cores row is not re-measured at that commit: it is from commit `864cb1f2`,
2026-10-10, the median of two rounds interleaved with the shredding benches in opposite orders,
each started at a one-minute load average of 0.21 to 1.00, each within 3 % of the median (1.5
points for a percentage). A third round ran later under other load on the machine and is left out
(the owner's ruling of 2026-10-10).

| Cores | tokio workers + compute threads | Engine | Overhead | 95 % interval | Instructions a cycle | CPUs busy | Spec's bound |
|---|---|---|---|---|---|---|---|
| 4 performance (`0-3`) | 2 + 2 | 28.9 ms | 15.4 % [14.8–16.0] | 14.9–16.1 % | 0.60 | 1.26 | ≤ 10 % |
| 4 efficient (`4-7`), commit `864cb1f2`, 2026-10-10 | 2 + 2 | 37.7 ms | 8.4 % [8.0–8.8] | 8.0–8.7 % | 0.55 | 1.30 | ≤ 10 % |

On the performance cores the overhead is above the spec's bound. On the efficient cores the overhead
is within the spec's bound.

The allocations of one run of each side on the performance cores, at commit `f64df77f`. Each side's
are the same in all five rounds; the engine's are given as the median of the rounds with their
range.

| Side | Allocations, reallocations and bytes a row | A batch |
|---|---|---|
| Bare loop | 0.001, 0.000, 3 B | 62.062, 22.156, 270028 B |
| Engine | 0.004, 0.000, 4 B | 351.969 [351.969–351.969], 30.484 [30.484–30.484], 318137 B [318137–318137] |
