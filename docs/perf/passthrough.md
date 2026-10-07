# Arrow passthrough

The spec gates in-process Arrow passthrough at an engine overhead of at most 10 % against a bare
read-then-write loop. The overhead is the median ratio of paired runs with its 95 % bootstrap
interval, measured on one machine, so this record keeps the method with the numbers.

## Method

- **Batches.** 64 batches of 80 000 rows, about 7 MB each: three 64-bit integers, two floats,
  two short strings, a timestamp, a boolean and a 32-bit integer.
- **Destination.** `ipc_sink` from the `bench` feature, which encodes each staged batch as an
  Arrow IPC stream into a buffer it reuses: the least a real destination does with a batch.
- **Bare loop.** The batches written to the sink's writer one after another, on one thread.
- **Engine.** `Engine::run` with the `Replay` source pushing the same batches, a checkpoint after
  each, one stream of one partition, commits only at the end, within the cores the bench may run
  on: a runtime of the workers `Cores::try_from_host` suggests for them, half and two at least,
  and a compute pool of the rest, one thread at least (`SystemEnv::try_new`), and lanes one per
  core.
- **Pairs.** `passthrough/paired/<W>+<C>`, named by its runtime workers and compute threads, runs
  blocks of bare loop, engine, engine, bare loop in each of its 30 samples, so each side runs
  first as often as last, and takes the ratio of the engine's time to the bare loop's in each
  sample. The overhead is the median of the ratios with a 95 % percentile bootstrap interval: a
  machine that slows down or speeds up during a run moves both sides of every ratio alike.
  Criterion's time is one engine run; throughput counts the batches' logical bytes, 6.9 MB a
  batch.
- **Allocations.** The `allocations` bench, whose global allocator is `stats_alloc`'s counting
  system allocator, runs each side once and prints its allocations, reallocations and bytes
  requested, a row and a batch. The timed bench keeps the system allocator.
- **Running.** `just bench passthrough 0-3` on the four performance cores, built with the release
  profile's fat LTO and one codegen unit. It records the commit, the load before and after, each
  CPU's governor, criterion's estimate of the engine's time, the ratio, `perf stat`'s
  instructions a cycle and CPUs busy over both sides of the pairs, and the allocations.
- **Profiles.** `just profiling passthrough` builds the bench with line tables and frame pointers.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-07, commit `d482d4ecb150`, governor `powersave`
with energy preference `performance`, one-minute load average 0.46–1.25 before and after the runs;
five runs of `just bench passthrough 0-3`.

| Run | Engine | Engine over bare loop (95 % interval) | Instructions a cycle | CPUs busy |
|---|---|---|---|---|
| 1 | 28.82 ms | 1.167 (1.164 to 1.179) | 0.62 | 1.33 |
| 2 | 29.04 ms | 1.167 (1.161 to 1.173) | 0.61 | 1.32 |
| 3 | 29.00 ms | 1.179 (1.171 to 1.188) | 0.62 | 1.32 |
| 4 | 28.93 ms | 1.168 (1.159 to 1.173) | 0.61 | 1.32 |
| 5 | 29.21 ms | 1.171 (1.160 to 1.176) | 0.62 | 1.32 |

The engine is 16.8 % over the bare loop (16.7–17.9 % across the five runs) with 2 runtime
workers and 2 compute threads, against the spec's 10 %. Instructions a cycle and CPUs busy cover
both sides of the pairs.

| Side | A row | A batch |
|---|---|---|
| Bare loop | 0.001 allocations, 0.000 reallocations, 3 bytes | 62.062 allocations, 22.156 reallocations, 270028 bytes |
| Engine (median of five runs, range) | 0.005 allocations, 0.001 reallocations, 4 bytes | 394.672 allocations (394.656 to 394.688), 40.266 reallocations, 321620 bytes |

## Layouts

How many of N cores the runtime's workers take, the bench built once per worker count and run
within N cores, five rounds interleaved across every row, 2026-10-06, `main` at `b46a4574` with
the change that sized the runtime by this table, load average 0.78–0.99; the engine's median and
range:

| Cores | Workers + compute threads | Bare loop | Engine |
|---|---|---|---|
| 2 performance cores (`taskset -c 0-1`) | 1 + 1 | 24.3 ms | 40.0 ms (39.5–41.0) |
| | 2 + 1 | 24.2 ms | 31.2 ms (30.8–31.6) |
| 4 performance cores (`taskset -c 0-3`) | 1 + 3 | 24.7 ms | 42.9 ms (42.7–44.6) |
| | 2 + 2 | 24.1 ms | 28.3 ms (28.0–31.3) |
| | 3 + 1 | 24.3 ms | 31.2 ms (30.9–31.6) |
| 8 efficient cores (`taskset -c 4-11`) | 1 + 7 | 35.0 ms | 52.8 ms (52.4–53.8) |
| | 2 + 6 | 35.1 ms | 38.3 ms (37.9–39.1) |
| | 3 + 5 | 34.6 ms | 38.2 ms (38.0–38.6) |
| | 4 + 4 | 34.8 ms | 38.4 ms (38.1–50.5) |
| | 5 + 3 | 34.6 ms | 38.2 ms (38.1–38.6) |
| | 6 + 2 | 35.1 ms | 38.4 ms (38.1–39.1) |
| | 7 + 1 | 35.1 ms | 38.3 ms (38.1–38.9) |

The 2 + 1 row comes from five further interleaved rounds against `main`'s build, whose runtime of
two workers beside a pool of four threads ran the engine in 29.6 ms (29.1–29.6) in them.

One worker is the slowest layout at every count, 1.5–1.8 times the bare loop: it runs the
engine's per-row work and every lane's writes alone. So `Cores::try_from_host` gives the runtime
half the cores and two workers at least: the fastest split at four cores, within the range of
every eight-core split but one worker's, and at two cores two workers beside a one-thread pool,
one thread more than there are cores, which ran the engine 22 % faster than one worker and one
thread, and still 6 % slower than `main`'s two workers beside four threads.

Where the difference goes, on the performance cores:

- The engine's own work — admission, coalescing, the lowering plan, lanes, commits — is 0.84 ms
  of a run with the sink encoding nothing: 13 µs a batch, 3 % of the bare loop.
- The metadata columns cost the sink 2.4 %: the bare loop takes 25.6 ms writing the same batches
  with two dictionary columns added.
- The rest is batches moving between the partition, the compute pool and the lane: deeper lane
  and partition queues bring it to 10–12 %, within this machine's noise.

Before M3b the same run was 92 % over the bare loop, unpinned: the metadata columns were 24 bytes
a row, built value by value, and the engine's threads landed on the slower cores.
