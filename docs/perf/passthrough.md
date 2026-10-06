# Arrow passthrough

The spec gates in-process Arrow passthrough at an engine overhead of at most 10 % against a bare
read-then-write loop (§21.1). This record keeps the method with the numbers, which are ratios
measured on one machine.

## Method

- **Batches.** 64 batches of 80 000 rows, about 7 MB each: three 64-bit integers, two floats,
  two short strings, a timestamp, a boolean and a 32-bit integer.
- **Destination.** `IpcSink` from the `bench` feature, which encodes each staged batch as an
  Arrow IPC stream into a buffer it reuses: the least a real destination does with a batch.
- **Bare loop.** The batches written to the sink's writer one after another, on one thread.
- **Engine.** `Engine::run` with the `Replay` source pushing the same batches, a checkpoint after
  each, one stream of one partition, commits only at the end, within the cores `taskset` gives the
  bench: a runtime of the workers `Cores::try_from_host` suggests for them, half, and a compute
  pool of the rest (`SystemEnv::try_from_runtime`), and lanes one per core.
- **Build and cores.** `cargo bench -p rdlt-engine --features bench --bench passthrough`, built
  with the release profile's fat LTO and one codegen unit, run with `taskset -c 0-3` on the
  performance cores; criterion's mean of ten samples, the median and range of five runs.
- **Profiles.** `just profiling passthrough` builds the bench with line tables and frame pointers.

## Results

Intel Core Ultra X7 358H, on mains power, governor `powersave`, 2026-10-06: `main` at `b46a4574`
with the change that last edited this record, load average 0.78–0.99 over the runs.

| Cores | Layout | Bare loop | Engine |
|---|---|---|---|
| 4 performance cores (`taskset -c 0-3`) | 2 workers, 2 compute threads | 24.1 ms (24.0–27.2) | 28.3 ms (28.0–31.3) |

## Layouts

How many of N cores the runtime's workers take, the bench built once per worker count and run
within N cores, five rounds interleaved across every row, the same day and load as above; the
engine's median and range:

| Cores | Workers + compute threads | Bare loop | Engine |
|---|---|---|---|
| 2 performance cores (`taskset -c 0-1`) | 1 + 1 | 24.3 ms | 40.0 ms (39.5–41.0) |
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

One worker is the slowest layout at every count: it runs the engine's per-row work and the lane's
writes alone. Half the cores, the split `Cores::try_from_host` suggests, is the fastest at four
cores and within the range of every eight-core split but one worker's. At two cores it is the only split that gives each
side a core; two workers beside a one-thread pool (`Cores::new` with two workers of two cores),
measured in five further interleaved rounds, ran the engine in 30.6 ms (30.4–31.2) against 39.8 ms
(39.3–39.9) for one worker and one thread.

Where the difference goes, on the performance cores:

- The engine's own work — admission, coalescing, the lowering plan, lanes, commits — is 0.84 ms
  of a run with the sink encoding nothing: 13 µs a batch, 3 % of the bare loop.
- The metadata columns cost the sink 2.4 %: the bare loop takes 25.6 ms writing the same batches
  with two dictionary columns added.
- The rest is batches moving between the partition, the compute pool and the lane: deeper lane
  and partition queues bring it to 10–12 %, within this machine's noise.

Before M3b the same run was 92 % over the bare loop, unpinned: the metadata columns were 24 bytes
a row, built value by value, and the engine's threads landed on the slower cores.
