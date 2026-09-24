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
  each, one stream of one partition, commits only at the end, and a pool of four threads.
- **Cores.** `cargo bench -p rdlt-engine --features bench --bench passthrough`, run with
  `taskset` on cores of one type, since the development machine mixes them; criterion's mean.

## Results

Intel Core Ultra X7 358H, on mains power, 2026-09-24.

| Cores | Run | Bare loop | Engine | Overhead | Gate |
|---|---|---|---|---|---|
| 4 performance cores (`taskset -c 0-3`) | first | 25.1 ms | 28.4 ms | 13.4 % | ≤ 10 % |
| | second | 24.3 ms | 27.0 ms | 11.0 % | ≤ 10 % |
| 8 efficient cores (`taskset -c 4-11`) | first | 36.5 ms | 38.4 ms | 5.3 % | ≤ 10 % |
| | second | 36.1 ms | 39.9 ms | 10.5 % | ≤ 10 % |

Two runs of the same build, hours apart, moved each overhead by several points, so the overhead
is 5–13 % on this machine: at or above the gate, not within it.

Where the difference goes, on the performance cores:

- The engine's own work — admission, coalescing, the lowering plan, lanes, commits — is 0.84 ms
  of a run with the sink encoding nothing: 13 µs a batch, 3 % of the bare loop.
- The metadata columns cost the sink 2.4 %: the bare loop takes 25.6 ms writing the same batches
  with two dictionary columns added.
- The rest is batches moving between the partition, the compute pool and the lane: deeper lane
  and partition queues bring it to 10–12 %, within this machine's noise.

Before M3b the same run was 92 % over the bare loop, unpinned: the metadata columns were 24 bytes
a row, built value by value, and the engine's threads landed on the slower cores.
