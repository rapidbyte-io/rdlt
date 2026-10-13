# Arrow passthrough

In-process Arrow passthrough is held to a bound on its engine overhead against a bare loop writing
the same batches to the same destination: on the performance cores at most 17 % at 64 batches a
run, of which the part each batch pays is at most 13 %, and on the efficient cores at most 10 %.
Every table stores the `_rdlt_load_id` and `_rdlt_loaded_at` columns, which the lane encodes with
each batch's own. Each batch crosses from its partition's thread to its lane's, and the runtime's
workers, parked and woken for it, run at a lower clock than a thread that never waits. These two
make the part each batch pays inherent. The overhead is a ratio measured on one machine, so this
record keeps the method with the figures.

## Method

- **Matrix.** Every cell moves the same 5 120 000 rows of ten columns, about 442 MB of logical
  bytes (the bytes their rows reference, which the throughput counts): three 64-bit integers, two
  floats, two short strings, a timestamp, a boolean and a 32-bit integer. A cell takes one value
  of each axis: batches of 1 000, 8 000 or 80 000 rows (5 120, 640 or 64 batches, 86 kB, 690 kB
  or 6.9 MB each); 1, 4 or 16 partitions of each stream; 1 or 4 streams; and a commit at the end
  or every sixteenth of the rows. Its id is
  `passthrough/matrix/<rows>x<batches>/<P>p/<S>s/<end|16th>`, `/null` after it for the null
  sink. By default the bench runs the chosen cells: the paired cell, each axis moved alone from
  it, 8 000 and 1 000 rows at 16 partitions, 1 000 rows at 16 partitions committed every
  sixteenth, and 1 000 and 8 000 rows into the null sink. With `full` as the filter
  (`just bench passthrough 0-3 full`) it runs every cell, 36 into the IPC sink and the two null
  ones, under `passthrough/full/`.
- **Destination.** `Sinking::Ipc` from the `bench` feature, which encodes each staged batch as an
  Arrow IPC stream into a buffer it reuses: the least a real destination does with a batch. The
  engine's sink declares sixteen writers, so a run has a lane a core up to sixteen. The two
  `/null` cells drop each batch instead.
- **Bare loop.** The paired cell's batches written to the sink's writer one after another, on one
  thread.
- **Engine.** `Engine::run` with the `Replay` source pushing each partition of each stream an even
  share of the batches in order, a checkpoint after each push, so each push is a flush of its
  own. The budget is the engine's default, 256 MiB. A cell committed at the `end` sets no
  threshold, so only the end of the read, or the budget, makes a commit due; one committed every
  `16th` commits every 320 000 rows. The bench splits the cores it is pinned to as a process
  that builds its own runtime does, through `Cores::try_from_host`: half of them, and two at
  least, its tokio workers, and the rest compute threads, one at least (`SystemEnv::try_new`).
- **Readings.** After each cell, one line from its last run's report: commits by what made each
  due; the time pushes and requests for lowering waited for room, each summed over the run's
  tasks, over the run's wall time; the compute pool's idle share, one less its jobs' running
  time over its threads times the wall time; its jobs' waits to start and ways back to their
  callers, summed, over the wall time; the lanes that wrote; and the budget's signals a batch.
  The waits and the queue and way back are summed over every task or job, so where several wait
  at once they read past 100 %: a reading of 900 % is nine tasks waiting the whole run, on
  average.
- **Pairs.** `passthrough/paired/<W>+<C>`, named by its tokio workers and compute threads, moves
  the paired cell, 64 batches of 80 000 rows from one partition of one stream committed at the
  end, within a 1 GiB budget and through a sink declaring one writer, as the bound is stated,
  and runs blocks of bare loop, engine, engine, bare loop in each of its 30 samples, so each
  side runs first as often as last, and takes the ratio of the engine's time to the bare loop's
  in each sample. The overhead is the median of the ratios, less one, with a 95 % percentile
  bootstrap interval: a machine that slows down or speeds up during a run moves both sides of
  every ratio alike. Criterion's time is one engine run.
- **Allocations.** The `allocations` bench, whose global allocator is `stats_alloc`'s counting
  system allocator, runs each side once on the same cores and prints its allocations,
  reallocations and bytes requested, a row and a batch. The timed bench keeps the system
  allocator.
- **Build.** The release profile: fat LTO and one codegen unit.
- **Running it.** `just bench passthrough <cores> [paired|full]`, which pins the cores with
  `taskset` and records the commit, the load average, each core's governor and energy
  preference, the thread counts, the allocations, and for each id `perf stat`'s instructions a
  cycle and CPUs busy, ten seconds of them. `paired` runs the pairs alone.
- **Profiles.** `just profiling passthrough` builds the bench with line tables and frame pointers;
  [profiling.md](profiling.md) holds the recipes.

## Results

This change on `90ce76c2`, 2026-10-13. Intel Core Ultra X7 358H (4 performance, 8 efficient and
4 low-power cores) on mains power, platform profile `performance`, governor `powersave`, energy
preference `performance`, turbo on, the bench on the performance cores (`0-3`).

### The paired cell

Five rounds of `90ce76c2` and five of this change, interleaved main, change, change, main, the
one-minute load average 1.2 to 1.7 at their starts. Each figure is the median of a side's five
rounds, their range in brackets; the interval's ends are the medians of the rounds' ends. Six
earlier rounds ran while other load held the machine (one-minute load 2.3, `90ce76c2`'s own
overhead reading 22.8 % in one of them) and are left out.

The efficient-cores row is not re-measured: it is from commit `864cb1f2`, 2026-10-10, the median
of two rounds interleaved with the shredding benches in opposite orders, each started at a
one-minute load average of 0.21 to 1.00, each within 3 % of the median (1.5 points for a
percentage).

| Cores | Tree | tokio workers + compute threads | Engine | Overhead | 95 % interval | Instructions a cycle | CPUs busy | Bound |
|---|---|---|---|---|---|---|---|---|
| 4 performance (`0-3`) | `90ce76c2` | 2 + 2 | 29.6 ms | 15.5 % [14.9–16.0] | 14.9–16.5 % | 0.59 | 1.26 | ≤ 17 % at 64 batches, ≤ 13 % a batch |
| 4 performance (`0-3`) | this change | 2 + 2 | 29.9 ms | 15.8 % [14.9–16.2] | 14.9–16.5 % | 0.60 | 1.26 | ≤ 17 % at 64 batches, ≤ 13 % a batch |
| 4 efficient (`4-7`) | `864cb1f2` | 2 + 2 | 37.7 ms | 8.4 % [8.0–8.8] | 8.0–8.7 % | 0.55 | 1.30 | ≤ 10 % |

On both kinds of core the overhead is within its bound, and the change's interval lies within
`90ce76c2`'s. The paired cell runs within 1 GiB through one writer, as the bound is stated:
within the default 256 MiB the same cell read 22.6 % and 23.3 % in two rounds through one writer,
and within 1 GiB through sixteen writers 17.9 % and 18.4 %.

The allocations of one run of each side, the same in every round of each tree:

| Side | Tree | Allocations, reallocations and bytes a row | A batch |
|---|---|---|---|
| Bare loop | both | 0.001, 0.000, 3 B | 62.062, 22.156, 270028 B |
| Engine | `90ce76c2` | 0.004, 0.000, 4 B | 352.188, 30.484, 318174 B |
| Engine | this change | 0.004, 0.000, 4 B | 354.172, 30.531, 318271 B |

### The chosen cells

Three rounds of `just bench passthrough 0-3`, the one-minute load average 0.9 to 2.1 at their
starts; each round took 414 to 418 s. Each figure is the median of the three rounds, the range
of the throughput in brackets; every cell's throughput lies within 3 % of its median but
`8000x640/1p/1s/end` (one round 8.3 % below) and the 80 000-row cells of four and sixteen
partitions (one round 11 to 12 % off). The readings are the last run's of each round; their medians:
commits made due by size and by the end, the work and intake waits and the pool's queue and way
back as percentages of the run summed over its tasks, the pool idle, the lanes written and the
budget's signals a batch.

| Cell | MB/s | Engine a run | Commits (size + end) | Work | Intake | Pool idle | Queue and way back | Lanes | Signals |
|---|---|---|---|---|---|---|---|---|---|
| `80000x64/1p/1s/end` | 14 768 [14 546–14 791] | 29.9 ms | 0 + 1 | 0 % | 66.7 % | 97.4 % | 20.8 % | 1 | 9.6 |
| `8000x640/1p/1s/end` | 13 890 [12 743–13 945] | 31.7 ms | 0 + 1 | 0 % | 0.7 % | 91.7 % | 31.5 % | 1 | 8.0 |
| `1000x5120/1p/1s/end` | 3 172 [3 107–3 183] | 139.0 ms | 0 + 1 | 0 % | 0.1 % | 86.5 % | 32.7 % | 1 | 8.0 |
| `80000x64/4p/1s/end` | 15 896 [15 857–17 813] | 27.7 ms | 0 + 1 | 0 % | 285 % | 95.0 % | 265 % | 4 | 11.4 |
| `80000x64/16p/1s/end` | 17 346 [15 419–17 988] | 25.4 ms | 0 + 1 | 0 % | 948 % | 95.2 % | 497 % | 4 | 11.8 |
| `80000x64/1p/4s/end` | 15 815 [15 796–16 048] | 27.9 ms | 0 + 1 | 0 % | 284 % | 94.3 % | 273 % | 4 | 11.5 |
| `80000x64/1p/1s/16th` | 14 945 [14 506–14 960] | 29.5 ms | 16 + 1 | 0 % | 62.7 % | 97.2 % | 13.8 % | 1 | 9.4 |
| `8000x640/16p/1s/end` | 14 844 [14 656–14 895] | 29.7 ms | 0 + 1 | 0 % | 1 137 % | 90.6 % | 1 398 % | 4 | 11.3 |
| `1000x5120/16p/1s/end` | 4 696 [4 635–4 702] | 93.9 ms | 0 + 1 | 0 % | 6.2 % | 79.2 % | 1 276 % | 4 | 8.0 |
| `1000x5120/16p/1s/16th` | 4 656 [4 641–4 689] | 94.7 ms | 16 + 1 | 0 % | 0 % | 79.3 % | 1 248 % | 4 | 8.0 |
| `1000x5120/1p/1s/end/null` | 3 261 [3 256–3 289] | 135.2 ms | 0 + 1 | 0 % | 0 % | 86.9 % | 30.2 % | 1 | 8.0 |
| `8000x640/1p/1s/end/null` | 13 303 [13 187–13 338] | 33.1 ms | 0 + 1 | 0 % | 0.3 % | 92.5 % | 32.5 % | 1 | 8.0 |

On these four cores a cell of four or sixteen partitions, or of four streams, writes through
four lanes. No run waited for room for lowering, and no commit was made due by the
budget's cursors or tables.

**Pushes of 80 000 rows wait for room within the default budget.** Within 256 MiB the paired
cell's pushes wait for room for 66.7 % [62.8–70.1] of the run, one partition's pushes waiting
nearly two-thirds of the time: its 442 MB of batches, 6.9 MB each, do not fit the pushes' share
of the budget at once, so the source waits for writes to release room. With four partitions or
streams the waits sum to 284–285 % of the run, about three pushes waiting at every moment, and
with sixteen partitions to 948 %. At 8 000 rows a batch from sixteen partitions they sum to
1 137 %; at 1 000 rows to 6.2 %, and to under 1 % from one partition at 1 000 or 8 000 rows. The
same cell within 1 GiB is the paired cell above. This record states the waits; it does not
change the budget.

## Where the overhead goes

The question was where the engine's time beyond the bare loop's goes on the performance cores,
component by component, and how the production path, a destination spawned as a process of its
own, spends its run. Measured only; nothing here changed the engine.

Commit `a64e6585`, 2026-10-11, the machine above, governor `powersave` with energy preference
`performance` on CPUs 0–3, each hold under the measurement lock and started at a one-minute load
average of 0.79–0.98.

- **Profiles.** `just profiling passthrough` and `just profiling served`, as
  [profiling.md](profiling.md) records them: `cpu_core/cycles/u` at 999 Hz with frame pointers
  over 15 seconds of `passthrough/paired/2+2` on CPUs 0–3, and `task-clock` at one sample a
  millisecond over 15 seconds of `served/process/socket/destination/64x80000`, the host on CPUs
  0–3 and the spawned destination on 4–11. Samples are classed by the frames on their stack; a
  sample whose leaf is `libc` (`memcpy`, mostly) goes to the first frame above it.
- **Each side alone.** A driver outside the repository, built against this tree's
  `rdlt_engine::bench` (`Passthrough`, `replay`, `ipc_sink`, `SinkWriter`), with the bench's
  configuration and cores, runs one side many times and prints each engine run's report
  counters. It also times the bare loop over the batches the lane is handed, captured from one
  run. This measurement code is not kept. Run times are medians, the first three runs of each
  invocation left out.
- **Scheduling and syscalls.** Each thread's switches from `/proc/<pid>/task/*/sched` as the
  process ends, and `strace -f -c` over 20 runs less a run of none, as profiling.md says.

### The lane bounds the run

The engine's runs alone took 28.80 ms against the bare loop's 25.17 ms (200 runs each), 14.4 %
more. The run report's counters, medians over 195 runs:

| Counter | A run |
|---|---|
| Lane writing (the sink's `write` calls) | 27.50 ms |
| Partitions blocked on the lane's full queue | 15.88 ms |
| Lane flushing | 0.002 ms |
| Commit phases: flush, horizon, log, destination commit, ack | 1.03, 0.0003, 0.014, 0.019, 0.006 ms |
| Pool jobs; queued and running, summed | 128; 1.01 and 1.43 ms |
| Budget waits | one, of the intake share, 0.29 ms |

The lane writes for 95 % of the run, and for more than half of it the partition waits, at the same
time, for room in the lane's queue: the two overlap rather than add, and the run is the lane's
writes plus what keeps the lane from writing. The commit's
flush phase is mostly the lane finishing the writes queued before it.

Run times against the batches a run moves split the overhead into a part each run pays once and
a part each batch pays. Two rounds, the sides in opposite orders, medians of 37 runs:

| Batches | Bare loop | Engine | Overhead |
|---|---|---|---|
| 8 | 2.95, 3.17 ms | 4.72, 4.66 ms | 60 %, 47 % |
| 16 | 6.04, 6.13 ms | 8.07, 8.59 ms | 34 %, 40 % |
| 32 | 12.10, 12.52 ms | 14.44, 14.76 ms | 19 %, 18 % |
| 64 | 23.87, 24.42 ms | 28.42, 28.33 ms | 19 %, 16 % |
| 128 | 48.06, 48.65 ms | 54.82, 54.38 ms | 14 %, 12 % |

A line through each round: the bare loop pays 0.0–0.2 ms a run and 0.375–0.379 ms a batch, the
engine 1.34–1.68 ms a run and 0.413–0.419 ms a batch. At 64 batches the engine's 3.9–4.6 ms over
the bare loop is 1.3–1.5 ms paid once a run and 2.2–2.8 ms paid by the batches, 34–43 µs each (9–12
% of a batch's bare time). Sides run alone drift more than the paired bench's blocks, whose ratio
stays the recorded overhead.

### What a batch pays: the lane's writes are slower

The lane's writes take 27.2–27.5 ms against the bare loop's 24.2–25.2 ms for the same batches,
9–13 % more, inside the sink's `write` call, which returns without yielding. What goes with it:

- **The metadata columns.** The lane is handed each batch with the two columns every table
  stores, `_rdlt_load_id` and `_rdlt_loaded_at`, as dictionaries of one value with 8-bit keys.
  Each batch's Arrow IPC grows from 6 991 944 to 7 173 000 bytes (2.6 %), and one thread encodes
  the 64 lowered batches in 24.59 ms against 23.78 ms for the batches pushed (3.4 %; 30 blocks
  of both, interleaved; 2.3–3.8 % in three later invocations on one core).
- **Other cores busy.** One core encoding the pushed batches takes 24.58 ms with CPUs 1–3 idle,
  25.12 ms with one of them running a busy loop and 25.55 ms with all three (2.2 % and 3.9 %).
  While the engine runs, its other threads keep about half a core busy beside the lane.
- **A lower clock.** In user mode the runtime's workers ran at 1.14–1.15 times the reference
  clock, the bare loop's thread at 1.22–1.28 (`cycles` over `ref-cycles`, each thread over 4
  seconds of runs, two rounds): 6–11 % slower. The workers park and are woken about twice a batch;
  the bare loop's thread never waits. With a queue of one write the workers ran at 1.03–1.04
  times the reference clock, and the writes took 28 % longer than with four (the table below).

A second thread scanning the 64-bit integers of the batch four ahead of the one being encoded,
as the partition does while the lane writes, did not slow the encode (25.63 ms without, 24.76 ms
with). Two runtime workers and one compute thread run as fast as two and two (28.39 and 28.33 ms
against 28.32 and 28.42 ms; the bare loop 24.80 and 24.67 ms).

The lane's queue holds four writes. Fewer slow the writes themselves; more do not speed them
(medians of 37 runs of 64 batches):

| Queue | Run | Lane writing | Partitions blocked | Commit's flush phase |
|---|---|---|---|---|
| 1 | 37.21 ms | 35.25 ms | 23.18 ms | 0.93 ms |
| 2 | 32.08 ms | 30.49 ms | 18.77 ms | 0.95 ms |
| 4 | 28.83 ms | 27.63 ms | 16.03 ms | 1.28 ms |
| 8 | 28.76 ms | 27.55 ms | 16.93 ms | 1.02 ms |
| 16 | 28.20 ms | 26.87 ms | 13.15 ms | 4.00 ms |

### What a run pays once

Before the lane's first write and after its last, the run plans its stream, opens the
destination, takes the first batch through the partition and commits. A profile of 400 runs of
four batches (`cpu_core/cycles/u` at 9 999 Hz) gives the CPU each takes a run:

| Work | CPU a run |
|---|---|
| Building the metadata columns' keys for the run's load (`Constants::new`) | 0.12 ms |
| The 64-bit integer scan of a batch (`exact::beyond`), each batch | 0.14 ms |
| The pool's jobs but the keys, eight a run | 0.23 ms |
| The partition's planning and writing but its jobs (`Resolver::resolve`, `Naming`) | 0.15 ms |
| The coordinator, its commit's state included | 0.09 ms |
| The run's own task (`attempt::launch`, `TableView::new`, `Naming::clean`) | 0.08 ms |

The first batch waits for the keys, the scan and two pool jobs before the lane can write it, and
each step is a hand-off between threads.

### Where the CPU goes

The engine takes 1.74 times the bare loop's user-mode CPU over the same runs: in the paired
profile, 61.5 % of the 19 384 samples are the engine's and 35.4 % the bare loop's (setup and
criterion the rest). The engine's samples by component, and each one's share of the CPU it takes
beyond the bare loop's:

| Component | Samples | Of the extra |
|---|---|---|
| The lane's writes (the sink's encode, `memcpy` 97 % of it) | 40.6 % (the bare loop's 35.4 %) | 19.7 % |
| The 64-bit integer scan (`exact::beyond`) | 10.6 % | 40.8 % |
| Pool jobs: cutting a piece and lowering it | 3.8 % | 14.7 % |
| Pool threads idle between jobs (rayon's spinning and `sched_yield`) | 2.4 % | 9.1 % |
| The partition's reads of the feed, its coalescing and its writes, but the above | 1.5 % | 5.6 % |
| Weighing the bytes written (`written_bytes`) | 1.3 % | 4.8 % |
| The source's reads and pushes | 0.6 % | 2.2 % |
| The coordinator and its commit | 0.3 % | 1.2 % |
| The lane's queue and loop, tokio's scheduling, the budget, the run's own task | 0.5 % | 1.9 % |

Over 200 runs alone the engine took 34.3 G user-mode cycles and 43.9 G instructions against
24.4 G and 12.0 G, the batches' generation included in both. The scan and the pool threads'
spinning hold most of the extra instructions; the scan runs on the partition's worker while the
lane writes, and leaves the lane's path but for the first batch.

### Scheduling and syscalls

A batch, over 200 runs of the engine (12 800 batches): 1.58 voluntary switches of the runtime's
workers and 0.05 involuntary, and 2.37 voluntary and 2.05 involuntary switches of the compute
threads. The bare loop's thread is switched out involuntarily 0.15 times a batch and never
waits. Syscalls a batch, the engine's, less a run of none: 20.2 `sched_yield` (the compute
threads waiting for work), 2.0 `epoll_wait` and 2.0 `write` (a worker parking and being woken), and
1.1 `futex`. The bare loop makes none a batch.

### The destination in a process of its own

`served/process/socket/destination/64x80000`, the host on CPUs 0–3, the sink spawned through `Local`
on 4–11. The bench's round under `perf record`: 1.201 CPU s a GB, the host's process 0.431 and the
destination's 0.778; the host's process 0.985 CPUs busy and the destination's 1.809. The run
report's counters, medians over 57 runs:

| Counter | A run |
|---|---|
| Run | 207.7 ms (184.6–222.6) |
| Lane writing (`RemoteDestination`'s writes) | 197.4 ms |
| Partitions blocked on the lane's full queue | 159.4 ms |
| Lane flushing | 6.4 ms |
| Commit phases: flush, destination commit | 14.8, 0.21 ms |
| Pool jobs; queued and running, summed | 128; 2.56 and 4.09 ms |

The lane writes for 95 % of the run, most of it waiting for the destination to take frames. The
host's engine work beside it is off that path: of the host's 7 816 samples, the lane holds 2 723,
the 64-bit integer scan 593 and the compute threads 422. Another 2 927, outside the timed runs,
hash the connector's binary as each run spawns it (`Binary::digest`; the bench's binary is 116 MB),
which is also the run's 3 552 `pread64` calls. The destination's processes hold 11 021 samples:
receiving and decoding frames 46 %, the sink's writes (the bare loop's work) 24 %, Arrow's
validation of what arrives (`validate_values`, `from_utf8`, `check_bounds`) 15 %, and `realloc` 7 %.
Syscalls a run, both processes, spawning included: 2 343 `recvfrom`, 1 556 `writev`, 8 828
`epoll_wait`, 4 094 `sched_yield` and 2 359 `futex`.

### Reading

- The run is lane-bound in both layouts. In process, the lane's writes are the bare loop's
  encode, 9–13 % slower; served, they wait on the destination's process.
- At 64 batches, about a third of the overhead is paid once a run: planning, the first batch's way
  through the partition to the lane, and the commit. It falls as runs move more batches: 12–14 %
  at 128.
- About two thirds is the lane's writes taking 9–12 % longer a batch. The metadata columns every
  table stores account for 3.4 points; the rest goes with the lane's workers running at a clock
  6–11 % lower than the bare loop's thread, of which other cores kept busy account for 2–4 points.
- The engine's extra CPU is mostly the 64-bit integer scan and the compute pool's jobs and idle
  spinning; both run beside the lane rather than on its path, but for the first batch.
