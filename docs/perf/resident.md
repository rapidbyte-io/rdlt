# Resident memory

What a run holds resident beyond its inputs, against the limit its budget gives it (ADR 0039).

## Method

- **Running it.** `just resident` builds the engine's `resident` bench and the host's `served`
  bench and runs each workload once for each core count and malloc setting, each in a process of
  its own.
- **Workloads.** `passthrough` moves 64 batches of the passthrough workload (80,000 rows of ten
  mixed columns, about 7 MB each) to the null sink. `normalized` loads 200,000 roots in pushes of
  50,000 rows and normalizes them. `served` (`served --resident <cores>`) moves the same 64
  batches, read from 16 partitions, an even share each, through a host that spawned both
  connectors over a socket pair, as production runs them: the replay source makes its batches in
  its own process, and the IPC sink, which declares one writer, writes them in its own.
- **Budget.** 256 MiB for every run.
- **Cores.** 2, 4 and `nproc`, 16 on this machine.
- **Malloc.** Once with glibc's defaults, and once with the tunables a spawned connector gets,
  `glibc.malloc.trim_threshold=268435456` and `glibc.malloc.mmap_threshold=33554432`
  (`/tunables` in a line's name). The setting is the bench's, the host's in `served`; the host
  starts every connector it spawns with the tunables.
- **Growth.** The inputs are made first, then the process's peak resident memory is reset
  (`/proc/self/clear_refs`) and its resident memory read; the run then raises the peak to
  `VmHWM`. Growth is that peak less the resident memory at the reset.
- **Limit.** The budget, a fifth of it and 32 MiB, plus the largest push the run holds
  unadmitted, plus 2 MiB of stack a thread (the runtime's workers, the compute threads and the
  main thread). `served`'s host holds a frame unadmitted a partition, and what its connectors'
  open calls hold in transport besides, a stream window of 4 MiB (`CREDIT_FLOOR`) a call: a read
  a partition and the sink's one writer. A run over its limit exits with 1.
- **Connector processes.** In `served`, each spawned connector's peak resident memory (`VmHWM`)
  is read before it stops: a process spawned for the run, so the peak is all it held, the
  source's batches among it. They are reported apart, and not held to a limit: the budget bounds
  the host.
- **peak_memory.** The budget the run's meter reached at its peak, from the engine's report.
- **Figures.** The bench prints whole MiB, so a ratio is of the printed figures.

## Results

`just resident` on this change over `fbef9f4b`, 2026-10-24, one run of each line, not timed. Intel
Core Ultra X7 358H, 16 CPUs, Linux 7.2.5, Rust 1.98.1. Growth ÷ peak_memory is not read for
`served`, whose host holds its frames and transport beside the budget.

| Workload | Cores | Malloc | peak_memory | Growth | Limit | Growth ÷ limit | Growth ÷ peak_memory |
|---|---|---|---|---|---|---|---|
| passthrough | 2 | defaults | 86 MiB | 12 MiB | 355 MiB | 3.4 % | 14.0 % |
| passthrough | 2 | tunables | 86 MiB | 10 MiB | 355 MiB | 2.8 % | 11.6 % |
| passthrough | 4 | defaults | 79 MiB | 12 MiB | 359 MiB | 3.3 % | 15.2 % |
| passthrough | 4 | tunables | 79 MiB | 10 MiB | 359 MiB | 2.8 % | 12.7 % |
| passthrough | 16 | defaults | 86 MiB | 10 MiB | 395 MiB | 2.5 % | 11.6 % |
| passthrough | 16 | tunables | 86 MiB | 11 MiB | 395 MiB | 2.8 % | 12.8 % |
| normalized | 2 | defaults | 144 MiB | 19 MiB | 358 MiB | 5.3 % | 13.2 % |
| normalized | 2 | tunables | 144 MiB | 22 MiB | 358 MiB | 6.1 % | 15.3 % |
| normalized | 4 | defaults | 144 MiB | 20 MiB | 362 MiB | 5.5 % | 13.9 % |
| normalized | 4 | tunables | 144 MiB | 29 MiB | 362 MiB | 8.0 % | 20.1 % |
| normalized | 16 | defaults | 144 MiB | 22 MiB | 398 MiB | 5.5 % | 15.3 % |
| normalized | 16 | tunables | 144 MiB | 35 MiB | 398 MiB | 8.8 % | 24.3 % |
| served | 2 | defaults | 94 MiB | 234 MiB | 522 MiB | 44.8 % | |
| served | 2 | tunables | 87 MiB | 243 MiB | 522 MiB | 46.6 % | |
| served | 4 | defaults | 94 MiB | 247 MiB | 526 MiB | 47.0 % | |
| served | 4 | tunables | 94 MiB | 219 MiB | 526 MiB | 41.6 % | |
| served | 16 | defaults | 94 MiB | 322 MiB | 562 MiB | 57.3 % | |
| served | 16 | tunables | 101 MiB | 359 MiB | 562 MiB | 63.9 % | |

The spawned connector processes' peaks, in the same runs:

| Cores | Malloc | Source | Destination |
|---|---|---|---|
| 2 | defaults | 831 MiB | 86 MiB |
| 2 | tunables | 840 MiB | 80 MiB |
| 4 | defaults | 823 MiB | 89 MiB |
| 4 | tunables | 821 MiB | 66 MiB |
| 16 | defaults | 776 MiB | 54 MiB |
| 16 | tunables | 833 MiB | 83 MiB |

- **Every run's growth is within its limit**: in process from 2.5 % to 8.8 % of it, the largest
  `normalized` on 16 cores with the tunables (35 MiB of 398 MiB); `served` from 41.6 % to 63.9 %,
  the largest on 16 cores with the tunables (359 MiB of 562 MiB).
- **The served host holds 125 to 258 MiB beyond the budget its meter reached**, 49 % to 101 % of
  the 256 MiB budget: the 16 frames its partitions hold before they are admitted (about 7 MB
  each) and its transport. One run a line moves by tens of MiB: an earlier run of the same lines,
  over `41284517`, read 205 to 401 MiB of growth, 16 cores with the tunables the most.
- **The spawned source peaks at 776 to 840 MiB**, the 64 batches it makes (about 450 MB) among
  it; the spawned destination at 54 to 89 MiB.
- **Pieces' allocation sets of live addresses** (this change): what holds a unit keeps its
  batches until its last piece is written, and a normalized piece holds its split's parts until
  their last piece is, both within what they are charged. Against the merge base `41284517`, two
  rounds of each in-process line interleaved, base and branch alike: `normalized` grows 1 to 5
  MiB more without the tunables (15 to 20 against 19 to 23 MiB) and up to 8 MiB more with them
  (16 to 32 against 21 to 34 MiB), at the same peak_memory, 144 MiB; `passthrough` is unchanged
  (10 to 13 against 11 to 13 MiB). A unit's batches now go with the pieces cut from them, and the
  served host grows 52 to 75 MiB less for it ([served.md](served.md), "A unit's batches freed as
  their pieces go").
