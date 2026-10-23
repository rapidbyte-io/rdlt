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

This change on `7d3604e0`, 2026-10-12, one run of each line, not timed. Intel Core Ultra X7 358H, 16
CPUs, Linux 7.2.5, Rust 1.98.1.

| Workload | Cores | Malloc | peak_memory | Growth | Limit | Growth ÷ limit | Growth ÷ peak_memory |
|---|---|---|---|---|---|---|---|
| passthrough | 2 | defaults | 86 MiB | 12 MiB | 353 MiB | 3.4 % | 14.0 % |
| passthrough | 2 | tunables | 86 MiB | 10 MiB | 353 MiB | 2.8 % | 11.6 % |
| passthrough | 4 | defaults | 86 MiB | 11 MiB | 355 MiB | 3.1 % | 12.8 % |
| passthrough | 4 | tunables | 86 MiB | 12 MiB | 355 MiB | 3.4 % | 14.0 % |
| passthrough | 16 | defaults | 86 MiB | 11 MiB | 379 MiB | 2.9 % | 12.8 % |
| passthrough | 16 | tunables | 86 MiB | 12 MiB | 379 MiB | 3.2 % | 14.0 % |
| normalized | 2 | defaults | 145 MiB | 15 MiB | 356 MiB | 4.2 % | 10.3 % |
| normalized | 2 | tunables | 145 MiB | 15 MiB | 356 MiB | 4.2 % | 10.3 % |
| normalized | 4 | defaults | 145 MiB | 15 MiB | 358 MiB | 4.2 % | 10.3 % |
| normalized | 4 | tunables | 145 MiB | 17 MiB | 358 MiB | 4.7 % | 11.7 % |
| normalized | 16 | defaults | 145 MiB | 18 MiB | 382 MiB | 4.7 % | 12.4 % |
| normalized | 16 | tunables | 145 MiB | 31 MiB | 382 MiB | 8.1 % | 21.4 % |

Every run's growth is within its limit: from 2.8 % to 8.1 % of it, the largest being
`normalized` on 16 cores with the tunables (31 MiB of 382 MiB). Growth is 10.3 % to 21.4 % of
the budget the meter reached at its peak.
