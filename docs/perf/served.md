# Served connectors

Untrusted connectors run out of process, served over the wire protocol: over a socket pair to a
connector the host spawned, or over mutual TLS to one listening on the network. This record keeps
what serving the destination, the source or both costs in throughput and in CPU, against the
in-process passthrough, over both transports.

## Method

- **Batches.** The passthrough bench's ten mixed columns, 441 MB a run: 64 batches of 80 000 rows
  (6.9 MB each, the frames the default coalescing target makes) or 512 of 10 000 (0.86 MB each),
  a checkpoint after each, so each batch is one frame.
- **Connectors.** The replay source and the IPC sink of the `bench` feature, in process or served
  from the bench's own process:
  - `socket`: over a `UnixStream` pair, as `rdlt-host`'s integration tests serve connectors (the
    bench takes their `served.rs`);
  - `tls`: over mutual TLS on loopback, through a listener with certificates from
    `rdlt_testkit::tls`, reached through `Remote`.
- **Cases.** Each of `destination`, `source` and `both` served over each transport in both frame
  sizes, at the credit the protocol grants: a window that opens at 4 MiB and grows to two of the
  largest frames taken, over HTTP/2 stream windows of 4 MiB and transport frames of 1 MiB.
- **Engine.** Within the four cores (2 runtime workers and 2 compute threads), a budget of 1 GiB,
  one commit. Only the run is timed, not its connections; every run checks every row arrives.
- **CPU.** The CPUs `perf stat` counts busy over each case's runs, every thread of the process,
  the served connectors' included, a GB moved; the median and range over the rounds.
- **Syscalls.** One run of each case in criterion's test mode under `strace -f -c`, its
  connection's setup included, both ends' threads counted.
- **Running.** `just bench served 0-3`, five rounds interleaved with five of the merge base, each
  started at a one-minute load average under 1.0 under the measurement lock; release profile.
  The load after each round, 2.0–2.8, is the bench's own threads.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-08, commit `188f9903e76b`, governor `powersave`
with energy preference `performance`, one-minute load average 0.15–0.99 before the rounds; the
median of five rounds and their range, and the merge base `ebb7c848edd7` measured in the same
interleaved rounds. The passthrough record's in-process engine moves the same batches at about
15 000 MB/s on the same cores.

| Case | MB/s | CPU s a GB | Instructions a cycle / CPUs busy | Rounds within ±3 % | Merge base, MB/s and CPU s a GB |
|---|---|---|---|---|---|
| `socket/destination/64x80000` | 2 055 (2 001–2 064) | 0.77 (0.77–0.79) | 0.73 / 1.58 | 5 of 5 | 2 055, 0.76 |
| `socket/source/64x80000` | 2 460 (2 412–2 949) | 0.71 (0.62–0.78) | 0.68 / 1.84 | 3 of 5 | 1 780, 0.91 |
| `socket/both/64x80000` | 1 362 (1 332–1 424) | 1.40 (1.29–1.43) | 0.66 / 1.90 | 3 of 5 | 1 325, 1.47 |
| `socket/destination/512x10000` | 3 529 (3 497–3 539) | 0.58 (0.56–0.64) | 1.17 / 2.07 | 5 of 5 | 3 521, 0.60 |
| `socket/source/512x10000` | 3 548 (3 486–3 600) | 0.65 (0.64–0.66) | 1.07 / 2.32 | 5 of 5 | 2 886, 0.76 |
| `socket/both/512x10000` | 2 033 (2 025–2 071) | 1.11 (1.10–1.13) | 1.13 / 2.28 | 5 of 5 | 1 761, 1.30 |
| `tls/destination/64x80000` | 1 713 (1 643–1 764) | 1.00 (0.98–1.00) | 1.39 / 1.71 | 3 of 5 | 1 703, 0.99 |
| `tls/source/64x80000` | 1 983 (1 852–1 999) | 0.92 (0.90–0.98) | 1.29 / 1.82 | 4 of 5 | 1 542, 1.18 |
| `tls/both/64x80000` | 953 (895–998) | 2.01 (1.95–2.12) | 1.30 / 1.92 | 3 of 5 | 904, 2.11 |
| `tls/destination/512x10000` | 2 410 (2 376–2 442) | 0.92 (0.90–0.95) | 1.87 / 2.21 | 5 of 5 | 2 415, 0.90 |
| `tls/source/512x10000` | 2 267 (2 188–2 332) | 0.97 (0.94–0.99) | 1.69 / 2.19 | 4 of 5 | 1 996, 1.11 |
| `tls/both/512x10000` | 1 301 (1 278–1 315) | 1.74 (1.74–1.77) | 1.78 / 2.27 | 5 of 5 | 1 192, 1.87 |

Syscalls a run, large frames (64 batches), the median of the five rounds:

| Case | `writev` | `recvfrom` | A batch | Merge base, a batch |
|---|---|---|---|---|
| `socket/destination` | 4 143 | 2 949 | 65 + 46 | 66 + 47 |
| `socket/source` | 4 344 | 2 998 | 68 + 47 | 65 + 47 |
| `socket/both` | 8 765 | 6 113 | 137 + 96 | 138 + 96 |
| `tls/destination` | 7 705 | 28 852 | 120 + 451 | 120 + 451 |
| `tls/source` | 7 632 | 28 407 | 119 + 444 | 118 + 441 |
| `tls/both` | 15 371 | 56 824 | 240 + 888 | 239 + 889 |

Small frames make about as many syscalls for the same bytes (4 745 `writev` and 3 344 `recvfrom`
for the served destination over the socket).

- A served source moves large frames 38 % faster over the socket and 29 % faster over mutual
  TLS than the merge base, and small frames 23 % and 14 % faster, at 13–22 % less CPU a GB:
  the host decodes each read's frame from the bytes its bounded body passes on, and the
  connector sends each frame's body as the bytes the encoder made, so neither end copies it into
  or out of a gRPC buffer.
- Both served gain 16 % over the socket and 9 % over mutual TLS in small frames, and 3 % and 5 %
  in large frames, where the rounds overlap the merge base's (953 against 904 MB/s over mutual
  TLS, at 2.01 against 2.11 CPU s a GB).
- A served destination is unchanged: its write was already on the data plane.
- The syscalls are unchanged: the read's frames go out in the same HTTP/2 frames.
- Mutual TLS costs 17–36 % of a mode's throughput over the socket, and 30–59 % more CPU a GB.
- Serving both connectors costs close to the sum of serving each in CPU a GB.

The review's measurements (ARCH_REVIEW.md §3.4) ran the same batches on the eight efficient cores
with a runtime of a worker for each of those cores beside a pool of four threads, the median of
seven runs, at the credit, HTTP/2 settings and generated data plane of their day:

| Mode | Review, efficient cores | Here, performance cores |
|---|---|---|
| Destination over the socket | 1 113 MB/s, 1.39 CPU s a GB | 2 055 MB/s, 0.77 |
| Source over the socket | 1 128 MB/s, 1.30 | 2 460 MB/s, 0.71 |
| Both over the socket | 953 MB/s, 2.92 | 1 362 MB/s, 1.40 |
| Destination over mutual TLS | 761 MB/s, 2.04 | 1 713 MB/s, 1.00 |
| Both over mutual TLS | 689 MB/s, 4.38 | 953 MB/s, 2.01 |
| `writev` + `recvfrom` a batch, destination over the socket | 457 + 891 | 65 + 46 |

The cores, their count, the layout and the flow control differ, so only the shape compares.
