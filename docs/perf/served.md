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
- **CPU.** The process's user and system time over each sample's runs (`getrusage`), the served
  connectors' included, a GB moved; the median and range of the samples' medians over the rounds.
- **Syscalls.** One run of each case in criterion's test mode under `strace -f -c`, its
  connection's setup included, both ends' threads counted.
- **Running.** `just bench served 0-3`, five rounds interleaved with five of the merge base, each
  started at a one-minute load average under 1.0 under the measurement lock; release profile.
  The load after each round, 1.7–3.2, is the bench's own threads.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-07, commit `9b7a0ec2bc41`, governor `powersave`
with energy preference `performance`, one-minute load average 0.27–0.99 before the rounds; the
median of five rounds and their range, and the merge base `7aea7ae0a0ed` measured in the same
interleaved rounds. The passthrough record's in-process engine moves the same batches at about
15 000 MB/s on the same cores.

| Case | MB/s | CPU s a GB | Instructions a cycle / CPUs busy | Rounds within ±3 % | Merge base, MB/s and CPU s a GB |
|---|---|---|---|---|---|
| `socket/destination/64x80000` | 1 814 (1 790–1 831) | 0.87 (0.86–0.90) | 0.56 / 1.61 | 5 of 5 | 1 439, 0.95 |
| `socket/source/64x80000` | 1 801 (1 752–1 875) | 0.91 (0.89–0.92) | 0.45 / 1.66 | 4 of 5 | 1 432, 1.00 |
| `socket/both/64x80000` | 1 095 (1 089–1 148) | 1.74 (1.70–1.77) | 0.45 / 1.92 | 4 of 5 | 1 028, 1.85 |
| `socket/destination/512x10000` | 2 892 (2 878–2 905) | 0.76 (0.76–0.76) | 0.99 / 2.16 | 5 of 5 | 2 123, 1.00 |
| `socket/source/512x10000` | 2 823 (2 729–2 840) | 0.78 (0.78–0.79) | 0.82 / 2.20 | 4 of 5 | 2 366, 0.95 |
| `socket/both/512x10000` | 1 573 (1 556–1 585) | 1.46 (1.46–1.47) | 0.79 / 2.30 | 5 of 5 | 1 275, 1.79 |
| `tls/destination/64x80000` | 1 432 (1 359–1 446) | 1.15 (1.14–1.17) | 1.11 / 1.69 | 3 of 5 | 1 039, 1.17 |
| `tls/source/64x80000` | 1 540 (1 517–1 567) | 1.16 (1.14–1.19) | 1.01 / 1.83 | 5 of 5 | 1 181, 1.21 |
| `tls/both/64x80000` | 817 (785–826) | 2.26 (2.20–2.32) | 1.02 / 1.93 | 4 of 5 | 786, 2.33 |
| `tls/destination/512x10000` | 1 960 (1 941–2 002) | 1.08 (1.05–1.09) | 1.57 / 2.17 | 5 of 5 | 1 653, 1.29 |
| `tls/source/512x10000` | 1 985 (1 944–1 993) | 1.10 (1.09–1.11) | 1.39 / 2.22 | 5 of 5 | 1 715, 1.26 |
| `tls/both/512x10000` | 1 056 (1 049–1 067) | 2.05 (2.02–2.07) | 1.42 / 2.22 | 5 of 5 | 945, 2.28 |

Syscalls a run, large frames (64 batches), the median of the five rounds:

| Case | `writev` | `recvfrom` | A batch | Merge base, a batch |
|---|---|---|---|---|
| `socket/destination` | 4 243 | 3 026 | 66 + 47 | 467 + 899 |
| `socket/source` | 4 119 | 2 977 | 64 + 47 | 453 + 864 |
| `socket/both` | 8 698 | 6 113 | 136 + 96 | 953 + 1 813 |
| `tls/destination` | 7 760 | 29 532 | 121 + 461 | 446 + 474 |
| `tls/source` | 7 561 | 28 725 | 118 + 449 | 434 + 452 |
| `tls/both` | 15 407 | 57 802 | 241 + 903 | 879 + 924 |

Small frames make about as many syscalls for the same bytes (4 065 `writev` and 3 124 `recvfrom`
for the served destination over the socket).

- A served destination or source over the socket moves large frames 26 % faster than the merge
  base, and small frames 19–36 % faster, at less CPU a GB in every case: a frame of 7 MB no
  longer waits for the one before it to be taken, and HTTP/2 carries it in frames of 1 MiB
  rather than 16 KiB.
- Over mutual TLS one served end gains 16–38 %. Both served over mutual TLS in large frames stays
  within the merge base's range (817 against 786 MB/s).
- Over mutual TLS the reads a batch are unchanged (461 `recvfrom` against 474): larger HTTP/2
  frames cut only the writes.
- Mutual TLS costs 14–25 % of a mode's throughput over the socket in large frames, and about 30 %
  more CPU a GB.
- Serving both connectors costs close to the sum of serving each in CPU a GB.

On the eight efficient cores (`just bench served 4-11`: 4 runtime workers and 4 compute threads),
one round of each, so a figure here is one round's, not a median:

| Case | MB/s, CPU s a GB | Merge base | Change |
|---|---|---|---|
| `socket/destination/64x80000` | 1 265, 1.35 | 1 166, 1.38 | +8 % |
| `socket/source/64x80000` | 1 569, 1.45 | 1 122, 1.32 | +40 % |
| `socket/both/64x80000` | 984, 2.93 | 929, 2.88 | +6 % |
| `socket/destination/512x10000` | 2 693, 1.17 | 2 295, 1.45 | +17 % |
| `socket/source/512x10000` | 2 325, 1.32 | 2 154, 1.56 | +8 % |
| `socket/both/512x10000` | 1 882, 2.11 | 1 473, 2.65 | +28 % |
| `tls/destination/64x80000` | 939, 1.85 | 782, 1.94 | +20 % |
| `tls/source/64x80000` | 1 308, 1.85 | 835, 1.87 | +57 % |
| `tls/both/64x80000` | 753, 3.94 | 685, 3.96 | +10 % |
| `tls/destination/512x10000` | 1 551, 1.68 | 1 398, 2.00 | +11 % |
| `tls/source/512x10000` | 1 549, 1.85 | 1 634, 1.99 | −5 % |
| `tls/both/512x10000` | 1 086, 3.32 | 971, 3.73 | +12 % |

The review's measurements (ARCH_REVIEW.md §3.4) ran the same batches on the eight efficient cores
with a runtime of a worker for each of those cores beside a pool of four threads, the median of
seven runs, at the credit and HTTP/2 settings of the merge base:

| Mode | Review, efficient cores | Here, performance cores |
|---|---|---|
| Destination over the socket | 1 113 MB/s, 1.39 CPU s a GB | 1 814 MB/s, 0.87 |
| Source over the socket | 1 128 MB/s, 1.30 | 1 801 MB/s, 0.91 |
| Both over the socket | 953 MB/s, 2.92 | 1 095 MB/s, 1.74 |
| Destination over mutual TLS | 761 MB/s, 2.04 | 1 432 MB/s, 1.15 |
| Both over mutual TLS | 689 MB/s, 4.38 | 817 MB/s, 2.26 |
| `writev` + `recvfrom` a batch, destination over the socket | 457 + 891 | 66 + 47 |

The cores, their count, the layout and the flow control differ, so only the shape compares.
