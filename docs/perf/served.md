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
  sizes at the default read window (4 MiB), and a served source over the socket in large frames
  at read windows of 8 and 16 MiB.
- **Engine.** Within the four cores (2 runtime workers and 2 compute threads), a budget of 1 GiB,
  one commit. Only the run is timed, not its connections; every run checks every row arrives.
- **CPU.** The process's user and system time over each sample's runs (`getrusage`), the served
  connectors' included, a GB moved; the median and range of the samples' medians over the rounds.
- **Syscalls.** One run of each case in criterion's test mode under `strace -f -c`, its
  connection's setup included.
- **Running.** `just bench served 0-3`, five rounds, each started at a one-minute load average
  under 1.0 under the measurement lock; release profile.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-07, commit `28726685db15`, governor `powersave`
with energy preference `performance`, one-minute load average 0.87–1.22 before the runs; the
median of five rounds and their range. The passthrough record's in-process engine moves the same
batches at about 15 000 MB/s on the same cores.

| Case | MB/s | CPU s a GB | Instructions a cycle / CPUs busy | Rounds within ±3 % |
|---|---|---|---|---|
| `socket/destination/64x80000/4MiB` | 1 437 (1 395–1 447) | 0.95 (0.94–0.99) | 0.69 / 1.37 | 4 of 5 |
| `socket/source/64x80000/4MiB` | 1 444 (1 322–1 467) | 1.01 (0.99–1.06) | 0.64 / 1.41 | 4 of 5 |
| `socket/both/64x80000/4MiB` | 1 026 (959–1 085) | 1.87 (1.80–1.99) | 0.57 / 1.91 | 3 of 5 |
| `socket/destination/512x10000/4MiB` | 2 129 (2 041–2 192) | 1.01 (0.97–1.03) | 0.94 / 2.07 | 4 of 5 |
| `socket/source/512x10000/4MiB` | 2 320 (2 306–2 435) | 0.96 (0.92–0.98) | 0.85 / 2.22 | 3 of 5 |
| `socket/both/512x10000/4MiB` | 1 253 (1 190–1 305) | 1.82 (1.75–1.92) | 0.84 / 2.26 | 3 of 5 |
| `tls/destination/64x80000/4MiB` | 1 019 (994–1 044) | 1.20 (1.17–1.25) | 1.24 / 1.29 | 5 of 5 |
| `tls/source/64x80000/4MiB` | 1 154 (1 012–1 204) | 1.23 (1.18–1.30) | 1.25 / 1.51 | 3 of 5 |
| `tls/both/64x80000/4MiB` | 758 (593–782) | 2.39 (2.33–3.07) | 1.15 / 1.93 | 3 of 5 |
| `tls/destination/512x10000/4MiB` | 1 638 (1 362–1 700) | 1.30 (1.25–1.59) | 1.53 / 2.17 | 3 of 5 |
| `tls/source/512x10000/4MiB` | 1 671 (1 248–1 753) | 1.27 (1.23–1.52) | 1.43 / 2.23 | 3 of 5 |
| `tls/both/512x10000/4MiB` | 938 (840–963) | 2.33 (2.23–2.58) | 1.45 / 2.25 | 4 of 5 |
| `socket/source/64x80000/8MiB` | 1 627 (1 506–1 716) | 1.03 (1.00–1.10) | 0.55 / 1.68 | 3 of 5 |
| `socket/source/64x80000/16MiB` | 1 613 (1 560–1 689) | 1.04 (1.01–1.05) | 0.55 / 1.66 | 2 of 5 |
| `socket/both/64x80000/8MiB` | 943 (906–998) | 1.98 (1.90–2.04) | 0.54 / 1.91 | 3 of 5 |
| `socket/both/64x80000/16MiB` | 940 (878–955) | 1.98 (1.95–2.08) | 0.54 / 1.87 | 4 of 5 |

Syscalls a run, large frames, the default window (64 batches):

| Case | `writev` | `recvfrom` | A batch |
|---|---|---|---|
| `socket/destination` | 29 915 | 57 512 | 467 + 899 |
| `socket/source` | 28 993 | 55 338 | 453 + 865 |
| `socket/both` | 60 995 | 116 032 | 953 + 1 813 |
| `tls/destination` | 28 557 | 30 465 | 446 + 476 |
| `tls/source` | 27 814 | 29 031 | 435 + 454 |
| `tls/both` | 56 231 | 59 108 | 879 + 924 |

Small frames make as many syscalls for the same bytes (31 022 `writev` and 57 192 `recvfrom` for
the served destination over the socket).

- Mutual TLS costs 20–30 % of a mode's throughput over the socket in large frames, and a quarter
  more CPU a GB.
- Frames of an eighth the size move 1.5–1.6 times the bytes a second for a served destination or
  source, at about the same CPU a GB.
- A larger read window speeds a served source in large frames by 13 % (8 MiB) and no more at
  16 MiB, and slows both served by 8–9 %.
- Serving both connectors costs close to the sum of serving each in CPU a GB.

The review's measurements (ARCH_REVIEW.md §3.4) ran the same batches on the eight efficient cores
with a runtime of a worker for each of those cores beside a pool of four threads, the median of
seven runs:

| Mode | Review, efficient cores | Here, performance cores |
|---|---|---|
| Destination over the socket | 1 113 MB/s, 1.39 CPU s a GB | 1 437 MB/s, 0.95 |
| Source over the socket | 1 128 MB/s, 1.30 | 1 444 MB/s, 1.01 |
| Both over the socket | 953 MB/s, 2.92 | 1 026 MB/s, 1.87 |
| Destination over mutual TLS | 761 MB/s, 2.04 | 1 019 MB/s, 1.20 |
| Both over mutual TLS | 689 MB/s, 4.38 | 758 MB/s, 2.39 |
| Read window 8 MiB, source over the socket | 1 171 → 1 559 MB/s (+33 %) | 1 444 → 1 627 MB/s (+13 %) |
| `writev` + `recvfrom` a batch, destination over the socket | 457 + 891 | 467 + 899 |

The cores, their count and the layout differ, so only the shape compares: the syscalls a batch
match, and the window's gain is smaller here.
