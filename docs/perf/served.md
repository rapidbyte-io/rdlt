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
  The load after each round, 1.7–2.6, is the bench's own threads.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-08, commit `e5a8df88c235`, governor `powersave`
with energy preference `performance`, one-minute load average 0.26–0.97 before the rounds; the
median of five rounds and their range, and the merge base `f52c360f61b9` measured in the same
interleaved rounds. The passthrough record's in-process engine moves the same batches at about
15 000 MB/s on the same cores.

| Case | MB/s | CPU s a GB | Instructions a cycle / CPUs busy | Rounds within ±3 % | Merge base, MB/s and CPU s a GB |
|---|---|---|---|---|---|
| `socket/destination/64x80000` | 2 077 (1 978–2 130) | 0.78 (0.74–0.81) | 0.81 / 1.61 | 4 of 5 | 1 782, 0.90 |
| `socket/source/64x80000` | 1 779 (1 758–1 860) | 0.91 (0.88–0.93) | 0.45 / 1.63 | 3 of 5 | 1 742, 0.93 |
| `socket/both/64x80000` | 1 282 (1 219–1 325) | 1.54 (1.47–1.57) | 0.54 / 1.95 | 3 of 5 | 1 156, 1.68 |
| `socket/destination/512x10000` | 3 492 (3 441–3 550) | 0.59 (0.57–0.64) | 1.17 / 2.11 | 5 of 5 | 2 862, 0.75 |
| `socket/source/512x10000` | 2 825 (2 807–2 876) | 0.78 (0.76–0.79) | 0.81 / 2.20 | 5 of 5 | 2 782, 0.79 |
| `socket/both/512x10000` | 1 767 (1 742–1 787) | 1.30 (1.29–1.33) | 0.94 / 2.30 | 5 of 5 | 1 566, 1.47 |
| `tls/destination/64x80000` | 1 751 (1 729–1 848) | 0.97 (0.92–0.99) | 1.40 / 1.69 | 4 of 5 | 1 419, 1.18 |
| `tls/source/64x80000` | 1 549 (1 500–1 570) | 1.18 (1.17–1.21) | 1.02 / 1.82 | 4 of 5 | 1 536, 1.19 |
| `tls/both/64x80000` | 925 (911–940) | 2.09 (2.04–2.12) | 1.15 / 1.93 | 5 of 5 | 825, 2.30 |
| `tls/destination/512x10000` | 2 433 (2 399–2 480) | 0.92 (0.90–0.94) | 1.90 / 2.23 | 5 of 5 | 2 012, 1.09 |
| `tls/source/512x10000` | 1 967 (1 958–1 973) | 1.12 (1.12–1.14) | 1.40 / 2.21 | 5 of 5 | 1 942, 1.14 |
| `tls/both/512x10000` | 1 174 (1 162–1 181) | 1.92 (1.91–1.93) | 1.62 / 2.25 | 5 of 5 | 1 036, 2.14 |

Syscalls a run, large frames (64 batches), the median of the five rounds:

| Case | `writev` | `recvfrom` | A batch | Merge base, a batch |
|---|---|---|---|---|
| `socket/destination` | 4 185 | 2 994 | 65 + 47 | 67 + 48 |
| `socket/source` | 4 271 | 3 029 | 67 + 47 | 66 + 47 |
| `socket/both` | 8 813 | 6 147 | 138 + 96 | 137 + 96 |
| `tls/destination` | 7 703 | 28 807 | 120 + 450 | 120 + 455 |
| `tls/source` | 7 525 | 28 333 | 118 + 443 | 118 + 447 |
| `tls/both` | 15 303 | 56 937 | 239 + 890 | 238 + 894 |

Small frames make about as many syscalls for the same bytes (4 758 `writev` and 3 374 `recvfrom`
for the served destination over the socket).

- A served destination moves large frames 17 % faster over the socket and 23 % faster over mutual
  TLS than the merge base, and small frames 21–22 % faster, at 13–21 % less CPU a GB: its
  connector decodes each write's frame from the bytes the transport's bounded body passes on,
  and the host sends each frame's body as the bytes the encoder made, so neither end copies it
  into or out of a gRPC buffer.
- Both served gain 11–13 % in every case, over mutual TLS too (925 against 825 MB/s in large
  frames): the write's half of their copies is gone, the read's remains.
- A served source is unchanged: its frames still go through the generated read.
- The syscalls a batch barely move (65 + 47 against 67 + 48 for the served destination over the
  socket): the write's frames go out in the same HTTP/2 frames.
- Mutual TLS costs 13–34 % of a mode's throughput over the socket, and 24–56 % more CPU a GB.
- Serving both connectors costs close to the sum of serving each in CPU a GB.

The review's measurements (ARCH_REVIEW.md §3.4) ran the same batches on the eight efficient cores
with a runtime of a worker for each of those cores beside a pool of four threads, the median of
seven runs, at the credit, HTTP/2 settings and generated data plane of their day:

| Mode | Review, efficient cores | Here, performance cores |
|---|---|---|
| Destination over the socket | 1 113 MB/s, 1.39 CPU s a GB | 2 077 MB/s, 0.78 |
| Source over the socket | 1 128 MB/s, 1.30 | 1 779 MB/s, 0.91 |
| Both over the socket | 953 MB/s, 2.92 | 1 282 MB/s, 1.54 |
| Destination over mutual TLS | 761 MB/s, 2.04 | 1 751 MB/s, 0.97 |
| Both over mutual TLS | 689 MB/s, 4.38 | 925 MB/s, 2.09 |
| `writev` + `recvfrom` a batch, destination over the socket | 457 + 891 | 65 + 47 |

The cores, their count, the layout and the flow control differ, so only the shape compares.
