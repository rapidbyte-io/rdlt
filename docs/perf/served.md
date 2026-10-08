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
- **Syscalls.** One run of each case in each round, in criterion's test mode under
  `strace -f -c`, its connection's setup included, both ends' threads counted; the median over
  the rounds.
- **Running.** `just bench served 0-3`, five rounds interleaved with five of the alternative
  below, each started at a one-minute load average under 1.0 under the measurement lock; release
  profile. The load after each round, 1.9–3.4, is the bench's own threads, but for one round at
  5.9.

## Results

Intel Core Ultra X7 358H, mains power, 2026-10-08, commit `799c06f3749d`, governor `powersave`
with energy preference `performance`, one-minute load average 0.65–0.95 before the rounds; the
median of five rounds and their range. The passthrough record's in-process engine moves the same
batches at about 15 000 MB/s on the same cores.

| Case | MB/s | CPU s a GB | Instructions a cycle / CPUs busy | Rounds within ±3 % |
|---|---|---|---|---|
| `socket/destination/64x80000` | 2 017 (1 915–2 121) | 0.79 (0.75–0.84) | 0.79 / 1.60 | 2 of 5 |
| `socket/source/64x80000` | 2 728 (2 396–2 826) | 0.69 (0.65–0.76) | 0.66 / 1.83 | 2 of 5 |
| `socket/both/64x80000` | 1 382 (1 220–1 514) | 1.39 (1.24–1.52) | 0.63 / 1.91 | 3 of 5 |
| `socket/destination/512x10000` | 3 447 (3 173–3 501) | 0.62 (0.61–0.69) | 1.19 / 2.13 | 3 of 5 |
| `socket/source/512x10000` | 3 501 (3 124–3 527) | 0.66 (0.65–0.72) | 1.06 / 2.30 | 3 of 5 |
| `socket/both/512x10000` | 2 002 (1 883–2 026) | 1.15 (1.13–1.21) | 1.13 / 2.28 | 3 of 5 |
| `tls/destination/64x80000` | 1 673 (1 535–1 730) | 1.05 (1.00–1.08) | 1.43 / 1.73 | 3 of 5 |
| `tls/source/64x80000` | 1 891 (1 794–1 985) | 0.93 (0.89–1.03) | 1.26 / 1.76 | 3 of 5 |
| `tls/both/64x80000` | 930 (856–958) | 2.04 (2.00–2.24) | 1.33 / 1.92 | 2 of 5 |
| `tls/destination/512x10000` | 2 354 (2 204–2 402) | 0.94 (0.87–1.00) | 1.90 / 2.21 | 3 of 5 |
| `tls/source/512x10000` | 2 278 (1 946–2 324) | 0.97 (0.94–1.13) | 1.69 / 2.20 | 3 of 5 |
| `tls/both/512x10000` | 1 275 (1 196–1 311) | 1.79 (1.73–1.86) | 1.79 / 2.28 | 4 of 5 |

Syscalls a batch, large frames (64 batches), the median of the five rounds:

| Case | `writev` + `recvfrom` a batch |
|---|---|
| `socket/destination` | 65 + 46 |
| `socket/source` | 67 + 47 |
| `socket/both` | 136 + 94 |
| `tls/destination` | 120 + 450 |
| `tls/source` | 119 + 443 |
| `tls/both` | 240 + 889 |

Small frames make about as many syscalls for the same bytes (9 `writev` and 7 `recvfrom` a batch
of 0.86 MB for the served destination over the socket).

- Mutual TLS costs 17–36 % of a mode's throughput over the socket, and 33–56 % more CPU a GB.
- Serving both connectors costs close to the sum of serving each in CPU a GB.
- Large frames over the socket spread widely between rounds: two or three of five fall within
  3 % of the median.

## A message's room grows as it arrives

A bounded body holds a message arriving in room that doubles, and goes straight to the message's
end once within twice what it holds, so a 6.9 MB frame arriving in 1 MiB transport frames is
reallocated about three times. The alternative reserves the whole message once its length is
secured, by room in the connection's window or by a charge of what a message of its length may
hold decoded, and never before: a five-byte prefix reserves nothing until then. It was measured
in the same interleaved rounds as the results above.

The rule set before measuring adopts it only where `socket/destination/64x80000` and
`socket/source/64x80000` both gain with the ranges of the five rounds apart, a counting test shows
one allocation and no reallocation a message, and budget memory waits are no higher. The
destination gains beyond noise; the source does not, so the room keeps growing.

| Case | Growing, MB/s | Reserved once, MB/s | Median | Ranges apart | CPU s a GB |
|---|---|---|---|---|---|
| `socket/destination/64x80000` | 2 017 (1 915–2 121) | 2 217 (2 183–2 350) | +10.0 % | yes | 0.79 → 0.73 |
| `socket/source/64x80000` | 2 728 (2 396–2 826) | 2 795 (2 751–2 949) | +2.4 % | no | 0.69 → 0.64 |
| `socket/both/64x80000` | 1 382 (1 220–1 514) | 1 455 (1 377–1 467) | +5.3 % | no | 1.39 → 1.33 |
| `tls/destination/64x80000` | 1 673 (1 535–1 730) | 1 734 (1 498–1 810) | +3.7 % | no | 1.05 → 1.00 |
| `tls/source/64x80000` | 1 891 (1 794–1 985) | 1 996 (1 814–2 042) | +5.5 % | no | 0.93 → 0.91 |
| `tls/both/64x80000` | 930 (856–958) | 971 (841–1 026) | +4.4 % | no | 2.04 → 1.97 |
| `socket/destination/512x10000` | 3 447 (3 173–3 501) | 3 449 (3 342–3 477) | +0.0 % | no | 0.62 → 0.65 |
| `socket/source/512x10000` | 3 501 (3 124–3 527) | 3 421 (2 746–3 454) | −2.3 % | no | 0.66 → 0.67 |
| `socket/both/512x10000` | 2 002 (1 883–2 026) | 1 952 (1 596–1 986) | −2.5 % | no | 1.15 → 1.18 |
| `tls/destination/512x10000` | 2 354 (2 204–2 402) | 2 343 (2 063–2 374) | −0.5 % | no | 0.94 → 0.95 |
| `tls/source/512x10000` | 2 278 (1 946–2 324) | 2 257 (2 198–2 267) | −0.9 % | no | 0.97 → 0.98 |
| `tls/both/512x10000` | 1 275 (1 196–1 311) | 1 253 (1 233–1 277) | −1.7 % | no | 1.79 → 1.81 |

- **Allocations.** Two 6.9 MB frames arriving in 1 MiB pieces, held by room and by a charge,
  take one allocation each and no reallocation when reserved once; growing reallocates them five
  times between them.
- **Hypothesis and outcome.** The expected gain was 2–6 % on large frames and none on small
  ones. The medians agree: every large-frame case gains 2.4–10 %, every small-frame case is
  within 3 %. Only the served destination over the socket clears the noise; the served source's
  rounds over the socket spread 15 % on their own.
- **Not measured.** Budget memory waits, the share of `realloc` in a profile, and rounds on
  CPUs 4–11.
- **When to measure again.** Once a served write runs in two stages, on the process bench:
  `tls/destination` and `tls/both` in large frames, and the destination process's CPU a GB, where
  the destination's core is what limits throughput. The same rule decides.

The review's measurements (ARCH_REVIEW.md §3.4) ran the same batches on the eight efficient cores
with a runtime of a worker for each of those cores beside a pool of four threads, the median of
seven runs, at the credit, HTTP/2 settings and generated data plane of their day:

| Mode | Review, efficient cores | Here, performance cores |
|---|---|---|
| Destination over the socket | 1 113 MB/s, 1.39 CPU s a GB | 2 017 MB/s, 0.79 |
| Source over the socket | 1 128 MB/s, 1.30 | 2 728 MB/s, 0.69 |
| Both over the socket | 953 MB/s, 2.92 | 1 382 MB/s, 1.39 |
| Destination over mutual TLS | 761 MB/s, 2.04 | 1 673 MB/s, 1.05 |
| Both over mutual TLS | 689 MB/s, 4.38 | 930 MB/s, 2.04 |
| `writev` + `recvfrom` a batch, destination over the socket | 457 + 891 | 65 + 46 |

The cores, their count, the layout and the flow control differ, so only the shape compares.
