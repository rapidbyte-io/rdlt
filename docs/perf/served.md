# Served connectors

Untrusted connectors run out of process, served over the wire protocol: over a socket pair to a
connector the host spawned, or over mutual TLS to one listening on the network. This record keeps
what serving the destination, the source or both costs in throughput and in CPU, against the
in-process passthrough, over both transports, with the connectors served in the bench's own
process or each in a process of its own.

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

  Served there, the host, both connectors, HTTP/2 and rustls share the engine's two runtime
  workers: these cases measure the wire path's CPU, not what a host moves when its connectors
  run elsewhere.
- **Processes.** The `served/process/...` cases run each served connector in a process of its
  own, as in production. The bench's own binary serves the replay source and the IPC sink when
  started as a connector (`--rdlt-fd` or `--listen`), through `Served::serve`, a connector
  binary's `main`; a source process makes the replay's batches, the same as the bench's, when
  the host first connects to it, before the run.
  - `process/socket`: the host spawns each served connector through `Local`, as it places a
    trusted binary: its socket on file descriptor 3, a process group of its own, one process a
    connector a run.
  - `process/tls`: a source and a sink each listen over mutual TLS on loopback in a process of
    their own, started once and reached through `Remote` for every run.
- **Cores.** `just bench served <cores> <filter> <connectors>` holds the bench to `cores` and
  each connector process to `connectors`, which a connector process sets on itself
  (`sched_setaffinity`) before its runtime starts, so its workers are as many as those CPUs; by
  default `connectors` is every online CPU outside `cores`.
- **Cases.** Each of `destination`, `source` and `both` served over each transport in both frame
  sizes, at the credit the protocol grants: a window that opens at 4 MiB and grows to two of the
  largest frames taken, over HTTP/2 stream windows of 4 MiB and transport frames of 1 MiB.
- **Engine.** Within the four cores (2 runtime workers and 2 compute threads), a budget of 1 GiB,
  one commit. Only the run is timed, not its connections; every run checks every row arrives.
- **CPU.** The CPUs `perf stat` counts busy over each case's runs, every thread of the process,
  the served connectors' included, a GB moved; the median and range over the rounds.
- **CPU of each process.** In the process cases, for each run, the bench's process's user and
  system time (`getrusage`: the host's engine and wire path, and a connector served in it) and
  each connector process's (its CPU clock, `clock_getcpuclockid`), read just before and just
  after the run; a case's CPU a GB is their sum, and each is reported on its own. The CPUs a
  process keeps busy are its CPU time over the runs' time. Linux alone tells one process
  another's CPU time without `unsafe`: elsewhere a connector process's is unmeasured. `perf
  stat`'s figures for these cases are not recorded: a spawned source makes its batches inside
  the window `perf stat` counts, and a listening connector killed as the bench ends takes its
  counts with it.
- **Partitions.** Most cases read the batches from one partition. Six process cases read the
  same batches from four and from eight partitions, each an even share in order
  (`rdlt_engine::bench::split_replay_config`): both connectors served over the socket and over
  mutual TLS, and the destination served over mutual TLS, in large frames. The engine reads the
  partitions side by side, and every write goes over the destination's one connection.
- **Syscalls.** One run of each case in each round, in criterion's test mode under
  `strace -f -c`, its connection's setup included, both ends' threads counted; the median over
  the rounds.
- **Running.** `just bench served 0-3`, five rounds interleaved with five of the alternative
  below, each started at a one-minute load average under 1.0 under the measurement lock; release
  profile. The load after each round, 1.9–3.4, is the bench's own threads, but for one round at
  5.9. The process cases: `just bench served 0-3 "" 4-11`, the host on the four performance
  cores and the connectors on the eight efficient cores, which share the performance cores' L3;
  the four low-power cores, 12–15, which have no L3, run neither. Five rounds of all 24 cases,
  in process and apart, each started at a one-minute load average under 1.0 under the
  measurement lock.
- **Provider.** Every figure here but the allocator's was taken with rustls on `ring`. Only the
  allocator's TLS cases were measured on aws-lc-rs, the provider TLS now runs on (ADR 0018).

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
- **Measured again** once a served write ran in two stages, on the process bench (below): with
  the two stages, reserving once moved `process/socket/destination/64x80000` 1 474 against
  1 460 MB/s and `process/tls/destination/64x80000` 1 286 against 1 268, within the rounds'
  ranges, so the room still grows.

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

## Connectors in processes of their own

Intel Core Ultra X7 358H, mains power, 2026-10-08, `799c06f3749d` with these cases, governor
`powersave` with energy preference `performance` on all twelve CPUs used, one-minute load average
0.88–0.98 before the rounds and 2.5–4.0 after. The median of five rounds and their range; a CPU
column is the CPU seconds a GB moved of that process alone, or `in host` for a connector run in the
bench's process. The last column is the in-process case of the same rounds, its CPU the bench's
`getrusage`, which the in-process results above count with `perf stat` instead.

| Case | MB/s | CPU s a GB, all | Host's process | Source's | Destination's | In process, same rounds: MB/s, CPU s a GB |
|---|---|---|---|---|---|---|
| `socket/destination/64x80000` | 1 517 (1 497–1 579) | 1.31 (1.30–1.33) | 0.46 (0.45–0.47) | in host | 0.85 (0.84–0.87) | 2 099, 0.77 |
| `socket/source/64x80000` | 2 241 (2 100–2 364) | 1.11 (1.07–1.13) | 0.55 (0.53–0.57) | 0.55 (0.53–0.57) | in host | 2 750, 0.66 |
| `socket/both/64x80000` | 1 394 (1 354–1 410) | 2.38 (2.32–2.43) | 0.96 (0.92–0.98) | 0.57 (0.56–0.57) | 0.86 (0.84–0.90) | 1 353, 1.39 |
| `socket/destination/512x10000` | 3 098 (2 800–3 189) | 1.03 (1.00–1.15) | 0.50 (0.49–0.56) | in host | 0.52 (0.51–0.59) | 3 474, 0.66 |
| `socket/source/512x10000` | 2 981 (2 773–3 162) | 1.12 (1.07–1.27) | 0.56 (0.54–0.63) | 0.56 (0.53–0.63) | in host | 3 347, 0.67 |
| `socket/both/512x10000` | 2 440 (2 265–2 455) | 2.06 (1.98–2.17) | 0.80 (0.78–0.84) | 0.63 (0.61–0.66) | 0.64 (0.59–0.67) | 2 003, 1.16 |
| `tls/destination/64x80000` | 1 060 (972–1 089) | 1.41 (1.39–1.51) | 0.51 (0.50–0.54) | in host | 0.91 (0.89–0.97) | 1 628, 0.98 |
| `tls/source/64x80000` | 1 585 (1 456–1 758) | 1.24 (1.16–1.28) | 0.58 (0.58–0.64) | 0.64 (0.57–0.68) | in host | 1 888, 0.93 |
| `tls/both/64x80000` | 1 019 (909–1 027) | 2.71 (2.66–2.95) | 1.12 (1.05–1.18) | 0.66 (0.60–0.76) | 0.95 (0.94–1.02) | 938, 1.95 |
| `tls/destination/512x10000` | 1 519 (1 008–1 576) | 1.51 (1.46–2.07) | 0.76 (0.71–0.96) | in host | 0.74 (0.73–1.14) | 2 279, 0.94 |
| `tls/source/512x10000` | 1 491 (1 068–1 547) | 1.75 (1.66–2.53) | 0.92 (0.90–1.30) | 0.83 (0.77–1.23) | in host | 2 174, 0.98 |
| `tls/both/512x10000` | 1 294 (1 215–1 328) | 2.90 (2.81–3.20) | 1.22 (1.18–1.25) | 0.88 (0.78–0.97) | 0.84 (0.82–0.97) | 1 198, 1.83 |

- With both connectors served, processes of their own move as much or more than the shared
  runtime: 2 440 against 2 003 MB/s over the socket in small frames, 1 019 against 938 and
  1 294 against 1 198 over mutual TLS, and 1 394 against 1 353 over the socket in large frames,
  within both ranges.
- With one connector served, they move 11–35 % less. The served connector runs on the
  efficient cores and the other in the host's process on the performance cores; these rounds do
  not tell the cores' share of the loss from the separation's.
- The whole pipeline takes 1.3–1.8 times the CPU a GB that the shared runtime does.
- The host's own process, both connectors served, takes 0.80–0.96 CPU seconds a GB over the
  socket and 1.12–1.22 over mutual TLS, about two fifths of the pipeline's.
- Both served over mutual TLS move 1 019–1 294 MB/s, against the 2 500–3 500 MB/s loopback
  target.

### Partitions

The same layout and cores, 2026-10-09, `799c06f3749d` with these cases and partitions, five rounds
of the nine cases alone. The one-minute load average was 0.94–0.99 before the rounds and 1.7–3.0
after. The table gives the median of the five rounds and their range, and the median of the CPUs
each process kept busy.

| Case | Partitions | MB/s | CPU s a GB, all | Host's | Source's | Destination's | CPUs busy: host / source / destination |
|---|---|---|---|---|---|---|---|
| `socket/both` | 1 | 1 402 (1 332–1 466) | 2.40 (2.31–2.43) | 0.99 (0.94–0.99) | 0.55 (0.54–0.56) | 0.83 (0.81–0.87) | 1.38 / 0.79 / 1.18 |
| `socket/both` | 4 | 1 359 (1 304–1 387) | 2.51 (2.42–2.53) | 0.99 (0.97–1.02) | 0.66 (0.65–0.66) | 0.84 (0.81–0.87) | 1.35 / 0.91 / 1.14 |
| `socket/both` | 8 | 1 283 (1 259–1 336) | 2.56 (2.51–2.59) | 1.00 (0.99–1.05) | 0.70 (0.69–0.71) | 0.84 (0.81–0.90) | 1.34 / 0.92 / 1.08 |
| `tls/destination` | 1 | 1 086 (1 078–1 110) | 1.41 (1.37–1.43) | 0.52 (0.49–0.53) | in host | 0.89 (0.88–0.91) | 0.56 / – / 0.97 |
| `tls/destination` | 4 | 1 076 (1 065–1 086) | 1.39 (1.36–1.41) | 0.49 (0.48–0.50) | in host | 0.90 (0.89–0.91) | 0.54 / – / 0.96 |
| `tls/destination` | 8 | 1 041 (1 024–1 042) | 1.39 (1.37–1.40) | 0.49 (0.48–0.50) | in host | 0.89 (0.88–0.90) | 0.52 / – / 0.93 |
| `tls/both` | 1 | 1 035 (1 017–1 041) | 2.65 (2.63–2.68) | 1.07 (1.05–1.09) | 0.66 (0.62–0.67) | 0.94 (0.92–0.94) | 1.09 / 0.68 / 0.96 |
| `tls/both` | 4 | 972 (951–978) | 2.79 (2.75–2.90) | 1.08 (1.05–1.18) | 0.77 (0.74–0.79) | 0.94 (0.92–0.98) | 1.06 / 0.75 / 0.91 |
| `tls/both` | 8 | 929 (918–949) | 2.82 (2.78–2.91) | 1.09 (1.04–1.17) | 0.82 (0.79–0.85) | 0.93 (0.92–0.93) | 1.01 / 0.77 / 0.87 |

- The destination's process takes the same CPU a GB at every partition count (0.83–0.84 over
  the socket, 0.89–0.94 over mutual TLS). It keeps under one CPU busy over mutual TLS
  (0.87–0.97) and about one over the socket (1.08–1.18), though it has eight cores. Its work
  does not spread over cores as partitions are added.
- Throughput does not rise with partitions, and falls slightly: 1 035 to 929 MB/s with both
  served over mutual TLS, 1 402 to 1 283 over the socket. The source's CPU a GB rises with
  them (0.66 to 0.82 over mutual TLS) while the host's stays the same.
- With one partition these cases repeat the baseline above within its range.
- Over mutual TLS, partitions do not spread the destination's work over cores: its process stays
  under one CPU busy and moves no more as they are added. What holds it there is not shown. One
  connection task framing TLS and HTTP/2 for every write would give these figures, but only a
  per-thread profile of the destination's process, which these rounds did not take, can show it.

## A served write in two stages

A served write ran one pump: it read a frame, decoded it, and waited for the destination's writer
before it read the next. Now one task receives and decodes the frames while another has the writer
stage them, joined by a channel of one: a frame decodes once the writer has taken the frame before
it, so a write holds its staged bound and one decoded frame waiting for its writer. Credit returns
as the writer takes each frame.

Intel Core Ultra X7 358H, mains power, 2026-10-09, `799c06f3749d` against the two stages on it,
and for the process cases `1c3357a8` with these cases against the two stages on it; five rounds
interleaved with the base, one-minute load average 0.25–0.98 before each round and 1.6–3.5 after.
The median of five rounds and their range. The rule set before measuring asked
`socket/destination/64x80000` on CPUs 4–11 to gain at least 10 % with no case lower.

In process, every case within the rounds' ranges of the base or above them:

| Case | CPUs | Base, MB/s | Two stages, MB/s | Median | Ranges apart | CPU s a GB | CPUs busy |
|---|---|---|---|---|---|---|---|
| `socket/destination/64x80000` | 0–3 | 1 973 (1 916–2 110) | 2 121 (1 816–2 345) | +7.5 % | no | 0.82 → 0.82 | 1.60 → 1.80 |
| `socket/both/64x80000` | 0–3 | 1 364 (1 334–1 430) | 1 345 (1 275–1 489) | −1.4 % | no | 1.38 → 1.38 | 1.87 → 1.94 |
| `socket/destination/512x10000` | 0–3 | 3 495 (3 466–3 556) | 3 477 (3 459–3 520) | −0.5 % | no | 0.65 → 0.68 | 2.13 → 2.21 |
| `tls/destination/64x80000` | 0–3 | 1 755 (1 625–1 805) | 1 788 (1 692–1 912) | +1.9 % | no | 0.93 → 0.95 | 1.67 → 1.79 |
| `socket/destination/64x80000` | 4–11 | 1 553 (1 493–1 613) | 1 623 (1 573–1 699) | +4.5 % | no | 1.11 → 1.21 | 1.65 → 1.96 |
| `socket/both/64x80000` | 4–11 | 1 302 (1 259–1 309) | 1 329 (1 233–1 359) | +2.1 % | no | 2.19 → 2.32 | 2.82 → 3.14 |
| `socket/destination/512x10000` | 4–11 | 3 380 (3 347–3 472) | 3 439 (3 408–3 460) | +1.7 % | no | 1.00 → 1.07 | 3.17 → 3.47 |
| `tls/destination/64x80000` | 4–11 | 1 156 (1 121–1 176) | 1 298 (1 243–1 335) | +12.3 % | yes | 1.56 → 1.66 | 1.88 → 2.20 |

With the connectors in processes of their own, the host on CPUs 0–3 and the connectors on 4–11;
the destination's CPU a GB and CPUs busy are its process's alone:

| Case | Base, MB/s | Two stages, MB/s | Median | Ranges apart | Destination's CPU s a GB | Destination's CPUs busy |
|---|---|---|---|---|---|---|
| `tls/destination/64x80000` | 1 121 (1 100–1 134) | 1 286 (1 222–1 304) | +14.8 % | yes | 0.87 → 0.89 | 0.96 → 1.14 |
| `tls/destination/64x80000/4-partitions` | 1 103 (1 082–1 144) | 1 281 (1 247–1 306) | +16.2 % | yes | 0.88 → 0.88 | 0.97 → 1.13 |
| `tls/both/64x80000` | 1 043 (1 014–1 062) | 1 167 (1 147–1 181) | +11.9 % | yes | 0.93 → 0.96 | 0.96 → 1.12 |
| `tls/both/64x80000/4-partitions` | 981 (952–996) | 1 102 (1 087–1 140) | +12.4 % | yes | 0.92 → 0.94 | 0.91 → 1.05 |
| `socket/both/512x10000` | 2 581 (2 456–2 673) | 2 850 (2 820–2 878) | +10.4 % | yes | 0.57 → 0.52 | 1.45 → 1.50 |
| `socket/destination/64x80000` | 1 590 (1 552–1 654) | 1 463 (1 377–1 517) | −8.0 % | yes, lower | 0.80 → 1.06 | 1.32 → 1.53 |

- **Over mutual TLS** the destination's process now keeps 1.12–1.14 CPUs busy where it kept
  0.96, at the same CPU a GB, and moves 12–16 % more, at one partition and at four. A sample of
  its threads during `tls/destination/64x80000/4-partitions` shows no thread saturated: four
  runtime workers each 20–33 % busy, `memmove` about half the cycles and AES-GCM about a fifth.
- **A spawned destination over the socket, in large frames, moves 8 % less**, at 1.06 against
  0.80 CPU seconds a GB in its process. Each run spawns that destination afresh, and its process
  takes 93 000 page faults a run of 441 MB against 44 000, all in `memmove`: with a frame decoding
  while the one before is written, two frames' buffers are alive, freed on another worker than
  the one that made them, and glibc's allocator gives more of what is freed back to the kernel,
  so more of each next frame is written to fresh pages. With the destination on two performance
  cores (host on 0–1, connectors on 2–3) the faults rise 14 % instead of 71 %. The long-lived
  destination listening over mutual TLS takes almost none.
- **The allocator, measured.** In process on CPUs 4–11, `socket/destination/64x80000`, one run
  each: with glibc's defaults the base moves 1 401 MB/s at 1.22 CPU seconds a GB and the two
  stages 1 551 at 1.26, with 2.2 and 2.6 million page faults a run of the bench; with heap
  trimming off and the mmap threshold fixed at 32 MiB
  (`GLIBC_TUNABLES=glibc.malloc.trim_threshold=268435456:glibc.malloc.mmap_threshold=33554432`)
  the base moves 1 635 at 1.01 and the two stages 2 007 at 1.03, with 0.2 million faults each.
  The allocator's returning memory costs both layouts, and the two stages more.
- **Against the rule.** `socket/destination/64x80000` on CPUs 4–11 gained 4.5 %, short of the
  10 % asked, and the spawned socket destination in large frames is lower. The two stages land
  for the production layout, a connector in a process of its own over mutual TLS, with the
  regression confined to a spawned connector over the socket in large frames under glibc's
  allocator defaults; the allocator is a decision of its own.


## Allocator

Each served connector's process and the bench's own ran on glibc's allocator, whose returning of
freed memory to the kernel cost the spawned socket destination its pages above. Two allocators
were tried in its place, each set as the bench binaries' `#[global_allocator]`, so the host, the
connector processes the bench spawns from its own binary, and the engine benches all ran on it:
mimalloc 0.1.52 (mimalloc v3) and tikv-jemallocator 0.7.0 (jemalloc 5.3.1), default features.

The rule set before measuring: an allocator lands only if, against glibc, it moves more beyond
the rounds' noise (ranges apart) on `process/socket/destination/64x80000` and
`process/tls/both/64x80000`, no measured case is lower beyond noise, no process of any case
peaks more than 10 % higher in resident memory, `cargo deny` passes, and it builds on Linux and
macOS with a C compiler alone. jemalloc fails the last before any round: its build script runs
`configure` and `make`.

Intel Core Ultra X7 358H, mains power, 2026-10-09, `39ee5153` (aws-lc-rs), governor `powersave`
with energy preference `performance`; rounds interleaved by candidate in a rotating order, each
case a process of its own, the host on CPUs 0–3 and connector processes on 4–11. Each group of
cases started after a minute's pause at a one-minute load average of 0.77–2.70 (the desktop
kept it mostly above 1.0 with the CPUs idle). Five rounds were planned; they stopped after glibc's
three, mimalloc's two and jemalloc's three, by the owner's decision, once every mimalloc and
jemalloc round fell below glibc's lowest on `process/tls/both/64x80000`. Each round's figure, in
round order:

| Case | CPUs | glibc | mimalloc | jemalloc |
|---|---|---|---|---|
| `process/socket/destination/64x80000`, MB/s | 0–3, 4–11 | 1 421 / 1 868 / 2 019 | 2 073 / 1 940 | 1 407 / 1 547 / 1 425 |
| `process/tls/destination/64x80000`, MB/s | 0–3, 4–11 | 1 262 / 1 243 / 1 291 | 1 192 / 1 089 | 1 202 / 1 186 / 1 146 |
| `process/tls/both/64x80000`, MB/s | 0–3, 4–11 | 1 159 / 1 133 / 1 136 | 1 008 / 924 | 960 / 975 / 882 |
| `process/socket/both/512x10000`, MB/s | 0–3, 4–11 | 2 714 / 2 483 / 2 656 | 2 136 / 2 192 | 3 091 / 2 956 / 2 968 |
| `socket/destination/64x80000`, MB/s | 0–3 | 1 884 / 2 591 / 2 373 | 2 816 / 2 892 | 1 689 / 1 692 / 1 568 |
| `tls/both/64x80000`, MB/s | 0–3 | 829 / 869 / 738 | 901 / 877 | 583 / 704 / 614 |
| `shred/nested`, MB/s | 0 | 617 / 629 / 602 | 613 / 612 | 577 / 604 / 624 |
| `normalize/keyless`, MB/s | 0 | 179 / 174 / 174 | 168 / 175 | 160 / 175 / 179 |
| `shred_cores/4`, MB/s | 0–3 | 1 551 / 1 466 / 1 403 | 1 467 / 1 359 | 1 432 / 1 278 / 1 449 |
| `passthrough/paired/1+1`, MB/s | 0 | 12 568 / 11 813 / 11 972 | 11 551 / 12 277 | 11 862 / 11 166 / 12 260 |
| `passthrough/paired/2+2`, MB/s | 0–3 | 15 451 / 14 130 / 12 633 | 14 143 / 12 965 | 13 865 / 13 911 / 14 065 |
| `normalized/keyless/2000`, million rows/s | 0 | 3.53 / 3.42 / 3.24 | 3.78 / 3.73 | 3.57 / 3.72 / 3.83 |
| `normalized/keyless/10000`, million rows/s | 0 | 2.29 / 2.01 / 2.02 | 2.27 / 2.26 | 2.30 / 1.98 / 2.34 |
| `normalized/keyless/50000`, million rows/s | 0 | 2.38 / 2.23 / 2.07 | 2.27 / 2.25 | 2.21 / 1.89 / 2.33 |
| `normalized/keyless/2000`, million rows/s | 0–3 | 2.95 / 2.78 | 3.00 / 2.86 | 2.84 / 2.95 / 2.97 |
| `normalized/keyless/10000`, million rows/s | 0–3 | 2.30 / 2.08 | 2.27 / 2.33 | 1.99 / 2.29 / 2.44 |
| `normalized/keyless/50000`, million rows/s | 0–3 | 2.50 / 1.92 | 2.24 / 2.37 | 2.39 / 2.36 / 2.28 |

Peak resident memory of each process (`VmHWM`, sampled every 10 ms), MiB, the lowest and highest
over the rounds that recorded it; the sampler missed some runs' bench process, so a cell rests on
the rounds named:

| Case | Process | glibc | mimalloc | jemalloc |
|---|---|---|---|---|
| `process/socket/destination/64x80000` | host | 518–520 (2) | 501–512 (2) | 487 (1) |
| | destination | 128–129 (3) | 98–99 (2) | 96–136 (3) |
| `process/tls/destination/64x80000` | host | 513–525 (3) | 521–524 (2) | 496–526 (3) |
| | destination | 155–176 (3) | 159–182 (2) | 128–147 (3) |
| `process/tls/both/64x80000` | host | 743–768 (3) | 648–657 (2) | 537–570 (3) |
| | source | 523–553 (3) | 484–489 (2) | 500–501 (3) |
| | destination | 164–174 (3) | 145–146 (2) | 109–128 (3) |
| `process/socket/both/512x10000` | host | 509–520 (3) | 521–526 (2) | 515–557 (3) |
| | source | 457–458 (3) | 471–491 (2) | 512–546 (3) |
| | destination | 32–36 (3) | 27–32 (2) | 30–31 (3) |
| `socket/destination/64x80000` | host | 578–586 (3) | 589–612 (2) | 517–552 (3) |
| `tls/both/64x80000` | host | 766–790 (3) | 712–753 (2) | 662–697 (3) |
| `shred/nested`, CPU 0 | host | 188–202 (2) | 253 (1) | 243 (2) |
| `normalize/keyless`, CPU 0 | host | 174–181 (3) | 234–250 (2) | 186–236 (3) |
| `shred_cores/4`, CPUs 0–3 | host | 176–182 (3) | 253 (2) | 198–234 (3) |
| `passthrough/paired/1+1`, CPU 0 | host | 452–458 (3) | – | 450–474 (3) |
| `passthrough/paired/2+2`, CPUs 0–3 | host | 468 (2) | 496–498 (2) | 485–502 (3) |
| `normalized/keyless/*`, CPU 0 | host | 57 (3) | – | 82–96 (3) |
| `normalized/keyless/*`, CPUs 0–3 | host | 67–68 (2) | 155 (1) | 127–135 (3) |

- **Neither qualifies.** On `process/tls/both/64x80000`, the production layout, every mimalloc
  and jemalloc round is below glibc's lowest (924–1 008 and 882–975 against 1 133–1 159 MB/s),
  and so is `process/tls/destination/64x80000`. mimalloc is also below on
  `process/socket/both/512x10000` (2 136–2 192 against 2 483–2 714), and jemalloc on the
  in-process `tls/both/64x80000` (583–704 against 738–869) and `socket/destination/64x80000`.
- **Memory.** Both peak higher in the engine's own benches: the 4-core normalize's process at
  155 (mimalloc) and 127–135 MiB (jemalloc) against 67–68, and `shred_cores/4`'s at 253 and
  198–234 against 176–182. With both connectors served over mutual TLS, every process peaks
  lower with either.
- **mimalloc does take away the large-frame cost of a socket destination**: 2 816–2 892 against
  1 884–2 591 MB/s in process, and 1 940–2 073 against 1 421–2 019 spawned, at 98–99 MiB of
  peak memory in that process against 128–129. It loses that and more over mutual TLS and in
  small frames, so glibc's allocator stays, and the regression the two stages recorded for a
  spawned socket destination in large frames remains.

## One connection a connector

Every call of a served connector, all its data streams, runs over one HTTP/2 connection, and that
connection's task, one task on one thread at a time, does the connection's TLS: on the host's side
hyper's client connection task encrypts each frame into records, on the connector's side the
server connection task decrypts them. The question was whether that task limits mutual TLS, so
that several connections a connector, one session over N connections, would pay.

The rule set before measuring: build several connections a connector only if
`process/tls/destination/64x80000` moves at most 70 % of `process/socket/destination/64x80000`
and one connection task holds at least 0.8 of a core on either end. Otherwise one connection is
kept.

Intel Core Ultra X7 358H, mains power, 2026-10-10, `dff5d4be42d6`, governor `powersave` with
energy preference `performance` on the twelve CPUs used; the host on CPUs 0–3 and the
destination's process on 4–11 (`just bench served 0-3 <filter> 4-11`). Three rounds, each a run
of the tls case and one of the socket case back to back, the order alternating (tls first in
rounds 1 and 3, socket first in round 2), under the measurement lock. The two profiles below ran
first, under the same lock. Load average over a minute was 1.02 before the profiles and
2.19–2.63 before and after each of the rounds' runs, the bench's own threads.
The rounds stopped at three: every one fell far from both thresholds.

| Round | tls, MB/s | socket, MB/s | tls of socket |
|---|---|---|---|
| 1 | 1 284 | 1 403 | 91.5 % |
| 2 | 1 273 | 1 475 | 86.3 % |
| 3 | 1 278 | 1 387 | 92.1 % |

The median of the three rounds and their range: tls 1 278 (1 273–1 284) MB/s, socket 1 403
(1 387–1 475), tls at 91 % of the socket's.

| Median (range) | tls | socket |
|---|---|---|
| CPU s a GB, all | 1.45 (1.44–1.49) | 1.51 (1.49–1.58) |
| Host's process, CPU s a GB | 0.55 (0.53–0.62) | 0.42 (0.41–0.52) |
| Destination's process, CPU s a GB | 0.90 (0.87–0.91) | 1.08 (1.06–1.09) |
| CPUs busy, host's process | 0.70 (0.68–0.79) | 0.59 (0.58–0.77) |
| CPUs busy, destination's process | 1.15 (1.11–1.15) | 1.51 (1.51–1.52) |
| `writev` a run (setup included) | 7 736 (7 726–7 763) | 1 684 (1 667–1 713) |
| `recvfrom` a run (setup included) | 29 167 (29 061–29 223) | 2 359 (2 233–2 370) |

One profile of each case, on the `just profiling served` build, `$bin`, with the host on CPUs 0–3
and the destination on 4–11, 15 seconds of the case's runs:

```
RDLT_BENCH_CONNECTOR_CORES=4-11 perf record -e task-clock -c 1000000 --call-graph fp -- \
  taskset -c 0-3 "$bin" --bench '^served/process/tls/destination/64x80000$' --profile-time 15
```

and the socket case alike. A sample is one millisecond of a thread's user-mode CPU; kernel time
is not sampled (`perf_event_paranoid` 2). The connection task is every sample whose stack holds
hyper's server connection (`hyper::server::conn::http2::Connection`, the connector's side) or
hyper's client connection task (`hyper::proto::h2::client::Conn`, the host's side). A core's
share is the task's samples in a second over the 1 000 a core gives, over the steady seconds:
those in which the process had at least half the samples of its busiest second.

- **Over mutual TLS, the destination's process** has 42.3 % of its samples under its connection
  task. The task held 0.37 of a core (median over 12 seconds, 0.40 at most), and the aws-lc-rs
  seal and open under it are 58 % of the task's samples.
- **The host's process** has 54.6 % of its samples under the client connection task, which held
  0.28 of a core (median over 13 seconds, 0.31 at most); crypto is 72 % of the task.
- **The task's share of the process's samples times the process's CPUs busy**, user and system,
  from the rounds, estimates the task's share of a core with the process's system time counted
  in proportion to its user time: 0.423 × 1.15 = 0.49 of a core on the destination,
  0.546 × 0.70 = 0.38 on the host. Even if all of a process's system time were its connection
  task's (CPUs busy in the rounds less the user-mode CPUs the profile counted, 0.87 on the
  destination and 0.48 on the host), the task holds at most 0.65 of a core on the destination
  (0.37 + 1.15 − 0.87) and 0.50 on the host (0.28 + 0.70 − 0.48).
- **Over the socket**, the connection tasks hold 2 % of the host's samples and 0–3 % of each
  spawned destination's, at most 0.03 of a core.
- **Against the rule.** tls moves 86–92 % of the socket's MB/s (rule: at most 70 %), and no
  connection task holds more than 0.65 of a core even with all system time counted (rule: at
  least 0.8). Neither condition holds, so one connection a connector is kept: several
  connections would spread a task that does not limit the run.
- **CPU.** tls costs no more CPU a GB than the spawned socket destination here: 1.45 against 1.51
  all, with the socket destination's process taking 1.08 against 0.90, which is consistent with the
  page-fault cost recorded under "A served write in two stages".
- **The open question under "Partitions"**, whether one connection task framing TLS and HTTP/2
  holds the destination under one CPU, is answered: with its process at 1.15 CPUs busy, the
  connection task holds 0.37 of a core in user mode and at most 0.65 with all system time
  counted, so it is not what limits the destination.
