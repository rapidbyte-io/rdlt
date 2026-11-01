# Served connectors

Untrusted connectors run out of process, served over the wire protocol: over a socket pair to a
connector the host spawned, or over mutual TLS to one listening on the network. This record keeps
what serving the destination, the source or both costs in throughput and in CPU, against the
in-process passthrough, over both transports, with the connectors served in the bench's own
process or each in a process of its own.

## Method

- **Batches.** The passthrough bench's ten mixed columns, 441 MB a run: 64 batches of 80 000 rows
  (6.9 MB each, the frames the default coalescing target makes), 512 of 10 000 (0.86 MB each) or
  5 120 of 1 000 (86 kB each), a checkpoint after each, so each batch is one frame.
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
- **Cases.** Each of `destination`, `source` and `both` served over each transport in large and
  in small frames, at the credit the protocol grants: a window that opens at 4 MiB and grows to
  two of the largest frames taken, over HTTP/2 stream windows of 4 MiB and transport frames of
  1 MiB.
- **Engine.** Within the four cores (2 runtime workers and a compute thread a core), the default
  budget of 256 MiB, one commit; the cases named `/1GiB` run within 1 GiB, as did the results
  recorded at `90ce76c2` and before. Only the run is timed, not its connections; every run checks
  every row arrives.
- **Readings.** Each process case prints its last run's readings from the host's report, as
  [passthrough.md](passthrough.md) defines them, a batch counted as a frame. In every case but
  those named `/<n>-writers` the served sink declares one writer, so the case writes through one
  lane whatever its partitions: its lanes written say nothing of how the engine spreads
  partitions over lanes. Those cases are below ("Through a writer a core").
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
  Each process's user and system time are read alike, the bench's from `getrusage` and a
  connector process's from its `/proc/<pid>/stat`, and printed as the share of its CPU time spent
  in user mode.
- **Partitions.** Most cases read the batches from one partition. Eleven process cases read the
  same batches from several partitions, each an even share in order
  (`rdlt_engine::bench::split_replay_config`): both connectors served over the socket, as
  production runs them, in large frames from 4, 8 and 16 partitions, in small frames from 4 and
  16, and in frames of 1 000 rows from 4 and 16, as the passthrough matrix's cells are read,
  besides one case of those frames from one partition; and over mutual TLS the destination and
  both, in large frames from 4 and 8. The engine reads the partitions
  side by side, and every write goes over the destination's one connection, through one lane:
  the served sink declares one writer.
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
- **Not measured.** Budget memory waits, and rounds on CPUs 4–11.
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

### The production slice

`main` at `6365cc13` with the bench's readings (`d2fc0f95`), 2026-10-25 and -26, the same layout
and cores: both connectors served over the socket in processes of their own, the engine within
the default 256 MiB. Three rounds of `just bench served 0-3 process/socket/both 4-11`, interleaved
with the other benches' rounds ([campaign.md](campaign.md)), each started once no other build ran;
the one-minute load average was 1.15, 2.69 and 2.22 at their starts and 1.05, 1.55 and 0.93 at
their ends. The table gives the median of the three rounds, the throughput's range in brackets,
and the medians of each process's CPU seconds a GB, the CPUs each kept busy, the waits of frames
for their decode charge and the lane's flushes of its writer by cause, as
[passthrough.md](passthrough.md) defines them. Every case writes through one lane, the served
sink declaring one writer, and makes one commit, at the end.

| Frames | Partitions | MB/s | CPU s a GB: all | Host's, source's, destination's | CPUs busy: host / source / destination | Decode waits | Flushes: pressure + commit |
|---|---|---|---|---|---|---|---|
| 64×80 000 | 1 | 1 524 [1 479–1 627] | 2.11 | 0.92, 0.56, 0.62 | 1.41 / 0.84 / 0.98 | 3.8 % | 5 + 1 |
| 64×80 000 | 4 | 1 588 [1 547–1 595] | 2.13 | 0.89, 0.60, 0.63 | 1.43 / 0.96 / 1.01 | 9.1 % | 5 + 1 |
| 64×80 000 | 8 | 1 493 [1 488–1 566] | 2.19 | 0.92, 0.67, 0.62 | 1.37 / 0.99 / 0.92 | 10.5 % | 5 + 1 |
| 64×80 000 | 16 | 1 449 [1 413–1 523] | 2.27 | 0.91, 0.72, 0.64 | 1.30 / 1.05 / 0.90 | 29.0 % | 9 + 1 |
| 512×10 000 | 1 | 2 690 [2 570–2 733] | 1.58 | 0.62, 0.50, 0.44 | 1.63 / 1.32 / 1.16 | 2.1 % | 5 + 1 |
| 512×10 000 | 4 | 2 532 [2 393–2 586] | 1.66 | 0.61, 0.56, 0.49 | 1.55 / 1.39 / 1.21 | 2.0 % | 5 + 1 |
| 512×10 000 | 16 | 2 361 [2 130–2 381] | 1.72 | 0.64, 0.61, 0.47 | 1.50 / 1.43 / 1.12 | 1.8 % | 5 + 1 |
| 5 120×1 000 | 1 | 1 758 [1 734–1 760] | 2.77 | 1.06, 0.79, 0.91 | 1.85 / 1.38 / 1.61 | 2.7 % | 5 + 1 |
| 5 120×1 000 | 4 | 1 516 [1 481–1 523] | 3.11 | 1.20, 0.96, 0.95 | 1.82 / 1.46 / 1.45 | 2.4 % | 5 + 1 |
| 5 120×1 000 | 16 | 1 459 [1 451–1 461] | 3.13 | 1.24, 0.94, 0.94 | 1.81 / 1.38 / 1.38 | 2.6 % | 6 + 1 |

- Throughput falls from 1 to 16 partitions at every size of frame: 1 524 to 1 449 MB/s in large
  frames, 2 690 to 2 361 in small and 1 758 to 1 459 in frames of 1 000 rows.
- The pipeline's CPU a GB rises with partitions, the source's most: 0.56 to 0.72 CPU seconds a GB
  in large frames and 0.50 to 0.61 in small.
- Only large frames from sixteen partitions wait on the budget: their pushes' intake waits sum to
  2.6 % of the run and their frames' decode waits to 29.0 %. No other case's intake waits pass
  0.1 %.
- The share of each process's CPU time spent in user mode, in `both/64x80000`: the host's 0.38,
  the source's 0.40 and the destination's 0.58; in frames of 1 000 rows 0.70, 0.62 and 0.65.

### What the production slice decides

The rules [passthrough.md](passthrough.md) applies to the matrix, read beside it on the production
slice: `main` at `2e49c6bae25b`, 2026-10-14, one round of `served/process/socket/both` on the
cores above, criterion's pass alone, started at a one-minute load average of 11.5 that was falling
from a mutation run just ended, the bench alone on the machine. Its readings lie close to the
medians above: 14.0–16.4 signals a frame (30.1 in large frames from sixteen partitions), intake
waits summing to 797 % in large frames from sixteen partitions, and in frames of 1 000 rows the
pool idle 91.3–91.7 % and its queue and way back 60.6 %, 82.2 % and 80.5 % from 1, 4 and 16
partitions. Its throughput, one round, lies below the medians above, by up to 17 %.

Two profiles of the host's process, `cpu_core/cycles/u` at 999 Hz with frame pointers over 15
seconds, its samples classified as passthrough.md says, less those hashing the connector's
binary as each run spawns it (in brackets with them in):

| Case | Host's samples, hashing included | Ledger and watch | Coordinator |
|---|---|---|---|
| `5120x1000/16-partitions` | 10 552 | 2.69 % (2.27 %) | 2.05 % (1.73 %) |
| `512x10000/16-partitions` | 6 633 | 1.40 % (0.96 %) | 1.01 % (0.69 %) |

The served sink declares one writer, so the rules about lanes are not read here.

The two changes passthrough.md records as built, on the production slice: `main` at
`b0f322dd311f`, the host on CPUs 0–3 and the connectors on 4–11, three rounds of
`5120x1000/16-partitions` with `main`, pressure alone and both changes run in turn, each round
started at a one-minute load under 1.0, and one profile each of the host's process, classified
as above:

| Side | MB/s | Signals a frame | Ledger and watch | Coordinator |
|---|---|---|---|---|
| `main` | 1 416 [1 410–1 424] | 14.0 | 2.58 % (2.21 %) | 2.19 % (1.87 %) |
| Pressure | 1 431 [1 418–1 436] | 0.0 | 1.59 % (1.36 %) | 1.50 % (1.28 %) |
| Both | 1 434 [1 433–1 447] | 0.0 | 1.54 % (1.31 %) | 1.01 % (0.86 %) |

Pressure took the ledger and watch 38 % lower, and progress the coordinator 33 % lower than
pressure alone: neither halved, as in process, and throughput rose 1.3 % past `main`'s range.

### Cutting and lowering where nothing is copied

A plain Arrow batch's cut and lowering run on the partition's task where they read and write no
value ([passthrough.md](passthrough.md), "Cutting and lowering where nothing is copied"). On the
production slice: `main` at `223b7f6a` against the change, 2026-10-14, the host on CPUs 0–3 and
the spawned connectors on 4–11 (apart) or on 0–3 beside it (shared), three rounds of
`process/socket/both` in frames of 80 000 and 1 000 rows from 1 and 16 partitions, each round
interleaved `main`, change, change, `main` under the measurement lock, started at a one-minute load
average of 1.1 to 1.5 (2.1 to 2.3 before the shared ones, the connectors' processes among it).
MB/s, the median of six runs and their range:

| Placement | Frames | Partitions | `223b7f6a` | The change | Change |
|---|---|---|---|---|---|
| Apart | 64×80 000 | 1 | 1 377 [1 306–1 473] | 1 451 [1 234–1 602] | +5 % |
| Apart | 64×80 000 | 16 | 1 195 [1 151–1 286] | 1 227 [1 160–1 284] | +3 % |
| Apart | 5 120×1 000 | 1 | 1 671 [1 615–1 715] | 1 953 [1 935–1 978] | +17 % |
| Apart | 5 120×1 000 | 16 | 1 438 [1 423–1 448] | 1 481 [1 474–1 494] | +3 % |
| Shared | 64×80 000 | 1 | 1 360 [1 232–1 403] | 1 267 [1 150–1 313] | −7 % |
| Shared | 64×80 000 | 16 | 1 065 [1 012–1 087] | 1 067 [1 043–1 077] | 0 % |
| Shared | 5 120×1 000 | 1 | 1 087 [1 034–1 103] | 1 495 [1 475–1 531] | +38 % |
| Shared | 5 120×1 000 | 16 | 1 180 [1 079–1 182] | 1 152 [1 104–1 178] | −2 % |

- Frames of 1 000 rows from one partition gain most, 17 % apart and 38 % shared: two pool trips
  a frame of 5 120 are gone, and their wakes with them.
- One partition's large frames on cores shared with the connectors lose 7 %: in each round both
  of the change's runs read below both of `main`'s but one (1 313 against 1 317). It is not CPU
  the change adds: over ten seconds of the case, `perf stat` counts the host and its connectors
  the same instructions and user cycles on both sides (130.5 G and 41.3 G on `main`, 130.4 G and
  41.5 G here), and the two jobs it moves take about 70 µs a frame. Two more rounds of the case
  alone, MB/s, the median of four runs: `main` 1 404, the change 1 304; with a yield after each
  cut and window on the partition's task 1 300; with the cut on the pool 1 353; with the lowering
  on the pool 1 381, each within the others' ranges. A yield does not bring it back; what the
  pool's trips gave a partition whose read and writes share four cores with its connectors,
  these rounds do not show.
- Sixteen partitions read within 3 % of `main` in both placements, inside each other's ranges.

### A compute thread per core

The host's compute pool with a thread for each of its four cores beside two runtime workers
([passthrough.md](passthrough.md), "A compute thread per core"), against two and two with cuts
and lowerings on the partition's task, in the last two rounds above, each interleaved before,
change, change, before. MB/s, the median of four runs and their range:

| Placement | Frames | Partitions | Before, 2 + 2 | This change, 2 + 4 |
|---|---|---|---|---|
| Apart | 64×80 000 | 1 | 1 382 [1 234–1 469] | 1 405 [1 203–1 533] |
| Apart | 64×80 000 | 16 | 1 227 [1 160–1 284] | 1 178 [1 159–1 206] |
| Apart | 5 120×1 000 | 1 | 1 950 [1 935–1 954] | 1 955 [1 940–1 964] |
| Apart | 5 120×1 000 | 16 | 1 487 [1 474–1 494] | 1 480 [1 463–1 491] |
| Shared | 64×80 000 | 1 | 1 248 [1 150–1 313] | 1 299 [1 236–1 331] |
| Shared | 64×80 000 | 16 | 1 064 [1 043–1 070] | 1 047 [1 032–1 054] |
| Shared | 5 120×1 000 | 1 | 1 498 [1 483–1 531] | 1 514 [1 508–1 531] |
| Shared | 5 120×1 000 | 16 | 1 139 [1 104–1 159] | 1 148 [1 072–1 166] |

Every case lies within the other side's range: the served Arrow path leaves the pool asleep, its
two more threads with it.

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
  allocator defaults; that regression remained until the host set glibc's malloc tunables for the
  connectors it spawns (below).

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
  spawned socket destination in large frames remained until the host set glibc's malloc tunables
  for the connectors it spawns (next section).

## Malloc tunables for spawned connectors

The host starts each connector it spawns with
`GLIBC_TUNABLES=glibc.malloc.trim_threshold=268435456:glibc.malloc.mmap_threshold=33554432`,
keeping any tunable the operator passes (in `GLIBC_TUNABLES`, or in glibc's older
`MALLOC_TRIM_THRESHOLD_` and `MALLOC_MMAP_THRESHOLD_`). The faults recorded above come from
glibc returning the memory a connector frees, frames freed on another thread than their
maker's, so each next frame is written to fresh pages; trimming only past 256 MiB free and a fixed
32 MiB mmap threshold keep that memory for the next frame.

The rule set before measuring: `process/socket/destination/64x80000`, the spawned destination, at
least +10 % in MB/s or at least 10 % lower in CPU seconds a GB; no served case lower beyond
noise; the connector's peak resident memory rising by no more than the frames it keeps. The
`process/tls/*` connectors listen in processes the bench starts itself, not the host, so they
carry no tunables and serve as controls.

Intel Core Ultra X7 358H, 2026-10-10, `1f8449b1` against `67a04794`, governor `powersave` with
energy preference `performance`, `just bench served 0-3 '<filter>' 4-11`: the 10 cases of
`process/socket/*` and `process/tls/{destination,both}/64x80000`, three rounds a side
interleaved (base, branch, branch, base, base, branch), each in one timed lock hold. The machine
was shared: the one-minute load average was 3–8 between rounds and reached 14 during a round's
own perf and strace runs, so absolute figures carry noise of about ±15 % round to round. A
sampler read each connector process's `/proc` page faults (minor plus major) and `VmHWM` every
10 ms. It missed base round 1 and the branch's first two rounds of the TLS listeners (its first
versions were still being fixed), so faults and peak memory rest on base rounds 2–3 and branch
rounds 1–3 (the TLS listeners' on branch round 3 alone). Every sampled spawned connector
process on the branch had the tunables in its environment, and no base process did (the TLS
listeners, which the host does not spawn, had none, as expected). Each round's figure, in
round order (the unit is MB/s, converted from criterion's MiB/s):

| Case | Base, MB/s | Branch, MB/s |
|---|---|---|
| `process/socket/destination/64x80000` | 1 258 / 1 265 / 1 279 | 1 615 / 2 135 / 2 432 |
| `process/socket/source/64x80000` | 1 921 / 2 016 / 1 944 | 1 518 / 1 991 / 2 597 |
| `process/socket/both/64x80000` | 1 261 / 1 295 / 1 257 | 1 253 / 1 311 / 1 800 |
| `process/socket/destination/512x10000` | 3 902 / 3 598 / 3 087 | 3 881 / 4 258 / 3 245 |
| `process/socket/source/512x10000` | 2 280 / 3 036 / 2 793 | 3 237 / 3 640 / 2 615 |
| `process/socket/both/512x10000` | 1 364 / 2 405 / 2 572 | 2 756 / 2 744 / 2 892 |
| `process/socket/both/64x80000/4-partitions` | 1 174 / 1 027 / 1 085 | 1 440 / 1 418 / 1 492 |
| `process/socket/both/64x80000/8-partitions` | 1 162 / 1 067 / 1 076 | 1 273 / 1 354 / 1 460 |
| `process/tls/destination/64x80000` (control) | 951 / 971 / 1 106 | 1 109 / 1 094 / 1 252 |
| `process/tls/both/64x80000` (control) | 958 / 941 / 1 016 | 953 / 1 039 / 1 146 |

The destination's CPU seconds a GB, for the cases with a destination process:

| Case | Base | Branch |
|---|---|---|
| `process/socket/destination/64x80000` | 1.158 / 1.144 / 1.218 | 1.003 / 0.836 / 0.779 |
| `process/socket/both/64x80000` | 1.168 / 1.015 / 1.063 | 0.906 / 0.875 / 0.849 |
| `process/socket/destination/512x10000` | 0.545 / 0.611 / 0.596 | 0.540 / 0.532 / 0.577 |
| `process/socket/both/512x10000` | 0.709 / 0.583 / 0.605 | 0.587 / 0.577 / 0.570 |
| `process/socket/both/64x80000/4-partitions` | 1.073 / 1.237 / 1.131 | 0.877 / 0.864 / 0.837 |
| `process/socket/both/64x80000/8-partitions` | 1.076 / 1.184 / 1.220 | 0.957 / 0.872 / 0.834 |
| `process/tls/destination/64x80000` (control) | 1.191 / 1.090 / 0.994 | 1.015 / 0.973 / 0.916 |
| `process/tls/both/64x80000` (control) | 1.041 / 1.154 / 1.028 | 1.088 / 1.016 / 0.967 |

Page faults a spawned connector process, the median over the processes a case spawned, then
the median of the rounds and their range:

| Case | Base | Branch | Change |
|---|---|---|---|
| `process/socket/destination/64x80000` | 94 512 (92 666–96 358) | 28 099 (11 550–28 604) | −70 % |
| `process/socket/source/64x80000` | 137 010 (135 067–138 954) | 129 914 (121 304–130 669) | −5 % |
| `process/socket/both/64x80000` | 129 467 (126 485–132 449) | 78 954 (67 962–80 346) | −39 % |
| `process/socket/destination/512x10000` | 14 702 (13 095–16 309) | 6 092 (5 796–6 360) | −59 % |
| `process/socket/source/512x10000` | 145 272 (142 235–148 310) | 119 960 (119 935–120 237) | −17 % |
| `process/socket/both/512x10000` | 82 088 (81 850–82 326) | 63 216 (63 196–63 325) | −23 % |
| `process/socket/both/64x80000/4-partitions` | 158 283 (155 080–161 486) | 92 710 (85 558–94 038) | −41 % |
| `process/socket/both/64x80000/8-partitions` | 173 432 (166 867–179 998) | 101 192 (95 743–103 276) | −42 % |

Peak resident memory of a spawned connector process (`VmHWM`), MiB, the median over the
processes a case spawned and the largest process, each the median of the rounds, base → branch:

| Case | Median process | Largest process |
|---|---|---|
| `process/socket/destination/64x80000` | 93.7 → 120.5 | 134.9 → 135.9 |
| `process/socket/source/64x80000` | 490.1 → 520.7 | 508.7 → 541.1 |
| `process/socket/both/64x80000` | 302.9 → 323.2 | 511.9 → 533.7 |
| `process/socket/destination/512x10000` | 30.5 → 32.4 | 34.5 → 36.2 |
| `process/socket/source/512x10000` | 450.8 → 477.6 | 454.4 → 479.5 |
| `process/socket/both/512x10000` | 241.2 → 256.2 | 456.3 → 480.5 |
| `process/socket/both/64x80000/4-partitions` | 333.7 → 375.3 | 581.6 → 648.8 |
| `process/socket/both/64x80000/8-partitions` | 372.3 → 410.1 | 668.1 → 732.5 |

- **The spawned destination in large frames moves 69 % more at 28 % less CPU a GB**: medians
  1 265 → 2 135 MB/s, the branch's rounds all above the base's, and 1.158 → 0.836 CPU seconds a
  GB in the destination's process, with 70 % fewer page faults. The regression the two stages
  recorded for a spawned socket destination in large frames is gone: base 1 258–1 279 MB/s
  against 1 615–2 432.
- **No case is lower.** No case's median throughput is lower on the branch; the smallest change
  is `process/socket/source/64x80000` at +2 %, its ranges overlapping. The destination's CPU a
  GB is lower in every case, and its range lies wholly below in four. The 4- and 8-partition
  cases move 33 % and 26 % more, and `process/socket/both/512x10000` 15 % more.
- **Faults are lower in every spawned case**, by 5–70 %, the branch's range wholly below the
  base's in each.
- **Peak memory rises in every spawned case**: the median process by 2–42 MiB, the largest
  process by 1–67 MiB, the largest being the 4-partition case's, 582 → 649 MiB, about 12 %.
  The 8-partition case's largest process rises 64 MiB, 668 → 733. The rule's memory clause, in
  the frames of the Method (6.9 MB, 6.6 MiB, and 0.86 MB, 0.82 MiB), case by case, median
  process: destination 64×80 000 +26.8 MiB, about 4 frames; source 64×80 000 +30.6, about 5;
  both 64×80 000 +20.4, about 3; destination 512×10 000 +1.9, about 2; source 512×10 000
  +26.7, about 33; both 512×10 000 +14.9, about 18; 4 partitions +41.6, about 6; 8
  partitions +37.8, about 6. The largest process of the partition cases rises +67.2 and
  +64.3 MiB, about 10 frames each. The clause holds for the destinations and for the large-frame
  source and both cases, a few frames each. It does not hold for `process/socket/source/512x10000`
  and `process/socket/both/512x10000`, whose medians rise by tens of small frames, nor for
  the partition cases, whose largest processes rise by about ten large ones.
- **The TLS controls stay within noise.** They carry no tunables, yet their medians read +14 %
  (destination) and +9 % (both) with every range overlapping the base's, and the base's own
  destination ranged 951–1 106 MB/s across its rounds; the round-to-round noise is about
  ±15 %, which is also the margin to read the small socket changes with. Their peak memory did
  not move (357 → 354 MiB for the destination's listeners). Their page faults fell from
  504 000 to 231 000 in the one branch round that sampled them, which the missing tunables do
  not explain; one round, so it is not read.
- **Adopted.** The host's own process, and a listening connector the host does not spawn, are not
  changed; an operator running either may set the same `GLIBC_TUNABLES` for that process.

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
  destination and 0.48 on the host), the task, at its most and with the process at its busiest,
  holds at most 0.68 of a core on the destination (0.40 + 1.15 − 0.87) and 0.62 on the host
  (0.31 + 0.79 − 0.48).
- **Over the socket**, the connection tasks hold 2 % of the host's samples and 0–3 % of each
  spawned destination's, at most 0.03 of a core.
- **Against the rule.** tls moves 86–92 % of the socket's MB/s (rule: at most 70 %), and no
  connection task holds more than 0.68 of a core even with all system time counted (rule: at
  least 0.8). Neither condition holds, so one connection a connector is kept: several
  connections would spread a task that does not limit the run.
- **CPU.** tls costs no more CPU a GB than the spawned socket destination here: 1.45 against 1.51
  all, with the socket destination's process taking 1.08 against 0.90, which is consistent with the
  page-fault cost recorded under "A served write in two stages".
- **The open question under "Partitions"**, whether one connection task framing TLS and HTTP/2
  holds the destination under one CPU, is answered: with its process at 1.15 CPUs busy, the
  connection task holds 0.37 of a core in user mode and at most 0.68 with all system time
  counted, so it is not what limits the destination.

## Frames decoded where they arrive

A served destination copies each batch frame once in user space as it arrives: `Bounded` gathers
the arriving message into one allocation, 64-byte aligned, and the decoder reads every buffer
where it lies, refusing a frame whose buffer is off the alignment its column's type needs (ADR
0038). Until this change it copied twice: relocation then laid each buffer out again at its
alignment, so that Arrow copied none, and the 2026-10-10 profile under "The copies before" found
relocation a third of the receive stage. The question was whether reading in place, with
relocation deleted, pays on production's placement, the connectors spawned over a socket.

The rule set before measuring: keep the change only if, with the connectors apart,
`served/process/socket/both/*` moves at least 10 % more MB/s or the spawned destination takes at
least 15 % less CPU a GB, and no case is more than 3 % slower than main.

Intel Core Ultra X7 358H, mains power, 2026-10-16, this change at `3585bc1f` against main at
`c990b7d1`, governor `powersave` with energy preference `performance`, each side built
beforehand and run under the measurement lock, the host on CPUs 0–3:

- **Apart.** The connectors' processes on CPUs 4–11:
  `just bench served 0-3 '^served/process/socket/(destination|source|both)/(64x80000|512x10000)$' 4-11`.
  Two rounds, main first in the first and this change first in the second. They stopped at two:
  both moved every `both` case the same way by more than their spread.
- **Sharing cores.** The same with the connectors on CPUs 0–3, three rounds, main first in the
  first and third.

The one-minute load average was 1.26–1.48 before each side's run and up to 3.04 after it, the
bench's own threads. Each figure is the median of the rounds and their range, with 1 GiB taken as
1 073.74 MB.

Apart:

| Case | Main, MB/s | This change, MB/s | Change |
|---|---|---|---|
| `source/64x80000` | 2 251 (2 203–2 299) | 2 624 (2 494–2 754) | +16.6 % |
| `source/512x10000` | 2 678 (2 665–2 691) | 2 811 (2 755–2 866) | +5.0 % |
| `destination/64x80000` | 1 718 (1 711–1 726) | 2 195 (2 079–2 311) | +27.7 % |
| `destination/512x10000` | 4 547 (4 482–4 612) | 4 452 (4 261–4 643) | −2.1 % |
| `both/64x80000` | 1 344 (1 269–1 420) | 1 776 (1 754–1 799) | +32.1 % |
| `both/512x10000` | 3 109 (2 877–3 341) | 3 372 (3 219–3 525) | +8.5 % |

| CPU s a GB, median (range) | Main | This change | Change |
|---|---|---|---|
| `destination/64x80000`, destination | 0.740 (0.739–0.742) | 0.583 (0.569–0.597) | −21.3 % |
| `destination/64x80000`, host (the bench's process) | 0.398 (0.389–0.408) | 0.377 (0.357–0.397) | −5.4 % |
| `destination/64x80000`, all | 1.135 (1.130–1.141) | 0.961 (0.929–0.993) | −15.4 % |
| `destination/512x10000`, destination | 0.464 (0.458–0.470) | 0.403 (0.389–0.418) | −13.0 % |
| `destination/512x10000`, host (the bench's process) | 0.305 (0.302–0.308) | 0.309 (0.302–0.317) | +1.5 % |
| `destination/512x10000`, all | 0.768 (0.760–0.777) | 0.712 (0.692–0.733) | −7.3 % |
| `both/64x80000`, destination | 0.868 (0.827–0.909) | 0.632 (0.613–0.651) | −27.2 % |
| `both/64x80000`, host (the bench's process) | 1.042 (0.965–1.118) | 0.800 (0.795–0.804) | −23.2 % |
| `both/64x80000`, source | 0.576 (0.548–0.604) | 0.544 (0.528–0.560) | −5.6 % |
| `both/64x80000`, all | 2.502 (2.340–2.665) | 1.947 (1.907–1.987) | −22.2 % |
| `both/512x10000`, destination | 0.479 (0.468–0.490) | 0.407 (0.386–0.428) | −15.0 % |
| `both/512x10000`, host (the bench's process) | 0.495 (0.486–0.505) | 0.456 (0.431–0.480) | −8.1 % |
| `both/512x10000`, source | 0.461 (0.452–0.470) | 0.470 (0.451–0.489) | +2.0 % |
| `both/512x10000`, all | 1.435 (1.399–1.472) | 1.338 (1.275–1.401) | −6.8 % |

Sharing cores:

| Case | Main, MB/s | This change, MB/s | Change |
|---|---|---|---|
| `source/64x80000` | 2 020 (1 914–2 022) | 2 255 (1 720–2 292) | +11.7 % |
| `source/512x10000` | 3 051 (3 023–3 141) | 3 267 (2 758–3 488) | +7.1 % |
| `destination/64x80000` | 1 774 (1 556–1 777) | 2 137 (1 437–2 158) | +20.4 % |
| `destination/512x10000` | 3 293 (3 041–3 308) | 3 715 (3 205–3 758) | +12.8 % |
| `both/64x80000` | 1 171 (1 109–1 238) | 1 338 (855–1 450) | +14.2 % |
| `both/512x10000` | 2 133 (2 038–2 136) | 2 284 (2 082–2 358) | +7.1 % |

| CPU s a GB, median (range) | Main | This change | Change |
|---|---|---|---|
| `destination/64x80000`, destination | 0.658 (0.656–0.663) | 0.523 (0.520–0.771) | −20.5 % |
| `destination/64x80000`, host (the bench's process) | 0.403 (0.399–0.483) | 0.389 (0.386–0.534) | −3.5 % |
| `destination/64x80000`, all | 1.057 (1.051–1.142) | 0.926 (0.910–1.305) | −12.4 % |
| `destination/512x10000`, destination | 0.447 (0.445–0.450) | 0.386 (0.383–0.440) | −13.6 % |
| `destination/512x10000`, host (the bench's process) | 0.332 (0.329–0.339) | 0.328 (0.326–0.370) | −1.2 % |
| `destination/512x10000`, all | 0.778 (0.773–0.785) | 0.714 (0.709–0.810) | −8.2 % |
| `both/64x80000`, destination | 0.658 (0.657–0.671) | 0.495 (0.482–0.747) | −24.8 % |
| `both/64x80000`, host (the bench's process) | 1.105 (1.029–1.182) | 1.021 (0.919–1.436) | −7.6 % |
| `both/64x80000`, source | 0.393 (0.388–0.400) | 0.377 (0.375–0.552) | −4.1 % |
| `both/64x80000`, all | 2.143 (2.077–2.239) | 1.900 (1.788–2.730) | −11.3 % |
| `both/512x10000`, destination | 0.435 (0.431–0.438) | 0.381 (0.361–0.417) | −12.4 % |
| `both/512x10000`, host (the bench's process) | 0.606 (0.596–0.609) | 0.557 (0.539–0.621) | −8.1 % |
| `both/512x10000`, source | 0.324 (0.321–0.326) | 0.318 (0.314–0.349) | −1.9 % |
| `both/512x10000`, all | 1.359 (1.352–1.368) | 1.261 (1.216–1.390) | −7.2 % |

The first round sharing cores ran this change's side disturbed: every case of it was slower than
main's and took up to two fifths more CPU a GB, `both/64x80000` at 855 MB/s, while the third
round, in the same order, and the second agree with each other and with the rounds apart. Its
figures stay in the medians and ranges above.

One profile on the `just profiling served` build, `$bin`, 15 seconds, the connectors apart:

```
RDLT_BENCH_CONNECTOR_CORES=4-11 perf record -e task-clock -c 1000000 --call-graph fp -- \
  taskset -c 0-3 "$bin" --bench '^served/process/socket/both/64x80000$' --profile-time 15
```

A sample is one millisecond of a thread's user-mode CPU. The decode is every sample whose stack
holds the decoder's reading of a frame's batch; the destination's processes are those whose
samples hold it on the served write's side.

| | Host (the bench's process) | Spawned destination | Spawned source |
|---|---|---|---|
| Samples | 3 667 | 1 719 | 7 588 |
| Decode, of the process | 6.9 % | 23.0 % | 0 |
| Arrow's UTF-8 validation, of the decode | | 84.8 % | |
| Relocation | 0 | 0 | 0 |
| `libc` leaf under the decode | 1 | 2 | 0 |

The spawned destination's receive stage, the served write's receiving of frames, is 56.1 % of its
samples, against 66.7 % in the destination case on 2026-10-10: the decode 41 % of it, Arrow's
validation most of that, and `Bounded`'s gathering copy the 58 % whose leaf is `libc`. The three
`libc` leaves under the decode are the allocator's, under Arrow's building of an array's data, not a
copy of a body.

- **Against the rule.** Apart, `both/64x80000` moves 32.1 % more MB/s and its destination takes
  27.2 % less CPU a GB; `both/512x10000` 8.5 % more and 15.0 % less. Both pass, by MB/s or by
  the destination's CPU. The slowest case is `destination/512x10000` apart, 2.1 % slower than
  main within ranges that overlap, against the 3 % allowed. The change stays.
- **CPU.** Every spawned destination takes 12–27 % less CPU a GB, apart or sharing cores. The
  host, which decodes what a served source reads, takes up to 23 % less and at most 1.5 % more;
  the sources are within 6 % either way.
- **What is left.** `Bounded`'s gathering copy is the one copy of an arriving frame, and a
  frame's decode is Arrow's validation of it, its UTF-8 most of all and its dictionary keys'
  bounds.

### The copies before

Before this change, a served destination copied each batch frame twice in user space as it
arrived. `Bounded` gathered the arriving message into one buffer, the room growing as it arrived,
and relocation then laid each buffer out at the alignment its column's type needs, so that Arrow
copied none to align it. The question then was whether dropping `Bounded`'s copy would pay, with
relocation kept as the one copy: `Bounded` would keep a message as the transport's chunks, and
relocation would gather from them.

The rule set before measuring: build it only if, profiled on `served/socket/destination/64x80000`
on CPUs 4–11, both hold: relocation plus `Bounded`'s copy are at least 15 % of the receive stage's
cycles, and the receive stage holds at least 0.8 of a core. Otherwise ADR 0038 stands unchanged.

Intel Core Ultra X7 358H, mains power, 2026-10-10, `61957288`, governor `powersave`, under the
measurement lock, in two layouts:

- **In one process.** The host, both connectors and the destination in the bench's process on
  CPUs 4–11: the judged case.
- **Apart.** The destination in a process of its own on CPUs 4–11, the host on 0–3
  (`served/process/socket/destination/64x80000`), which corroborates.

Each layout was profiled twice on the `just profiling served` build, 15 seconds a profile:

```
perf record -e task-clock -c 1000000 --call-graph fp -- \
  taskset -c 4-11 "$bin" --bench '^served/socket/destination/64x80000$' --profile-time 15
```

and, apart, `RDLT_BENCH_CONNECTOR_CORES=4-11 taskset -c 0-3` over the process case. A sample is one
millisecond of a thread's user-mode CPU, so a share of samples is a share of CPU time, which
stands in for the rule's cycles only as far as the cores' clocks held steady; the verdict rests on
the cores condition, which fails either way. The one-minute load averages around the profiles were
2.2–3.2. One `just bench served` round of each layout (`just bench served 4-11
'^served/socket/destination/64x80000$'` and `just bench served 0-3
'^served/process/socket/destination/64x80000$' 4-11`) gives the throughput; its load average
before was 1.77 for the first and 3.56 for the second. Rounds stopped at two a layout: they agree
to within three points and sit far from the cores threshold, and a rule that two consistent rounds
settle needs no more.

The receive stage is every sample whose stack holds the read of a frame into a decoded batch. The
leaf of 74–77 % of its samples is `libc`, stripped here, and frame pointers lose its caller, so
each such sample went to the first frame above it outside `libc`: `realloc` under the growth of
`Bounded`'s room is its realloc copy, a callee of the decoder's frame is relocation, and a callee
of the `Bounded` future is its gathering copy. A profile with DWARF unwinding gives the same three
classes directly for the samples it could unwind, within a few points (relocation 37 %, gathering
35 %, realloc 7 %), but most of its `libc` samples had no stack, so only the proportions are
taken from it.

| | In one process, round 1 | Round 2 | Apart, round 1 | Round 2 |
|---|---|---|---|---|
| Receive stage, cores, median (max) | 0.495 (0.532) | 0.524 (0.556) | 0.371 (0.410) | 0.369 (0.407) |
| Receive stage, share of the process | 40.9 % | 40.3 % | 66.7 % | 67.0 % |
| Relocation, of the receive stage | 33.7 % | 33.9 % | 33.9 % | 34.4 % |
| `Bounded`'s copy, of the receive stage | 42.3 % | 42.4 % | 40.3 % | 40.0 % |
| Relocation and `Bounded`'s copy | 76.0 % | 76.3 % | 74.2 % | 74.3 % |
| Scan (`scan::decoded`) | 0.1 % | 0.1 % | 0.2 % | 0.1 % |
| Decode total | 56.8 % | 56.8 % | 58.9 % | 59.3 % |

"Share of the process" is of the destination's processes apart. `Bounded`'s copy is the gathering
copy and the realloc growth: 34.4 and 8.0 % in the first round in one process, 31.3 and 11.1 % in
the second, 33.9 and 6.4 % and 34.1 and 5.8 % apart. Decode total includes relocation and Arrow's
building of the arrays. The copy that splits off a finished message never shows.

The bench round, with 1 GiB taken as 1 073.74 MB: in one process 1 567 MB/s (1.459 GiB/s, range
1.425–1.499), 1.263 CPU s a GB and 2.01 CPUs busy at 0.66 instructions a cycle; apart 1 427 MB/s
(1.329 GiB/s, range 1.217–1.460), 1.569 CPU s a GB (0.494 the bench's process, 1.047 the
destination's), the destination's process 1.528 CPUs busy (1.422–1.620), 2.03 busy overall at 0.83
instructions a cycle.

#### Kernel time

User-mode samples miss the receive stage's page faults, the only kernel time known for it: it makes
no system call of its own. The process is 2.01 CPUs busy by `perf stat` against 1.18 user cores
sampled, so about 0.8 of a core is kernel time. Page faults were sampled
(`perf record -e page-faults -c 8 --call-graph fp`, 15 seconds a layout) and attributed with the
same classes:

| | In one process | Apart |
|---|---|---|
| Faults a second, steady median | 239 000 | 236 000 |
| In the receive stage | 59.0 % (132 000 a second) | 97.5 % (229 000 a second) |
| Its `Bounded` gathering, relocation, realloc growth | 27.8, 24.5, 6.7 % | 45.2, 46.3, 6.0 % |
| In the writer stage | 1.1 % | 1.8 % |

In one process, 39.8 % of the faults are the host's and the source side's, which share the process.

A fault's kernel cost was measured as the change in system time over the change in faults, one
process against itself with glibc's heap trimming off and the mmap threshold fixed at 32 MiB
(`GLIBC_TUNABLES=glibc.malloc.trim_threshold=268435456:glibc.malloc.mmap_threshold=33554432`, as
under "A served write in two stages"), interleaved, 15 seconds each:

| Run | User s | System s | Page faults |
|---|---|---|---|
| Defaults 1 | 17.66 | 12.93 | 2 828 861 |
| Tuned 1 | 20.81 | 10.27 | 161 078 |
| Defaults 2 | 18.13 | 13.09 | 2 944 622 |
| Tuned 2 | 21.69 | 10.23 | 161 306 |

That is 1.00 and 1.03 µs a fault, which includes the `mremap`, `munmap` and `madvise` the
thresholds also remove. The tuned runs do about a fifth more work in the same time, so the cost is
taken as 1.5 µs at the pessimistic end. Faults are 22–33 % of the process's system time (2.8
million faults at 1.0–1.5 µs is 2.8–4.3 s of 13 s); the rest, the socket's reads and writes, runs
in the connection task and the host's.

The receive stage's cores with its faults counted, its user cores plus its faults a second times
the cost of a fault:

| | In one process | Apart |
|---|---|---|
| User cores | 0.51 | 0.37 |
| Faults at 1.0 µs, 1.5 µs | 0.13, 0.20 | 0.23, 0.34 |
| Receive stage, cores at 1.0 µs, 1.5 µs | 0.64, 0.71 | 0.60, 0.71 |
| Upper bound, all the process's faults at 1.0 µs, 1.5 µs | 0.75, 0.87 | 0.61, 0.72 |

The upper bound is not reachable in one process: 40 % of its faults are the host's. Only all of
them at 1.5 µs, 0.87, crosses 0.8, and the measured split rules it out.

- **Where the destination's cycles go.** Apart, in user mode: the receive stage 66.7 % of the
  process's samples, the writer stage (`serve::write::write`, the sink included) 27.0 %, the
  runtime 4.2 % and the connection task 1.8 %. In one process, with the host and the source side
  beside it, the receive stage is 40.9 %, the writer stage 15.3 % and the connection task 2.7 %.
  The process holds 1.2 user cores in one process and the destination's process 0.55 apart, for
  1 427–1 567 MB/s.
- **Arrow's UTF-8 validation** is about a fifth of the receive stage: `run_utf8_validation` 7 %
  and the array-building closure around it 11–13 %. The scan is 0.1 %.
- **Realloc growth** is 6–11 % of the receive stage, of 74–77 % that is copying.
- **Against the rule.** The copy condition holds: 76 % in one process and 74 % apart, against
  15 %. The cores condition does not: 0.495 and 0.524 in user mode in one process, 0.37 apart, and
  at most 0.71 with the faults counted at 1.5 µs, against 0.8. So ADR 0038 stands, and `Bounded`
  keeps gathering each message whole.
- **What removing both copies could free at most**, the stage's copy share times its cores in user
  mode: 0.76 × 0.495 = 0.38 of a core in one process (0.763 × 0.524 = 0.40 in the second round) and
  0.742 × 0.371 = 0.28 apart, from a process that holds 1.2 user cores and moves 1 427–1 567 MB/s.

## Codec work on the runtime's workers

Every encode and decode of a batch frame runs inline on a tokio worker: the host's write encode, the
served write's decode, the served read's encode and the host read's decode. The lane yields to the
runtime after each piece it encodes. The question was whether moving that work onto a pool would
pay, in latency (a long poll holding a worker, a heartbeat delayed) or in throughput.

The rule set before measuring: build the move if either of these holds.

- **The latency condition.** At frames near the 64 MiB limit, a codec poll holds a worker for at
  least 1 ms, and heartbeats arrive later than a quarter of their patience.
- **The prototype.** A prototype that runs the lane's encode off the workers moves at least 10 %
  more on `served/socket/destination/64x80000` with the bench on CPUs 0–3. Better is read by this
  file's protocol: the prototype's lowest round is above the base's highest, and the median gain
  meets the threshold.

Intel Core Ultra X7 358H, mains power, 2026-10-10, `d2bcc6e4`, the bench on CPUs 0–3 under the
measurement lock. For the latency condition, a build with tokio's unstable poll-time histogram (100
µs buckets) on each worker, a timer around each codec call, a timer from each heartbeat's send to
its echo, a heartbeat every 100 ms, and frames of 8 batches of 640 000 rows (the same 5.12 M rows,
57.36 MB a frame on the host's write side and 55.92 MB from the served read, uncut, under the 64 MiB
limit). Two runs of each of the destination and the source case, socket transport. This measurement
code is not kept. For the prototype, the lane's encode ran on a `spawn_blocking` thread, a thread
beside the two workers within CPUs 0–3, not the engine's compute pool, with the yield dropped; the
case and the tls guard each ran five rounds, base and prototype interleaved, the order alternating.
The rounds never agreed closely enough to stop early. Load average over a minute was 1.3–2.0 over
the latency runs and 1.6–2.8 over the rounds.

Each codec call, in milliseconds over two runs of its case:

| Site | Calls a run | Median | p99 | Maximum |
|---|---|---|---|---|
| Host write encode (half of the calls do nothing) | 272 | 0.001–0.002 | 29.62–29.75 | 31.08–32.14 |
| Served write decode | 170 | 26.05–26.26 | 28.82–29.36 | 30.45–32.11 |
| Served read encode | 136 | 24.88–25.69 | 30.84–31.85 | 33.68–35.76 |
| Host read decode | 136 | 25.53–26.47 | 28.58–29.63 | 29.30–33.04 |

Each worker's polls of at least 1 ms in a run, and the heartbeats' echoes (100–109 beats a run):

| Run | Polls of 1 ms or more, per worker | Longest poll, at least, ms | Echo median, ms | Echo p99, ms | Echo maximum, ms |
|---|---|---|---|---|---|
| Destination, first | 364 / 357 | 57.3 | 0.088 | 1.93 | 29.4 |
| Destination, second | 368 / 367 | 59.8 | 0.073 | 25.3 | 28.4 |
| Source, first | 405 / 433 | 59.9 | 0.106 | 0.91 | 12.0 |
| Source, second | 432 / 395 | 61.3 | 0.068 | 1.53 | 2.47 |

The five rounds of `socket/destination/64x80000`, MB/s, and the CPU a GB:

| Round | Base | Prototype | Gain | CPU s a GB, base → prototype |
|---|---|---|---|---|
| 1 | 1 951 | 2 618 | +34.2 % | 0.88 → 0.73 |
| 2 | 2 340 | 2 805 | +19.9 % | 0.71 → 0.66 |
| 3 | 2 713 | 2 531 | −6.7 % | 0.64 → 0.81 |
| 4 | 2 443 | 2 459 | +0.7 % | 0.68 → 0.79 |
| 5 | 2 165 | 2 588 | +19.5 % | 0.76 → 0.74 |
| Median (range) | 2 340 (1 951–2 713) | 2 588 (2 459–2 805) | +10.6 % | 0.71 → 0.74 |
| `tls/destination/64x80000`, median (range) | 1 668 (1 473–1 731) | 1 774 (1 661–1 788) | +6.3 % | 1.07 → 1.14 |

- **A long poll, an on-time heartbeat.** At about 56 MB a frame, each codec call that encodes or
  decodes a frame holds a worker for 25–36 ms (the host write's calls that find nothing to encode
  return within microseconds), and each worker has 357–433 polls of at least 1 ms a run. With two workers, a
  heartbeat's echo waited at most 29.4 ms: about one codec call, 0.4 % of a quarter of the default
  patience (7.5 s) and a fifth of the 150 ms a quarter of it comes to at the 100 ms setting.
- **The prototype.** Its median clears 10 % by 0.6 of a point. Its rounds sit within 2 459–2 805,
  but the base's spread over 1 951–2 713, so the ranges overlap and the gain is not established on
  this machine; the per-round gains run from −6.7 to +34.2 %. It also ran beside the workers on a
  thread of its own, within the same four CPUs, rather than on the compute pool: the process kept
  1.9–2.0 CPUs busy against 1.65–1.8. Its CPU a GB rose 5 %.
- **Against the rule.** The latency condition fails on its heartbeats and the prototype on its
  ranges, so the codec work stays on the workers and no pool is built.

## Credit a quarter window at a time

A receiver grants back each frame's bytes as it takes it, one credit message a frame: the host
reading from a spawned source, and a spawned destination answering the host's write. At the
0.86 MB frames of `512x10000` that is 512 messages a run each way. The question was whether
granting less often pays on the production placement, connectors spawned over a socket.

The rule set before measuring: build it only if a prototype moves
`process/socket/destination/512x10000` or `process/socket/source/512x10000` at least 5 % beyond
noise. Beyond noise means the prototype's median MB/s is at least 1.05 times main's over
interleaved rounds, and either its range lies wholly above main's or the paired ratios' bootstrap
95 % interval lies above 1.0. No other `process/socket` case may fall beyond noise.

The prototype kept what a receiver owes, each frame's bytes and whatever the window grows by, and
granted it in one message once it reached a quarter of the window. Both receivers sent a credit
only then. At 0.86 MB frames under the 4 MiB window that is a credit every second frame: 0.5
messages a frame, against 1.0 on main, as a test counting the grants of 512 frames showed. At the
6.9 MB frames of `64x80000`, whose window grows to 13.8 MB, every frame still carries its own
credit, 1.0 both ways. A property test held its liveness: a sender out of credit was always
granted once its receiver had taken every frame it sent.

Intel Core Ultra X7 358H, mains power, 2026-10-13, main at `8daeb40b` against the prototype on
it, governor `powersave` with energy preference `performance`. `just bench served 0-3
'process/socket/(destination|source|both)/512x10000' 4-11`: the host on CPUs 0–3, each spawned
connector on 4–11. Three pairs of rounds, main and the prototype interleaved, the order
alternating, under the measurement lock. Each round started at a one-minute load average of
0.86–1.13; the load after each was 1.6–2.7, the bench's own threads.

MB/s, each round, and the prototype's change on its paired main round:

| Case | Pair | Main | Prototype | Change |
|---|---|---|---|---|
| `destination` | 1 | 4 155 | 3 062 | −26.3 % |
| | 2 | 4 050 | 3 768 | −7.0 % |
| | 3 | 4 275 | 3 933 | −8.0 % |
| | Median (range) | 4 155 (4 050–4 275) | 3 768 (3 062–3 933) | −9.3 % |
| `source` | 1 | 3 766 | 3 619 | −3.9 % |
| | 2 | 3 318 | 3 549 | +7.0 % |
| | 3 | 3 722 | 3 573 | −4.0 % |
| | Median (range) | 3 722 (3 318–3 766) | 3 573 (3 549–3 619) | −4.0 % |
| `both` | 1 | 3 018 | 2 933 | −2.8 % |
| | 2 | 2 659 | 2 932 | +10.3 % |
| | 3 | 3 061 | 2 991 | −2.3 % |
| | Median (range) | 3 018 (2 659–3 061) | 2 933 (2 932–2 991) | −2.8 % |

CPU seconds a GB of each process, and the syscalls of one run in test mode, both processes'
threads counted, each the median over the three rounds, main → prototype:

| Case | Bench's process | Source | Destination | `writev` | `recvfrom` |
|---|---|---|---|---|---|
| `destination` | 0.421 → 0.454 | in the bench's | 0.476 → 0.486 | 3 067 → 2 736 | 2 783 → 2 583 |
| `source` | 0.514 → 0.505 | 0.429 → 0.424 | in the bench's | 3 450 → 3 101 | 4 302 → 4 168 |
| `both` | 0.662 → 0.662 | 0.489 → 0.471 | 0.509 → 0.525 | 7 812 → 7 402 | 7 507 → 7 225 |

- **Fewer messages.** A run made 331–410 fewer `writev` calls, for the 256 credit messages each
  receiver no longer sent (two receivers in `both`), and 134–282 fewer `recvfrom` calls. Each
  process's CPU a GB moved by −4 to +8 %, the most in the host's when the destination is spawned.
- **No faster.** The destination ran slower in every pair, its rounds wholly below main's. A
  likely reason: granted a quarter window late, the host's writer runs up to a frame less ahead
  of the destination. The source and both connectors served moved either way across pairs, with
  medians 4.0 and 2.8 % below main's. The three pairs agreed closely enough to stop early: no
  case's median came near 5 % above main.
- **Against the rule.** Not built: the rule, at least 5 % more MB/s on the spawned destination or
  source in small frames, was not met (destination −9.3 %, source −4.0 % at the median). Each
  frame's credit is still granted as it is taken.

## A served read bounded by bytes

A served read's channel held sixteen of its source's events of any size. It now admits them
within a window of the host's frame limit: a push takes the lesser of what it keeps alive and the
window until it leaves the channel, and every other event passes free. The host also passes a JSON
push on in the message it arrived in rather than copying it. The question was whether bounding
the channel by bytes costs the spawned path anything. The bench's host tells its connectors the
wire's default limits, a 64 MiB frame, so the window holds several of its largest pushes, 6.9 MB
each, and sixteen of every smaller frame's, as many as the channel's count lets in.

The rule set before measuring: every `process/socket` case that serves the source, alone or with
the destination, moves within 3 % of main's median MB/s.

Intel Core Ultra X7 358H, mains power, 2026-10-24, main at `e99a9dc6` against the change on it,
governor `powersave` with energy preference `performance`. Each tree's `served` bench, built with
`cargo bench --no-run`, ran `--bench 'process/socket/(source|both)'` with the host on CPUs 0–3
(`taskset`) and each spawned connector on 4–11. Five pairs of rounds, main and the change
interleaved, the order alternating, under the measurement lock, then six more pairs of the two
`source` cases alone. Each round started at a one-minute load average of 2.1–3.8, the bench's own
threads among it.

MB/s over the five pairs, the median with its range, and the change's median on main's:

| Case | Main | Change | Medians |
|---|---|---|---|
| `source/64x80000` | 1 804 (1 767–2 272) | 1 837 (1 572–2 172) | +1.8 % |
| `both/64x80000` | 1 551 (1 484–1 818) | 1 615 (1 335–1 932) | +4.1 % |
| `source/512x10000` | 2 018 (1 556–2 354) | 1 669 (1 523–2 014) | −17.3 % |
| `both/512x10000` | 2 533 (2 392–3 013) | 2 473 (2 240–2 828) | −2.4 % |
| `both/64x80000/4-partitions` | 1 660 (1 575–1 793) | 1 652 (1 524–1 807) | −0.5 % |
| `both/64x80000/8-partitions` | 1 608 (1 537–1 684) | 1 591 (1 459–1 697) | −1.1 % |
| `both/64x80000/16-partitions` | 1 436 (1 387–1 528) | 1 487 (1 346–1 542) | +3.6 % |
| `both/512x10000/4-partitions` | 2 622 (2 361–2 771) | 2 607 (2 319–2 622) | −0.6 % |
| `both/512x10000/16-partitions` | 2 388 (2 237–2 443) | 2 355 (2 255–2 425) | −1.4 % |
| `both/5120x1000` | 1 749 (1 723–1 762) | 1 719 (1 493–1 736) | −1.7 % |
| `both/5120x1000/4-partitions` | 1 518 (1 404–1 537) | 1 480 (1 348–1 501) | −2.5 % |
| `both/5120x1000/16-partitions` | 1 453 (1 412–1 468) | 1 434 (1 251–1 467) | −1.3 % |

- **One case strayed, and did not hold.** After the first three pairs every median was within 3 %
  of main's, from −2.3 % to +1.8 %. Two more pairs left `source/512x10000` 17.3 % below main's: main
  ran it at 2 354 and 1 556 MB/s in them, the change at 1 669 and 1 614. The six pairs of the
  `source` cases alone put it 4.1 % above main's (main 1 756, 1 610–2 005; the change 1 828,
  1 633–1 886), and `source/64x80000` 5.5 % above. Over all eleven pairs `source/512x10000` is
  2.1 % below main's median (main 1 851, the change 1 812; the paired changes' median −4.0 %) and
  `source/64x80000` 0.2 % below.
- **Within the rule.** Every median is within 3 % of main's on all the pairs each case ran, or above
  it. Single rounds strayed 10–30 % either way, main's as much as the change's. The change adds to
  each push only the window's admission: what the push keeps alive, counted by its buffers, and a
  permit of a semaphore that sixteen pushes of `512x10000` never fill.

## Through a writer a core

The production slice above writes through one lane, its sink declaring one writer, so it cannot
tell whether the engine, the wire or the sink stops it scaling with partitions. Nine more process
cases serve both connectors over the socket in each size of frame, 64 × 80 000, 512 × 10 000 and
5 120 × 1 000 rows, from 1, 4 and 16 partitions, into a sink declaring as many writers as the
host has lanes, one a core: four on CPUs 0–3. Each is named after its one-writer row with
`/<n>-writers`, as `served/process/socket/both/64x80000/16-partitions/4-writers`. The one-writer
rows stay, and the caveat about lanes in the method applies to them alone.

- **Running.** `just bench served 0-3 process/socket/both 4-11` runs these beside the one-writer
  rows and the many-table cells below; `just bench served 0-3 writers 4-11` runs them alone.
- **Readings.** As the other process cases: MB/s, CPU seconds a GB of each process, the CPUs each
  keeps busy, and the last run's readings, intake waits, lanes written and lane flushes by cause
  among them.
- **Checks.** Every run checks every row arrives, and fails where it wrote through fewer lanes
  than its partitions or its sink's writers, whichever are fewer: such a run is a broken bench,
  not a figure.

Measured in the same three rounds as the production slice above, with it, on the same tree
and cores; the medians of the rounds, the throughput's range in brackets. A run that wrote through
fewer lanes than its partitions and writers allow would have failed; none did.

| Frames | Partitions | MB/s | CPU s a GB: all | Host's, source's, destination's | CPUs busy: host / source / destination | Lanes written | Flushes: pressure + commit |
|---|---|---|---|---|---|---|---|
| 64×80 000 | 1 | 1 612 [1 559–1 875] | 2.05 | 0.87, 0.55, 0.63 | 1.41 / 0.88 / 1.02 | 1 | 5 + 1 |
| 64×80 000 | 4 | 1 576 [1 523–1 758] | 2.17 | 0.89, 0.60, 0.69 | 1.39 / 0.99 / 1.08 | 4 | 20 + 4 |
| 64×80 000 | 16 | 1 358 [1 261–1 374] | 2.46 | 1.02, 0.70, 0.75 | 1.31 / 0.96 / 0.99 | 4 | 27 + 3 |
| 512×10 000 | 1 | 2 937 [2 772–3 050] | 1.50 | 0.57, 0.50, 0.43 | 1.68 / 1.48 / 1.30 | 1 | 5 + 1 |
| 512×10 000 | 4 | 2 217 [2 150–2 442] | 1.75 | 0.65, 0.55, 0.55 | 1.45 / 1.23 / 1.23 | 4 | 20 + 4 |
| 512×10 000 | 16 | 1 997 [1 969–2 000] | 1.91 | 0.73, 0.61, 0.56 | 1.41 / 1.20 / 1.13 | 4 | 20 + 4 |
| 5 120×1 000 | 1 | 1 746 [1 698–1 756] | 2.77 | 1.06, 0.80, 0.92 | 1.85 / 1.38 / 1.60 | 1 | 5 + 1 |
| 5 120×1 000 | 4 | 1 697 [1 682–1 708] | 3.01 | 1.16, 0.89, 0.96 | 1.95 / 1.49 / 1.62 | 4 | 19 + 4 |
| 5 120×1 000 | 16 | 1 690 [1 674–1 692] | 3.05 | 1.15, 0.96, 0.94 | 1.94 / 1.62 / 1.59 | 4 | 21 + 4 |

**Why throughput falls with partitions.** Writers do not stop it: through a writer a core, as
through one, throughput falls from 1 to 16 partitions, by 16 %, 32 % and 3 % in large, small and
1 000-row frames, against 5 %, 12 % and 17 % through one writer. Nor does the budget: intake waits
stay under 3 % of the run, and the same large frames from sixteen partitions within 1 GiB move no
more (below). What rises with partitions is CPU a GB while the CPUs each process keeps busy fall:
through one writer the source's, 0.56 to 0.72 CPU seconds a GB in large frames and 0.50 to 0.61
in small, the host's and the destination's within 0.03; through a writer a core every process's,
by 0.11 to 0.16 in both. The decline is work done a partition, in the source above all, and a
writer, in the host and the destination; not the sink's single writer.

**One partition over four writers.** Four partitions through four writers move 1 576 MB/s
[1 523–1 758] against 1 612 [1 559–1 875] from one partition through the same sink, 0.98 times,
the ranges overlapping: spreading one partition's writes over more lanes has no throughput to
reach on this path.

**Within 1 GiB.** Large frames from sixteen partitions, run again with the engine within 1 GiB in
the same rounds (`/1GiB`):

| Sink's writers | Budget | MB/s | CPU s a GB | Decode waits | Flushes: pressure + commit |
|---|---|---|---|---|---|
| 1 | 256 MiB | 1 449 [1 413–1 523] | 2.27 | 29.0 % | 9 + 1 |
| 1 | 1 GiB | 1 346 [1 304–1 348] | 2.32 | 0.7 % | 1 + 1 |
| 4 | 256 MiB | 1 358 [1 261–1 374] | 2.46 | 32.0 % | 27 + 3 |
| 4 | 1 GiB | 1 298 [1 280–1 351] | 2.43 | 0.1 % | 4 + 4 |

The larger budget removes nearly every pressure flush and decode wait, and the runs are 7.1 % and
4.4 % slower at the median, not faster: the pressure flushes of served lanes are not what this
case pays.

## Many tables

The passthrough bench's many-table cells ([passthrough.md](passthrough.md), "Many tables"), with
both connectors spawned over the socket for each run and the sink declaring a writer a core:
`served/process/socket/both/tables/followed/{16,64,192}` and
`served/process/socket/both/tables/snapshot/256`. The engine is configured as in process: a
followed cell reads a partition more than its streams, within the default 256 MiB; the snapshot
runs at the engine's defaults.

A served source reads each partition over a call of its one connection, and a followed read holds
its call for the run. A connection carries at most 200 calls (`rdlt_wire::limits::MAX_CALLS`), the
heartbeat among them; the engine keeps one of the rest for its other call into the source and a
slot for every other read, so a run follows at most 197 unbounded partitions of a served source and
refuses one more as it starts: with `source_calls_exceeded` where its partitions are more than 198,
as in a cell of 256 streams, which reads a partition more than its streams, and with
`partitions_too_few` where they are not (ADR 0042). 256 followed streams cannot be served; 192 is
the largest of the in-process counts within that.

- **Running.** `just bench served 0-3 tables 4-11`. A followed run lasts its ten-second span, so
  each followed cell takes about two minutes: a warm-up run and ten samples of one run each.
- **Readings.** Over a cell's ten runs, the median and range of each commit phase's time a commit
  (the flush, the commit and the acknowledgement, which is one `committed` call a stream); the CPU
  seconds a wall second of the bench's process, the source's and the destination's, while the
  streams are quiet, from seven seconds to ten, or over the snapshot's run; the most the reads kept
  of their share, from the report's `kept_peak`, against the share, a quarter of the budget; each
  process's resident peak, and the bench's above its base before the run; then the last run's
  readings, lane flushes by cause, retirements among them.
- **Checks.** Every run checks every row its streams pushed lands.

Measured in the same three rounds as the production slice above, where each read took a part of its
own, so the cell of 192 streams ran within the least budget that admitted its partitions; the
medians of each cell's ten runs in each round, then the median of the rounds and their range. A
commit's phases are its flush, its commit and the acknowledgement, one `committed` call a stream.
The tree these rounds ran, a design before today's, also timed a horizon, which read 0.000
throughout, and a log between the flush and the commit, which the table leaves out; idle CPU is each
process's CPU seconds a wall second while the streams are quiet, or over the snapshot's run.

| Cell | Budget | Flush | Commit | Acknowledgement | Idle CPU: bench / source / destination | Reads kept at most, of their share | Flushes: commit + retired |
|---|---|---|---|---|---|---|---|
| `followed/16` | 256 MiB | 0.83 ms (0.80–0.83) | 0.26 ms (0.23–0.27) | 0.82 ms (0.79–0.82) | 0.006 / 0.000 / 0.000 | 29 KiB of 64 MiB | 32 + 0 |
| `followed/64` | 256 MiB | 2.37 ms (2.28–2.44) | 0.39 ms (0.37–0.41) | 3.10 ms (3.05–3.18) | 0.006 / 0.001 / 0.000 | 117 KiB of 64 MiB | 128 + 0 |
| `followed/192` | 386 MiB | 3.48 ms (3.30–3.64) | 0.77 ms (0.74–0.80) | 6.88 ms (6.69–7.20) | 0.006 / 0.002 / 0.000 | 352 KiB of 96.5 MiB | 128 + 832 |
| `snapshot/256` | 256 MiB | 0.79 ms (0.78–0.85) | 1.95 ms (1.89–2.09) | 10.86 ms (10.85–10.98) | 1.955 / 0.822 / 1.219 | 29 KiB of 64 MiB | 28 + 176, and 64 for pressure |

- Acknowledgement grows with the streams, 0.82 to 6.88 ms a commit from 16 to 192: about 36 µs a
  stream, one round trip each, in turn. At 192 streams it is the largest phase, 61 % of a commit's
  11.2 ms.
- Followed reads keep their decoders' schemas and dictionaries alone: 352 KiB at 192 streams, 0.36 %
  of the reads' share at the least budget that admitted them, which charged each read 2 MiB.
- Quiet, the pipeline takes 0.008 of a core at 192 streams, nearly all of it the host's.
- At 192 streams the run's 193 tables outnumber its 128 writers, so writers retire: 832 of its
  960 flushes. Every cell writes through two of the four lanes.
- Each process's resident peak at 192 streams: the host's process 2.2 GiB, the bench's batches of
  every other case among it; the spawned source 124 MiB and the destination 35 MiB.

### Within the default budget

`main` at `7b946d75` against the change on it, 2026-10-26, the same layout and cores: three rounds
of `served/process/socket/both/tables/followed/192`, interleaved with the in-process cell
([passthrough.md](passthrough.md), "Within the default budget"), the order reversed between
rounds; the medians of each run's ten samples, then the median of the rounds and their range.

| `followed/192` | Budget | Flush | Commit | Acknowledgement | Idle CPU: bench / source / destination | Reads kept at most | Resident peak: bench / source / destination |
|---|---|---|---|---|---|---|---|
| `main` | 386 MiB | 3.68 ms (3.64–4.07) | 0.79 ms (0.75–0.80) | 6.94 ms (6.85–7.04) | 0.006 / 0.001 / 0.000 | 352 KiB of 96.5 MiB | 96.3 / 124.9 / 35.4 MiB |
| The change | 256 MiB | 3.79 ms (3.44–4.24) | 0.75 ms (0.73–0.79) | 7.19 ms (7.00–7.27) | 0.006 / 0.001 / 0.000 | 352 KiB of 64 MiB | 94.0 / 124.9 / 35.8 MiB |

- 192 followed streams served within the default memory, every row landing: what their reads keep
  takes one of the 127 parts of 512 KiB followed reads may hold there.

The production slice against the same `main`: five rounds of its ten cases
(`served/process/socket/both/{64x80000,512x10000,5120x1000}` from 1, 4, 8 and 16 partitions,
through one writer), the order reversed between rounds, each started at a one-minute load under
3.0; the third round's run of the change and the fourth's of `main` ran beside other work on the
machine, the one-minute load reaching 7.3 and 6.2 by their ends. The median of the rounds and
their range, in MB/s.

| Case | `main` | The change | Change | Within `main`'s range |
|---|---|---|---|---|
| 64×80 000 | 1 637 [1 311–1 706] | 1 440 [1 411–1 692] | −12.0 % | yes |
| 512×10 000 | 2 749 [2 483–2 924] | 2 493 [1 326–3 002] | −9.3 % | yes |
| 64×80 000, 4 partitions | 1 644 [1 435–1 775] | 1 613 [1 419–1 741] | −1.8 % | yes |
| 64×80 000, 8 partitions | 1 524 [1 350–1 617] | 1 610 [1 183–1 693] | +5.6 % | yes |
| 64×80 000, 16 partitions | 1 392 [1 306–1 482] | 1 424 [1 053–1 560] | +2.3 % | yes |
| 512×10 000, 4 partitions | 2 613 [1 531–2 653] | 2 578 [1 390–2 629] | −1.4 % | yes |
| 512×10 000, 16 partitions | 2 331 [1 643–2 412] | 2 287 [1 378–2 440] | −1.9 % | yes |
| 5 120×1 000 | 1 761 [1 376–1 818] | 1 707 [1 209–1 765] | −3.0 % | yes |
| 5 120×1 000, 4 partitions | 1 450 [1 279–1 557] | 1 526 [1 196–1 548] | +5.3 % | yes |
| 5 120×1 000, 16 partitions | 1 443 [1 120–1 479] | 1 472 [1 047–1 490] | +2.0 % | yes |

- Every case's median lies within `main`'s range. The change adds no work a frame: a read that is
  not followed takes its part once, as it takes its slot.
- The two cases from one partition, whose medians lie lowest, ran again on their own, `main` and
  the change alternating six times in one hold of the machine, under more load than the rounds:
  1 096 MB/s [939–1 227] against 1 127 [1 095–1 270] in large frames, +2.8 %, and 1 307
  [1 250–1 710] against 1 438 [1 285–1 676] in small ones, +10.0 %.

## The boundary's copies

How much of a spawned pipeline's CPU the boundary's two user-space copies take: each receiver
gathering a frame's transport chunks into one aligned allocation that doubles as it fills
(`Bounded::arrive`), and each sender copying every buffer of a batch into a fresh allocation to
encode it (`Encoder::encoded`, Arrow IPC's `write_array_data`). Measured on the tree of the
production slice above, `served/process/socket/both/64x80000`.

- **Profile.** The profiling build (`just profiling served`), host on CPUs 0–3 and connectors on
  4–11, `perf record -e cycles:u -F 999 --call-graph fp` over the bench's process and every
  process it spawned, 30 seconds of `--profile-time`: 42 636 samples, one host and 23 runs'
  sources and destinations. Each process is told by its frames: the host by the bench's own,
  a source by the served read's pump, a destination by the served write's receive stage.
- **Left out.** Samples outside the runs: the host hashing the connector's binary as each run
  spawns it (3 443 samples) and making the bench's batches (960), and each source making its
  batches before its run (16 663, 75 % of the sources' samples).
- **The gather.** Samples under `Bounded::arrive`, and samples in libc's `memmove` whose call site
  is where `Incoming::arrived` calls `Bounded::poll_arrived`: `memmove` keeps no frame, so
  frame-pointer unwinding loses its caller, and these samples are recognised by their return
  address, the one samples inside `poll_arrived` itself show.
- **The encode copies.** Samples in libc's copy routines with `Encoder::encoded` in their stack.
- **Weighting.** A profile samples user mode alone, so each process's share of its samples is
  weighted by its user share of CPU time and its CPU seconds a GB, both read by the bench in each
  of the production slice's three rounds; the figure is their sum over the pipeline's CPU seconds a
  GB, computed for each round.

| Process | Samples kept | Gather | Encode copies | User share | CPU s a GB | Copies, CPU s a GB |
|---|---|---|---|---|---|---|
| Host | 8 563 | 33.0 % | 39.5 % | 0.375 (0.352–0.398) | 0.915 (0.849–0.930) | 0.231–0.268 |
| Source | 5 700 | 0.0 % | 71.0 % | 0.400 (0.385–0.409) | 0.559 (0.498–0.575) | 0.145–0.159 |
| Destination | 7 307 | 39.4 % | – | 0.577 (0.571–0.593) | 0.619 (0.591–0.651) | 0.135–0.147 |

The gathers and the encode copies take 26.3 % of the pipeline's CPU seconds a GB (25.7–26.5 % over
the three rounds): the gathers 12.4 % and the encode copies 14.0 %. Counting only the gather's
samples seen inside `Bounded::arrive`, without the copies whose caller unwinding loses, they take
15.5–16.0 %.

- Every host and connector thread that copies is a runtime worker: the host's samples are 99.6 %
  its workers', the partition's read and the lane's writes among them.
- Kernel time is not sampled: 38 % of the host's kept samples are taken at a kernel entry, a page
  fault or a system call, and counted with the user frames that made it.

## One framed stream a data call

Each data call, a read, a write and certification's read-back, built to ride a byte stream of its
own in place of a gRPC call on the control connection. For a spawned connector the stream is a
socket pair whose connector end the host passes over the opener on descriptor 3; for one dialed
over TLS it is a `Data` call of its connection. A frame is a 16-byte head and its body. The
receiver reserves a frame from its budget before it reads the body, into an allocation of exactly
its length, where it is decoded. The sender writes a batch's frames from the batch's own buffers,
copying those under 4 KiB together. Credit goes: a socket's buffer, or HTTP/2's windows, paces the
sender. "The boundary's copies" above found the gathers and the encode copies at 26.3 % of the
pipeline's CPU seconds a GB, over the 12 % that warranted building it.

The rule was set before measuring. Adopt only if every condition holds:

- In `process/socket/both/64x80000`, every process's CPU seconds a GB falls at least 10 %, and
  the rounds' ranges are apart from `main`'s.
- In no `process/socket` case does a process's CPU seconds a GB rise above `main`'s median.
- No served case's median MB/s falls more than 3 % below `main`'s. The cases are the production
  slice's cells, the multi-writer rows, the wide case and `process/tls/{destination,both}/64x80000`.

Two variants were to be measured. In A, the reader holds one frame beyond its consumer. B was to
be run only if A failed the MB/s clause: there the reader reads ahead up to 4 MiB of reserved
frames. The change also ran with no malloc tunables for the spawned connectors. Had every case
stayed within 3 % of the run with them, the tunables would have gone.

- **Trees.** `main` at `bb4d460c`. The change, with A, on the same `main`. B, which reads ahead
  up to 4 MiB on both readers the change has: the host's reader of a read, and a served write's
  reader. Each reader is held to that by a byte semaphore and to at most 1 024 frames. The
  change with no malloc tunables. Every tree was built before its rounds.
- **Running.** `just bench served 0-3 '^served/process/' 4-11`, on 2026-10-29 between 04:55 and
  10:40. The host ran on CPUs 0–3 and the connectors on 4–11, with governor powersave and energy
  preference performance. Each run took the measurement lock alone and started once the
  one-minute load average fell below 1.0. The loads at the starts were 0.69 to 0.91, and at the
  ends 1.18 to 2.71.
- **Rounds.** Round 1 ran `main`, A, no tunables, then B. Round 2 ran B, A, then `main`. Rounds
  stopped there, since the rule failed alike in both. The tables give each side's median over its
  two rounds, with the range of its two in brackets.

The CPU rule's case, `process/socket/both/64x80000`, CPU seconds a GB:

| Side | All processes | Host | Source | Destination |
|---|---|---|---|---|
| `main` | 2.136 [2.103–2.169] | 0.988 | 0.531 | 0.615 |
| A | 1.159 [1.099–1.219] | 0.537 | 0.238 | 0.394 |
| B | 1.084 [1.079–1.089] | 0.516 | 0.227 | 0.339 |

The production slice, the multi-writer rows, the wide case and the TLS cells the rule names, in
MB/s (the wide case in thousands of rows a second):

| Case | `main` | A | B |
|---|---|---|---|
| `both/64x80000` | 1 458 [1 421–1 496] | 2 092 [1 988–2 196] | 2 257 [2 251–2 264] |
| `both/64x80000/4-partitions` | 1 488 [1 412–1 563] | 2 493 [2 470–2 516] | 2 512 [2 461–2 564] |
| `both/64x80000/8-partitions` | 1 521 [1 502–1 540] | 2 338 [2 281–2 395] | 2 452 [2 427–2 477] |
| `both/64x80000/16-partitions` | 1 410 [1 372–1 449] | 2 356 [2 352–2 360] | 2 343 [2 314–2 373] |
| `both/512x10000` | 2 396 [2 280–2 511] | 2 937 [2 556–3 318] | 3 449 [3 352–3 546] |
| `both/512x10000/4-partitions` | 2 452 [2 355–2 549] | 3 816 [3 806–3 826] | 3 695 [3 685–3 705] |
| `both/512x10000/16-partitions` | 2 341 [2 321–2 362] | 2 851 [2 848–2 853] | 2 925 [2 893–2 957] |
| `both/5120x1000` | 1 818 [1 809–1 827] | **1 369** [1 369–1 369] | **1 418** [1 405–1 430] |
| `both/5120x1000/4-partitions` | 1 559 [1 550–1 569] | **1 263** [1 259–1 267] | **1 248** [1 244–1 251] |
| `both/5120x1000/16-partitions` | 1 503 [1 499–1 507] | **1 195** [1 192–1 197] | **1 239** [1 228–1 249] |
| `both/64x80000/4-writers` | 1 787 [1 694–1 879] | 2 528 [2 474–2 583] | 2 608 [2 585–2 632] |
| `both/64x80000/4-partitions/4-writers` | 1 673 [1 589–1 757] | 4 183 [4 150–4 216] | 4 003 [3 993–4 014] |
| `both/64x80000/16-partitions/4-writers` | 1 366 [1 360–1 372] | 3 917 [3 892–3 941] | 3 766 [3 748–3 785] |
| `both/512x10000/4-writers` | 2 879 [2 785–2 973] | 3 220 [3 206–3 233] | 3 669 [3 605–3 732] |
| `both/512x10000/4-partitions/4-writers` | 2 261 [2 255–2 267] | 4 979 [4 966–4 993] | 4 736 [4 696–4 776] |
| `both/512x10000/16-partitions/4-writers` | 2 021 [2 018–2 024] | 4 166 [4 120–4 211] | 4 074 [4 040–4 109] |
| `both/5120x1000/4-writers` | 1 811 [1 805–1 817] | **1 355** [1 340–1 370] | **1 415** [1 382–1 447] |
| `both/5120x1000/4-partitions/4-writers` | 1 722 [1 702–1 742] | 2 152 [2 149–2 154] | 2 098 [2 083–2 112] |
| `both/5120x1000/16-partitions/4-writers` | 1 708 [1 707–1 709] | 2 029 [2 023–2 035] | 1 991 [1 983–1 999] |
| `both/wide/arrow/5000` | 8.77 [8.77–8.77] | 8.78 [8.45–9.11] | 8.98 [8.93–9.03] |
| `tls/destination/64x80000` | 1 324 [1 320–1 329] | 1 398 [1 388–1 407] | 1 425 [1 422–1 427] |
| `tls/both/64x80000` | 1 141 [1 139–1 143] | 1 326 [1 304–1 347] | 1 258 [1 206–1 311] |

**Decision: closed.** The first condition holds by far: every process's CPU seconds a GB falls
36–57 %, and the ranges are apart. The other two fail in both rounds, alike for A and B:

- The production slice's 1 000-row cells move 18–25 % less, through one writer from 1, 4 and 16
  partitions: the four bold rows in each variant's column.
- The wide case's source process rises above `main`'s median, 1.607 to 1.821 CPU seconds a GB
  (+13.3 %) with A, and to 1.850 with B.

What the rule does not gate, recorded:

- **Where the 1 000-row cells lose.** A run of them writes through one lane. Through one writer
  it moves 18–25 % less, while through four lanes it moves 17–25 % more. Every process keeps
  fewer CPUs busy in the 1-partition cell: the host 1.86 to 1.27, the source 1.41 to 0.64 and the
  destination 1.61 to 0.79. So the run waits on one task, not on CPU. No profile was taken; the
  likeliest task is the lane. Each frame it writes, it now copies into the kernel itself, where
  before it handed frames to HTTP/2's connection task, and with frames of a thousand rows that
  copy and its system call would bound it. B's deeper look-ahead brings no failing cell within
  3 %, so no reader is what waits. A task a call that took the lane's frames, as HTTP/2's
  connection task did, is what this points to. It was not run, since only A and B were
  registered before measuring.
- **Larger frames gain.** The production slice's cells of 80 000 and 10 000 rows move 22–69 %
  more, and through four writers 1.1 to 2.9 times as much. With A, a source alone moves 41–86 %
  more, and a destination alone 41 % more in 80 000-row frames.
- **Two cases outside the clause lose.** `destination/512x10000` moves 10.7 % less (14.2 % with
  B), and with A the TLS rows of 10 000-row frames 6–10 % less (`tls/both/512x10000` 1 423 to
  1 334, `tls/destination/512x10000` 1 632 to 1 471).
- **Malloc tunables.** The change without them ran one round, beside A's first. Eighteen of its
  cases moved more than 3 % from A's in MB/s or CPU seconds a GB, up to 13 % either way, the
  multi-writer and TLS rows mostly slower. With the change closed, the tunables stay.
- **Instructions and memory.** `just instructions` held every case within 2 % except
  `ipc/roundtrip`: +2.8 % instructions and 148 to 210 allocations, as frames are gathered from
  their pieces there. The new `wire/stream` case counted 823 681 instructions and 234
  allocations. `just resident` held its bound. The served line's host peaked at 100–136 MiB, its
  spawned source at 461–463 MiB and its destination at 47–54 MiB.

## One charge a served frame

A served read's frame is charged, as it arrives, what decoding it holds; that charge goes as the
read takes the frame, before its batch is decoded, and its push is charged again as it waits for
room among pushes, while what the read's decoder keeps of schemas and dictionaries is charged
apart. The
change charged a frame once. Before decoding it, the host charged it what its push would be
charged where that was more than decoding holds, knowing what the frame carried by the field its
encoding set first: a batch's message with the schema and dictionaries the read's decoder kept and
the batch's arrays, or a JSON push's charge. The engine's admission shrank that charge to the
push's and waited for nothing, and refused as a malformed frame a push charged more than its frame.
It was to be built only where frames waited for their charge at least a third as long as pushes
waited for room, and they did: in "The production slice" above, `64×80000` from 16 partitions
spends 29.0 % of its run in its frames' decode waits and 2.6 % in intake waits.

The rule was set before measuring. Adopt only if every condition holds:

- No production-slice case's median MB/s lies below `main`'s range.
- The intake waits of `64×80000` from 16 partitions are no higher than `main`'s.
- The served line of `just resident` is no higher than `main`'s.
- `just instructions` holds every case within 2 %.

- **Trees.** `main` at `01cd996b`, and the change on it. Each tree's benches were built before its
  rounds, the spawned connectors among them.
- **Running.** `just bench served 0-3 'process/socket/both/[0-9]+x[0-9]+(/[0-9]+-partitions)?$'
  4-11`, the production slice's ten cases, on 2026-10-30 and -31 between 23:46 and 11:38: the
  host on CPUs 0–3 and the connectors on 4–11, governor powersave with energy preference
  performance. Each run took the measurement lock alone and started once the one-minute load
  average fell below 1.0: 0.61–0.98 at the starts, 1.17–2.35 at the ends.
- **Rounds.** Six a side, interleaved, each side first in three: three pairs, then three more with
  the change first in two, as `main` had run first in two of the first three. A build of another
  bench overlapped the opening of one of `main`'s runs; it was set aside and run again. The table
  gives each side's median of its six runs and their range, in MB/s.

| Case | `main` | Change | Median change | Decode waits, `main` / change |
|---|---|---|---|---|
| `both/64x80000` | 2 118 [2 073–2 208] | 2 125 [2 063–2 220] | +0.3 % | 3.1 / 3.2 % |
| `both/64x80000/4-partitions` | 2 044 [1 991–2 080] | 2 078 [1 995–2 124] | +1.6 % | 7.6 / 8.7 % |
| `both/64x80000/8-partitions` | 1 951 [1 888–1 982] | 1 933 [1 867–1 959] | −0.9 % | 13.4 / 16.2 % |
| `both/64x80000/16-partitions` | 1 760 [1 731–1 919] | 1 790 [1 684–1 825] | +1.7 % | 28.1 / 29.0 % |
| `both/512x10000` | 3 406 [3 269–3 673] | **3 229** [3 126–3 284] | −5.2 % | 1.2 / 1.3 % |
| `both/512x10000/4-partitions` | 2 834 [2 768–3 050] | 2 786 [2 739–2 944] | −1.7 % | 1.9 / 2.6 % |
| `both/512x10000/16-partitions` | 2 590 [2 551–2 844] | 2 581 [2 474–2 664] | −0.3 % | 1.7 / 1.9 % |
| `both/5120x1000` | 1 904 [1 878–1 959] | 1 903 [1 814–1 972] | −0.1 % | 2.3 / 2.3 % |
| `both/5120x1000/4-partitions` | 1 640 [1 601–1 661] | 1 634 [1 474–1 702] | −0.3 % | 2.2 / 2.1 % |
| `both/5120x1000/16-partitions` | 1 565 [1 519–1 624] | 1 571 [1 543–1 614] | +0.4 % | 2.6 / 2.5 % |

**Decision: closed.** One case's median falls below `main`'s range: `both/512x10000`, from one
partition, moves 5.2 % less than `main`'s median, and five of its six runs are below all six of
`main`'s (`main` 3 269–3 673, the change 3 126–3 284). The other cases' medians lie
within `main`'s ranges, from 1.7 % below to 1.7 % above. The intake waits of `64×80000` from 16
partitions are 0.0 % on both sides in every run. `just instructions`, counted against the `main`
of its day (`05c69b59`, four charge fixes on), held every case within 2 %: `wide/arrow_1000` took
1.34 % fewer instructions, and every other case moved 0.31 % or less, its allocations 0.85 % or
less. The served line of `just resident`, five runs a side of each of its six configurations,
interleaved under the measurement lock, does not hold either: the host's resident growth has a
higher median than `main`'s in three, 220 and 228 MiB at two cores, 223 and 241 at four and 377
and 392 at sixteen with the malloc tunables, and a lower one in the other three, 256 and 241 at
two with them, 251 and 249 at four with them and 296 and 285 at sixteen, every configuration's
ranges overlapping, against limits of 522–562 MiB. The budget peaked at 107–115 MiB alike, the
spawned source at 864–909 MiB and the destination at 75–89 MiB on both sides.

What the rule does not gate, recorded:

- **The decode waits did not fall.** `64×80000` from 16 partitions waits as long for its frames'
  charges with one charge as with two: 28.1 % of the run on `main` and 29.0 % with the change.
  What its frames wait for is room among pushes, which the change leaves as it was; the second
  charge, the push's, waited for nothing on `main` either, its intake waits 0.0 %, where "The
  production slice" measured 2.6 % on an earlier `main`.
- **Where `512x10000` loses.** No profile was taken. Its waits and its lane's flushes are as on
  `main`: decode waits of 1.2 and 1.3 %, four flushes for pressure and one for its commit. Every
  process takes 2–3 % more CPU seconds a GB (the host 0.494 to 0.510, the source 0.459 to 0.470,
  the destination 0.388 to 0.395) and keeps fewer CPUs busy (the host 1.67 to 1.64, the source
  1.54 to 1.49, the destination 1.31 to 1.27). The change does less with the budget for each frame
  than `main`, one reservation shrunk in place of one released and another taken, and changes
  little that a spawned source or destination does a frame. That the spawned binaries' code moved,
  and not the change's work, is not ruled out.

## A unit's batches freed as their pieces go

[before-after.md](before-after.md) found the production slice's frames of 10 000 rows slower on
`main` at `10669fa9` than on the engine before the improvements, `41284517`: 0.86 and 0.94 times
its MB/s from one and from sixteen partitions, the host's process taking 1.07 to 1.16 times its
CPU seconds a GB, and the served host growing more in `just resident`. This section finds the
change that cost it and removes the cost.

### A count, not a time

The machine ran a virtual machine and its owner's other work throughout, so each tree was judged
by what load does not move: counts.

- **Counting the host alone.** A patch of the served bench, applied outside the tree, had
  `perf stat` count only while `passthrough.run` ran (`--control` over a fifo, enabled and
  disabled around each run) and logged each run's `getrusage` of the bench's process: user and
  system time, minor faults and context switches. The host ran on CPUs 0–3 and the spawned
  connectors on 4–11; the performance cores' PMU (`cpu_core`) counts nothing the efficient cores
  run, so `cpu_core/instructions/u` is the host's alone. Kernel instructions cannot be counted
  here (`perf_event_paranoid` 2); faults, switches and system time stand for the kernel's work.
- **In the record's order.** Alone in a fresh process, `512x10000` did not separate the trees: the
  base moved between two regimes from one process to the next, 104 000 and 29 000 minor faults a GB
  and 0.663 and 0.512 host CPU seconds a GB over two sessions of 58 runs. The record ran the slice's
  ten cells in one process, `64x80000` first; run so, `64x80000` then `512x10000` in one process,
  both trees held steady and the record's gap appeared. Per run of `512x10000`, the median over its
  runs from the fourth on, two sessions a tree:

| Tree | MB/s | Host CPU s a GB | Minor faults | User instructions | System time |
|---|---|---|---|---|---|
| `41284517` | 3 418, 3 433 | 0.469, 0.475 | 4 372, 4 628 | 2.06, 2.05 G | 94, 96 ms |
| `10669fa9` | 2 969, 2 992 | 0.546, 0.542 | 11 201, 13 668 | 1.86, 1.87 G | 119, 116 ms |

`main` executed fewer user instructions and took 2.4 to 3.1 times the page faults: the host's
added CPU was the kernel's, faulting pages in.

### Bisection

`git bisect run` over the 346 commits, each step built as `just bench served` builds the bench and
counted twice under the measurement lock, a third time where the two disagreed: good below 7 000
minor faults a run of `512x10000` (after `64x80000`, the median from its fourth run), bad above
9 000. Nine steps, none skipped, ended at `348b5b649` ("fix(engine): charge each piece for what it
keeps alive beyond its unit"): its parent `8e09a16ff` took 4 120 and 4 176 faults a run and 2.07
and 2.06 G user instructions, as the base does, and it 19 957 and 15 703, and 1.96 and 1.94 G, as
`main` does. Every bad step counted 10 000 to 25 000.

### Cause

That change made what a unit's pieces share hold the unit's batches beside its permits, so that
the set of allocations each piece's growth was counted against named only live addresses. A
written piece's share waits on its lane with the piece's reservation until the lane flushes, so
every batch a partition pushed stayed allocated until the lane's next flush (on `main` four for
pressure and one for the commit in a run of `512x10000`), where before a batch went once the
pieces cut from it were lowered or written and only its permit waited. `257004c8b` later deleted
the set; the retention stayed.

- **What lived longer.** DHAT (`valgrind --tool=dhat`) over one run of `512x10000`: the frames'
  allocations (`Aligned::grow` from `Bounded::poll_arrived`, about 1 500 blocks and 436 MB a run)
  held 5.3 MB at the heap's peak at `8e09a16ff`, each living 2.8 million instructions on average,
  and 87.5 MB at `348b5b649`, each living 102.5 million; the heap's peak rose from 452.8 to
  536.2 MB (the bench's own batches, about 440 MB, in both), and nothing else moved: the same
  3.374 GB in 27.38 million blocks.
- **What it cost.** Freed together at each flush, the frames' pages went back to the kernel and
  the next frames faulted them in again: `perf record -e page-faults` over `512x10000`'s runs
  counted the host about 38 000 faults in ten runs at `8e09a16ff` and 182 000 at `348b5b649`,
  99 % on its two runtime workers, first touches in libc's copies where `Bounded::poll_arrived`
  gathers a frame, arrow-ipc's `encode_sink_buffer` encodes one and a vector grows. The served
  host's higher resident growth is the same retention.

### The change

The cutter owns a unit's batches and each piece's slices keep alive what they cut, so a batch
goes once the pieces cut from it do; the pieces share only what was charged for the unit, which
holds until the last piece is flushed, as before. What is charged, when, and the pressure flushes
it makes are unchanged: a unit's charge may outlive what it holds, never the reverse. It is not
the closed proposal of freeing a consuming writer's write as it returns
([campaign.md](campaign.md)), which would release the charge early.

- **Tests.** `a_unit_keeps_no_batch_beside_its_pieces_and_its_charge_until_its_last_piece_goes`:
  once every piece but the last is gone, nothing of the unit holds either of its batches, and its
  charge holds until the last piece goes.
  `written_batches_go_before_the_flush_that_releases_their_charge`: 256 fresh 1 MiB pushes into a
  destination that keeps nothing, within 1 GiB, one commit; the charge still reaches all 256 MiB and
  the heap stays under half of it, where `main`'s held more than every push (in a first form of 128
  pushes, `main`'s heap peaked at 185.7 MB and the change's at 69.3 MB).

### On the production slice

`main` at `10669fa9` against the change on it, 2026-11-01. The counts first: one process a
session, the ten cells in the record's order, each for 15 seconds of criterion's profiling mode
under the counting patch, two sessions a tree interleaved; a figure is the median a run over a
cell's runs from the fourth on, the host's CPU seconds a GB over those runs, the change's two
sessions then `main`'s.

| Cell | Minor faults a run, change | `main` | Host CPU s a GB, change | `main` |
|---|---|---|---|---|
| `64x80000` | 10 090, 18 896 | 10 491, 37 074 | 0.721, 0.761 | 0.699, 0.726 |
| `64x80000`, 4 partitions | 10 309, 15 314 | 13 304, 44 621 | 0.756, 0.767 | 0.726, 0.791 |
| `64x80000`, 8 partitions | 8 782, 12 265 | 16 936, 53 676 | 0.749, 0.752 | 0.754, 0.855 |
| `64x80000`, 16 partitions | 12 362, 18 101 | 34 491, 64 935 | 0.746, 0.761 | 0.816, 0.875 |
| `512x10000` | 4 530, 4 347 | 28 546, 32 689 | 0.468, 0.470 | 0.547, 0.612 |
| `512x10000`, 4 partitions | 5 468, 4 928 | 26 294, 15 790 | 0.505, 0.505 | 0.569, 0.558 |
| `512x10000`, 16 partitions | 8 644, 8 187 | 30 932, 21 279 | 0.569, 0.568 | 0.626, 0.601 |
| `5120x1000` | 2 982, 2 884 | 40 696, 38 012 | 0.899, 0.899 | 0.991, 0.982 |
| `5120x1000`, 4 partitions | 8 034, 7 932 | 43 865, 39 940 | 1.007, 1.007 | 1.116, 1.108 |
| `5120x1000`, 16 partitions | 27 960, 28 061 | 49 417, 50 962 | 1.088, 1.087 | 1.152, 1.159 |

- **Every cell faults less**, `512x10000` from one partition 4 347 to 4 530 times a run against
  28 546 to 32 689, as the base did; frames of 1 000 rows, which the record found about level,
  fault far less too, from one partition 2 884 to 2 982 against 38 012 to 40 696.
- **The host's CPU a GB falls in eight cells of ten**, by 6 to 19 % at the medians; in `64x80000`
  from one and from four partitions the two trees' sessions overlap, the change's figures a little
  above `main`'s in three of four.
- The change executes more user instructions where it faults less, 1.99 G a run of `512x10000`
  against 1.74 to 1.85 G.

Then the time: the plain benches, built as `just bench served` builds them, the host on CPUs 0–3
and the connectors on 4–11, criterion's pass over the ten cells (ten samples of one run each),
each round one hold of the measurement lock started at a one-minute load under 1.0: five rounds
of the change and `main`, the order alternating, and in two of them the base `41284517` too.
The loads at the starts were 0.83 to 2.55, the benches' own threads after a round's first tree.
MB/s, the median of the rounds and their range; a verdict where the ranges are apart.

| Cell | Base (2) | `main` (5) | Change (5) | Change ÷ `main` | Verdict | Change ÷ base |
|---|---|---|---|---|---|---|
| `64x80000` | 1 917 [1 892–1 943] | 1 940 [1 808–2 141] | 1 731 [1 671–2 029] | 0.89 | level | 0.90 |
| `64x80000`, 4 partitions | 1 644 [1 537–1 750] | 1 880 [1 640–2 004] | 1 893 [1 512–1 938] | 1.01 | level | 1.15 |
| `64x80000`, 8 partitions | 1 613 [1 596–1 630] | 1 683 [1 629–1 899] | 1 868 [1 617–1 905] | 1.11 | level | 1.16 |
| `64x80000`, 16 partitions | 1 594 [1 578–1 610] | 1 572 [1 477–1 636] | 1 640 [1 484–1 723] | 1.04 | level | 1.03 |
| `512x10000` | 3 528 [3 493–3 563] | 3 061 [2 841–3 206] | 3 535 [3 326–3 654] | 1.15 | faster | 1.00 |
| `512x10000`, 4 partitions | 2 995 [2 979–3 011] | 2 703 [2 660–2 769] | 3 012 [2 741–3 044] | 1.11 | level | 1.01 |
| `512x10000`, 16 partitions | 2 669 [2 649–2 689] | 2 487 [2 432–2 532] | 2 665 [2 575–2 785] | 1.07 | faster | 1.00 |
| `5120x1000` | 1 995 [1 991–1 999] | 1 873 [1 812–1 901] | 2 071 [2 025–2 101] | 1.11 | faster | 1.04 |
| `5120x1000`, 4 partitions | 1 770 [1 764–1 776] | 1 623 [1 598–1 632] | 1 847 [1 791–1 881] | 1.14 | faster | 1.04 |
| `5120x1000`, 16 partitions | 1 603 [1 594–1 613] | 1 567 [1 548–1 587] | 1 686 [1 657–1 756] | 1.08 | faster | 1.05 |

The host's CPU seconds a GB, the same rounds:

| Cell | Base (2) | `main` (5) | Change (5) | Verdict |
|---|---|---|---|---|
| `64x80000` | 0.752 [0.748–0.756] | 0.739 [0.673–0.780] | 0.850 [0.710–0.901] | level |
| `64x80000`, 4 partitions | 0.896 [0.844–0.948] | 0.764 [0.720–0.853] | 0.747 [0.731–0.964] | level |
| `64x80000`, 8 partitions | 0.871 [0.865–0.876] | 0.817 [0.739–0.848] | 0.731 [0.707–0.878] | level |
| `64x80000`, 16 partitions | 0.815 [0.796–0.833] | 0.822 [0.815–0.882] | 0.800 [0.738–0.883] | level |
| `512x10000` | 0.465 [0.463–0.466] | 0.542 [0.516–0.583] | 0.451 [0.447–0.452] | leaner |
| `512x10000`, 4 partitions | 0.497 [0.493–0.502] | 0.543 [0.542–0.556] | 0.486 [0.477–0.534] | leaner |
| `512x10000`, 16 partitions | 0.564 [0.560–0.567] | 0.602 [0.591–0.616] | 0.553 [0.546–0.578] | leaner |
| `5120x1000` | 0.944 [0.943–0.945] | 0.990 [0.983–1.018] | 0.897 [0.886–0.917] | leaner |
| `5120x1000`, 4 partitions | 1.052 [1.048–1.057] | 1.122 [1.108–1.145] | 1.011 [0.985–1.037] | leaner |
| `5120x1000`, 16 partitions | 1.139 [1.136–1.142] | 1.156 [1.143–1.166] | 1.087 [1.050–1.106] | leaner |

- **Frames of 10 000 rows are back at the base's**: `512x10000` 3 535 MB/s against `main`'s 3 061
  and the base's 3 528, the host 0.451 CPU seconds a GB against 0.542 and 0.465; from sixteen
  partitions 1.07 times `main`'s, from four 1.11 at the medians.
- **Frames of 1 000 rows move 1.08 to 1.14 times as much** as on `main`, and 1.04 to 1.05 times the
  base's, the host leaner than both.
- **Frames of 80 000 rows from one partition cost about 7 % more.** Their rounds split by order:
  run first in a hold 2 026 and 2 029 MB/s, after another tree 1 671 to 1 731. Run alone, a fresh
  process each, the two trees alternating in one hold (change, `main`, `main`, change, twice),
  the change moved 1 960, 1 932, 1 867 and 1 839 MB/s, the host taking 0.740, 0.730, 0.749 and
  0.755 CPU seconds a GB, and `main` 2 062, 2 079, 2 017 and 1 911, taking 0.695, 0.685, 0.695
  and 0.721: each of the change's runs below the `main` runs beside it. From four, eight and
  sixteen partitions they are level.

### Why large frames from one partition pay

**A known trade-off.** 80 000-row frames from one partition are the one cell the change makes
costlier: run alone, alternating with `main`, about 7 % fewer MB/s (1 900 against 2 040 at the
medians of four runs each) and about 7 % more host CPU a GB (0.745 against 0.695).

Counted with the system's huge-page faults beside the process's own (`thp_fault_alloc` in
`/proc/vmstat` over each run; this machine's transparent huge pages are `always`, and a fault on
one zeroes 2 MiB and counts once), `64x80000` then `512x10000` then `64x80000` from four
partitions in one process, three sessions a tree: the change halves `64x80000`'s 4 KiB faults
(8 513 to 15 454 a run against 13 206 to 19 260) and doubles its huge-page faults (196 to 306
against 74 to 143), its host CPU level (0.690 to 0.740 against 0.714 to 0.728 seconds a GB).
Freed as their pieces go, one partition's frames of 6.9 MB leave memory that glibc, at its
default dynamic thresholds, gives back to the kernel, and the next frames fault it back in, on
huge pages; from more partitions the frees interleave and the cells are level. With the
connectors' malloc tunables set for the host too (a trim threshold of 256 MiB and a mapping
threshold of 32 MiB, two sessions a tree), the change's huge-page faults on `64x80000` fall to 40
and 42 a run against `main`'s 46 and 73, and the change is leaner in every cell counted:
`64x80000` 0.633 and 0.666 CPU seconds a GB against 0.663 and 0.695, from four partitions 0.675
and 0.701 against 0.690 and 0.736, `512x10000` 0.463 and 0.476 against 0.540 and 0.565. The cost
is glibc's, at its default thresholds; the base paid it too (the record's medians, 0.723 against
`main`'s 0.674). What would remove it in the engine is the host reusing its frames' allocations
rather than handing them back to the allocator, a design of its own.

### Memory

The served line of `just resident`, `main` and the change alternating line by line, five runs a
line each, the host's resident growth in MiB, the median and range:

| Line | `main` | Change | Limit | Verdict |
|---|---|---|---|---|
| 2 cores | 234 [223–264] | 174 [155–205] | 522 | lower |
| 2 cores, tunables | 277 [230–291] | 202 [180–248] | 522 | level |
| 4 cores | 244 [238–263] | 191 [174–199] | 526 | lower |
| 4 cores, tunables | 261 [233–280] | 204 [201–219] | 526 | lower |
| 16 cores | 305 [276–346] | 247 [202–350] | 562 | level |
| 16 cores, tunables | 400 [351–411] | 342 [332–378] | 562 | level |

Every line's median is 52 to 75 MiB lower, at two and four cores back at the base's 181 and 186
([before-after.md](before-after.md), "Resident memory"); `peak_memory` reads 100 to 115 MiB on
both, as the charge is unchanged, and the spawned processes peak alike.

### Instructions and the rules

`just instructions` against `main` holds every case within 0.63 %, the allocations unchanged:
`passthrough/null_sink` takes 0.63 % fewer instructions and `wide/arrow_1000` 0.03 % more. No case
holds the change itself: callgrind counts no page fault, and the change allocates as `main` does;
`written_batches_go_before_the_flush_that_releases_their_charge` holds it, by the heap's peak.

The rules were set before the rounds. The change, which removes a retention whose reason had
gone, would land unless measured to cost: (1) the host's faults a run of `512x10000` fall to at
most half of `main`'s and no cell's rise by more than 10 %; (2) `512x10000`'s median MB/s lies
above `main`'s range and no cell's median below it; (3) `just instructions` within 2 %; (4) the
served line of `just resident` no higher than `main`'s at two and four cores.

**Decision: landed.** (1), (3) and (4) hold. (2) holds but for `64x80000` from one partition,
whose median, 1 731 MB/s, lies below `main`'s range, 1 808 to 2 141: a known trade-off of about
7 % on that cell, glibc's at its default thresholds and paid by the base too, against gains of 7
to 15 % in six cells and the regression this section set out to remove.
