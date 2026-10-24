# Latency

How long a row takes from its push to landing, and to its acknowledgement, in a following
pipeline. This record keeps `main`'s figures, in process and with both connectors spawned, at the
streaming default's interval and at one second, for the changes that move landing or
acknowledgement to state their latency rules against.

## Method

- **Rows.** A following source pushes 1 000 rows a push of one 64-bit column, the instant of the
  push on `CLOCK_MONOTONIC`, which every process on the host reads alike, a checkpoint after each
  push. It reads two streams of one unbounded partition each, incrementally: `replayable`, which it
  can read again, and `unreplayable`, which it cannot, so each load logs ahead with `LocalWal` in a
  directory beside the build (btrfs on an NVMe disk). A read that starts again pushes the rows no
  report covered with their first instants, so a retried row counts late, not new.
- **Rate.** Each placement first runs unpaced, committing every second for three seconds, and its
  cases push at half the pushes a second that run landed, an even share a stream.
- **Runs.** Each run follows its source for three intervals (`Until::For`), the source pushing
  throughout, and commits what it read at the end. Criterion's ten samples are ten runs, its
  warm-up's left out.
- **Landed.** The bench destination keeps its pipeline's state and epoch and nothing of the rows
  but their instants. It reads the clock as the commit holding a push returns: push to landed.
- **Acknowledged.** The source reads the clock as a report of committed cursors covers a push:
  push to acknowledged. On `main` the stream that cannot read again is told once its rows are
  logged, before the commit lands.
- **Placements.** `in-process`: both connectors in the bench's process, on its runtime.
  `process`: both spawned by `Local` from the bench's own binary over a socket pair, as production
  places them (`Placement::Process`), held to the connectors' CPUs as the served bench holds its.
- **Policies.** `CommitPolicy::streaming`'s thresholds, 1 GiB or the interval, at its interval of
  10 s and at 1 s.
- **Faults.** `just bench latency <cores> transient-<n>` adds the cases whose destination answers
  one commit in n, drawn, `Transient` before it lands, and `refused-<n>` those whose source refuses
  one report in n. A run a fault ended is run again on the same connectors, a commit interval each
  time, until what it read lands, as its next run would land it. Neither is recorded here.
- **Readings.** For each stream, p50, p99 and the maximum by nearest rank over a run's pushes, of
  push to landed and of push to acknowledged; the most a push was acknowledged after it landed; the
  pushes landed and never acknowledged. Each is the median of a round's ten runs; the tables give
  the median of three rounds and their range. The commit phases are a commit's mean in each
  round's last run, the median of the three.
- **Running.** `just bench latency 0-3 "" 4-11`: the bench and an in-process run's connectors on
  the four performance cores, spawned connectors on the eight efficient ones. Three rounds in one
  hold of the measurement lock, started once no other heavy work ran; release profile.

## Results

Intel Core Ultra X7 358H, 2026-10-24, governor `powersave` with energy preference `performance`;
`main` at `fbef9f4b` with the bench and the log's commit estimate fixed (below). The one-minute
load was 1.9 as the first round began and 4.6–6.5 between rounds, the bench's own processes
beside three idle virtual machines of the owner's; no other heavy work ran.

The rate each placement pushed, half what it landed unpaced: `in-process` 18 028 pushes a second
(16 961–20 294), 18 million rows a second; `process` 14 663 (13 252–14 712).

Push to landed and push to acknowledged, seconds:

| Case | Stream | Landed p50 | Landed p99 | Landed max | Acknowledged p50 | Acknowledged p99 | Acknowledged max |
|---|---|---|---|---|---|---|---|
| `in-process/10s` | `replayable` | 3.84 (3.55–4.03) | 6.74 (6.11–7.06) | 6.82 (6.19–7.18) | 4.14 (3.82–4.25) | 7.05 (6.46–7.34) | 7.16 (6.56–7.49) |
| `in-process/10s` | `unreplayable` | 3.84 (3.55–4.03) | 6.74 (6.11–7.06) | 6.82 (6.19–7.18) | 3.81 (3.53–4.00) | 6.72 (6.09–7.04) | 6.80 (6.17–7.16) |
| `in-process/1s` | `replayable` | 0.70 (0.69–0.73) | 1.48 (1.45–1.56) | 1.51 (1.48–1.59) | 0.74 (0.73–0.78) | 1.55 (1.51–1.65) | 1.58 (1.54–1.68) |
| `in-process/1s` | `unreplayable` | 0.70 (0.69–0.73) | 1.48 (1.45–1.56) | 1.51 (1.48–1.59) | 0.70 (0.69–0.73) | 1.48 (1.45–1.56) | 1.50 (1.48–1.59) |
| `process/10s` | `replayable` | 4.45 (4.43–4.82) | 8.12 (8.10–8.83) | 8.22 (8.21–8.96) | 4.72 (4.72–5.00) | 8.39 (8.39–9.07) | 8.52 (8.50–9.20) |
| `process/10s` | `unreplayable` | 4.45 (4.43–4.82) | 8.12 (8.10–8.83) | 8.22 (8.21–8.96) | 4.43 (4.42–4.80) | 8.10 (8.09–8.82) | 8.20 (8.19–8.94) |
| `process/1s` | `replayable` | 0.67 (0.66–0.67) | 1.42 (1.37–1.43) | 1.45 (1.40–1.46) | 0.70 (0.68–0.70) | 1.48 (1.40–1.48) | 1.50 (1.43–1.51) |
| `process/1s` | `unreplayable` | 0.67 (0.66–0.67) | 1.42 (1.37–1.43) | 1.45 (1.40–1.46) | 0.67 (0.66–0.67) | 1.42 (1.37–1.43) | 1.44 (1.40–1.46) |

The most a push was acknowledged after it landed, milliseconds, and the last run's commits:

| Case | `replayable` | `unreplayable` | Commits a run | Flush | Log | Commit | Ack |
|---|---|---|---|---|---|---|---|
| `in-process/10s` | 347 (323–399) | −18 (−20 to −8) | 5–6: one at the end, the rest on size, cursors or the log | 5.1 ms | 978 ms | 276 ms | 4.7 ms |
| `in-process/1s` | 65 (56–95) | −3 (−4 to −3) | 3: two on the interval, one at the end | 2.7 ms | 196 ms | 37 ms | 0.9 ms |
| `process/10s` | 293 (265–319) | −4 (−12 to −4) | 4–5: one at the end, the rest on size, cursors or the log | 4.5 ms | 967 ms | 217 ms | 5.2 ms |
| `process/1s` | 51 (37–53) | −2 (−2 to −2) | 3: two on the interval, one at the end | 2.7 ms | 163 ms | 26 ms | 1.2 ms |

- Every push landed and was acknowledged in every run; the last run of each round took one attempt.
- At the streaming default the 10 s interval is not what commits: at half the rate, the policy's
  1 GiB of these rows arrives in about 7 s in process and 9 s spawned (**computed**: 1 GiB over
  8 kB a push at the rate pushed), and p99 is that wait and the commit's phases. At 1 s the
  interval governs, and p99 is the interval plus the phases.
- The log's phase is most of each commit: about 1 s of a 10 s case's, writing tens of thousands of
  seal frames, and 0.16–0.2 s at 1 s. The commit phase holds the destination's commit and the
  log's record of its receipt.
- The stream that cannot read again is acknowledged as its rows are logged, up to 20 ms before
  they land; the other is acknowledged after the commit phase and the log's receipt, up to about
  0.4 s after it lands.
- The in-process 10 s case spreads most: its rounds pushed at 17 000–20 300 a second, as the
  placement's unpaced run measured it.
- `perf stat` cannot difference the 10 s cases; the 1 s cases keep 1.35–1.57 CPUs busy in process
  and 2.50–2.63 spawned, the connector processes counted, at 1.41–1.62 instructions a cycle. One
  run in test mode makes 34–40 `fsync`s, its setup included.

## A logged commit of many segments

The first rounds failed at 10 s in both placements: a commit naming about 85 000 segments was
refused `log_frame_exceeds_budget`, its frame estimated at 86.9 MB against the log's 16 MiB share
of the default budget. The log charged 1 KiB for each segment a commit names, though its frame
holds the segments as ranges and takes a few kilobytes. It now charges each range, of the
commit's segments and of those it abandons; the figures above were taken with that fix.
