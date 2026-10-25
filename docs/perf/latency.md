# Latency

How long a row takes from its push to landing, and to its acknowledgement, in a following
pipeline. This record keeps `main`'s figures, in process and with both connectors spawned, at the
streaming default's interval and at one second, for the changes that move landing or
acknowledgement to state their latency rules against.

## Method

- **Rows.** A following source pushes 1 000 rows a push of one 64-bit column, the instant of the
  push on `CLOCK_MONOTONIC`, which every process on the host reads alike, a checkpoint after each
  push. It reads two streams of one unbounded partition each, incrementally: `replayable`, which it
  can read again, and `unreplayable`, which it cannot. A read that starts again pushes the rows no
  report covered with their first instants, so a retried row counts late, not new.
- **Rate.** Each placement first runs unpaced, committing every second for three seconds, and its
  cases push at half the pushes a second that run landed, an even share a stream.
- **Runs.** Each run follows its source for three intervals (`Until::For`), the source pushing
  throughout, and commits what it read at the end. Criterion's ten samples are ten runs, its
  warm-up's left out.
- **Landed.** The bench destination keeps its pipeline's state and epoch and nothing of the rows
  but their instants. It reads the clock as the commit holding a push returns: push to landed.
- **Acknowledged.** The source reads the clock as a report of committed cursors covers a push:
  push to acknowledged. Both streams are told once the commit holding the push lands.
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
  the four performance cores, spawned connectors on the eight efficient ones. Three rounds, each
  in a hold of the measurement lock of its own, started once no other heavy work ran; release
  profile.

## Results

Intel Core Ultra X7 358H, 2026-10-26, governor `powersave` with energy preference `performance`;
this change on `main` at `a5cb0617`, each round interleaved with one of the base's. Each round
began at a one-minute load under 1.5, the bench's own processes lifting it to 3–6, beside three
idle virtual machines of the owner's. A round another agent's profile overlapped was taken again.

The rate each placement pushed, half what it landed unpaced: `in-process` 85 201 pushes a second
(84 081–86 516), 85 million rows a second; `process` 23 260 (23 215–23 308). The base pushed
19 710 and 14 366 in its rounds.

Push to landed and push to acknowledged, seconds:

| Case | Stream | Landed p50 | Landed p99 | Landed max | Acknowledged p50 | Acknowledged p99 | Acknowledged max |
|---|---|---|---|---|---|---|---|
| `in-process/10s` | `replayable` | 0.62 (0.60–0.62) | 1.20 (1.18–1.21) | 1.23 (1.22–1.25) | 0.62 (0.61–0.63) | 1.20 (1.18–1.22) | 1.24 (1.22–1.26) |
| `in-process/10s` | `unreplayable` | 0.62 (0.60–0.62) | 1.20 (1.18–1.22) | 1.23 (1.22–1.25) | 0.63 (0.62–0.63) | 1.21 (1.19–1.23) | 1.24 (1.24–1.26) |
| `in-process/1s` | `replayable` | 0.52 (0.52–0.52) | 1.01 (1.01–1.01) | 1.05 (1.05–1.05) | 0.52 (0.52–0.53) | 1.02 (1.02–1.02) | 1.06 (1.05–1.06) |
| `in-process/1s` | `unreplayable` | 0.52 (0.52–0.52) | 1.02 (1.01–1.02) | 1.05 (1.05–1.05) | 0.53 (0.53–0.53) | 1.02 (1.02–1.03) | 1.06 (1.06–1.06) |
| `process/10s` | `replayable` | 2.16 (2.16–2.16) | 4.39 (4.39–4.40) | 4.45 (4.44–4.45) | 2.17 (2.17–2.17) | 4.40 (4.39–4.41) | 4.46 (4.45–4.46) |
| `process/10s` | `unreplayable` | 2.16 (2.16–2.16) | 4.39 (4.39–4.40) | 4.45 (4.44–4.45) | 2.17 (2.17–2.18) | 4.41 (4.40–4.42) | 4.46 (4.45–4.47) |
| `process/1s` | `replayable` | 0.51 (0.51–0.51) | 1.00 (1.00–1.01) | 1.03 (1.03–1.03) | 0.51 (0.51–0.51) | 1.01 (1.01–1.01) | 1.03 (1.03–1.03) |
| `process/1s` | `unreplayable` | 0.51 (0.51–0.51) | 1.00 (1.00–1.01) | 1.03 (1.03–1.03) | 0.52 (0.51–0.52) | 1.01 (1.01–1.01) | 1.03 (1.03–1.03) |

The most a push was acknowledged after it landed, milliseconds, and the commits, the phases a
commit's mean in each round's last run, the median of the three:

| Case | `replayable` | `unreplayable` | Commits a run | Flush | Commit | Ack |
|---|---|---|---|---|---|---|
| `in-process/10s` | 7 (7–8) | 14 (14–15) | 25–26: one at the end, the rest on size or cursors | 8.8 ms | 16.3 ms | 10.1 ms |
| `in-process/1s` | 5 (5–6) | 10 (10–11) | 3: two on the interval, one at the end | 7.2 ms | 13.9 ms | 8.4 ms |
| `process/10s` | 10 (10–10) | 16 (15–16) | 7: one at the end, the rest on size or cursors | 3.7 ms | 18.8 ms | 10.2 ms |
| `process/1s` | 2 (2–2) | 4 (4–4) | 3: two on the interval, one at the end | 7.4 ms | 4.7 ms | 3.0 ms |

- Every push landed and was acknowledged in every run; the last run of each round took one attempt.
- At the streaming default the 10 s interval is not what commits: the policy's 1 GiB of these
  rows arrives 24 or 25 times in a run's 30 s in process and 6 times spawned, at the rates pushed,
  and p99 is that wait and the commit's phases. At 1 s the interval governs, and p99 is the
  interval plus the phases.
- Both streams are told after the commit phase, the one that cannot read again a few
  milliseconds after the other, as its stream is told second.
- `perf stat` cannot difference the 10 s cases; the 1 s cases keep 1.03–1.08 CPUs busy in process
  and 2.21–2.41 spawned, the connector processes counted, at 1.48–1.95 instructions a cycle. No
  run syncs a file.
