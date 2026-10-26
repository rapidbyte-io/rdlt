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
  the four performance cores, spawned connectors on the eight efficient ones. Three rounds, each
  in a hold of the measurement lock of its own, started once no other heavy work ran; release
  profile.

## Results

Intel Core Ultra X7 358H, 2026-10-25 and -26, governor `powersave` with energy preference
`performance`; `main` at `6365cc13` with the served bench's readings (`d2fc0f95`; the engine's
code is `main`'s). Three rounds, each one hold of the measurement lock, interleaved with the other
benches' rounds ([campaign.md](campaign.md)) and started once no other build ran. The one-minute
load was 0.77, 1.12 and 2.15 as the rounds began and 4.5–5.7 as they ended, the bench's own
processes beside three idle virtual machines of the owner's.

The rate each placement pushed, half what it landed unpaced: `in-process` 17 193 pushes a second
(16 985–20 508), 17 million rows a second; `process` 14 221 (11 993–14 289).

Push to landed and push to acknowledged, seconds:

| Case | Stream | Landed p50 | Landed p99 | Landed max | Acknowledged p50 | Acknowledged p99 | Acknowledged max |
|---|---|---|---|---|---|---|---|
| `in-process/10s` | `replayable` | 3.97 (3.56–4.05) | 7.01 (6.06–7.06) | 7.11 (6.14–7.16) | 4.26 (3.88–4.28) | 7.29 (6.41–7.36) | 7.43 (6.54–7.48) |
| `in-process/10s` | `unreplayable` | 3.97 (3.56–4.05) | 7.01 (6.06–7.06) | 7.11 (6.14–7.16) | 3.95 (3.54–4.02) | 6.98 (6.03–7.04) | 7.08 (6.11–7.13) |
| `in-process/1s` | `replayable` | 0.72 (0.70–0.74) | 1.57 (1.47–1.61) | 1.60 (1.50–1.64) | 0.77 (0.73–0.80) | 1.62 (1.53–1.71) | 1.65 (1.55–1.74) |
| `in-process/1s` | `unreplayable` | 0.72 (0.70–0.74) | 1.57 (1.47–1.61) | 1.60 (1.50–1.64) | 0.72 (0.69–0.74) | 1.56 (1.46–1.60) | 1.59 (1.49–1.63) |
| `process/10s` | `replayable` | 4.68 (4.67–4.77) | 8.36 (8.36–9.67) | 8.55 (8.46–9.79) | 4.97 (4.95–4.99) | 8.65 (8.64–9.89) | 8.81 (8.80–10.02) |
| `process/10s` | `unreplayable` | 4.68 (4.67–4.77) | 8.36 (8.36–9.67) | 8.55 (8.46–9.79) | 4.66 (4.66–4.75) | 8.34 (8.34–9.65) | 8.53 (8.44–9.78) |
| `process/1s` | `replayable` | 0.68 (0.65–0.68) | 1.43 (1.35–1.44) | 1.46 (1.38–1.47) | 0.70 (0.67–0.71) | 1.47 (1.37–1.48) | 1.50 (1.40–1.51) |
| `process/1s` | `unreplayable` | 0.68 (0.65–0.68) | 1.43 (1.35–1.44) | 1.46 (1.38–1.47) | 0.68 (0.65–0.68) | 1.43 (1.35–1.44) | 1.45 (1.38–1.47) |

The most a push was acknowledged after it landed, milliseconds, and the last run's commits:

| Case | `replayable` | `unreplayable` | Commits a run | Flush | Log | Commit | Ack |
|---|---|---|---|---|---|---|---|
| `in-process/10s` | 326 (315–419) | −21 (−23 to −7) | 5–6: one at the end, the rest on size, cursors or the log | 9.9 ms | 1 013 ms | 300 ms | 5.1 ms |
| `in-process/1s` | 85 (60–94) | −3 (−3 to −3) | 3: two on the interval, one at the end | 5.5 ms | 220 ms | 56 ms | 1.2 ms |
| `process/10s` | 326 (261–333) | −4 (−8 to −3) | 4–5: one at the end, the rest on size, cursors or the log | 7.0 ms | 993 ms | 234 ms | 4.8 ms |
| `process/1s` | 48 (30–52) | −2 (−2 to −2) | 3: two on the interval, one at the end | 4.3 ms | 165 ms | 29 ms | 1.1 ms |

- Every push landed and was acknowledged in every run; the last run of each round took one
  attempt and was not run again.
- At the streaming default the 10 s interval is not what commits: at half the rate, the policy's
  1 GiB of these rows arrives in about 7 s in process and 9 s spawned (**computed**: 1 GiB over
  8 kB a push at the rate pushed), and p99 is that wait and the commit's phases. At 1 s the
  interval governs, and p99 is the interval plus the phases.
- The log's phase is most of each commit: about 1 s of a 10 s case's, and 0.17–0.22 s at 1 s.
  The commit phase holds the destination's commit and the log's record of its receipt.
- The stream that cannot read again is acknowledged as its rows are logged, up to 23 ms before
  they land; the other is acknowledged after the commit phase and the log's receipt, up to about
  0.4 s after it lands.
- The in-process 10 s case spreads most: one round pushed at 20 500 a second and landed at
  p99 6.06 s, the other two at 17 000–17 200 and 7.01–7.06 s.

## A logged commit of many segments

The first rounds failed at 10 s in both placements: a commit naming about 85 000 segments was
refused `log_frame_exceeds_budget`, its frame estimated at 86.9 MB against the log's 16 MiB share
of the default budget. The log charged 1 KiB for each segment a commit names, though its frame
holds the segments as ranges and takes a few kilobytes. It now charges each range, of the
commit's segments and of those it abandons; the figures above were taken with that fix.
