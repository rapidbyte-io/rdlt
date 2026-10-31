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
  time, until what it read lands, as its next run would land it. The refused cases are recorded
  below, under their own heading.
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

## Refused reports

The `refused-10` cases, whose source refuses one report of committed cursors in ten, drawn, on
`main` at `ed6d230c` and with a refused report owed to the next commit's report rather than ending
the attempt. Intel Core Ultra X7 358H, 2026-10-28, three rounds interleaved (base, branch; branch,
base; base, branch), each tree's cases run as `taskset -c 0-3` with spawned connectors on CPUs
4-11, the first started at a one-minute load of 1.32, the bench lifting it to 2-3, beside the
owner's idle virtual machines. Seconds; the median of three rounds and their range, each round's
figure the median of its ten runs; "latest after landing" is a run's most a push was acknowledged
after it landed, and "highest" the most of any run.

| Case | Stream | Landed p99, `main` | Landed p99, owed | Latest after landing, `main` | Latest after landing, owed | Highest, `main` | Highest, owed |
|---|---|---|---|---|---|---|---|
| `in-process/10s` | `replayable` | 1.161 (1.136–1.162) | 1.138 (1.137–1.199) | 2.033 (1.530–2.074) | 1.219 (1.157–2.303) | 3.829 | 3.640 |
| `in-process/10s` | `unreplayable` | 1.161 (1.134–1.164) | 1.141 (1.140–1.203) | 3.069 (2.093–3.416) | 1.163 (1.161–1.224) | 6.211 | 2.436 |
| `in-process/1s` | `replayable` | 1.009 (1.008–1.011) | 1.011 (1.009–1.013) | 0.005 (0.005–0.968) | 0.006 (0.005–0.006) | 1.817 | 2.070 |
| `in-process/1s` | `unreplayable` | 1.010 (1.009–1.010) | 1.012 (1.012–1.014) | 0.011 (0.011–1.528) | 0.011 (0.010–0.012) | 3.028 | 1.056 |
| `process/10s` | `replayable` | 4.303 (4.289–4.331) | 4.292 (4.289–4.349) | 4.561 (0.010–5.011) | 0.010 (0.010–3.729) | 10.602 | 8.777 |
| `process/10s` | `unreplayable` | 4.303 (4.289–4.331) | 4.292 (4.289–4.349) | 5.147 (4.769–5.232) | 0.024 (0.020–4.399) | 10.618 | 12.985 |
| `process/1s` | `replayable` | 1.004 (1.004–1.005) | 1.004 (1.004–1.004) | 0.003 (0.002–0.003) | 0.003 (0.003–0.003) | 1.493 | 1.028 |
| `process/1s` | `unreplayable` | 1.004 (1.004–1.005) | 1.004 (1.004–1.004) | 0.004 (0.004–0.004) | 0.005 (0.005–1.019) | 2.046 | 2.009 |

- On `main` a refused report ends its attempt: the last run of a round made two to seven attempts
  in the 10 s cases and two in process at 1 s. Owed, every last run made one attempt, and saw up to
  seven reports fail.
- The rule: a refused cursor's push to acknowledged is at most one commit interval above its push
  to landed, and p99 push to landed is not above `main`'s.
  - At 10 s the policy's 1 GiB commits every 1.1 s in process and 4.4 s spawned, and the next of
    them tells a refused cursor: the latest after landing, 1.2 s in process and up to 4.4 s
    spawned, against `main`'s 2.0–5.1 s. The highest, up to 13.0 s spawned, are runs whose cursor
    two or three reports in a row refused, each draw one in ten, or whose last commit's report was
    refused, which ends the attempt for the next run to tell; the readings do not tell them apart.
  - At 1 s a refused cursor is told 1.018–1.056 s after it landed: the interval and 18–56 ms, the
    refused report's own time and the next commit's flush, commit and report, whose mean is
    14.5–31.8 ms a commit. The rule holds within that time, not within the interval alone. The
    2.0 s and 2.1 s are cursors two reports in a row refused.
  - Landed p99 is lower than `main`'s at 10 s and equal spawned at 1 s. In process at 1 s it is
    2 ms (0.2 %) higher, in every round for the stream that cannot read again (1.012–1.014 s
    against 1.009–1.010 s): there the rule's second half misses by 2 ms.

## Transient commits

The `transient-10` cases, whose destination answers one commit in ten, drawn, `Transient` before
it lands, and the plain cases beside them, on `main` at `ea57c46d` and with such a commit sent
again in place. Intel Core Ultra X7 358H, 2026-10-31, three rounds interleaved (base, branch;
branch, base; base, branch, for the plain cases and then for the `transient-10` ones), each
tree's cases run as `taskset -c 0-3` with spawned connectors on CPUs 4-11, each round in a hold of
the measurement lock of its own, started at a one-minute load of 1.46, 0.88 and 1.45, the bench
lifting it to 2-3.7. Push to landed p99, seconds, the median of three rounds and their range,
each round's figure the median of its ten runs:

| Case | Stream | `main` | Sent again | `main`, `transient-10` | Sent again, `transient-10` |
|---|---|---|---|---|---|
| `in-process/10s` | `replayable` | 1.052 (1.046–1.055) | 1.059 (1.053–1.064) | 2.272 (2.263–2.320) | 1.687 (1.540–2.591) |
| `in-process/10s` | `unreplayable` | 1.056 (1.049–1.058) | 1.063 (1.056–1.068) | 2.272 (2.265–2.321) | 1.687 (1.540–2.592) |
| `in-process/1s` | `replayable` | 1.009 (1.009–1.010) | 1.009 (1.007–1.014) | 1.009 (1.008–1.011) | 1.010 (1.007–1.013) |
| `in-process/1s` | `unreplayable` | 1.010 (1.009–1.010) | 1.011 (1.008–1.014) | 1.010 (1.010–1.011) | 1.016 (1.009–1.017) |
| `process/10s` | `replayable` | 4.088 (4.053–4.099) | 4.100 (4.083–4.105) | 6.385 (6.113–6.469) | 4.166 (4.099–4.369) |
| `process/10s` | `unreplayable` | 4.088 (4.052–4.099) | 4.100 (4.083–4.105) | 6.383 (6.113–6.469) | 4.166 (4.099–4.369) |
| `process/1s` | `replayable` | 0.996 (0.996–0.996) | 0.996 (0.996–0.996) | 0.996 (0.996–0.996) | 0.996 (0.996–0.996) |
| `process/1s` | `unreplayable` | 0.996 (0.996–0.996) | 0.996 (0.996–0.996) | 0.996 (0.995–0.996) | 0.996 (0.996–0.996) |

- On `main` a transient answer ends its attempt: the last run of a round made two to four
  attempts in the 10 s cases. Sent again in place, every last run made one attempt.
- The rule: with one commit in ten answered `Transient`, p99 push to landed is not above `main`'s
  plus the retry budget, 15 s; without it, not above `main`'s.
  - With it, p99 is below `main`'s own: 1.69 s against 2.27 s in process at 10 s, 4.17 s against
    6.38 s spawned. At 1 s the interval governs both.
  - Without it, at 10 s p99 is 7 ms (0.7 %) above `main`'s in process and 12 ms (0.3 %) spawned.
    It is the time the policy's 1 GiB takes to arrive at the rate each case pushes, half what an
    unpaced run of its own landed: the branch's plain cases pushed 0.4-0.9 % fewer pushes a second
    than the base's in each round (96,184-97,166 in process against 97,054-97,797), and pushes a
    second times p99, the pushes 1 GiB holds, agrees within 0.1 % in every round and placement
    (102,301-102,694 against 102,325-102,690). Where both pushed alike, spawned in the third
    round (25,048 against 25,051 a second), p99 was 4.0999 s against 4.0992 s. Over all six
    unpaced runs of each tree the rates match: a median of 97,174 pushes a second against 97,185.
    At 1 s p99 is `main`'s within 2 ms.
- Three of the branch's 1 s `transient-10` rounds ran a run again: the draw failed the commit the
  stop at the run's end asked for, and a stop ends such a commit rather than sends it again, as on
  `main`, where a failed attempt meets the stop.
