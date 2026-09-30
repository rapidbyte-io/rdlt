# ADR 0032: What a source tells a run beside its data

Status: accepted, 2026-09-30.

## Context

ADR 0031 split the owner's four streaming items (2026-09-26, confirmed 2026-09-28) across M5d,
M5d2 and M5d3. M5d built continuous runs, interval re-planning and the offset-log source. M5d2
takes the rest of what a streaming source must be able to tell a run:

1. **The re-plan signal** (the rest of item 1). A topic whose partitions were increased, or a
   consumer group that rebalanced, knows its partitions changed long before the next interval.
2. **The lag hook** (item 3). Only the source knows how far a read is behind its newest data:
   cursors are opaque to the engine.
3. **Retention loss** (item 4). A log that dropped the messages a read would resume from must
   fail the run with a typed error by default; an opt-in policy resets to the earliest offset,
   and the report counts each reset.
4. **`S-PARTITION`** (ADR 0021): planned partitions cover each stream exactly once. It waited for
   M5 because its truth, a stream planned again from where its partitions stood, needs
   re-planning.

## Decision

- **The re-plan signal.** `Emitter::replan()` sends `SourceEvent::Replan`, on the wire a
  `ReadFrame` of kind `ReplanFrame` (8). In a run that follows its source and is not stopping,
  the coordinator plans the stream again as soon as it hears the signal, as the interval would,
  with the same guards: a new phase waits for a commit, and an ended partition waits for its
  end's commit. Signals heard together plan each stream once. Elsewhere the signal is ignored: a
  run that does not follow plans only at its phases. A source signals when its partitions
  change; each signal costs one `plan` call.
- **The lag hook.** `Emitter::behind(records)` sends `SourceEvent::Behind { records }`, on the
  wire a `BehindFrame` (9): how many records the read is behind its source's newest, as the
  source measures it. The coordinator keeps, per stream, the latest count of each partition by
  id, so a partition read again keeps its place. It forgets a partition a re-plan retires,
  whether its read was running or had ended, and every partition when the stream begins a new
  phase; a read asked to stop no longer moves it.
  `StreamReport::behind` is the total, `None` where no partition that still lags said. Freshness
  (time since the last commit) is the engine's own measure and lands with M7's metrics.
- **Retention loss.**
  - `ConnectorError::retention_lost(message)`: kind `Data`, so not retryable, with code
    `retention_lost` (`RETENTION_LOST`).
  - `StreamPlan::on_retention_loss(RetentionLoss::{Fail, Reset})`, `Fail` by default: the run
    fails with the source's typed error.
  - `Reset`: a partition whose read had a place to lose, resuming from a cursor or having
    checkpointed since, and fails as `retention_lost` reads again from its beginning, which a log
    serves from its earliest offset. So a consumer overtaken mid-read, months into a following
    run, resets too. The rows in between are lost; `StreamReport::retention_resets` counts each
    reset. A read from the beginning that fails so before any checkpoint fails the run: there is
    nothing earlier to reset to.
  - Only a stream read incrementally, from a source that can read it again, may reset; any
    other is refused before the run reads, as `retention_reset_unsupported`. A full read would
    load its rows twice into its cycle, a change stream would miss changes its table needs (a
    re-snapshot is M5d3's `reset(streams)`), and a source that cannot read again has no earliest
    to serve.
  - The failed read's open segment is abandoned, as a stopped partition's is: no checkpoint
    seals its rows, and the write-ahead log must not hold them for the rest of a load that may
    run for months. An abandoned segment's rows no longer count toward the commit policy's
    thresholds (`Progress::Abandoned`), so a long run's abandoned rows never keep a commit due.
- **The reference offset-log source** gains `retention` (messages each partition keeps). A read
  from before its earliest offset fails as `retention_lost`; a read with no cursor starts at the
  earliest. It reports `behind` after each checkpoint, and partition p0's following read signals
  a re-plan when the partition count changes, waking for it as it wakes for a message.
- **`S-PARTITION`**, "a stream's planned partitions cover it exactly once, and those planned
  again from where they stood cover what is left exactly once":
  - The truth is one uninterrupted read of every partition the stream's first plan names.
  - The clause reads each partition to its first checkpoint, plans the stream again from that
    state, as the engine does after a commit, and reads what the plan names from there. The two
    reads' rows must be equal as multisets: a JSON row rendered as its text, an Arrow row as
    each column's name and value.
  - Partitions are placed as the engine places them: a plan beginning a new phase at its starts,
    any other from each partition's recorded cursor, or else its beginning.
  - A stream with unbounded partitions in its first plan is skipped (no single read covers one),
    and so is one none of whose partitions checkpoints. The clause passes if it checked a stream.
  - A source that ignores its cursor now fails `S-PARTITION` as well as `S-RESUME`: it repeats
    rows however it is planned.
- **The simulation's streaming reads** report how far behind they are and signal a re-plan after
  every checkpoint of a following read. The partitions never change, so each signalled plan must
  change nothing, through every crash and fault the seed draws.

## Consequences

- A source can bring a new partition into a following run at once, report its lag, and say that
  its retention overtook a read; an operator chooses per stream whether that fails the run or
  loses the dropped rows and counts them.
- `S-PARTITION` completes the source clauses ADR 0021 named, so certification now checks that
  re-planning neither skips nor repeats rows.
- M5d3 remains: `reset(streams)` (ADR 0023, 0025) and change streams whose source cannot read
  again (ADR 0029).
