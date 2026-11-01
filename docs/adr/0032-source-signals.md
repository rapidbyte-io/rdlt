# ADR 0032: What a source tells a run beside its data

Status: accepted, 2026-09-30.

## Context

A run that follows a streaming source plans its streams again at an interval (ADR 0031). A
streaming source must also be able to tell the run more than its data:

1. **The re-plan signal.** A topic whose partitions were increased, or a consumer group that
   rebalanced, knows its partitions changed long before the next interval.
2. **The lag hook.** Only the source knows how far a read is behind its newest data: cursors are
   opaque to the engine.
3. **Retention loss.** A log that dropped the messages a read would resume from must fail the run
   with a typed error by default; an opt-in policy resets to the earliest offset, and the report
   counts each reset.

Certification must check that planned partitions cover each stream exactly once (`S-PARTITION`),
whose truth, a stream planned again from where its partitions stood, needs re-planning.

## Decision

- **The re-plan signal.** `Emitter::replan()` sends `SourceEvent::Replan`, on the wire a
  `ReadFrame` of kind `ReplanFrame` (6). In a run that follows its source and is not stopping,
  the coordinator plans the stream again as soon as it hears the signal, as the interval would,
  with the same guards: a new phase waits for a commit, and an ended partition waits for its
  end's commit. Signals heard together plan each stream once. Elsewhere the signal is ignored: a
  run that does not follow plans only at its phases. A source signals when its partitions
  change; signals a partition sends before the coordinator hears them are kept as its state and
  cost one `plan` call together (ADR 0039).
- **The lag hook.** `Emitter::behind(records, headroom)` sends
  `SourceEvent::Behind { records, headroom }`, on the wire a `BehindFrame` (7): how many records
  the read is behind its source's newest, as the source measures it, and how much more its
  retention keeps before it drops where the read stands, in bytes, time or both, where it says.
  The coordinator keeps, per stream, the latest count and headroom of each partition by id, so a
  partition read again keeps its place. It forgets a partition a re-plan retires, whether its
  read was running or had ended, and every partition when the stream begins a new phase; a read
  asked to stop does not move it. `StreamReport::behind` is the total, `None` where no partition
  that still lags said, and `StreamReport::headroom` the least each partition said, of each
  measure. Retention is a pipeline's buffer through an outage, so an operator watches the
  headroom; no decision reads it. Freshness, when the last commit that landed a stream's rows or
  positions landed, is the engine's own measure, in the run's status (ADR 0004).
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
  - Only a stream read incrementally, from a source that can read it again, may reset; any other is
    refused before the run reads, as `retention_reset_unsupported`. A full read would load its rows
    twice into its cycle, a change stream would miss changes its table needs (a re-snapshot is a
    reset, ADR 0033), and a source that cannot read again has no earliest to serve.
  - The failed read's open segment is abandoned, as a stopped partition's is: no checkpoint
    seals its rows. An abandoned segment's rows do not count toward the commit policy's
    thresholds (`Progress::Abandoned`), so a long run's abandoned rows never keep a commit due.
- **The reference offset-log source** has `retention` (messages each partition keeps). A read
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
  - A source that ignores its cursor fails `S-PARTITION` as well as `S-RESUME`: it repeats rows
    however it is planned.
- **The simulation's streaming reads** report how far behind they are and signal a re-plan after
  every checkpoint of a following read. Where the seed's source does not churn (ADR 0031), the
  partitions never change, so each signalled plan must change nothing, through every crash and
  fault the seed draws; where it churns, a signalled plan names what the source names by then.

## Consequences

- A source can bring a new partition into a following run at once, report its lag, and say that
  its retention overtook a read; an operator chooses per stream whether that fails the run or
  loses the dropped rows and counts them.
- Certification checks that re-planning neither skips nor repeats rows (`S-PARTITION`).
