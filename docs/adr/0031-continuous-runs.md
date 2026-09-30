# ADR 0031: Continuous runs

Status: accepted, 2026-09-30.

## Context

Spec §9.6 gives a run `until: exhausted | forever | <duration>`. It defines `exhausted` for CDC
("caught up to the change-stream position observed when the run started"), and says a timer
keeps quiet streams committing and that the retry budget resets after progress. Until now a run
ended once every partition's read returned: `exhausted` was all the engine had.

The owner asked (2026-09-26, confirmed 2026-09-28) that M5 make streaming sources, such as Kafka,
first-class. Four items settle what the spec leaves open:

1. **Partitions that appear or change mid-run.** A continuous run plans its partitions again on an
   interval and when the source signals a change. Running partitions keep their segments, and
   removed ones drain to a commit.
2. **A reference streaming source** in the reference connectors and the simulation.
3. **A lag hook**, for a source to report its high-water mark per partition.
4. **Retention loss** is a typed error by default. An opt-in policy resets to the earliest
   offset, and the report counts each reset.

M5d is split by what each part needs:

- **M5d (this ADR): continuous runs.**
  - `until`, and how a source learns to follow.
  - Interval re-planning, including item 1's removed partitions.
  - Read slots, the streaming commit default and bounded reports.
  - The reference offset-log source (item 2), and the simulation's streaming feature.
- **M5d2:** the source's re-plan signal (the rest of item 1), `S-PARTITION`, the lag hook (item 3)
  and retention loss (item 4). `S-PARTITION` needs re-planning for a truth to compare with
  (ADR 0021).
- **M5d3:** `reset(streams)` (ADR 0023, 0025) and change streams whose source cannot read again
  (ADR 0029).

Item 4's reset and a stream reset share only the clearing of state entries. The first acts on one
partition inside a run; the second is an operator's command between runs. So they are built apart.

## Decision

- **`Until`.** `PipelinePlan::with_until(Until::{Exhausted, Forever, For(Duration)})`, with
  `Exhausted` as the default. The deadline is run-wide and measured from the run's start with
  `Env`, so it is deterministic in the simulation.
- **The follow contract.**
  - `ReadRequest::follow` (wire `ReadStart.follow = 7`, authors `Emitter::follows()`) says
    whether a read of an unbounded partition follows it once caught up, waiting for more until
    asked to stop.
  - Otherwise the read returns once caught up to where its source stood when the read started:
    the spec's `exhausted`, now for every unbounded partition, not only CDC's.
  - `exhausted` reads without following; `forever` and `<duration>` follow the streams they plan
    again as they read (incremental and change streams). A full read ends at its head, so its
    cycle completes and a replace stream's generation is swapped in. A bounded partition ignores
    the flag.
  - `Emitter::stopped()` resolves once the engine asks the read to stop or drops its end, so a
    read waiting for data can return. Before this, a stop reached a waiting read only at its next
    emit.
- **`until: <duration>` ends the run `Succeeded`.** At the deadline the engine stops as
  `StopMode::AfterCommit` does: a last commit of everything checkpointed. The run read for as long
  as it was asked, so `Stopped` stays the status of an external stop. A deadline that finds the
  run's last attempt failed, and waiting to be retried, ends it `Failed` with that error.
- **Re-planning.** In a following run every incremental stream tracks its partitions, as a
  change stream's phases do. Every `replan` interval (`EngineConfigBuilder::replan`, default
  60 s, never zero, on `Env`'s clock) the coordinator plans each such stream again from its
  committed positions:
  - a plan naming a new phase waits for the next commit to begin it: phases begin only after a
    commit, which takes every seal, or a seal of the phase before would record its position
    after the new phase began;
  - a partition the plan names that is neither running nor `Done` starts from its committed
    position, or else from its beginning, as initial planning starts it: a plan's starts place
    only a new phase's partitions. New partitions start this way, and so do bounded partitions
    that ended, so an incremental table is polled;
  - a running partition the plan no longer names is stopped through a token of its own. It seals
    its last checkpoint, and its end is committed. Its state entry stays, as initial planning keeps
    the entries of partitions a plan drops;
  - an ended partition whose end is not yet committed waits for a later plan. Read again from its
    committed position, it would read again what its end seals;
  - a partition read again takes the place its ended read had, so a run that reads for ever
    tracks each partition once.

  Re-planning does not commit. The commit policy alone decides when a following run commits.
- **Read slots.** In a following run an unbounded partition's read holds no read slot: it lives as
  long as the run, mostly waiting, and a topic may have more partitions than slots. The memory
  budget still bounds what they hold. Bounded partitions share the slots as before.
- **The commit default.** An unset commit policy resolves per run:
  - `CommitPolicy::streaming()` (every 10 s, or 1 GiB) where the run follows its source or reads
    changes (spec §6.4);
  - 60 s or 1 GiB otherwise.

  A policy that is set is kept. `EngineConfig::commit` is now `Option`.
- **A bounded write-ahead log.** A partition that ends without sealing its open segment (a
  stopped one, or an unbounded one with rows after its last checkpoint) tells the log, which
  settles the segment, so it holds no chunk back. The writer forgets a settled segment once no
  chunk holds it, so a load that commits for months keeps a bounded set.
- **`S-STOP` certifies the follow contract.** Beside the read stopped before it starts, it reads
  each unbounded partition following it, reached through the stream's phases as the engine
  reaches them. It drains the read until it goes quiet, as one caught up waiting for data does,
  then asks it to stop: the read must end within the stop window, cleanly.
- **Bounded reports.** An attempt folds each commit into running totals rather than keeping every
  commit record. A run keeps only its latest two attempts unfolded, since a later attempt may
  credit a commit in flight to the one before it. A credit that finds its attempt already folded
  goes to the report, by the load its receipt names. The report lists the latest 128 attempts
  (`REPORTED_ATTEMPTS`) and counts them all (`Report::attempted`).
- **`S-ACK` probes incremental streams too:** the first stream read as changes, else the first
  read incrementally. An offset log's consumer group keeps its committed offsets outside the
  engine, as a slot keeps its position, so the clause certifies it. Its statement drops "change".
- **The reference offset-log source** `io.rapidbyte.log`:
  - Each stream is a set of partitions `p0..`, and each partition is a log of seeded messages
    that grows by `per_second` from a process-wide origin on tokio's clock, so paused tests are
    deterministic.
  - Committed offsets are kept in a named consumer group, never moving back (`kept.rs` is shared
    with the change source's slots).
  - Partitions may be added later (`partitions_later`) or retired (`partitions_retired`), may end
    at their head (`bounded`), and may forget what their group committed (`replayable: false`).
  - It is certified, `S-ACK` included, replayable or not.
- **The simulation's `streaming` feature**, drawn apart from the seed's generator:
  - The incremental streams follow, and every other one's partitions never end.
  - Each phase begins with runs `until: For(d)`, disrupted and faulted as the seed says, while a
    production future joined with them makes rows arrive. The source serves what has arrived, and
    a following read waits on the world's notification, or a stop.
  - The source serves only whole checkpoint groups (whole batches where the stream checkpoints
    on demand) until every row has arrived. Every read then sends the batches and checkpoints the
    model infers types and discards by. Seed 4516 found reads that cut batches wherever rows had
    arrived.
  - Then every row arrives, and the phase converges as before. The model's checks cover
    exactly-once.
  - A refusal the model says every run must meet only may happen in a following run, which may
    end before the rows that need it arrive.
  - Streaming worlds plan again every quarter second.

## Consequences

- A pipeline can load a Kafka-style topic, a change stream or a polled table continuously, with
  exactly-once delivery through crashes, stops, faults and partitions that come and go.
- The first run of the simulation's streaming feature found a report-folding defect: a failed
  attempt's commit in flight can wait several attempts for its credit. It is fixed as above.
- A following run calls `plan` once per stream every `replan` interval.
- `ReadRequest` is `#[non_exhaustive]` with a constructor, so later fields need no change to
  callers.
