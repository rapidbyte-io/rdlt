# ADR 0034: Change streams whose source cannot read again

Status: accepted, 2026-09-30.

## Context

A change stream's phases move its partitions' positions other than forward. A commit that
begins a phase deletes the previous phase's partition positions and puts the new phase's start
cursors, and partition ids may recur across phases. A source that cannot read again, a
replication slot or a queue, drops what it was told was committed. Such a stream has to load a
snapshot and then its changes exactly once through crashes, failed commits and racing runs.

## Decision

- **Told only what landed.** Like every stream (ADR 0030), a change stream is told a cursor only
  once the commit holding the rows before it has landed, phase transition included. What its
  source was told is never ahead of what the destination recorded, so every load starts from the
  destination's state: its phase, and each partition's position in it.
- **The source contract has two clauses for these streams** (`StreamSpec::with_replayable`,
  `ReadStream::plan`).
  - A phase's start cursors come from the stream's state alone, never from what the source was
    told: that trails what landed after a crash between a commit landing and its telling, and a
    start from there would land rows twice. A load that begins the phase again after a fenced or
    failed one begins it where that one did.
  - A source that cannot read again serves again everything after the last cursor it was told,
    refuses, as transient, a read from before that cursor, and accepts one from a cursor it
    issued at or after it. A refusal means the destination lost a commit it had answered; it
    fails the run where starting later would lose the rows between.
- **No read in full.** A full read of such a stream starts from its beginning, which its source
  dropped, and is refused as `full_read_unreplayable`.
- **The reference change source can forget.** `replayable: false` makes it refuse, as transient,
  a read of its changes from before the position its slot was told, as a replication slot does. It
  passes certification, `S-ACK` included.
- **The simulation.** Every other incremental and change stream's source forgets what it was told,
  on every partition, and never serves it again, independently of the other drawn features. Racing
  runs give two readers to one such partition. The oracles check that every position a source was
  told landed; a snapshot partition's position is gone once its stream reads its changes, which
  counts as landed.

## Consequences

- A source that cannot read again, a replication slot or a queue, loads a snapshot then its
  changes exactly once through crashes and failed commits.
- Its source is told one destination commit later than the rows were read, and a slot keeps that
  much more of its database's log.
- Such a stream cannot be reset (ADR 0033): what its source was told is gone.
