# ADR 0024: Hardening, data correctness

Status: accepted, 2026-09-28.

## Context

The write and merge paths must keep data exact against inputs that look ordinary:
- a column named as one of the planner's internal names, such as `_rdlt_rank`, which would drop
  rows from SQLite merges;
- a merge key whose type widens, which would duplicate rows;
- JSON integers beyond 64 bits, which would round into one key;
- run-end encoded columns, which would hold a hundred times what they are charged;
- one JSON field, which would land in two columns.

Isolation between pipelines, what a connector sends and the protocol's order are ADR 0025's, and
JSON exactness ADR 0026's.

## Decision

- **sqlgen's internal names never collide with a table's.**
  - A merge ranks its rows under an alias that no column of the table has, compared without
    case.
  - Derived tables and indexes keep within `SqlDialect::max_identifier`. A name too long is the
    prefix `_rdlt_fit_` and the SHA-256 of the whole name in base 32: no name the planner derives
    uncut and no catalog table begins with that prefix, so a cut name meets only another cut name
    (ADR 0049).
  - The name functions are planner methods.
  - User columns may start with `_rdlt_`, but one named exactly as a metadata column, folded and
    cleaned, is refused, whatever metadata columns its table has yet (ADR 0046).
- **A merge key changes type only where equal keys keep matching.**
  - That holds where the destination stores both types by value: as themselves, or as integers of
    another width.
  - A key rendered into text renders the same value differently once its type changes: a
    decimal's scale, a date widened to a timestamp, a time zone. Such a change is refused as
    `merge_key_changed` before any row.
  - The simulation's model predicts the refusal independently.
- **JSON integers beyond 64 bits are read exactly** (ADR 0011).
  - The fast parse reads such an integer as the float nearest it, a whole float of magnitude 2⁶³
    or more. Such a float flags its chunk only where its record holds a run of 19 or more digits,
    as the only integer literals sonic-rs reads as floats do; a float written with an exponent,
    as `1.5e30` is, does not by itself.
  - A flagged chunk is parsed again with exact numbers: sonic-rs's raw numbers, walked through the
    same visitors.
  - An integer beyond u64 within 38 digits is a `Decimal(38,0)`, within 76 digits a
    `Decimal(76,0)`, and beyond that its column holds JSON text, as ADR 0007 states.
  - The exact parse reads every other number as the fast parse does: `-0` as the float zero, and a
    float beyond the finite ones refused as `json_invalid`. What a record shreds to never depends
    on what came before it in its chunk.
  - Rejected: `arbitrary_precision` for the workspace, which costs an allocation per number on the
    hot path and changes `serde_json` everywhere.
- **Table writers' memory is charged until they flush.** A writer holds each write's reservation
  until its table writer flushes, since a table writer may buffer what it stages. It flushes when the coordinator asks,
  and on its own when the budget is pressed while it holds writes it has not flushed. The budget
  is pressed while a request for pushes or for lowering waits, or while a holder of queued
  pieces waits for their writes.
- **Unbounded partitions.** `Partition::unbounded()` marks a partition that never ends, such as a
  change stream's, a log's, or the one partition of a stream that plans none
  (`Partition::single()`), as a source polled from a cursor reads.
  - Outside a full read such a partition is never `Done`. Rows it pushed after its last
    checkpoint are not sealed: its next read resumes from that checkpoint and reads them again.
  - A full read seals them with its end, as a bounded partition's, since its next cycle starts
    from the beginning: the cycle lands everything it read, and a `replace` table is whole.
  - Its seal leaves no cursor past them, so a read of the stream that is not full fails on a
    partition so finished, a Config error coded `cursor_missing` (ADR 0004), rather than read
    them again or pass over the partition for good.
  - An incremental read of one that pushes rows and no checkpoint after any fails, a `Source`
    error with the engine's code `checkpoint_missing` that no retry mends, since none of its rows
    could land: the engine judges the source broke the read's contract, as it judges a push the
    read mode does not take (`push_unexpected`), and its message says to checkpoint after the
    last row. A checkpoint before its first row does not count, and a read that pushes nothing
    ends cleanly. A change read is not held to this: a change source may read ahead of its round.
  - The engine decides each from the stream's read mode, which no source is told.
  - The plan's proto lists the unbounded ids; a stray id is refused.
  - Change partitions are unbounded, and snapshot partitions stay bounded.
  - The coordinator drains its writers before it closes the session, since a write no commit took
    may still be queued.
- **The simulation catches these classes.**
  - Drift columns take the engine's own names.
  - Merge keys arrive as the same numbers at two scales.
  - A change source reads ahead of its round, pushing changes with no checkpoint after them.
  - Half of all streams' partitions never end, whatever their read, and half of all streams'
    reads end on rows: a full read's end seals them, and a bounded partition so read is done and
    not read again. An incremental read of a partition that never ends leaves the rows after its
    last checkpoint to its next read, which reads them again, each landing once; where its source
    checkpoints on demand, it checkpoints nowhere, and is refused `checkpoint_missing` with none
    of its rows landed, until an operator mends the source.
  - A quarter of the streams that do not replace their table are read the other way in the next
    phase: an incremental read resumes from the cursor a full read ended on, and a full read after
    an incremental one reads every row again. Where the full read finished a partition that never
    ends, the incremental read is refused `cursor_missing`. Where the stream's rows read again
    hold as the model has them, an operator then resets it with its tables, as the refusal says,
    and its next read loads every row from its beginning, once; otherwise the phase stops short.
  - Incremental reads of partitions that never end are polled, not followed, outside streaming
    worlds: each run reads the rows added since the last, each once.
  - JSON pushes are written as text, so any integer pushes exactly.
  - With the unbounded, key or `cursor_missing` rule reverted, the simulation fails within its
    first seeds.

## Consequences

- **A removed row stays removed while its table is merged.** A change stream merges only into a
  destination that declares it merges changes (`change_merge_unsupported` otherwise), and every
  such destination keeps tombstones (ADR 0027), so a change sequenced before a hard delete or
  truncate, sent again, never brings its row back; `D-DELETE` and `D-TRUNCATE` certify it. A table
  replaced whole by a generation forgets its tombstones, as `D-DELETE` checks.
- **Portable sqlgen** is ADR 0028's.
