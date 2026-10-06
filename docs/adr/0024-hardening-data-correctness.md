# ADR 0024: Hardening, data correctness

Status: accepted, 2026-09-28.

## Context

An adversarial review the owner ran after M5a found thirteen defects and five test gaps. Five
were reproduced by running:
- a column named `_rdlt_rank` dropping rows from SQLite merges;
- a widened merge key duplicating rows;
- JSON integers beyond 64 bits rounding into one key;
- run-end encoded columns exceeding the memory budget a hundredfold;
- one JSON field landing in two columns.

The rest were confirmed by reading the code. The findings are split in two:
- **H1a** (this ADR): data correctness in the write and merge paths.
- **H1b**: isolation and robustness: table ownership across pipelines, validation of what a
  connector sends, the handshake's order, the files destination's durability, certification that
  compares content, and the int-then-float columns.

M5b follows H1b.

## Decision

- **sqlgen's internal names never collide with a table's.**
  - A merge ranks its rows under an alias that no column of the table has, compared without
    case.
  - Derived tables and indexes keep within `SqlDialect::max_identifier`. A name too long is the
    prefix `_rdlt_fit_` and the SHA-256 of the whole name in base 32: no name the planner derives
    uncut and no catalog table begins with that prefix, so a cut name meets only another cut name
    (ADR 0049).
  - The name functions became planner methods.
  - User columns may still start with `_rdlt_`: the engine names its own metadata columns around
    them. Amended 2026-10-03 (ADR 0046): one named exactly as a metadata column, folded and
    cleaned, is refused, whatever metadata columns its table has yet.
- **A merge key changes type only where equal keys keep matching.**
  - That holds where the destination stores both types by value: as themselves, or as integers of
    another width.
  - A key rendered into text renders the same value differently once its type changes: a
    decimal's scale, a date widened to a timestamp, a time zone. Such a change is refused as
    `merge_key_changed` before any row.
  - The simulation's model predicts the refusal independently.
- **JSON integers beyond 64 bits are read exactly** (ADR 0011 holds again).
  - The fast parse reads such an integer as the float nearest it, and nothing else parses as a
    whole float of magnitude 2⁶³ or more, so such a float flags its chunk.
  - A flagged chunk is parsed again with exact numbers: sonic-rs's raw numbers, walked through the
    same visitors.
  - An integer beyond u64 within 38 digits is a `Decimal(38,0)`, within 76 digits a
    `Decimal(76,0)`, and beyond that its column holds JSON text, as ADR 0007 states.
  - The exact parse reads every other number as the fast parse does: `-0` as the float zero, and a
    float beyond the finite ones refused as `json_invalid`. What a record shreds to never depends
    on what came before it in its chunk.
  - Rejected: `arbitrary_precision` for the workspace, which costs an allocation per number on the
    hot path and changes `serde_json` everywhere.
- **Writers' memory is charged until they flush.** A lane holds each write's reservation until its
  writer flushes, since a writer may buffer what it stages. It flushes when the coordinator asks,
  and on its own when the budget is pressed while it holds writes it has not flushed. The budget
  is pressed while a request for pushes or for lowering waits, or while a holder of queued
  pieces waits for their writes.
- **Unbounded partitions.** `Partition::unbounded()` marks a partition that never ends, such as a
  change stream's or a log's.
  - Such a partition is never `Done`.
  - Rows it pushed after its last checkpoint are not sealed: its next read resumes from that
    checkpoint and reads them again.
  - The plan's proto lists the unbounded ids; a stray id is refused.
  - Change partitions are unbounded, and snapshot partitions stay bounded.
  - The coordinator drains its lanes before it closes the session, since a write no commit took
    may still be queued.
- **The simulation catches these classes.**
  - Drift columns take the engine's own names.
  - Merge keys arrive as the same numbers at two scales.
  - A change source reads ahead of its round, pushing changes with no checkpoint after them.
  - JSON pushes are written as text, so any integer pushes exactly.
  - With the unbounded or key rule reverted, the simulation fails within its first seeds.

## Consequences

- **Deferred to M5b: tombstones for hard deletes and truncates.**
  - A row that arrives later with a lower sequence than a hard delete or truncate would come back.
  - No path delivers one today: one changes partition, the snapshot before its changes, and runs
    fenced by epoch.
  - The design belongs with M5b's change merges for the SQL destinations and the `D-DELETE`
    clause.
- **Deferred to H1b: JSON integers beyond 64 bits in the simulation, and int-then-float
  columns.** The model's JSON number inference needs value-level exactness for both.
- **Deferred to M5b: portable sqlgen.** Moving DDL out of the commit transaction and replacing
  `ON CONFLICT` and row-value `IN` are part of making sqlgen fit Postgres and warehouses.
