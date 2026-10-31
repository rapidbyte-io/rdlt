# ADR 0023: Change streams

Status: accepted, 2026-09-28.

## Context

Change streams carry what a source captures of a database's changes: inserts, updates, partial
updates, deletes and truncates, merged by key with each delete and truncate applied, or appended
to a log. Such a source reads a snapshot first and its changes after, from the position the
snapshot captured, for as long as the database changes. The engine, every destination that
merges, the wire and the simulation must agree on what each change does and in which order.

## Decision

- **Truncate is op 3 in `_rdlt_op`**, in band and ordered by `_rdlt_seq`, and its rows name no
  key. A truncate at seq `s` removes every row whose seq is below `s`, or soft-deletes it, even
  rows staged earlier in the same commit. Bounded by its seq, a truncate sent again cannot remove
  rows that changes after it wrote.
- **A destination learns the merge semantics from `MergeKey.changes`**:
  `Option<ChangeColumns { op, unchanged, deletion: Hard | Soft { at } }>`. When it is set, seq is
  compared across commits: a row applies only if its seq is greater than the stored row's (the seq
  guard).
- **`_rdlt_op` and `_rdlt_unchanged` are directive columns on a merged table.** Written batches
  carry them, after the stored columns; the table never stores them, and they are not part of its
  schema changes. A change log (`cdc` read, `append` write) stores `_rdlt_op`, `_rdlt_seq` and
  `_rdlt_unchanged` as data. Because truncate rows have null keys, the written schema relaxes the
  key columns' nullability.
- **`_rdlt_seq` is stored as 16 bytes of `Binary` on every merged table.** A change stream's
  `FixedSizeBinary(16)` is converted to it, so a destination handles one type.
- **`_rdlt_unchanged` is a bitmap over the pushed batch's field ordinals.** The engine rewrites it
  over the written batch's ordinals, which the destination can resolve. An unchanged column with
  no stored row lands null. A partial update merged into a destination without
  `partial_updates` is a typed Config error (`partial_updates_unsupported`) naming the stream
  and its columns. A source does not declare that it sends partial updates, so the error comes
  with the first one, after earlier rows committed, and every retry meets it again: the stream
  needs a destination that keeps columns. A change log stores the flags as data, so it needs no
  such destination.
- **Soft deletes.** The engine fills `_rdlt_deleted_at` with the load's `loaded_at` on delete
  rows and leaves it null on others. The destination keeps the stored values, sets the column, and
  records the delete's seq. A row deleted already keeps its deletion time: a later delete or
  truncate advances only its seq. A destination declares the delete modes it supports; asking
  for one it lacks is a Config error (`delete_mode_unsupported`). A destination declaring none
  merges no change stream, even one ignoring every delete and truncate, since it would still need
  the seq guard and the change columns.
- **Defaults are `deletes: hard` and `on_truncate: apply`**, which replicate faithfully. `ignore`
  is opt-in, and the report counts every ignored delete and truncate (`deletes_ignored`,
  `truncates_ignored`). Only a change stream merged by key takes the settings
  (`plan_deletes_unused` otherwise). A `cdc` or `incremental` read written with `replace` is
  `plan_mode_invalid`.
- **A table records who made its sequences** (`StateEntry::Sequences`: `Engine` or `Source`).
  The first commit of each attempt records it where it changed. A change merge compares its
  source's positions with the stored rows', which only works when the stored sequences are that
  source's positions: the engine's, made for ordering within one commit, compare above any of
  them. A change merge into a table whose rows the engine sequenced, or that a load created
  before state recorded its sequences, is therefore refused before any row
  (`table_sequences_mismatch`, a Config error): a backfill by a full merge and the CDC that
  follows load into tables of their own.
- **Only change streams read in phases.** A plan beginning a phase for a stream not read as
  `cdc` is a typed Source error (`phase_unexpected`), since the stream would start over from its
  plan's starts every run.
- **A `cdc` stream accepts only change pushes.** Any other push is a typed Source error
  (`push_unexpected`), and so is a change push on a stream not read as `cdc`.
- **Normalizing a `cdc` stream is refused** (`normalize_changes_unsupported`) until child tables
  learn deletes and truncates. This applies to change logs as well, for one rule.
- **`Source::plan` returns a `PartitionPlan`**:
  `{ phase: Option<u16>, partitions, starts: BTreeMap<PartitionId, Cursor> }`. It converts from a
  `Vec<Partition>`. `starts` says where each partition of a new phase begins, such as the changes
  starting from the position the snapshot captured. A plan whose phase differs from the recorded
  one is a transition. The commit that first records it puts the phase and deletes the old phase's
  partition entries.
- **Phases advance within an attempt.** Once every partition of a phased stream's phase has ended
  and its end is committed, the coordinator plans the stream again. A plan naming a new phase
  launches that phase's partitions, from their starts, into the attempt's scope. A plan naming the
  same phase settles the stream for the attempt. The partitions run under the same supervision,
  and a following run's interval re-plans reuse this machinery (ADR 0031).
- **A new phase is recorded with its starts.** The commit that records a phase also records each
  of its partitions at its start. An attempt that ends before a partition's first checkpoint
  then resumes the partition where the plan said, not from the beginning, which would re-read
  what the snapshot already holds.
- **A stop ends a phased stream stopped.** Once the coordinator has acted on a stop, no phase
  begins. A stop that arrives as the last partition of a phase ends still lets the next phase
  begin; the load that follows stops its partitions, and the attempt ends stopped, not
  exhausted, since the stream had more to read.
- **Compaction of change rows.** Within a coalesced batch, the engine drops a row that a later
  row of its key supersedes: an insert or update that flags nothing unchanged, or a hard delete.
  Partial updates and soft deletes are kept for the destination, which applies rows in seq order.
  Merging a partial update into its key's row column by column is left to the destination, which
  has the stored row. A batch holding a truncate is not compacted.
- **The reference CDC source** is `io.rapidbyte.changes` (`ChangesSource`). It reads a snapshot in
  partitions, taken after its first `captured` changes and carrying that position, then a `changes`
  partition starting after it. The simulation's `io.rapidbyte.sim.changes` serves a seeded workload
  of inserts, updates, partial updates, deletes and truncates over two rounds, through faults,
  crashes, stops and racing runs, on the simulated network one seed in four (ADR 0019). It captures
  its snapshot partway through the changes, as a database's change capture does. Snapshot rows
  carry the captured position, so a change read again from before it lands twice in a log.
  `check_changes` compares each merged table and each log with a model of the workload.

## Consequences

- A CDC source loads into the memory, JSON lines and Arrow IPC destinations with every delete and
  truncate mode: in process, spawned, and remote. The SQL destinations take change merges too
  (ADR 0027).
- `ChangeColumns`, `Deletion` and `PartitionPlan` are public contract; a source without phases
  plans a `Vec<Partition>`, which converts through `From`.
- A destination that implements change merges must honour the seq guard across commits, and a
  truncate's seq bound. `D-MERGE` and `D-TRUNCATE` check this for any destination.
- A destination declares that it merges change streams (`Capabilities::merge_changes`; ADR 0027),
  and one that does not is sent no change stream to merge, so none upserts change rows as data.
- State records name their format and refuse fields their reader does not know, so a build
  reading another's state refuses it (ADR 0041).
- Switching an existing merge table to CDC needs a new table: a `ResetScope::Tables` reset drops
  the stream's tables, and the next run creates them anew (ADR 0033).
