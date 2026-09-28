# ADR 0023: Change streams

Status: accepted, 2026-09-28.

## Context

M5 (§22) brings CDC, the WAL, continuous runs and history: change streams merged with deletes,
partial updates and truncates (§9.3, §9.4), the WAL (§15.6), `until` (§9.6), SCD2 (§9.5), and the
clauses and failpoints that check them. That is too much for one plan, so it is split. The owner
confirmed two streaming directions for M5 (2026-09-28): continuous runs re-plan their partitions,
and a source's lost retention is a typed error by default, with an opt-in reset that is counted.
The owner also ruled on the WAL (2026-09-28): a local store is enough, since in the worst case the
data is replayed from the source.

## Decision

- **M5 is split into six milestones.** M5a is fixed here; the rest are re-cut when each is
  planned.
  - **M5a**, change streams through the engine: the contract (ops, truncate, phases, change
    columns on `MergeKey`), the engine's path from push to commit, phases with re-planning, the
    memory, JSON lines and Arrow IPC destinations, a reference CDC source, the wire, and a
    simulated CDC workload checked against a model.
  - **M5b**, change streams for the SQL destinations (`sqlgen`, SQLite), and the clauses
    `S-ACK`, `D-DELETE`, `D-PARTIAL`, `D-TRUNCATE`, and `D-MERGE`'s seq guard.
  - **M5c**, the WAL: `WalStore`, `LocalWal`, replay, and acknowledging non-replayable sources
    once their seal is durable. There is no `ObjectStoreWal` (see below).
  - **M5d**, continuous runs (`until`) and the owner's streaming items 1–4, and `S-PARTITION`.
  - **M5e**, history (SCD2) and `D-HIST`.
  - **M5f**, the failpoint sweep, the kill matrix with spawned connectors, and M5's exit
    criterion.
- **The WAL has one store, `LocalWal`** (owner, 2026-09-28). §15.6's `ObjectStoreWal` is dropped:
  a worker that loses its disk replays from the source, which every replayable source can do. A
  non-replayable source still needs the WAL, and a stateless worker reading one keeps its WAL on
  a volume that outlives it. `WalStore` stays a trait, so a remote store can come back without
  changing the engine.
- **Truncate is op 3 in `_rdlt_op`**, in band and ordered by `_rdlt_seq`, and its rows name no
  key. A truncate at seq `s` removes every row whose seq is below `s`, or soft-deletes it, even
  rows staged earlier in the same commit. §9.4's "deletes all rows" is refined this way so that a
  replayed truncate cannot remove rows that changes after it wrote.
- **A destination learns the merge semantics from `MergeKey.changes`**:
  `Option<ChangeColumns { op, unchanged, deletion: Hard | Soft { at } }>`, a compatible addition
  to the proto. When it is set, seq is compared across commits: a row applies only if its seq is
  greater than the stored row's (the seq guard).
- **`_rdlt_op` and `_rdlt_unchanged` are directive columns on a merged table.** Written batches
  carry them, after the stored columns; the table never stores them, and they are not part of its
  schema changes. A change log (`cdc` read, `append` write) stores `_rdlt_op`, `_rdlt_seq` and
  `_rdlt_unchanged` as data. Because truncate rows have null keys, the written schema relaxes the
  key columns' nullability.
- **`_rdlt_seq` is stored as 16 bytes of `Binary` on every merged table.** A change stream's
  `FixedSizeBinary(16)` is converted to it, so a destination handles one type.
- **`_rdlt_unchanged` is a bitmap over the pushed batch's field ordinals.** The engine rewrites it
  over the written batch's ordinals, which the destination can resolve. An unchanged column with
  no stored row lands null. A partial update sent to a destination without `partial_updates` is a
  typed Config error (`partial_updates_unsupported`) naming the stream and its columns.
- **Soft deletes.** The engine fills `_rdlt_deleted_at` with the load's `loaded_at` on delete
  rows and leaves it null on others. The destination keeps the stored values, sets the column, and
  records the delete's seq. A destination declares the delete modes it supports; asking for one it
  lacks is a Config error (`delete_mode_unsupported`). A destination declaring none merges no
  change stream, even one ignoring every delete and truncate, since it would still need the seq
  guard and the change columns.
- **Defaults are `deletes: hard` and `on_truncate: apply`**, which replicate faithfully. `ignore`
  is opt-in, and the report counts every ignored delete and truncate (`deletes_ignored`,
  `truncates_ignored`). Only a change stream merged by key takes the settings
  (`plan_deletes_unused` otherwise). A `cdc` or `incremental` read written with `replace` is
  `plan_mode_invalid`.
- **A `cdc` stream accepts only change pushes.** Any other push is a typed Source error
  (`push_unexpected`), and so is a change push on a stream not read as `cdc`.
- **Normalizing a `cdc` stream is refused** (`normalize_changes_unsupported`) until child tables
  learn deletes and truncates. This applies to change logs as well, for one rule.
- **`Source::plan` returns a `PartitionPlan`**:
  `{ phase: Option<u16>, partitions, starts: BTreeMap<PartitionId, Cursor> }`. It converts from a
  `Vec<Partition>`, and the proto gains compatible fields for it. `starts` says where each
  partition of a new phase begins, such as the changes starting from the position the snapshot
  captured. A plan whose phase differs from the recorded one is a transition. The commit that
  first records it puts the phase and deletes the old phase's partition entries.
- **Phases advance within an attempt.** Once every partition of a phased stream's phase has ended
  and its end is committed, the coordinator plans the stream again. A plan naming a new phase
  launches that phase's partitions, from their starts, into the attempt's scope. A plan naming the
  same phase settles the stream for the attempt. The partitions run under the same supervision,
  so M5d's interval re-plans reuse this machinery.
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
  §9.3's column-wise merging of partial updates is left to the destination, which has the stored
  row. A batch holding a truncate is not compacted.
- **The reference CDC source** is `io.rapidbyte.changes` (`ChangesSource`). It reads a snapshot in
  partitions at seq 0, then a `changes` partition whose seqs start at 1. The simulation's
  `io.rapidbyte.sim.changes` serves a seeded workload of inserts, updates, partial updates,
  deletes and truncates over two rounds, through faults, crashes, stops and racing runs. It
  captures its snapshot partway through the changes, as §9.4 says a CDC source does. Snapshot
  rows carry the captured position, so a change read again from before it lands twice in a log.
  `check_changes` compares each merged table and each log with a model of the workload.

## Consequences

- A CDC source loads into the memory, JSON lines and Arrow IPC destinations with every delete and
  truncate mode: in process, spawned, and remote. The SQL destinations refuse change merges
  (`delete_mode_unsupported`) until M5b.
- `ChangeColumns`, `Deletion` and `PartitionPlan` are public contract. Connectors built on
  `Vec<Partition>` still compile through `From`.
- A destination that implements change merges must honour the seq guard across commits, and a
  truncate's seq bound. M5b's clauses check this for any destination.
- The WAL cannot survive the loss of a worker's disk. That costs a re-read from the source, or,
  for a non-replayable source, the changes the lost WAL held. A deployment that cannot afford
  that keeps its WAL on durable storage.
