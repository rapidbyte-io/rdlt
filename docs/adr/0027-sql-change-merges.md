# ADR 0027: SQL change merges and tombstones

Status: accepted, 2026-09-29.

## Context

Change streams merge into the memory and files destinations through the Arrow merge, and into
SQL destinations through what `sqlgen` plans. A destination that does not know
`MergeKey::changes` would upsert change rows as data, and its delete modes cannot tell the engine
otherwise: "no delete mode" does not mean "cannot merge changes".

The Arrow merge applies a change only past the sequence of the row its key holds. After a hard
delete or a hard truncate no row holds the key, so without a record of the removal a change
sequenced before it, sent again by a source delivering at least once, would bring the row back.
Soft deletes keep their rows, whose sequences guard them.

Certification must check deletes, partial updates and truncates, and the seq guard across
commits, beside merges by key.

## Decision

- **A destination declares that it merges change streams**, `Capabilities::merge_changes`, carried
  on the wire (`merge_changes`, field 11 of the capabilities message; absent reads false). The
  engine refuses a change stream's merge into a destination that does not, as
  `change_merge_unsupported`, whatever its delete modes: ignoring every delete and truncate still
  needs the seq guard and the change columns. A destination that merges changes needs no delete
  mode for a stream that removes nothing.
  - `partial_updates_unsupported` is a configuration error rather than an unsupported one: the
    plan asks for something the destination cannot do, which the operator fixes.
- **Tombstones.** A change stream's table remembers each key a hard delete removed, with the
  delete's sequence, and the latest hard truncate's sequence, the bound. A change sequenced at or
  before its key's tombstone, or before the bound, never applies. A later change of the key lifts
  its tombstone; raising the bound drops the tombstones before it.
  - Every destination keeps them: the Arrow merge (memory, files and the simulation's
    destination), the SQL destinations, and certification's vault. The memory and files
    destinations store them beside the table's rows, the files destination in its manifest, which
    its garbage collection keeps; a SQL destination in the table `_rdlt_tombstones__{name}`, whose
    bound is a row naming no key.
  - Tombstones are kept for as long as the table is merged, one per key a hard delete removed:
    that is the cost of hard deletes, and a hard truncate prunes them. No rule prunes them by
    age or count, and a delete of a key the table never held leaves one too (ADR 0049), since a
    source sends a change before a delete again whenever it resumes past it. A table replaced
    whole, by a generation, forgets them.
  - Changes the snapshot holds are never sent again: a source resumes past its position, so
    tombstones start with the changes, not the snapshot.
- **SQL change merges** (`sqlgen`, and the SQLite destination through it).
  - A change table's staging has the op column and the unchanged flags, which the writer stores
    as bytes, one per target column, set where it is flagged, which a commit reads at a position
    (ADR 0049). It resolves them when it stages, in the transaction that stages the rows; a flag
    naming a column the table lacks, or its key or sequence column, is a `Data` error. The
    staging columns and the tombstones table are created where rows are staged, so a commit
    changes no table's columns.
  - One statement reads the table, its tombstones and the commit's staged rows, and computes into
    staging each changed key's row, each key removed, and the bound; the statements after it only
    move those rows. Staged duplicates of one key and sequence land once. The SQL uses common table
    expressions, window functions and correlated subqueries: no `ON CONFLICT`, no row values, no
    `LIMIT`, no DDL, so it runs on any SQL database the planner targets.
  - A change table, its staging and its tombstones are indexed by the key, where rows are
    staged, and a commit finds every row its changes touch by the key: it reads the table whole
    only to apply a truncate. That holds where deletes are soft too, and a commit meets its
    truncates in one ordered pass, not once a row (ADR 0049). A commit of a thousand changes to a
    table of a million rows takes about 50 ms in SQLite, as it does at a hundred thousand rows;
    without the indexes, 28 s at a hundred thousand.
  - A staged row whose op is none of a change stream's is refused where it is staged: the codes
    past a truncate's are those the commit computes its rows under.
  - A change stream merges into its table, never a generation, and a generation swapped in clears
    the base's tombstones.
  - A differential property test commits the same random written batches to SQLite and to the
    Arrow merge, comparing rows and tombstones after each commit, with soft and hard deletes.
  - Two rows of one key and one sequence with different values, which no source sends, land as
    either in SQL; the reference takes the first written.
  - A soft delete of a key no row holds marks nothing, and leaves nothing a change sequenced before
    it would meet: soft deletes keep no tombstones.
- **Certification** has `D-DELETE`, `D-PARTIAL` and `D-TRUNCATE`, each sending changes again from
  before the removal they check, each commit in a session of its own as a load started again is, and
  `D-MERGE` checks the seq guard across commits where the destination merges changes. `D-DELETE`
  also checks that a table replaced whole forgets its tombstones, where the destination replaces.
  Each is skipped where the destination declares it does not do what it checks. `D-PARTIAL` and
  `D-MERGE` merge into a table whose deletes would remove rows, which a destination merging changes
  takes whatever its delete modes: a stream ignoring its deletes and truncates writes one.
- **The simulation's change source sends a window of earlier changes again** whenever it resumes
  past them, for merged streams; the model expects the table unchanged. Without tombstones the
  simulation fails.
- **Portable planning** is ADR 0028's, and `S-ACK`, a source advancing its position only in
  `committed`, ADR 0030's.

## Consequences

- The SQLite destination loads change streams with every delete mode, partial updates and
  truncates.
- A connector that does not declare `merge_changes` reads as merging no change stream, so the
  engine refuses change merges into it.
- Destinations store tombstones: a hard delete costs a row that stays until a later change of the
  key or a hard truncate.
- A change sent again cannot bring back a removed row, in any destination.
