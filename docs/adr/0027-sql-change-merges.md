# ADR 0027: SQL change merges and tombstones

Status: accepted, 2026-09-29.

## Context

M5a merged change streams in the memory and files destinations only. The SQLite destination, and
any SQL one `sqlgen` plans for, refused them, and a destination that did not know
`MergeKey::changes` would have upserted change rows as data: nothing in its capabilities told the
engine otherwise, since the engine took "no delete mode" to mean "cannot merge changes".

The reference merge applies a change only past the sequence of the row its key holds. After a
hard delete or a hard truncate no row holds the key, so a change sequenced before the delete, sent
again by a source delivering at least once, brought the row back. Soft deletes kept their rows,
whose sequences guard them.

Certification checked merges by key (`D-MERGE`) but no delete, partial update or truncate, and
the spec's `D-MERGE` names the seq guard, which the clause never exercised.

## Decision

- **A destination declares that it merges change streams**, `Capabilities::merge_changes`, carried
  on the wire (`merge_changes`, field 11 of the capabilities message; absent reads false). The
  engine refuses a change stream's merge into a destination that does not, as
  `change_merge_unsupported`, whatever its delete modes: ignoring every delete and truncate still
  needs the seq guard and the change columns. A destination that merges changes needs no delete
  mode for a stream that removes nothing.
  - `partial_updates_unsupported` stays a configuration error, as M5a made it: the plan asks for
    something the destination cannot do, which the operator fixes. Spec §9.3 calls it
    unsupported, and is amended by this ADR.
- **Tombstones.** A change stream's table remembers each key a hard delete removed, with the
  delete's sequence, and the latest hard truncate's sequence, the bound. A change sequenced at or
  before its key's tombstone, or before the bound, never applies. A later change of the key lifts
  its tombstone; raising the bound drops the tombstones before it.
  - Every destination keeps them: the reference merge (memory, files), the simulation's
    destination, the SQL destinations, and certification's vault. The memory and files
    destinations store them beside the table's rows, the files destination in its manifest, which
    its garbage collection keeps; a SQL destination in the table `_rdlt_tombstones__{name}`, whose
    bound is a row naming no key.
  - Tombstones are kept for as long as the table is merged, one per key a hard delete removed:
    that is the cost of hard deletes, and a hard truncate prunes them. Amended 2026-10-01: no
    rule prunes them by age or count, and a delete of a key the table never held leaves one too
    (ADR 0049). A table replaced whole, by a
    generation, forgets them.
  - Changes the snapshot holds are never sent again: a source resumes past its position, so
    tombstones start with the changes, not the snapshot.
- **SQL change merges** (`sqlgen`, and the SQLite destination through it).
  - A change table's staging gains the op column and the unchanged flags, which the writer stores
    as the text `,i,j,` of the ordinals of the target columns it flags. Amended 2026-10-02: as
    bytes, one a target column, set where it is flagged, which a commit reads at a position
    (ADR 0049). It resolves them when it
    stages, in the transaction that stages the rows; a flag naming a column the table lacks, or
    its key or sequence column, is a `Data` error. The staging columns and the tombstones table are
    created where rows are staged, so a commit changes no table's columns.
  - One statement reads the table, its tombstones and the commit's staged rows, and computes into
    staging each changed key's row, each key removed, and the bound; the statements after it only
    move those rows. Staged duplicates of one key and sequence land once. The SQL uses common table
    expressions, window functions and correlated subqueries: no `ON CONFLICT`, no row values, no
    `LIMIT`, no DDL, so it runs on any SQL database the planner targets.
  - A change table, its staging and its tombstones are indexed by the key, where rows are
    staged, and a commit finds every row its changes touch by the key: it reads the table whole
    only to apply a truncate. Amended 2026-10-01: that holds where deletes are soft too, and a
    commit meets its truncates in one ordered pass, not once a row (ADR 0049). A commit of a thousand changes to a table of a million rows takes
    about 50 ms in SQLite, as it does at a hundred thousand rows; without the indexes it took 28 s
    at a hundred thousand.
  - A staged row whose op is none of a change stream's is refused where it is staged: the codes
    past a truncate's are those the commit computes its rows under.
  - A change stream merges into its table, never a generation, and a generation swapped in clears
    the base's tombstones.
  - A differential property test commits the same random written batches to SQLite and to the
    reference merge, comparing rows and tombstones after each commit, with soft and hard deletes.
  - Two rows of one key and one sequence with different values, which no source sends, land as
    either in SQL; the reference takes the first written.
  - A soft delete of a key no row holds marks nothing, and leaves nothing a change sequenced before
    it would meet: soft deletes keep no tombstones.
- **Certification** gains `D-DELETE`, `D-PARTIAL` and `D-TRUNCATE`, each sending changes again
  from before the removal they check, each commit in a session of its own as a load started again
  is, and `D-MERGE` checks the seq guard across commits where the destination merges changes.
  `D-DELETE` also checks that a table replaced whole forgets its tombstones, where the destination
  replaces. Each is skipped where the destination declares it does not do what it checks.
  `D-PARTIAL` and `D-MERGE` merge into a table whose deletes would remove rows, which a
  destination merging changes takes whatever its delete modes: a stream ignoring its deletes and
  truncates writes one. `D-TRUNCATE` is added to spec §19's clause list.
- **The simulation's change source sends a window of earlier changes again** whenever it resumes
  past them, for merged streams; the model expects the table unchanged. Without tombstones the
  simulation fails.
- **Split.** The portable `sqlgen` work M5b named, a dialect's upsert style in place of
  `ON CONFLICT`, row values, DDL out of the commit, and the child table's root index name
  `{target}__rdlt_root`, which a user table of that name collides with, move to M5b2. `S-ACK`, a
  source advancing its position only in `committed`, needs a probe of what a source acknowledged,
  across the wire as read-back is; it moves to M5c, whose write-ahead log changes when a position
  may be acknowledged.

## Consequences

- The SQLite destination loads change streams with every delete mode, partial updates and
  truncates.
- A connector built before this reads as merging no change stream, so the engine refuses change
  merges into it until it declares them.
- Destinations store tombstones: a hard delete costs a row that stays until a later change of the
  key or a hard truncate.
- A changed stream's replayed change can no longer bring back a removed row, in any destination.
