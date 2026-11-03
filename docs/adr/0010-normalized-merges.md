# ADR 0010: Merges and dropped rows on normalized streams

Status: accepted, 2026-09-25.

## Context

On a normalized stream, a merge replaces all child rows of each affected root in the same commit,
and `discard_row` on a parent discards its children, resolved parents first: a root row's
children follow it wherever a merge or a policy takes it.

## Decision

- **Child tables merge by their root.** `MergeKey` has `root: Option<RootKey>`, naming the root
  table and its id and sequence columns. A child table's key is its root-id column, and its
  `_rdlt_seq` holds its root row's sequence. A commit that publishes root rows removes every
  published child row of those roots, then publishes the staged child rows of each root's winning
  row: the row with the greatest sequence. A root whose winning row has no children is left with
  none. The destination contract states this, and certification clause `D-CHILDREN` checks it,
  skipped with `D-MERGE` for destinations that cannot merge.
- **Every child table is listed.** A commit may stage nothing for a child table, or a grandchild,
  whose rows its roots' new rows no longer hold. `CommitMeta::child_tables` lists every child table
  of the attempt's merge streams with its key, so destinations replace those rows too.
- **Reference destinations.** Memory and files share one Arrow implementation of the rule. SQLite
  deletes the published child rows whose root id is among the root staging's ids, inserts the staged
  child rows whose root id and sequence match a root's greatest, and publishes child tables before
  it drops the root staging they read. The root link travels in the merge key it records, a JSON
  object of the recorded key's format; a key recorded in another format is refused (ADR 0041).
- **Sequences count rows received.** A row's sequence is its position among the rows its segment
  received, before the schema policy drops any, so a child row, which takes its root row's
  position, keeps matching its root when earlier rows are dropped.
- **A flush stages each key's last row with its children alone.** Every piece of a flush is
  written to one segment, which a commit publishes whole, so a root row that a later row of its
  flush shares a key with never publishes, nor do its children. Before any piece of a merge
  stream's flush is split, the root rows' key columns are compared on the compute pool, as the
  split holds them, keys of different types apart; every table of the stream, its own and its
  child tables at every depth, then stages only the rows descending from the root rows no later
  row of the flush shares a key with. Every row still meets its table's policy, counts among the
  stream's discards and makes its table's columns, so only what is staged changes, and where the
  flush is cut into pieces decides none of it. The comparison holds what the budget grants it at
  once, and the bits it leaves a piece are charged with the piece's split. A stream whose policy
  drops rows stages as each piece's tables keep, a stream's own table the last row of each key of
  a piece and a child table every row: a key's newest row the policy drops leaves an older row to
  apply, which only planning its piece finds. So does a flush the budget has no room for at once,
  or whose key columns are of a type whose comparable rows their values do not bound.
- **Dropped rows take their descendants.** A unit's parts are lowered parents first. A part first
  loses the rows whose parent was dropped, then meets its table's policy; every row dropped
  either way is kept, by its part and position, for the parts below. Rows are never known by
  their ids here, since a merge stream's rows sharing a key share an id. Every dropped row counts
  in the stream's `discarded_rows`, whichever table it belongs to.
- **A new array is a change its parents carry.** Under `discard_row`, a new array drops the rows
  holding it, with their descendants; the array's own rows are never written. A new array that
  is discarded, under either policy, takes the arrays within it along, uncounted, as part of the
  value already discarded.
- **New arrays meet the policy once the stream's table exists.** A new column or array is governed
  once the table has been created, whether by a declared schema, an earlier flush, or an earlier
  load, judged as each flush starts, so every flush that finds the table not yet created takes every
  array any of its chunks or pieces holds, and each table it finds not yet created takes every
  column they hold. A column whose first kept value lies in a later piece of such a flush is added
  to the table an earlier piece created, and two partitions whose flushes both find the table not
  yet created both take every column, the second adding to the table the first created; a
  destination that cannot add columns refuses either addition, `schema_change_unsupported`. That is
  the price of letting the cut into chunks and pieces decide no column's existence or type. A
  table's column order, and which of two colliding names takes a hashed suffix, follow the order in
  which its columns are first valued: a column first valued in a later piece follows every column
  the table was created with, metadata included. A declared array is taken as given only while the
  stream's table is created, which creates its table too, or where state records its table; after
  that a new one is a change its column's policy decides. An array deeper than the stream normalizes
  is stored whole and has no table.
- **Simulation.** Simulated streams normalize under every write mode and policy. The simulated
  destination merges child tables by their root, through the SDK's Arrow merge, and the oracle
  checks each child table against the rows the model keeps: each key's last kept row for merges.

## Consequences

Merge streams can normalize, and every schema policy applies to normalized streams. A destination
that merges replaces child rows by root; `D-CHILDREN` holds it to that.
