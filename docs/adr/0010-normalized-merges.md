# ADR 0010: Merges and dropped rows on normalized streams

Status: accepted, 2026-09-25.

## Context

Spec §8.7 has a merge on a normalized stream replace all child rows of each affected root in the
same commit, and `discard_row` on a parent discard its children, resolved parents first. M3c
refused both (ADR 0009). M3 was split once more (owner's decision, 2026-09-25): M3d lifts the two
refusals; M3e brings the per-value reference lowering differential of §20.4 and M3c's deferred
minors.

## Decision

- **Child tables merge by their root.** `MergeKey` gains `root: Option<RootKey>`, naming the root
  table and its id and sequence columns. A child table's key is its root-id column, and its
  `_rdlt_seq` holds its root row's sequence. A commit that publishes root rows removes every
  published child row of those roots, then publishes the staged child rows of each root's winning
  row: the row with the greatest sequence. A root whose winning row has no children is left with
  none. The destination contract states this, and certification clause `D-CHILDREN` checks it,
  skipped with `D-MERGE` for destinations that cannot merge.
- **Every child table is listed.** A commit may stage nothing for a child table, or a grandchild,
  whose rows its roots' new rows no longer hold. `CommitMeta::child_tables` lists every child table
  of the attempt's merge streams with its key, so destinations replace those rows too. The field
  defaults to empty, so earlier commit records still read.
- **Reference destinations.** Memory and files share one Arrow implementation of the rule. SQLite
  deletes the published child rows whose root id is among the root staging's ids, inserts the staged
  child rows whose root id and sequence match a root's greatest, and publishes child tables before
  it drops the root staging they read. The root link travels in the merge key it records, whose
  JSON is now an object; the array form of earlier records still reads.
- **Sequences count rows received.** A row's sequence is its position among the rows its segment
  received, before the schema policy drops any, so a child row, which takes its root row's
  position, keeps matching its root when earlier rows are dropped.
- **A merge stages the rows of each key's last row alone.** Once the stream's own table's policy
  has dropped rows, the key values the split normalized find which of a piece's root rows are the
  last of their key among those it keeps, in one sort, and every table of the stream, its own and
  its child tables at every depth, keeps only the rows descending from them: a key's newest row
  the policy drops leaves the row before it to apply, with its children. A row left out still
  meets its table's policy, counts among the stream's discards and makes its table's columns, so
  where a flush is cut into pieces decides no column and no count, only what is staged; across
  pieces, the destination keeps each key's greatest sequence.
- **Dropped rows take their descendants.** A unit's parts are lowered parents first. A part first
  loses the rows whose parent was dropped, then meets its table's policy; every row dropped
  either way is kept, by its part and position, for the parts below. Rows are never known by
  their ids here, since a merge stream's rows sharing a key share an id. Every dropped row counts
  in the stream's `discarded_rows`, whichever table it belongs to.
- **A new array is a change its parents carry.** Under `discard_row`, a new array drops the rows
  holding it, with their descendants; the array's own rows are never written. A new array that
  is discarded, under either policy, takes the arrays within it along, uncounted, as part of the
  value already discarded.
- **New arrays meet the policy once the stream's table exists.** ADR 0009 governed a new array only
  once state recorded the stream's table. A new column is governed once the table has been created,
  whether by a declared schema, an earlier flush, or an earlier load; a new array now is too,
  judged as each flush starts, so every flush that finds the table not yet created takes every
  array any of its chunks or pieces holds, and each table it finds not yet created takes every
  column they hold. A column whose first kept value lies in a later piece of such a flush is
  added to the table an earlier piece created, and two partitions whose flushes both find the
  table not yet created both take every column, the second adding to the table the first
  created; a destination that cannot add columns refuses either addition,
  `schema_change_unsupported`. That is the price of letting the cut into chunks and pieces
  decide no column's existence or type. A table's column order, and which of two colliding names
  takes a hashed suffix, follow the order in which its columns are first valued: a column first
  valued in a later piece follows every column the table was created with, metadata included. A
  declared array is taken as given only while the stream's table is created, which creates its
  table too, or where state records its table; after that a new one is a change its column's
  policy decides. An array deeper than the stream normalizes is stored whole and has no table.
- **Simulation.** Simulated streams normalize under every write mode and policy. The simulated
  destination merges child tables by their root, through the SDK's Arrow merge, and the oracle
  checks each child table against the rows the model keeps: each key's last kept row for merges.

## Consequences

Merge streams can normalize, and every schema policy applies to normalized streams, so the
refusals `normalize_merge_unsupported` and `normalize_discard_row_unsupported` are gone. A
destination that merges must now replace child rows by root; `D-CHILDREN` holds it to that.
