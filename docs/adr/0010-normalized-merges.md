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
  position, keeps matching its root when earlier rows are dropped. Only a stream's own table
  compacts a batch to the last row of each key; a child table's key is shared by its root's rows.
- **Dropped rows take their descendants.** A unit's parts are lowered parents first. A part first
  loses the rows whose parent was dropped, then meets its table's policy; the ids of every row
  dropped either way are kept for the parts below. Every dropped row counts in the stream's
  `discarded_rows`, whichever table it belongs to.
- **A new array is a change its parents carry.** Under `discard_row`, a new array drops the rows
  holding it, with their descendants; the array's own rows are never written.
- **New arrays meet the policy once the stream's table exists.** ADR 0009 governed a new array only
  once state recorded the stream's table. A new column is governed once the table has been created,
  whether by a declared schema, an earlier unit, or an earlier load; a new array now is too,
  judged as its unit starts, so the unit that creates the table takes every array it holds.
- **Simulation.** Simulated streams normalize under every write mode and policy. The simulated
  destination merges child tables by their root, and the oracle checks each child table against
  the rows the model keeps: each key's last kept row for merges.

## Consequences

Merge streams can normalize, and every schema policy applies to normalized streams, so the
refusals `normalize_merge_unsupported` and `normalize_discard_row_unsupported` are gone. A
destination that merges must now replace child rows by root; `D-CHILDREN` holds it to that.
