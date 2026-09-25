# ADR 0009: Normalizing nested data into child tables

Status: accepted, 2026-09-25.

## Context

Spec §8.7 lets a stream `normalize` its nested data: arrays of objects become child tables at any
depth, objects flatten into `a__b` columns, arrays of values become child tables with a single
`value` column, and containers deeper than `max_depth` (default 8) are stored as `Json`. Every row
carries its lineage (§8.5, §7.4): `_rdlt_id`, and in child tables `_rdlt_parent_id`,
`_rdlt_root_id` and `_rdlt_idx`. Ledger defect D4 is child tables aliasing across parents.

M3 was split again (owner's decision, 2026-09-25): M3c normalizes for `append` and `replace`
streams; M3d brings merges that replace a root's child rows, `discard_row` cascading to children,
and the per-value reference lowering differential of §20.4. Building M3c took decisions the spec
leaves open, and one it words otherwise.

## Decision

- **Normalizing follows shredding, on Arrow.** Spec §7.4 has the shredder compute lineage ids as it
  observes JSON. Normalizing Arrow batches after shredding instead, owner-approved, serves Arrow and
  JSON pushes with one implementation and keeps the shredder's single pass as it is. A partition of
  a normalized stream concatenates and normalizes each unit on the compute pool, finds each part's
  table and plan in order, then lowers the parts on the pool; plain streams keep their single pool
  round trip.
- **Depth.** Entering an object or an array is one level; a container deeper than `max_depth` stays
  whole and lowers to `Json`, or text where the destination has no JSON type. Only pipelines and
  streams normalize: a column set to `native` or `json` in a normalized stream is kept whole, and a
  column set to `normalize` is refused when the plan is built.
- **Identity.** A root row's id is the xxh3-128 of a canonical encoding of its key (the plan's merge
  key or the source's primary key) or, without one, of the whole row; a child's is the xxh3-128 of
  its parent's id and its position. The encoding tags each value's kind, renders integers as their
  digits and floats as their shortest round-trip text (so `1` and `1.0` agree), lists an object's
  non-null fields in name order (so a missing field and a null one agree) and writes lengths in
  LEB128. It works on Arrow values, so an id does not depend on the batch, chunk or Arrow type that
  carried the row. A reference normalizer over JSON values, with its own encoding, checks it.
- **Lineage columns' types.** The spec gives the ids as `FixedSizeBinary(16)` and the position as
  `UInt32`. The contract has no fixed-size binary or unsigned logical types, so the ids are 16
  bytes of `Binary`, as `_rdlt_seq` already is, and the position is `Int64`; every destination
  stores both. A stream's own table carries `_rdlt_id`; child tables carry all four.
- **Child tables.** A child table's path is its parent table's path and the array's path within
  the parent row, and its name comes from the naming rules for that path, so `a__b` as a key and
  `a.b` as nesting never alias (D4). Tables that state records keep their names. A child table is
  added the first time its rows arrive, creating it through the session; lanes open a table's writer
  on its first write rather than at the attempt's start.
- **Segments span tables.** A partition's rows and its child rows share the partition's segments.
  The destination contract now says a segment may hold rows for several tables, and certification
  clause `D-TABLES` checks it; the reference destinations already met it.
- **Replace.** A replace stream's child tables fill the same generation as its table and swap in
  with it. Child tables state records are added when the attempt plans the stream, so one the new
  cycle never writes swaps in empty.
- **Declared schemas.** A normalized stream's declared schema creates the columns of its own table
  that it holds, objects flattened; child tables are created by their first rows.
- **Reports.** A stream's reported rows count the rows of its child tables too; reports stay per
  stream.
- **Not yet.** Normalizing a stream that merges, or whose policy drops rows, is a `Config` error
  until M3d, since both have to reach the rows' children.

## Consequences

A normalized stream's rows land in several tables committed together, each row naming its parent
and root. Shredding and normalizing takes, on one core, 1.3× the shredding time for keyed rows and
2.0× for keyless ones, whose canonical encoding is the extra cost (docs/perf/shred.md). The simulation's
oracle checks the child tables of normalized streams, through crashes and concurrent runs, against
the rows their parents hold.
