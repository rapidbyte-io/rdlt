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
  table and plan in order, then lowers the parts on the pool; a plain stream's batches are cut and
  lowered on the pool only where that reads or writes values (ADR 0008).
- **Depth.** Entering an object or an array is one level; a container deeper than `max_depth` stays
  whole and lowers to `Json`, or text where the destination has no JSON type, and an object or
  array held in a dictionary or runs stays a whole column at any depth. Only pipelines and streams
  normalize: a column set to `native` or `json` in a normalized stream is kept whole, and a column
  set to `normalize` is refused when the plan is built.
- **Identity, by role.** Who chooses a row's identity decides how it is made. Record authors
  choose a merge key, so a merge stream's root row is identified as ADR 0046 holds any input
  someone else chooses: by the BLAKE3 hash, all 256 bits, of a canonical encoding of its merge
  key, after a tag of its own. The merge matches the row by it and replaces its children by it;
  roots that share a key share an id. Only the engine and the destination's fence choose a row's
  position, so every other row, a merge stream's child rows and every row of a stream that does
  not merge, is identified by its position with no hash: the session's epoch, the segment, the
  position of its piece's first root row among the rows the segment received, its part's
  ordinal among the tables the piece normalizes into, the stream's own being 0, and its place in
  the part before any row was dropped. Each is a SQLite 4 variable-length integer
  (<https://sqlite.org/src4/doc/trunk/www/varint.wiki>), whose bytes compare as the number does,
  so five of them in a row read back alike and order as load order does. The encoding is written
  in the engine and held by recorded vectors, since stored ids must not move with a dependency's
  release.
  - Each open of a destination returns an epoch above every earlier open of the pipeline's,
    whatever commits since dropped its tables or deleted its state (`D-EPOCH`); an attempt
    numbers its segments apart, a segment its pieces, and a piece its parts and their rows. A
    replay or retry publishes rows already staged, which keep their epoch. So no two rows a
    pipeline loads share an id, and a child joined to its parent meets one row, in every write
    mode. A row loaded twice outside a merge has two ids: deduplicating by key is what merge is
    for. Ids reveal an epoch, a segment and positions, none of them secret.
  - Normalizing builds a merge stream's root key ids, one array every part of a batch shares, and
    every row's positions; lowering builds the lineage columns from them and the batch's stamp,
    after the schema policy and the merge dropped rows. The canonical encoding tags each value's
    kind, renders integers as their digits and a float as the 64-bit float it widens to, written
    as its shortest round-trip text with a tie going to the even digit (so `1` and `1.0` agree),
    gives each number its length, encodes a `Date64` as the day it is within, lists an object's
    non-null fields in name order (so a missing field and a null one agree), reads the JSON text
    a column of mixed types holds as the values it renders, without recursion and its numbers by
    their exact value, tags every field and writes lengths in LEB128, so no encoding is a prefix
    of another. It works on Arrow values, so a key's id does not depend on the batch, chunk or
    Arrow type that carried it; history versions' hashes encode their whole rows so (ADR 0035).
    A reference normalizer over JSON values, with its own encoding of keys, checks the tables,
    and that every row's lineage names exactly one row, across pieces, segments and sessions.
  - A merge stream's batch lacking a key column, or holding a null or NaN key, is refused before
    it is identified (`merge_key_missing`, `merge_key_null`, `merge_key_nan`); a stream that does
    not merge loads such rows, as it does unnormalized. A merge key stored as JSON on a
    normalized stream is refused, `merge_key_json`: its rows would merge by stored text while
    their children follow ids that read the values. Text in a column of JSON that is not JSON
    fails the write, `json_invalid`. An array under a key that cannot name a child table follows
    the stream's policy, refused as `table_path_invalid` where it does not discard.
  - A merge into a table holding roots identified by position would replace them by key and
    leave their children behind, so it is refused, `table_identity_mismatch`, a Config error,
    until the stream's tables are reset. A normalized load that does not merge records that its
    table holds such roots in the table's sequences' record, and takes a table a merge loaded.
- **Lineage columns' types.** Ids are `Binary`: a merge root's the 32 bytes of BLAKE3, every
  other row's the 5 to 37 bytes of its position. The position in the parent's array is `Int32`,
  as every array is cast to 32-bit offsets before it normalizes. The contract has no fixed-size
  binary or unsigned logical types, so the spec's `FixedSizeBinary(16)` and `UInt32` are not
  used, as `_rdlt_seq` is `Binary` too. Every destination stores both. The column set is the
  same in every write mode: a stream's own table carries `_rdlt_id`; child tables carry all
  four. They are nullable: a stream that starts to normalize keeps its table, to which planning
  adds the lineage columns, and its earlier rows have none.
- **Child tables.** A child table's path is its parent table's path and the array's path within
  the parent row, and its name comes from the naming rules for that path, so `a__b` as a key and
  `a.b` as nesting never alias (D4). Tables that state records keep their names. A child table is
  added the first time its rows arrive, creating it through the session, even when its rows hold
  nothing but their lineage (an array of arrays); lanes open a table's writer on its first write
  rather than at the attempt's start.
- **Schema policy per child table.** Child tables follow the stream's settings, not those of the
  stream's own table's columns, whose names they may share. A new array below a stream's table
  that state records is a schema change the stream's policy decides: `freeze` refuses it with
  `schema_frozen`, `discard_value` drops its rows and counts them, `evolve` adds its table.
- **Segments span tables.** A partition's rows and its child rows share the partition's segments.
  The destination contract now says a segment may hold rows for several tables, and certification
  clause `D-TABLES` checks it; the reference destinations already met it.
- **Replace.** A replace stream's child tables fill the same generation as its table and swap in
  with it. Child tables state records are added when the attempt plans the stream, so one the new
  cycle never writes swaps in empty.
- **Declared schemas.** A normalized stream's declared schema creates the columns of its own table
  that it holds, objects flattened; when it creates that table, it creates the child table of each
  array it holds within depth too, with the columns the array's rows bring, so state records it
  before any row does.
- **Reports.** A stream's reported rows count the rows of its child tables too; reports stay per
  stream.
- **Merges, rows a policy drops and arrays new to a table** follow ADR 0010.

## Consequences

A normalized stream's rows land in several tables committed together, each row naming its parent
and root. Normalizing costs more than shredding alone, a merge stream's roots their keys' hashes
too, and lowering builds every other row's id from its position;
[docs/perf/shred.md](../perf/shred.md) and [docs/perf/normalized.md](../perf/normalized.md)
record the figures. The simulation's oracle checks the child tables of normalized streams,
through crashes and concurrent runs, against the rows their parents hold.
