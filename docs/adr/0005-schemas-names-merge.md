# ADR 0005: Schemas, names and merge

Status: accepted, 2026-09-24.

## Context

M2b of the spec gives the engine schema resolution and evolution, persisted name maps, native
nested data and merge. It was split once more: M2b is the engine side, and M2c brings `sqlgen`
with the `sqlite` reference destination and the `files` reference connector, which build on the
contract M2b settles. Building M2b surfaced decisions the spec leaves open or gets wrong.

## Decision

- **A table is a model of columns by identifier.** State keeps each table's schema as its
  columns' destination identifiers with their logical types, and a name map from each column to
  its identifier. Name map keys are `ColumnKey::Source(path)` or `ColumnKey::Variant { column,
  kind }`, so a variant column can never be confused with a source column of the same name. The
  map is injective and append-only. The table's own identifier is stored with its name map, in the
  `table/<path>/names` record.
- **Changes describe the destination's columns.** `TableChange` names columns by identifier and
  gives the types the destination stores, metadata columns included. Applying a change the table
  already reflects succeeds and changes nothing, since an attempt that fails after applying a
  change and before committing it makes the next attempt apply it again. Writers created before a
  change receive batches with the new columns after it, and a batch may carry a column at a type
  narrower than the column's, since a partition resolved before a widen writes it after.
- **Crashed attempts' columns are named around.** An attempt that applied changes and never
  committed leaves columns that the next attempt, starting from the committed names, may assign
  to other source columns at other types. `Create` on an existing table adds the columns it
  lacks; a column already holding the declared type (the lattice joins the two to its own type)
  reflects the change; any other clash is a `Data` error coded `schema_conflict` that changes
  nothing. The engine then resolves the change again with every new identifier hash-suffixed,
  the hash seeded with a fresh salt on each of up to four retries, and a further conflict fails
  the run. Recording names in state before applying changes was rejected: it needs a commit per
  schema change, against one commit per barrier. A committed column that a crashed attempt
  widened along another branch of the lattice than the next attempt needs cannot be renamed and
  fails the stream until destinations report their columns.
- **The first batch, or the declared schema, creates the table.** A stream no longer needs a
  declared schema; a declared one is resolved like a batch before anything is read. After the
  table exists, a new column or a value its column cannot hold is a change, which the column's
  policy applies (`evolve`), refuses (`freeze`) or discards (`discard_row`, `discard_value`).
  Settings resolve column → table → stream → pipeline in one function; the table level has no
  builder until child tables exist (M3).
- **Evolution widens in place or adds a variant.** A change the destination can apply, or that
  does not change how it stores the column, widens the column. Otherwise, under `variant_column`,
  values land in `<name>__<kind>`, the variant of the joined type's kind, widened while the
  destination can, or else in `<name>__json`, which holds anything. Values the original column
  holds keep landing there. Merge key columns never take variants: a key change the destination
  cannot apply fails the stream (`merge_key_changed`).
- **Hints fix a column's type.** A hinted column never widens; values that do not fit it are
  incompatible changes.
- **Columns are nullable except merge keys.** A source declaring a column non-null states
  something about its data, not a destination constraint, so a later batch with nulls needs no
  change.
- **Lowering.** A type the destination does not store natively lands as text: UUIDs hyphenated,
  bytes in hex, anything else as Arrow renders it. Nested values stay native only when the policy
  is `native` and every type inside them is native; otherwise they, and `Json`, become `Json`, or
  JSON text where the destination has no JSON type. The `json` nested policy only lowers: the
  logical type stays a struct or list.
- **Metadata columns.** Every row carries `_rdlt_load_id` (the load's UUID) and
  `_rdlt_loaded_at` (the attempt's start, microseconds, UTC); merge rows carry `_rdlt_seq`, 16
  bytes of `Binary`: the segment id then the row's index in the segment, big-endian. Their
  identifiers follow the destination's rules, and source columns never take them. The spec's
  dictionary-encoded load id arrives with the lowering plan (M3).
- **Merge.** The key comes from the plan or the catalog's primary key. `TableRef::merge` names the
  key columns and the sequence column; the engine keeps the last row of each key within a batch,
  and the destination keeps one row per key: the newest commit's, and within a commit the
  greatest sequence's. When one key appears in several partitions within one commit, the row from
  the segment the engine opened later wins, since the engine assigns sequences before it knows
  the order segments seal in; the spec said the later-sealed segment.
- **One session, shared.** The coordinator's commits and the partitions' schema changes share the
  destination session behind an async lock. Changes to one table are worked out and applied one
  at a time, each against the table's latest version, and a commit records every table's schema
  and names as they stand when it collects its segments.
- **Growth is charged at once** (spec §7.5): preparing a batch may make it larger than its push,
  and the difference is reserved without waiting.
- **Discards are counted per stream** in the commit that publishes their segment. Samples and
  `Discarded` events arrive with events (M7).
- **Identifiers.** A source name is folded and cleaned under the destination's rules; a taken,
  reserved or metadata identifier gets `_` and six base32 digits of the xxh3 hash of the exact
  source path, extended while still taken. The columns one batch adds are named in sorted order,
  so arrival order never matters.
- **M2a's deferred minors.** Configured lanes never exceed the destination's
  `max_parallel_writers`; a budget waiter that gave up no longer counts toward `peak_memory`;
  `RunOutcome` documents the error a stop during backoff carries; the coordinator's commit
  building moved to its own file.
- **Dependencies.** `arrow-array` enables `chrono-tz`, since rendering a timestamp with a named
  zone as text or JSON needs it. `xxhash-rust` is licensed BSL-1.0, now allowed.
- **Deferred:** JSON and change pushes, `normalize` and child tables (M3); the lowering plan and
  preparing batches on the compute pool (M3); discard samples, `Discarded` events and configurable
  metadata names (M7).

## Consequences

The simulation oracle draws destination capabilities (widenings, nested support, identifier rules,
writer counts) and workloads with merge streams and drifting columns, and compares what the
destination holds with the reference model by source column, whichever of a column's variants a
value landed in. The simulated destination keeps each table's physical columns, answers conflicts as the contract
says, and flags a write naming a column the table lacks or at a type its column does not hold.
Destinations implementing the contract must apply schema changes idempotently, report conflicts,
accept narrower batches and merge by `TableRef::merge`; the certification suite checks all of it
(`D-SCHEMA`, `D-MERGE`), along with replace generations (`D-REPLACE`).
