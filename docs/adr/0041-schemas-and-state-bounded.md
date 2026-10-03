# ADR 0041: Schemas and state bounded, in forms that always read back

Status: accepted, 2026-10-03.

## Context

ADR 0037 has the engine survive any connector, and asks that persistent state is never written
in a form its reader cannot read back. What a pipeline keeps from one push to the next, and from
one run to the next, broke both:

- Stored forms that their readers refused. A table schema was stored as derived JSON, three levels
  of nesting a struct, which a JSON reader stops reading at 128: a document nested 42 levels deep,
  within the limit of 64, left state no later run could open, a log no replay could read and a
  catalog no change could follow. The wire's encoder could not send a schema nested past about
  fifteen levels at all. Records read a field they did not know as nothing, and fields with
  meaning had been added under one format number.
- Growth with no limit on the whole. Limits held one push: a table's columns and a struct's
  fields grew by thousands a push for as long as pushes came, a normalized stream added a child
  table for every array path, a lane kept a destination writer open for every schema version,
  the log kept every version's schema frame, and the entries of partitions no plan named stayed
  in state for good.
- State no message could carry. An open answers with all of a pipeline's state, a commit's
  request carries its changes, a plan's request a stream's positions. Cursors within their limit,
  long column names and abandoned partitions grew state past what any of these could take, and a
  commit too large for its request was logged, and the source told its positions were committed,
  before the send failed: every later run failed the same way.
- Recorded values taken on trust: a schema version of `u32::MAX` wrapped to "not created", and
  two tables recorded under one identifier wrote into one table.
- One open segment kept every chunk of the log, all partitions' committed data with it, and a
  widen the destination refused was retried unchanged until the load failed.

## Decision

- **A stored type nests no deeper however deep the type.** `LogicalType` and `Field` are stored as
  their nodes in preorder, as the wire sends them: a struct's node says how many fields follow, a
  list's is followed by its item's, a field's node carries its name and nullability. They are
  serialized without recursion and read with recursion bounded by the nesting limit, counting a
  top-level column as the first level; nodes left over, missing, half-named or of a field the
  reader does not know are refused. `TableSchema::new` refuses a schema nested deeper than the
  limit (`TypeError::TooDeep`), so every schema that exists is one every reader of state, of the
  log and of the files catalog reads back, and writing a record cannot fail. The wire's encoder
  verifies the schema message it reads back to the schema's own depth.
- **Persisted forms are versioned and strict.** Every persisted struct refuses a field it does not
  know, and fields that defaulted for records written before them are required. State records
  are format 2 and the log's frames version 2; the files destination's manifests and catalog
  versions, and the merge key a SQL destination's staged segment records, carry a format, 1,
  checked on read. What an earlier build wrote is refused, as `state_invalid`,
  `wal_unreadable`, `manifest_invalid`, `catalog_invalid` or an internal error for a staged
  key; nothing is published, so nothing earlier needs reading. A stream or table name a later build refuses
  (ADR 0042) therefore never reaches the engine from state written before it.
- **A table is never wider than a schema.** A change that would make a table's columns, every
  nested field counted, more than the schema columns `EngineConfig::limits` derives from the
  memory budget (ADR 0039; 7,489 at the defaults) is refused as `table_columns_exceeded` before
  its records are reserved or the destination sees it; a struct column's union of fields counts.
  The metadata columns lowering adds are the engine's, and not counted. A table already wider
  takes batches that change nothing. Resolution finds a table's columns, and conversion a
  struct's fields, through an index.
- **What tables and state may grow to is configured, and derived from the budget.**
  `GrowthLimits`, on `EngineConfigBuilder::growth`, sets:

  | Limit | Default | What it holds |
  |---|---|---|
  | `child_tables` | 1024 | the child tables a normalized stream's table has, recorded and new |
  | `writers` | 128 | the destination writers an attempt holds open, across its lanes |
  | `state_bytes` | 16 MiB | one message carrying state: an open's answer, a commit's request |

  - `EngineConfig::state_limit` holds stored state to the lesser of `state_bytes` less 256 KiB,
    for what such a message holds beside, and a 16th of the budget, the share the state an attempt
    opens on is held within (ADR 0042): the message's room at the defaults, about 2 MiB at the
    least memory.
  - `EngineConfig::child_table_limit` is the lesser of `child_tables` and as many tables of a few
    columns, 4 KiB of records each, as that state holds.
  - A child table beyond it is refused as `child_tables_exceeded` before it is added. Wider
    child tables can pass the state limit first: a commit whose state would fit but for the
    records of the child tables state does not record yet is refused as
    `child_tables_exceeded` too, for their stream, before `state_bytes_exceeded` is considered.
- **Counters count, and recorded identifiers are distinct.** A table's schema version, and the
  changes an attempt counts, advance with checked arithmetic: a change past `u32::MAX` is
  `schema_version_exhausted`. A schema recorded at version 0 is `state_invalid`. Building an
  attempt's tables from state refuses an identifier recorded for two tables as `state_invalid`;
  a reset, which recovers a pipeline and so reads state it may not trust, forgets such a table's
  records and never drops it. A plan names new tables outside every recorded identifier.
- **Writers retire.** A lane flushes and closes a table's writers of older versions when it writes
  a newer one, and holds at most its share of `writers` open, one at least, closing the one
  written longest ago; a batch of an older version reopens that version's writer. An attempt runs
  no more lanes than writers. Replay stages a logged commit's batches in their logged order by
  the same rule, holding at most `writers` open: a commit holds every version its attempt
  described. A served connection carries at most 200 calls, and every open writer is one.
- **The log keeps a version's schema frame while a batch of it may come.** The log notes every
  view a table version's batches were logged for: a resolution that only rounds a column makes a
  new view of the same version. Before each commit the versions all of whose views are gone,
  every batch of which was logged before, are retired: the writer forgets their frames and the
  log their indexes. A batch of such a version is described again, under an index never used.
- **An open segment is carried out of settled chunks.** When a receipt leaves old chunks holding
  only settled segments and open ones, none taken by a commit still waiting, and at least as many
  bytes of the settled, the writer appends the open segments' batch frames, and each table's
  schema frame read from the chunk, to the chunk it writes, then removes the old chunks. Replay
  stages only the segments of commits without receipts, all logged after the carry, so copies a
  crash leaves are never staged; a chunk's removal is durable before anything after it. Copying
  costs at most what it frees, and an open segment keeps at most as much of others' data as of
  its own. Each batch frame carries its ordinal among the load's batches, and replay stages a
  segment's batches in that order wherever a carry left them: a change stream's rows of one key
  and one sequence apply in the order they were written, so a crash never changes which wins.
- **Stored state stays what a message can carry.** The engine measures each stored record as an
  open's answer carries it, from the records it opened on and each commit that lands, and a
  commit's request by its encoding. A commit whose request would pass `state_bytes` less 256 KiB,
  or whose state once landed would pass the state limit, is refused as `state_bytes_exceeded`, a
  non-retryable `Config` error, before it is logged or any source hears of a position. A plan's
  request and a report of committed positions carry less of a stream than its stored records.
  A commit is never split: its segments and positions land together or not at all. State past
  the limit, stored before the memory was lowered or by a replayed commit logged under a larger
  one, takes a commit that does not grow it, the receipt left out of that measure since every
  commit replaces it and its numbers gain digits, so a pipeline whose limit fell keeps loading.
- **A plan never forgets a position.** A partition a plan omits keeps its entry, running or
  `Done`: a plan may omit a partition for a moment, a listing that failed in part, and a
  partition planned again without its entry is read again from its beginning, its rows twice.
  Entries leave state only as before, with a new full read's cycle, a change stream's new phase,
  or a reset, so a source whose partition ids keep changing grows its state until a commit is
  refused as `state_bytes_exceeded`; a reset of the stream is the remedy (ADR 0031, ADR 0033).
- **A widen the destination refuses is routed aside.** The engine applies a change one table
  change a call, so a `schema_conflict` names the change it refused. A refused widen keeps its
  column as it is for the rest of the attempt, the column's own and no other: values it cannot
  hold go to a variant column, and a variant kept so leaves them to the JSON variant. Where the
  column's policy refuses variants, or the destination cannot add a column, the change is
  refused as `schema_change_unsupported`. A refused new column or table has its new names
  hashed, as before. A child table keeps none of its root's columns from widening.

Rejected:
- **Reading stored JSON with no recursion limit**, on a stack grown as needed: a stored form that
  nests as deep as its type still lets one record cost the reader its depth in stack and time.
- **Checking each record reads back as it is written.** Every write would decode its record again.
  A schema that cannot be stored cannot be built, which holds for every writer.
- **Paging state at open, or splitting a commit across requests**: a protocol change for what a
  limit on the whole bounds, and a commit's atomicity given up.
- **Refusing a load whose open segment keeps chunks**, or a chunk series for each partition: the
  first fails an honest source holding a long transaction open, the second changes the log's
  format.
- **Routing a table's columns to JSON beyond its width**: values of one column would change where
  they go in the middle of a stream, without a word.
- **Forgetting the positions of partitions a plan omits**, at an attempt's first plan, or only
  those `Done`: a plan may omit a partition for a moment, a listing that failed in part, and a
  partition named again without its entry is read from its beginning, its rows twice. Keeping an
  entry for some number of plans would make the count state, written by every plan.

## Consequences

- A document nested to the limit loads and opens again; one nested deeper is refused where it
  arrives, as before. The wire carries schemas nested to the limit in frames too.
- State, logs and files catalogs written by earlier builds must be cleared; a reset reads the state
  it resets, so it cannot clear state it cannot read.
- A stream whose table would pass the schema columns stops at its change, typed, until its table
  is reset or its policy discards new columns; more memory raises the limit.
- A normalized stream with more array paths than its child table limit stops, typed; at the least
  memory the limit is about 516.
- A lane may reopen a writer: a table written by more partitions at once than the lane's share,
  or a batch lowered for a version a newer one has replaced.
- A pipeline whose state is honestly larger than its state limit stops at the commit that would
  pass it until memory or `state_bytes` is raised at both ends, or its streams are reset; one
  whose limit fell below its state keeps loading while its commits do not grow it. The
  cursor limit is derived for the partitions read at once, not for those state records: about a
  hundred partitions each holding a cursor as long as a cursor may be reach the limit, at the
  defaults and at the least memory alike.
- A source whose partition ids keep changing grows its state with each new one until a commit is
  refused as `state_bytes_exceeded`; a reset of its stream clears the entries.
- `LocalWal` makes a chunk's removal durable with a sync of the load's directory.
- The control plane (ADR 0042) bounds the messages carrying state on the wire and refuses, at
  commit, state its open could not decode; when both are on `main`, one commit-time refusal
  stays, and `state_bytes` is the protocol's limit of that name.
