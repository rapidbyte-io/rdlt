# ADR 0033: Resetting streams

Status: accepted, 2026-09-30.

## Context

An operator must be able to reload a stream from its beginning, to switch an existing merge table
to a change stream, which needs a new table (ADR 0023), and to hand a table to another pipeline,
since a table belongs to the pipeline that created it (ADR 0025). Each needs an admin reset: a
commit that clears the named streams' state, after which a `replace` stream's next run fills a new
generation.

## Decision

- **`Engine::reset(pipeline, streams, scope, source, destination) -> ResetReport`**, on the
  engine's API. One admin commit: the first of a session of its own, expecting the key the open
  found landed, with no segments.
  - **`ResetScope::Positions`** deletes each stream's phase, partition positions, full read in
    progress and completed reads. The next run reads the stream from its beginning into the tables
    it has; a replace stream fills a new generation, swapped in when complete, so its old rows stay
    visible until then: no generation is recorded until the next run starts one.
  - **`ResetScope::Tables`** also drops the stream's tables, its child tables with them, and
    deletes their schemas, names and sequences, so the next run creates them anew: a merge table
    may become a change table, and any pipeline may create a table of the name.
  - A stream the pipeline recorded nothing of is refused as `stream_not_found`, so a typo in a
    command that deletes never passes silently. The source's catalog decides nothing here: a stream
    the source no longer serves can be reset. A stream is recorded only by state keyed by its own
    name, never by a table path its displayed name begins, and one whose displayed name
    another recorded stream shares is refused as `stream_ambiguous`. An incremental stream that no
    earlier reset marked and none of whose partitions has had a position committed is recorded
    nowhere, though a run may have created its table, and its reset is refused alike; a partition
    that never held a row can still have a position, which a read that follows it sends as it waits.
    The simulation requires the refusal exactly where the pipeline's state at the destination
    records nothing of the stream.
  - A stream whose source cannot read again is refused as `reset_unreplayable`. Read from its
    beginning, it would wait for rows its source forgot.
  - A destination that does not declare `drop_tables` refuses a `Tables` reset as
    `drop_unsupported`, and a reset naming no streams is refused as `no_streams`.
  - These refusals, and `reset_unreplayable`, come before the destination opens, fencing
    nothing. `stream_not_found` can only come after: the pipeline's state is read by opening the
    destination, which fences its older sessions as any reset does.
  - A full read a run began before the stream was reset is over: a retry of that run, which the
    reset fenced, starts a new read in a generation of its own rather than resuming the one it
    remembered, whose rows were the stream's before the reset. The run keeps each read's
    generation with the epoch of the stream's last reset when it began.
  - A connector that panics fails the reset as an internal error, as it fails an attempt.
- **The reset marker.** The reset commit puts `StateKey::Reset(stream)`, the epoch of its session.
  Opening the reset's session fences every older session, so a run still loading fails its next
  commit, and nothing it read before the reset lands after it; a full read it began before the
  reset is over, by the marker (above). Epochs are the destination's, ordered per pipeline: no
  clocks.
- **Drops ride the commit: `CommitMeta::drop_tables: Vec<DroppedTable { path, name }>`**, wire
  `CommitMeta.drop_tables = 8`, capability `drop_tables` (catalog field 12). A drop is not a
  `TableChange`: schema changes take effect at once in every destination, so a drop there and a
  crash before the state commit would leave state naming a table that is gone. On the commit it
  lands with the state delta. The name comes from the engine's own recorded names, never from a
  destination's path map, which the simulation's destination keeps for the whole store (SQLite
  and the memory destination keep one for each pipeline, ADR 0049).
  - A destination drops the table with its generations, staging, tombstones and owner record. A
    table another pipeline owns fails the whole commit as `table_owned`; one that does not exist is
    no drop at all, so a reset retried after a crash is harmless. A table that exists and no
    pipeline owns is refused as `table_unowned`, and one named as the destination's own as
    `table_name_reserved`, in SQLite and memory (ADR 0049).
  - SQLite drops in the commit's transaction, refused as `Unsupported` where schema changes do not
    commit with transactions. The files destination makes the manifest the truth: the commit
    records the dropped names, removes their catalogs after it lands, and the next open removes
    any it left, before the session can create one again. A lock per table name, outside the
    catalog, is held across each claim, schema change and release, so a release never removes a
    catalog another pipeline claimed after it looked at the owner.
- **Ownership holds through commits and drops.**
  - A generation swapped into another pipeline's table is refused as `table_owned`, in every
    destination: the simulation resolves a path to a table through a map the store shares, where
    one pipeline's swap could empty another's table; SQLite and memory resolve the path among the
    pipeline's own tables, so a swap changes its own table or none, and the owner is checked
    before it does (ADR 0049).
  - A session a newer one fenced may not claim a table no pipeline owns: its schema change or
    writer fails as fenced. Otherwise a load the reset fenced, still writing, claims the table
    the reset just released, and another pipeline stays shut out.
- **Certification.** `D-DROP`: a dropped table leaves nothing, dropping again changes nothing, a
  session fenced before cannot claim it again, another pipeline may create a table of the name,
  and dropping another pipeline's table is refused as `table_owned`. `D-OWNED` also refuses a
  generation swap into another pipeline's table, unless it changes nothing.
- **The simulation's `reset` feature**, drawn apart from the seed's generator: once every phase
  converged, a stream is reset, from its beginning or with its tables, while a run of every pipeline
  loads, the network disrupted meanwhile where the seed has it and faults, and the phase converges
  again on what the model says. Only resets after which the model's rows still hold are drawn:
  streams the source can read again, whose columns and key do not drift, whose partitions share no
  keys, never appended to again, and whose tables are dropped only where the read is incremental or
  replaces them.

## Consequences

- An operator can reload a stream from its beginning, move a merge table to a change stream, or
  hand a table to another pipeline.
- A stream loaded from a source that cannot read again cannot be reset: rows its source
  acknowledged are gone, so there is nothing to read again.
- `Engine` is `Clone`: it holds its configuration and environment behind `Arc`s.
- A state inspection that does not fence running loads needs a destination call the contract
  does not have.
