# ADR 0033: Resetting streams

Status: accepted, 2026-09-30.

## Context

Spec §16.1 gives the engine an admin `reset(streams)`: "a commit that clears the named streams'
state and, for `replace` streams, starts a new generation". Two earlier decisions wait on it:

- ADR 0023: switching an existing merge table to a change stream needs a new table; "a reset that
  clears a stream's table and state is owed".
- ADR 0025: a table belongs to the pipeline that created it, and "handing a table over waits for
  reset".

ADR 0031 and 0032 planned M5d3 as this reset plus change streams whose source cannot read again
(ADR 0029, `change_read_unreplayable`). This ADR splits them. M5d3 is the reset. The change
streams, which need the write-ahead log to know phases, are **M5d4**: the M5 exit gate
("simulation with CDC and non-replayable sources green") requires them.

## Decision

- **`Engine::reset(pipeline, streams, scope, source, destination) -> ResetReport`** (engine API,
  §16.2; the facade's and the CLI's `state reset` are M7's). One admin commit: a fresh load id,
  commit 1, no segments.
  - **`ResetScope::Positions`**, the spec's reset, deletes each stream's phase, partition
    positions, full read in progress and completed reads. The next run reads the stream from its
    beginning into the tables it has; a replace stream fills a new generation, swapped in when
    complete, so its old rows stay visible until then. The spec's "starts a new generation" is
    read this way: no generation is recorded until the next run starts one.
  - **`ResetScope::Tables`** also drops the stream's tables, its child tables with them, and
    deletes their schemas, names and sequences, so the next run creates them anew: a merge table
    may become a change table, and any pipeline may create a table of the name.
  - A stream the pipeline recorded nothing of is refused as `stream_not_found`, so a typo in a
    command that deletes never passes silently. The source's catalog decides nothing here: a stream
    the source no longer serves can be reset. A stream is recorded only by state keyed by its own
    name, never by a table path its displayed name begins (ADR 0045), and one whose displayed name
    another recorded stream shares is refused as `stream_ambiguous`. An incremental stream that no
    earlier reset marked and none of whose partitions has had a position committed is recorded
    nowhere, though a run may have created its table, and its reset is refused alike; a partition
    that never held a row can still have a position, which a read that follows it sends as it waits.
    The simulation requires the refusal exactly where the pipeline's state at the destination
    records nothing of the stream.
  - A stream whose source cannot read again is refused as `reset_unreplayable`. Read from its
    beginning, it would wait for rows its source forgot. The simulation found that a source
    starting such a read where it last acknowledged instead is unsafe: a commit it acknowledged
    may still wait in a crashed load's log, and a racing run would move past it for good.
    Amended 2026-10-03 (ADR 0045): a stream whose source no longer lists it, or now calls it one
    it reads again, is refused as well while a log holds rows of it that its source was told
    were committed and no commit received; a run of the pipeline lands them first. This
    refusal comes after the reset's session opens, which fences any load still running, and
    after each log is fenced, so no load logs rows once the reset read them.
  - A destination that does not declare `drop_tables` refuses a `Tables` reset as
    `drop_unsupported`, and a reset naming no streams is refused as `no_streams`.
  - These refusals, and `reset_unreplayable`, come before the destination opens, fencing nothing.
    `stream_not_found` can only come after: the pipeline's state is read by opening the
    destination, which fences its older sessions as any reset does. A state read that fences
    nothing lands with M7.
  - A full read a run began before the stream was reset is over: a retry of that run, which the
    reset fenced, starts a new read in a generation of its own rather than resuming the one it
    remembered, whose rows were the stream's before the reset. The run keeps each read's
    generation with the epoch of the stream's last reset when it began.
  - A connector that panics fails the reset as an internal error, as it fails an attempt.
- **The reset marker.** The reset commit puts `StateKey::Reset(stream)`, the epoch of its session.
  Replay skips every logged seal of a reset stream whose commit's session is older than the
  marker. Opening the reset's session fences every older session, so a run still loading fails
  its next commit, and what it or a crashed load logged before the reset never lands after it.
  Epochs are the destination's, ordered per pipeline: no clocks. Replay already leaves replayable
  streams to the next load once another commit landed, so the marker guards what nothing else
  does: a stream the source no longer serves, or one whose replayability changed.
- **Drops ride the commit: `CommitMeta::drop_tables: Vec<DroppedTable { path, name }>`**, wire
  `CommitMeta.drop_tables = 8`, capability `drop_tables` (catalog field 12). A drop is not a
  `TableChange`: schema changes take effect at once in every destination, so a drop there and a
  crash before the state commit would leave state naming a table that is gone. On the commit it
  lands with the state delta. The name comes from the engine's own recorded names, never from a
  destination's path map, which is store-wide. Amended 2026-10-01: SQLite and the memory
  destination keep a path map for each pipeline (ADR 0049).
  - A destination drops the table with its generations, staging, tombstones and owner record. A
    table another pipeline owns fails the whole commit as `table_owned`; one that does not exist is
    no drop at all, so a reset retried after a crash is harmless. Amended 2026-10-01: a table
    that exists and no pipeline owns is refused as `table_unowned`, and one named as the
    destination's own as `table_name_reserved`, in SQLite and memory (ADR 0049).
  - SQLite drops in the commit's transaction, refused as `Unsupported` where schema changes do not
    commit with transactions. The files destination makes the manifest the truth: the commit
    records the dropped names, removes their catalogs after it lands, and the next open removes
    any it left, before the session can create one again. A lock per table name, outside the
    catalog, is held across each claim, schema change and release, so a release never removes a
    catalog another pipeline claimed after it looked at the owner.
- **Ownership holds through commits and drops.**
  - A generation swapped into another pipeline's table is refused as `table_owned`, in every
    destination: memory, the simulation and SQLite resolve a path to a table through store-wide
    maps, where one pipeline's swap could empty another's table. Amended 2026-10-01: SQLite
    and memory resolve the path among the pipeline's own tables, so a swap changes its own
    table or none, and the owner is checked before it does (ADR 0049).
  - A session a newer one fenced may not claim a table no pipeline owns: its schema change or
    writer fails as fenced. Otherwise a load the reset fenced, still writing, claims the table
    the reset just released, and another pipeline stays shut out. The simulation found this.
- **Certification.** `D-DROP`: a dropped table leaves nothing, dropping again changes nothing, a
  session fenced before cannot claim it again, another pipeline may create a table of the name,
  and dropping another pipeline's table is refused as `table_owned`. `D-OWNED` also refuses a
  generation swap into another pipeline's table, unless it changes nothing.
- **The simulation's `reset` feature**, drawn apart from the seed's generator: once every phase
  converged, a stream is reset, from its beginning or with its tables, while a run of every
  pipeline loads, and the phase converges again on what the model says. Only resets after which
  the model's rows still hold are drawn: streams the source can read again, whose columns and key
  do not drift, whose partitions share no keys, never appended to again, and whose tables are
  dropped only where the read is incremental or replaces them.

## Consequences

- An operator can reload a stream from its beginning, move a merge table to a change stream, or
  hand a table to another pipeline.
- A stream loaded from a source that cannot read again cannot be reset: rows its source
  acknowledged are gone, so there is nothing to read again.
- `Engine` is `Clone`: it holds its configuration and environment behind `Arc`s.
- A state inspection that does not fence running loads (`state show`) needs a new destination
  call, and lands with M7's surface.
