# ADR 0035: History (SCD2)

Status: accepted, 2026-09-30.

## Context

Spec §9.5 gives streams a `history` write mode: every version of each key, a change closing the
key's current version where the next begins (SCD2), with `_rdlt_valid_from`, `_rdlt_valid_to`,
`_rdlt_is_current` and `_rdlt_row_hash` (§8.5), the chain implemented once in `sqlgen` and checked
by `D-HIST`. ADR 0023 planned it as M5e. Two parts of §9.5 need designs of their own and are
**M5e2**: history for normalized streams, whose child rows would need versions of their own, and
updates that leave columns unchanged, whose hash the engine cannot compute without the columns it
never sees.

## Decision

- **`WriteMode::History`**, on full, incremental and change streams, keyed as merge streams are
  (the plan's key or the primary key). A change stream's deletes and truncates apply as for a
  merge, `hard`, `soft` or `ignore`. The destination declares `write_modes.history`; a change
  stream needs `merge_changes` and its delete mode's capability too.
- **The contract** is `HistoryColumns` on `MergeKey` (wire field 5, recorded with SQLite's staged
  segments): per key in sequence order, a change equal to the key's current, not deleted, version
  changes nothing; any other closes it (`valid_to` is the change's `valid_from`, `is_current`
  false) and becomes current; a hard delete closes it and records a tombstone; a soft delete closes
  it and opens a deleted version keeping its data; a truncate does so to every version sequenced
  before it. A change stream's change applies only past its key's newest version, tombstone and
  bound, so replays, retries and the snapshot's overlap change nothing. Amended 2026-10-02: a
  soft delete or truncate carries its deletion time or is refused, a truncate applies before a
  change of its own sequence, and an upsert lifts its key's tombstone (ADR 0049). A version keeps the
  sequence of the row that opened it; closing changes only `valid_to` and `is_current`. A change
  equal to the current version leaves the guard where it was: sources send each key's changes in
  order, sending some again at most, so a change sent again is behind the guard or equal to the
  version still current.
- **The engine writes whole versions**: every row carries `valid_from`, a null `valid_to`, a true
  `is_current` and its `row_hash`; batches are not compacted, since every version counts.
  - `valid_from` is the stream's change time (`StreamSpec::with_change_time`), a top-level date or
    timestamp column, or else when its batch arrived. The spec's `_rdlt_loaded_at` would begin
    every version of a run at its start, so a run following its source for days would leave each
    version an empty span. A change time of another type, or beyond what microseconds hold, is
    `change_time_invalid`, refused where the catalog declares it and else at the batch; a null one
    is `change_time_null`. Versions follow the sequence, whatever their times.
  - Amended 2026-10-03 (ADR 0046): a version begins no earlier than the latest instant its key's
    versions hold, so none ends before it begins; a change time no version can begin at follows
    its column's schema policy row by row; versions begun when their batch arrived follow a load
    clock that never reads earlier; validity a destination stores without timestamps is integer
    microseconds.
  - Amended 2026-10-03 (ADR 0046): `row_hash` is BLAKE3's 256 bits of the data columns as their
    logical types hold them, before they are lowered.
  - `row_hash` is the xxh3-128 of the row's data columns as one object of its non-null columns by
    name, the encoding row ids use (ADR 0009), JSON by the values its text says: an added null
    column, a wider type, another encoding or JSON rendered again changes no hash, so schema
    changes open no versions. The change time is left out:
    when a change happened is not what it changed. It is 16 bytes of `Binary`, as the sequence and
    lineage ids are, not the spec's `FixedSizeBinary(16)`; a delete's row carries none.
  - A table records whether it keeps history with its sequences' state record. A history stream
    into a table created without history, or any other stream into a history table, is refused as
    `table_history_mismatch`: its rows have no versions to close, or a merge would take its
    versions for one row. `ResetScope::Tables` converts a table.
  - A full read keeps the current version of a key it no longer reads, as a full read merged
    keeps its row.
  - Refused for M5e2: a normalized history stream (`history_normalize_unsupported`), and an
    update leaving columns unchanged (`partial_updates_unsupported`).
- **Destinations.** `sqlgen` publishes history through window functions over each key's staged
  rows in sequence order, each step decided by the key's last version or upsert before it and
  whether a delete came since: a skipped upsert holds the data of the version it matched, and a
  deleted version the data of the one it closed. SQLite declares `history`. The reference Arrow merge keeps it for the memory and files destinations, and the
  simulated destination has its own.
- **Certification.** `D-HIST` commits plain and change streams' histories, hard and soft, with
  replays, re-inserts after deletes and truncates, and compares every version with expectations
  worked out by hand; the certification fake fails it for each of six broken behaviors.
- **The simulation.** Drawn apart from the seed's generator (`seed ^ HISTORY`), some of the change
  world's merge streams keep history instead: their changes carry their position as their change
  time, set whole rows, and now and then send their key's live data again. The change oracle
  checks every version against a model of the chain.

## Consequences

- A pipeline can keep the history of every key of a table, from any read mode, exactly once
  through retries, crashes and replays.
- A Postgres source whose updates leave TOAST columns unchanged cannot load into a history table
  until M5e2.
