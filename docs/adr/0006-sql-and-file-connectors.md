# ADR 0006: SQL and file reference connectors

Status: accepted, 2026-09-24.

## Context

M2c of the spec brings `sqlgen`, the planner every SQL destination shares, the transactional
`sqlite` reference destination built on it, and the `files` reference source and destination,
which publish with manifests. They are the first destinations that keep data outside the process,
and building them surfaced decisions the spec leaves open.

## Decision

- **`sqlgen` plans; destinations run.** A dialect gives quoting, placeholders, column types, when
  a declared type holds a logical type, an optional in-place widen and its catalog query;
  `SqlPlanner` turns them into parameterized statements for the catalog (epochs, state,
  receipts, registered tables and generations, staged segments), schema changes, staging,
  publishing and generation swaps. The destination runs them in its own transactions. The SQLite
  dialect ships with `sqlgen`, so its tests run every statement against SQLite through a
  development dependency on `rusqlite`; Postgres brings its own dialect in `rdlt-connectors`.
- **Staged rows carry who staged them.** Every table has a staging table beside it whose rows
  record the pipeline, epoch, segment and generation. A commit publishes only its own epoch's
  rows, so a fenced writer's late rows are never published, and `discard_staged` removes the
  pipeline's rows. A catalog of staged segments gives each commit its tables, rows and bytes
  without scanning staging.
- **Merges use `ON CONFLICT`.** A merge table has a unique index on its key; a commit ranks its
  staged rows per key by sequence and upserts the first. SQLite and Postgres share the form, so
  the spec's other upsert styles wait for a dialect that needs them.
- **Generations are tables.** Rows of a replace generation fill their own table, created with the
  table's columns if the generation was never declared; the finishing commit drops the table and
  renames the generation over it, and drops older generations. A generation with no table leaves
  the table empty.
- **Ids keep every bit.** SQL integers are signed, so ids are stored with the same bits and read
  back unsigned; the engine's generation ids use the full range, and `D-REPLACE` now uses one
  beyond the signed range, so a destination that narrows ids fails certification.
- **SQLite stores storage classes.** Columns are declared `BOOLEAN`, `INTEGER`, `REAL`, `TEXT` or
  `BLOB`, so widening within the integer or float family changes nothing, and the engine lowers
  every other type to text. Identifiers fold to lower case, since SQLite compares them without
  case, and the catalog tables are reserved. Calls run on the blocking pool, each in an
  immediate transaction.
- **Manifests are created exclusively.** The files destination publishes a commit by creating
  the pipeline's next manifest version with a hard link, which fails if another session created
  it first: the conditional put on a local filesystem. Opening creates a version with the next
  epoch. Each pipeline's directory is `_rdlt/pipelines/<id>-<hash>`, so ids that differ only in
  case never share one. Staged files live at
  `_rdlt/pipelines/<dir>/staging/<epoch>/<load>/<segment>/<table>/<generation>/<part>.<ext>` and
  are published where they are; opening removes the files older epochs staged that the latest
  manifest does not list. A merge table's commit rewrites the table as one file. The manifest
  keeps the receipts of the 16 most recent loads and the last 8 versions stay on disk.
- **Formats decide types.** Arrow IPC files keep every type; JSON lines keep scalars, structs and
  lists, and the engine stores the rest as text. A table's columns live in a catalog under the
  root, which schema changes update with the rules the memory destination follows.
- **The files source discovers streams from names.** `<stream>.jsonl` or `<stream>.arrow` is a
  stream of one partition and a directory is a stream of its files; a partition is read only if
  it is listed, never by joining its id to a path. JSON lines are read with a schema inferred from
  the whole file, and every batch is checkpointed.
- **The engine's integration suite runs against every destination** where the destination
  matters: exactly-once loads, resumes, append and replace, stopped replaces, merges, lost
  responses, fencing, schema changes and nested values run against memory, SQLite and both file
  formats; tests of the engine's internals keep the memory destination.
- **The simulation draws destinations without JSON**, which store JSON and nested values as text;
  the oracle parses them back through the committed schema's types.

## Consequences

SQL destinations inherit publishing, fencing and idempotence from `sqlgen` and test them against
SQLite. The files destination runs on local filesystems; object storage replaces the hard link
with its conditional put. Readers of the files destination follow the latest manifest; a
consumer-facing layout is the connector's business and can be built from the manifest.
