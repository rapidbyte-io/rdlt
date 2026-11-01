# ADR 0006: SQL and file reference connectors

Status: accepted, 2026-09-24.

## Context

`sqlgen` is the planner every SQL destination shares, and the transactional `sqlite` reference
destination is built on it; the `files` reference source and destination publish with
manifests. These destinations keep data outside the process, so publishing, fencing and the
landed key must hold across a database's transactions and a file system's operations.

## Decision

- **`sqlgen` plans; destinations run.** A dialect gives quoting, placeholders, column types, when
  a declared type holds a logical type, an optional in-place widen and its catalog query;
  `SqlPlanner` turns them into parameterized statements for the catalog (each pipeline's fence
  row of its epoch and landed key, state, registered tables and generations, staged segments),
  schema changes, staging, publishing and generation swaps. The destination runs them in its own
  transactions. The SQLite dialect ships with `sqlgen`, so its tests run every statement against
  SQLite through a development dependency on `rusqlite`; Postgres brings its own dialect in
  `rdlt-connectors`.
- **Staged rows carry who staged them.** Every table has a staging table beside it whose rows
  record the pipeline, epoch, segment and generation. A commit publishes only its own epoch's
  rows, so a fenced writer's late rows are never published, and `discard_staged` removes only
  what older sessions staged, so a discard that waits behind a newer session and lands late
  never removes its staging (the memory destination keys its staging by epoch for the same
  reason). While a table's writer appends, its staging is indexed by pipeline, epoch and segment,
  as is the catalog of staged segments, so an append commit finds its own rows, and every commit
  its tables, rows and bytes, without scanning what is staged for later commits or by other
  pipelines; a load of one commit pays for the index as it stages, about a third more time (2.74 s
  against 2.05 s for a million rows; docs/perf/sqlite.md). A keyed writer drops the index
  (ADR 0028): a keyed commit scans its staging and finds each key's rows by its key index, a
  lookup an index by who staged a row would take.
- **A publish keeps every column its rows were written with, or publishes nothing.** A column
  the rows a commit publishes were written with that the table lacks, as one dropped outside the
  pipeline, fails the commit with a `Data` error coded `column_missing` that names it, rather
  than publishing the rows without it, as Confluent's JDBC sink fails a task with the missing
  columns; a generation is checked as a table is. The destination says what the rows were
  written with: the SQLite session notes the columns its writers stage each segment's rows with,
  since a commit publishes only its own session's rows and the catalog records no columns of
  them, and forgets a segment once a commit publishes or abandons it. A column the staging holds
  that the rows were not written with, as a change stream's flags after the table is appended
  to, or a column an abandoned generation added, holds none of their values and stops nothing;
  a change stream's op and unchanged flags direct its merge, and no table holds them.
- **Merges delete and insert.** A commit ranks its staged rows per key by sequence, deletes the
  published rows of those keys and inserts the first of each. The writer's `TableRef` decides:
  each staged segment records its merge key, so a table that switches between append and merge
  follows its writer. A merge needs no unique key, which a table that appended repeated keys
  could not take: the key indexes a writer creates are not unique (ADR 0028). SQLite and
  Postgres share the form; `sqlgen` plans no `MERGE`, which no dialect needs.
- **Generations are tables.** Rows of a replace generation fill their own table, created with the
  table's columns if the generation was never declared; the finishing commit drops the table and
  renames the generation over it, and drops older generations. A generation with no table leaves
  the table empty.
- **Ids keep every bit.** SQL integers are signed, so ids are stored with the same bits and read
  back unsigned; the engine's generation ids use the full range, and `D-REPLACE` uses one
  beyond the signed range, so a destination that narrows ids fails certification.
- **SQLite stores storage classes.** Columns are declared `BOOLEAN`, `INTEGER`, `REAL`, `TEXT` or
  `BLOB`, so widening within the integer or float family changes nothing, and the engine lowers
  every other type to text. Identifiers fold to lower case, since SQLite compares them without
  case. `IdentifierRules` gains `reserved_table_prefixes`, and the engine prefixes a table
  identifier with `_` until it starts with none of them: SQLite reserves `_rdlt_`, which every
  `sqlgen` table starts with, and `sqlite_`, which SQLite keeps for itself, and reserves the
  staging columns' names as words. Calls run on the blocking pool, each in an
  immediate transaction.
- **Manifests are created exclusively.** The files destination publishes a commit by creating
  the pipeline's next manifest version with a hard link, which fails if another session created
  it first: the conditional put on a local filesystem. Opening creates a version with the next
  epoch. Each pipeline's directory is `_rdlt/pipelines/<id>-<hash>`, so ids that differ only in
  case never share one, and a manifest names its files by their paths beneath it. Directories
  are created 0700 and files 0600, the destination's user's alone (ADR 0047). A segment's files
  are staged at `staging/<epoch>/<segment>/<table>/<generation>/<part>.<ext>` beneath the
  pipeline's directory. A commit's own files, a merge table's rewrite, its tombstones and a
  compaction, are staged there too, with `merged`, `tombstones` or `compacted` and a name unique
  to the commit's try (`<epoch>-<seq>-<random>`, its key) in place of the segment. Files are
  published where they are. A commit, once its manifest is durable, removes the files that
  manifest no longer lists, and opening removes what older epochs staged or wrote that the latest
  manifest does not list. A merge table's commit rewrites the whole table: JSON lines as one
  file, Arrow as a file for each set of columns its rows hold, the smallest sets joined where
  there are more than 16. The manifest keeps the receipt of the last commit that landed, its key
  answering that commit sent again, and the 8 versions before the latest stay on disk.
- **Formats decide types.** Arrow IPC files keep every type; JSON lines keep scalars, structs and
  lists, and the engine stores the rest as text. A table's columns live in a catalog under the
  root, which schema changes update with the rules the memory destination follows; each change
  creates the table's next catalog version exclusively and works itself out again when another
  change lands first, so none is lost, and the 8 versions before the latest stay on disk.
- **The files source discovers streams from names.** `<stream>.jsonl` (or `.ndjson`) or
  `<stream>.arrow` is a stream of one partition and a directory is a stream of its files; a
  partition is read only if it is listed, never by joining its id to a path. A JSON lines file's
  records are pushed as JSON, as they are written, `batch_rows` records a push, for the engine to
  type; an Arrow file's batches are pushed as written. Its streams are read in full: every push
  but a file's last is followed by a checkpoint, and the last is not, so the partition ends done
  without a cursor and the next run reads the file from its start.
- **The engine's integration suite runs against every destination** where the destination
  matters: exactly-once loads, resumes, append and replace, stopped replaces, merges, lost
  responses, fencing, schema changes and nested values run against memory, SQLite and both file
  formats; tests of the engine's internals keep the memory destination.
- **The simulation draws destinations without JSON**, which store JSON and nested values as text;
  the oracle parses them back through the committed schema's types.

## Consequences

SQL destinations inherit publishing, fencing and the landed key from `sqlgen` and test them
against SQLite. The files destination runs on local filesystems; object storage replaces the hard
link with its conditional put. Readers of the files destination follow the latest manifest; a
consumer-facing layout is the connector's business and can be built from the manifest.
