# ADR 0049: SQL destinations, merges and reference sources that check what they are given

Status: accepted, 2026-10-01.

## Context

ADR 0037 makes the host a peer a connector does not trust, and one database or one store a place
several pipelines load into. The SQL planner `sqlgen`, the SQLite and memory destinations, the
reference merge and the generated sources were written before it, and each took something as
given:

- `sqlgen` planned a statement for any table name a session sent. The owner record
  (ADR 0025) was read by the connector, in some calls and not in others: a commit's child tables,
  a generation swap resolved through a path every pipeline shared, the staging discarded at open,
  and a drop of a table with no owner record all went unchecked. A table could be named as the
  catalog's own.
- A derived table's name was cut to the dialect's identifier limit with a 32-bit hash, which
  two table names can share.
- Statements carried text the database or the host supplied: a column's declared type copied
  from the catalog into DDL, and a merge key's columns, whether or not the table held them.
- SQLite was opened as its defaults leave it: a file readable by whoever the process's mask
  allowed, a path read as a URI where it started with `file:`, a schema that could carry triggers and
  views run with the connection's rights, and double-quoted text taken for a string where no
  column matched.
- SQLite binds a float that is no number as `NULL` and stores negative zero in a `REAL` column
  as the integer 0. A batch was bound column by column, so a refused value left the rows before
  it staged.
- The reference merge retyped stored rows with Arrow's cast, which nulls a value the wider type
  cannot hold and panics at the edge of the calendar, and it paired every truncate of a commit
  with every row, every flag with every column. The statements `sqlgen` planned for a change
  stream did the same in SQL, and read the table whole in every commit.
- The generator, log, changes and memory sources took partition counts, batch sizes, key counts,
  cursors and keeper paths as given.

## Decision

- **A statement that changes a table takes a witness that the session owns it.**
  - `sqlgen::Owned` holds a table's name and the pipeline that owns it, and every planner
    method that creates, alters, stages into, publishes, swaps, drops or discards a table takes
    one. The planner stays free of I/O, so a blocking and an async driver both use it.
  - What the witness gives, and what it does not. `SqlPlanner::check` hands a connector two
    queries, who owns the table and what the database takes its name for; `Check::answered`
    reads the rows they returned into a `Standing`; and only a `Standing` makes an `Owned`.
    The witness cannot be copied, and it borrows what the connector hands `answered` with the
    rows; the SQLite session hands its transaction, so there the witness is not kept across
    transactions. A connector cannot plan a write without asking, and one that hands its
    transaction cannot reuse an answer later. It can still hand back rows the database did not return: the
    planner runs no statement, so that the answers are the database's is the connector's to
    keep.
  - **An owner record stands for a table its pipeline created.** The record, the table, its
    staging table and its registration are one plan, written by `TableChange::Create` alone, in
    one transaction. A writer or any other change of a table no pipeline owns is refused as
    `table_unowned` and claims nothing; from a session a newer one fenced it is refused as
    fenced, as ADR 0033 has it. Where a pipeline opens, an owner record of its that stands for
    no staging table is released: the record goes and nothing is dropped.
  - **Names are compared as the database resolves them.** A dialect says what the database
    takes a name for (`SqlDialect::resolves`: for SQLite every table, view or index of that
    name without ASCII case). A table the database already holds under the name, in any case,
    without an owner record is not adopted: creating it is refused as `table_unowned`, never
    done as nothing. A name the database takes for a table named otherwise is refused the same
    way wherever the table is changed or dropped, so a drop reaches only a table its
    pipeline's owner record names as the database holds it. A staging, tombstone, generation or
    catalog table the database holds in another case is a `table_name_clash`, checked before
    the catalog is created, where a table is created and where a writer opens; the clash check
    between tables folds names as the dialect does.
  - A name under `_rdlt_`, in any case, or one the dialect keeps (`SqlDialect::reserves_table`:
    for SQLite `sqlite_`, `pragma_`, and any name that is not its own lower case, since SQLite
    matches names without case) is refused as `table_name_reserved`. A table that does not
    exist and no pipeline owns is no drop at all, as ADR 0033 has it.
  - A path resolves to a table per pipeline: `_rdlt_tables` is keyed by pipeline and path. A
    generation finished for a path swaps the pipeline's own table or nothing.
  - A commit's child table is checked where it publishes, and an open discards the staging of
    the tables its pipeline owns and of no other.
  - The memory destination holds the same rules in its own store.
- **A cut name is a hash no uncut name can take.** A derived name longer than the dialect's
  identifiers is `_rdlt_fit_` and the SHA-256 of the whole name, in base32. Every name derived
  from a table is compared with every other the commit touches, and a clash is refused as
  `table_name_clash`. A dialect's identifiers hold at least 63 bytes, which fits the hash;
  `SqlPlanner::try_new` refuses a shorter one.
- **No statement carries text the database or the host chose.**
  - A column type copied from the catalog is written only where it is one the dialect itself
    declares (`SqlDialect::declares`), as the dialect renders it.
  - A merge key is checked against the table where its writer opens: at least one column, and
    every column, the sequence, the deletion time and the history columns the table's
    (`merge_key_invalid`). The reference merge refuses the same, where it panicked.
  - SQLite reads its columns from `sqlite_schema` joined to the table's own columns, never from
    a table-valued function a stream's name could select.
- **SQLite is opened hardened, private and by file name.**
  - `SQLITE_DBCONFIG_DEFENSIVE` on, `TRUSTED_SCHEMA` off, double-quoted strings off for DDL and
    DML, no attached database (`SQLITE_LIMIT_ATTACHED` 0, and none created or written),
    `cell_size_check` on, no memory map, and the write-ahead log cut back to 64 MiB once its
    content is in the database.
  - The path is a file's name. The bundled SQLite reads a name starting with `file:` as a URI
    whatever flags it is opened with, and a URI's parameters choose another file, switch its
    locks off or keep it in memory. A path starting so, in any case, is refused as
    `database_path_invalid`, and SQLite is handed the path from the root, which starts with a
    separator: `?`, `%` and `#` in it are characters of the name. A path that ends in no file's
    name is refused the same way.
  - A database's place is its user's alone, by the rule ADR 0047 gives the files connectors.
    Its directory, asked once open, belongs to the user the process runs as and is writable by
    neither its group nor others (`not_private`; `not_a_directory` where it is none). The
    database and each file SQLite keeps beside it (`-wal`, `-shm`, `-journal`), where they
    exist, are regular files of that user's that no one else reaches: a link, at the database's
    name too, is refused as `not_a_regular_file`, and a file others reach as `not_private`,
    never re-moded, since tightening it would hide that it had been exposed. A new database is
    created exclusively, mode 0600, and SQLite gives the files it creates beside it that mode.
    SQLite adopts a log that is already there as it is, and whoever can read the lock file can
    hold every writer out, which is why the rule covers more than the database.
  - A statement waits thirty seconds for another connection's write, then fails as transient. A
    full disk is transient too, coded `disk_full`.
  - Links above the database's directory are followed, as the files connectors follow them
    to their root: SQLite's own refusal of links applies to every component, and data
    directories are often links. Only the user of the private directory puts a name in it, so
    the database's own name is no link another user planted.
- **SQLite stages a row at a time and refuses a float it would change.** A float that is no
  number, or negative zero, is a `Data` error coded `float_unstorable` before its row is bound,
  and nothing of its batch stays. Storing them exactly needs a column without `REAL` affinity,
  which changes what users query; a destination has no way yet to ask the engine to lower them.
- **The reference merge converts exactly or fails.** A stored column becomes its table's wider
  type by an allow-list: the lattice's widenings and re-encodings, temporal units through
  checked arithmetic, a wall-clock time placed in a zone as the instant it names there. A value
  the wider type cannot hold fails the merge, and a pair outside the list is refused. An id or
  a sequence a destination keeps as text compares as the bytes of the text.
- **A merge costs its rows.** Columns a row never had are one shared array of nulls; a
  truncate is found for each row by a search of the commit's truncates in order, and a flag is
  read from its bitmap, in the reference merge. In SQL a window over a key's events gives each
  row the first truncate past it, the table is read whole only by a commit that truncates, and
  the bound a truncate leaves is deleted by its own statement so that the keyed delete uses the
  index. An unchanged flag on a key or sequence column is refused by the reference merge as by
  `sqlgen`'s staging.
- **Receipts are kept.** A SQL destination and the memory destination store a receipt for every
  commit and answer a repeated commit from it, however far back in its load and however many
  loads ago. The engine's log can hold a commit's frame after the frames of many later commits
  are gone, so a destination that forgot receipts by a rule of its own would refuse a replay
  the engine may make, or apply a commit twice. Which loads and commits can still be repeated
  is the engine's knowledge: a horizon it declares is left to the work on the write-ahead log.
- **Tombstones are kept** by the rules of ADR 0027 and no other. A delete of a key the table
  never held leaves one too: its insert may be sent again alone. What a merge no longer does is
  pay for each tombstone it leaves as it was.
- **A sequence no change can follow is the source's to give.** A destination cannot tell an
  implausible source position from a real one, so the guard of ADR 0027 trusts them; a table
  whose bound stands past every change is reset as ADR 0033 resets a stream.
- **The generated sources check their configuration and cursors.**
  - Limits, in the reference crate's `limits` module, refused where a source connects as
    `limit_exceeded`: 1024 partitions a stream; the rows of one Arrow batch; 100000 messages a
    JSON push of the log; a million keys and captured changes of a change stream's snapshot,
    which a snapshot read builds; 1024 truncates.
  - Sums of a cursor and a configured size saturate. The generator refuses a cursor at a row
    of another partition (`cursor_invalid`) and ends at the last row a number holds. A cursor
    past the end reads nothing; a log's cursor past its head waits, since the head starts again
    with each process.
  - A keeper's file is named from the root, each directory by its name, as `*.group` or
    `*.slot` (`keeper_path_invalid`): one way to write each file, and no file replaced that is
    not named as a keeper's. How the file is opened and written is ADR 0047's.
  - A source acknowledges only partitions its stream has, all of a call's or none.
  - A stream that does not serve again what it acknowledged needs a named group or slot
    (`keeper_unnamed`): the default keeper is shared by every source of a process naming none.

## Consequences

- A host that sends SQLite a table name in mixed case, or one under `sqlite_` or `pragma_`, is
  refused. A table whose owner record was lost must be dropped by hand, and a table another
  program made is never loaded into: a pipeline loads only tables it created.
- A database with a column of a type the dialect does not declare cannot be adopted until the
  column is retyped.
- Dialects with identifiers under 63 bytes are unsupported.
- A stream holding a float that is no number, or negative zero, does not load into SQLite.
- `_rdlt_receipts` grows by a row a commit, and a change table's tombstones by a row a key
  hard-deleted, until the engine declares what it may still repeat.
- An operator who shared a SQLite file or its directory with a group must serve its readers
  another way. Where the open of a new database fails after its file was created, an empty
  private file stays.
- The check of a database's place is by path after its directory is asked: it holds while the
  directories above are the operator's, as ADR 0047 leaves them.
- The exact conversion repeats what the engine's own lowering does for arriving values; one
  implementation in the connector SDK would serve both.
- The files destination shares the reference merge, and still writes a merged table column by
  column, so a table that widened costs it rows times width there.
- Cost tests compare the least of three timings of many operations with one operation, so they
  hold on a loaded machine.
