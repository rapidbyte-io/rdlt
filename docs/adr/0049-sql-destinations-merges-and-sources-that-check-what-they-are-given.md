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
    matches names without case) is refused as `table_name_reserved`, and so is a name holding a
    NUL, which no statement's text carries. A table that does not
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
  - A merge key is checked against the table where its writer opens: it names at least one
    column, and every column it names, its sequence, its deletion time and its history columns
    are columns of the table (`merge_key_invalid`). The reference merge refuses the same, where it panicked.
  - SQLite reads its columns from `sqlite_schema` joined to the table's own columns, never from
    a table-valued function a stream's name could select.
- **SQLite is opened hardened, private and by file name.**
  - `SQLITE_DBCONFIG_DEFENSIVE` on, `TRUSTED_SCHEMA` off, double-quoted strings off for DDL and
    DML, no attached database (`SQLITE_LIMIT_ATTACHED` 0, and none created or written),
    `cell_size_check` on, no memory map, and the write-ahead log cut back to 64 MiB once its
    content is in the database.
  - Triggers, views and foreign keys are off (`ENABLE_TRIGGER`, `ENABLE_VIEW`, `ENABLE_FKEY`).
    The destination creates none, so one in the file is another program's: a trigger would
    change other tables during a load with the connection's rights, and a foreign key would
    delete rows with a drop. Whoever plants one can write the file already; the setting keeps
    a load from doing what its statements do not say.
  - Loading extensions is left as the build has it: the SQL function is off, and rusqlite has
    no safe call for the C switch, which nothing reachable from SQL uses.
  - The path is a file's name. The bundled SQLite reads a name starting with `file:` as a URI
    whatever flags it is opened with, and a URI's parameters choose another file, switch its
    locks off or keep it in memory. A path starting so, in any case, is refused as
    `database_path_invalid`, and SQLite is handed the path from the root, which starts with a
    separator: `?`, `%` and `#` in it are characters of the name. A path that ends in no file's
    name is refused the same way.
  - A database's place is its user's alone, by the rule of ADR 0047 and through the same code:
    its directory is opened as the files connectors open a root, and belongs to the user the
    process runs as and is writable by neither its group nor others (`not_private`;
    `not_a_directory` where it is none). The database and each file SQLite keeps beside it
    (`-wal`, `-shm`, `-journal`), where they exist, are asked of that directory and must be
    regular files of that user's under one name each, and beyond the files connectors' rule no
    one else reaches them at all: a link, at the database's name too, or a second name of the
    file, which would have a log of its own, is refused as `not_a_regular_file`, and a file
    others reach as `not_private`, never re-moded, since tightening it would hide that it had
    been exposed. Each is a configuration error, since the path is the operator's to name.
  - None of the four is opened to be inspected. SQLite locks its files by record, and closing
    any descriptor of a file releases every such lock its process holds on it: a check that
    opened and closed the database let another process write in the middle of a connection's
    transaction, whenever a second session, a check or a read-back came in the same process.
    A missing database is created through the directory, exclusively, mode 0600, with the
    place checked and the descriptor closed one at a time in a process, so no connection has
    locked the file by then; SQLite gives the files it creates beside it that mode. The locks
    the files connectors take, on a table and on a keeper, are on the open file, not the
    process (`flock`, on Linux and macOS alike), and no other open or close releases them.
    SQLite adopts a log that is already there as it is, and whoever can read the lock file can
    hold every writer out, which is why the rule covers more than the database.
  - A statement waits thirty seconds for another connection's write, then fails as transient. A
    full disk is transient too, coded `disk_full`.
  - Links above the database's directory are followed, as the files connectors follow them
    to their root: SQLite's own refusal of links applies to every component, and data
    directories are often links. Only the user of the private directory puts a name in it, so
    the database's own name is no link another user planted.
- **Read-back reads what pipelines published and changes nothing.** The SQLite destination
  reads a table back on a connection that only reads, hardened and checked for its place as
  any other. A database that is missing is not created. A name the destination keeps, the
  catalog's among them, is refused as `table_name_reserved`, and a table no pipeline owns as
  `table_unowned`. Read-back names no pipeline, so any pipeline's published table is read.
- **A catalog of an earlier shape is refused, not converted.** The catalog's table of
  registered paths keys each path by its pipeline now. The catalog is created only where it is
  missing, so a database an earlier build of the destination wrote keeps the earlier table; a
  catalog table that lacks a column it has now is refused where the destination checks or
  opens the database, as `catalog_outdated` (`Config`), before anything of the catalog is
  written. The catalog's shape is this project's own and has no release to carry forward: a
  database from before is recreated or its tables loaded again.
- **SQLite stages a row at a time and refuses a float it would change.** A float that is no
  number, or negative zero, is a `Data` error coded `float_unstorable` before its row is bound,
  and nothing of its batch stays. Storing them exactly needs a column without `REAL` affinity,
  which changes what users query; a destination has no way yet to ask the engine to lower them.
- **The reference merge converts exactly or fails.** A stored column becomes its table's wider
  type by an allow-list: the lattice's widenings and re-encodings, temporal units through
  checked arithmetic, a wall-clock time placed in a zone as the instant it names there. A value
  the wider type cannot hold fails, and a pair outside the list is refused, a struct losing a
  field among them. An id or a sequence a destination keeps as text compares as the bytes of
  the text. A `Date64` that is no whole day is the day it falls in, as the engine reads it.
  - Amended 2026-10-04 (ADR 0046): temporal values widen through `rdlt_connector::instants`, as
    the engine's do: a date is its midnight in UTC, and a zone never moves an instant.
- **A schema change the held rows do not fit is refused where it is applied.** The memory
  destination converts what a table holds, its rows, generations and tombstones, before a
  column takes a type, and refuses the change as `schema_conflict`: the table stays at the
  type it had and goes on merging. A widen accepted and then failed at every merge would stop
  the stream until an operator reset the table.
- **What a merge table cannot take is refused under a code, where it is staged.** The
  reference merge, the memory destination at each flush, and `sqlgen`'s staging refuse the
  same conditions under the same codes: a flag on a key or the sequence (`flag_on_key`) or on
  what is no stored column (`flag_on_missing_column`), flags that are no bitmap
  (`flags_invalid`), an op no change stream has (`op_invalid`), a row without a sequence
  (`sequence_missing`), and for a history table whose deletes are soft, a delete or a truncate
  that says no deletion time (`deletion_untimed`). The merge adds a conversion that keeps no
  value (`type_unconvertible`), a value its wider type cannot hold (`value_unholdable`) and a
  key the rows cannot be merged by (`merge_key_invalid`). Any other failure of the merge is
  its own, an internal error.
- **A soft history delete says when.** A history table reads a version as deleted by its
  deletion time. A delete or a truncate without one would open a deleted version that reads as
  live, and the reference merge and the SQL plans each made something else of it; none is
  asked to, since it is refused before a row is staged.
- **At one sequence a truncate applies first.** A source gives each row a position of its own,
  but where a truncate and a key's row share one, the truncate removes what is before it and
  the row, which is not, applies: the reference merge orders them so wherever each was written,
  a row a truncate of the merge marked takes a change at that sequence, and the SQL plans order
  a history key's events and choose a soft row's deletion time the same way.
- **A merge costs its rows, and a row the cells it holds.**
  - The reference merge reads each batch as the columns one of its rows holds a value in, and
    gives its rows back grouped by the columns they hold: a table is as many batches as its
    rows have shapes. Twenty thousand rows of two columns and one row of a thousand more hold
    their own cells, in the commit the wide row arrives in and after it, for an upsert, a change
    stream and a history alike. The memory destination keeps its rows so, and hands a reader
    every column, an absent one as nulls its rows share. The batches a reader is given share one
    schema, in which a column some batch lacks is nullable, whatever the table declares of it:
    rows written without a column declared never null are read back with it null.
  - The files destination keeps its rows so too. JSON lines name their own columns, so a
    merged table is one file whatever columns its batches hold; an Arrow file holds one set of
    columns, so each batch is a file of its own. Published files are read back as the columns
    their rows hold: an Arrow file's as written, JSON lines in runs decoded under the columns
    their lines name. A line starts a new run where joining would leave the batch lacking more
    cells than it holds, once the run holds 256 lines or would lack 65536 cells: every batch
    costs its columns something, so lines that take turns between few columns and many read
    as batches of all of them, at what the whole schema costs, and a line of very many columns
    still starts a run of its own. What a writer staged is read as it was written, since its
    flags count its columns. A reader of the destination is given every column, an absent one
    as nulls its rows share. No entry of the merge gives every column of every row any more,
    and nothing is refused for its width.
  - A line's columns are found by one pass over its bytes that decodes nothing but its keys
    and nests no call, so a record is taken or refused as its decoder takes or refuses it. A
    line of more than one record is refused as `line_invalid`: no writer of these files, on
    this branch or before it, writes one, and compaction copies lines whole.
  - A merge gives a batch for each set of columns rows hold, however many, and makes no cell.
    A set comes with each written batch that has no value in some column, and with each row
    built from flagged columns, so one write could make as many as it has rows. The memory
    destination keeps them as they are, a batch a set: each costs what its held columns do,
    and nothing is copied. JSON lines share a file whatever their sets. Only an Arrow file
    pays for a set, a file each, rewritten by every commit, so the fold is there: past sixteen
    batches the files destination joins the smallest under the columns any of them holds, in
    groups that each hold at most 2^20 cells without a value. A cell without a value counts
    whether a join made it or a row was written with it, so a batch joined by one commit is
    measured by the next as what it is, and what a table holds of such cells does not grow
    with its commits. The same rows fold into the same files.
  - The files destination refuses at the flush what its merge cannot take, under the merge's
    codes, as the memory and the SQLite destinations do, and a merge that fails at the commit
    keeps its code. A change of a column's type is checked against what the pipeline publishes
    of the table, its generations, what the session staged, and its tombstones under the
    tombstone schema of the changed table, whose sequence is the bytes it compares by; it is
    refused as `schema_conflict` where a value does not fit, and a change of no column's type
    reads nothing. The rows are read before the table's lock is taken, and the change is taken
    under the lock only where the schema, the manifest and the staged files are those that
    were read; a table that changed meanwhile is read again, three times, then answered as
    transient (`table_changed`).
  - An id or a sequence compares as bytes, text as the bytes it is, and integers of any width
    and sign as numbers; a dictionary as the values it stands for. A root's id or sequence
    that is a number where its children's is bytes, or the other way, is refused as
    `merge_key_invalid`.
  - A truncate is found for each row by a search of the commit's truncates in order, a flag is
    read from its bitmap, and a row a delete or a truncate marks costs the two cells marked.
  - In SQL a window over a key's events gives each row the first truncate past it, the table is
    read whole only by a commit that truncates, and the bound a truncate leaves is deleted by
    its own statement so that the keyed delete uses the index. A row's flags are staged as
    bytes, one a column, read at a position (`SqlDialect::byte_at`), and two aggregate passes
    over a key's upserts find what each column was last set to: a commit of flagged updates
    costs its rows times its columns.
- **Receipts are kept.** A SQL destination and the memory destination store a receipt for every
  commit and answer a repeated commit from it, however far back in its load and however many
  loads ago. The engine's log can hold a commit's frame after the frames of many later commits
  are gone, so a destination that forgot receipts by a rule of its own would refuse a replay
  the engine may make, or apply a commit twice. Which loads and commits can still be repeated
  is the engine's knowledge: a horizon it declares is left to the work on the write-ahead log.
  Amended 2026-10-03 (ADR 0045): each commit declares that horizon, and a SQL destination and the
  memory destination forget the receipts before it within the commit.
- **Tombstones are kept** by the rules of ADR 0027 and no other. A delete of a key the table
  never held leaves one too: its insert may be sent again alone. What a merge no longer does is
  pay for each tombstone it leaves as it was. A history table's upsert lifts its key's
  tombstone as a change table's does.
- **A sequence no change can follow is the source's to give.** A destination cannot tell an
  implausible source position from a real one, so the guard of ADR 0027 trusts them; a table
  whose bound stands past every change is reset as ADR 0033 resets a stream.
- **The generated sources check their configuration and cursors.**
  - Limits, in the reference crate's `limits` module, refused where a source connects as
    `limit_exceeded`: 1024 partitions a stream; the rows of one Arrow batch; 100000 messages a
    JSON push of the log, and a thousand million messages a second of its growth; a million
    keys and captured changes of a change stream's snapshot, which a snapshot read builds; 1024
    truncates.
  - Sums of a cursor and a configured size saturate. The generator refuses a cursor at a row
    of another partition (`cursor_invalid`) and ends at the last row a number holds. When a
    message a log's read waits for arrives is computed in numbers twice as wide as an offset,
    and the wait is never under a millisecond.
  - A log's read starts only where the log issued the offset, in this process or one before
    it, since a start a source accepts is a position its host may report (ADR 0044): an offset
    up to the head, or up to what the group has committed. One past that is refused for good
    as `cursor_unissued`. So the head may not fall back between processes. A group kept in a
    file began when its lock file was made, which the keeper's first process does and nothing
    writes again, and every process counts the head from that moment of the calendar: an
    offset an earlier process issued is at or under the head of a later one. The logs of
    groups kept nowhere grow from when their process first connected a log source, whichever
    group, so none falls back while the process runs, and begin again with the next process.
    A log of such a group that does not grow has the head its configuration says, in every
    process. One
    that grows cannot tell an offset an earlier process issued from one nobody did: it accepts
    such a start, sends nothing before its head is there, and until then ends the read as a
    failure to try again (`cursor_ahead`), a stopped following read too, so the start is
    neither refused for good nor one its host is heard for. A read at the head waits, or ends
    cleanly where it does not follow.
  - A keeper's file is named `*.group` or `*.slot` (`keeper_path_invalid`): a keeper replaces
    its file whole, so none is replaced that is not named as a keeper's. Which file a path
    leads to, and how it is opened and written, is ADR 0047's: a keeper is known by the
    directory that holds its file and the name there, so every path to one file is one keeper.
  - A group's or a slot's name is any but the empty one (`keeper_name_invalid`), which is the
    default keeper's. A named keeper and one kept in a file are different kinds of key, so no
    name is a file's keeper.
  - A keeper is its host's, as a memory store is (ADR 0044): where a connector listens for
    hosts, a group or a slot is known by the host named to the connector and its name, the
    default one too, so two hosts naming one group share nothing. A keeper's file is one
    host's while a source holds it, and refused any other. A keeper kept for a host named to
    a listening connector, or in a file, is freed with the last source that holds it: a named
    one forgets what it held, as with its process, and a file's lock is let go; the name is
    forgotten when a keeper is next asked for, so what hosts name costs a listening connector
    nothing once they are gone. A named keeper of the process's own host is kept for the
    process, as a broker keeps a group between its consumers: its names are its one host's
    configuration, and a run that connects its source again finds what the last committed.
  - A source acknowledges only partitions its stream has, all of a call's or none, and the log
    source reads no other.
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
  hard-deleted, until the engine declares what it may still repeat. Amended 2026-10-03
  (ADR 0045): receipts are forgotten before the horizon each commit declares; tombstones are
  not, as no commit horizon shows one unneeded.
- A growing log whose group is kept nowhere accepts a start past its head and fails the read
  to be tried again until the head is there: a host that wants a forged start refused keeps
  the group in a file. A group file's beginning is the calendar's: a clock set back between
  processes lowers the head, and an offset issued and not yet committed may then be refused;
  so may one where the lock file is made anew.
- A named group or slot a listening connector keeps for a host forgets what it was told once
  no source of that host holds it.
- An operator who shared a SQLite file or its directory with a group must serve its readers
  another way. Where the open of a new database fails after its file was created, an empty
  private file stays.
- The check of a database's place is by path after its directory is asked: it holds while the
  directories above are the operator's, as ADR 0047 leaves them.
- The exact conversion repeats what the engine's own lowering does for arriving values; one
  implementation in the connector SDK would serve both.
- An Arrow merge table lists a file for each shape of its rows: the fifteen of most rows, and
  the others in groups, about one for each 2^20 cells the group's rows lack of each other's
  columns, so the count follows what was written and no constant bounds it short of the
  manifest's size. Each file is synced and every commit rewrites them all, as it rewrote the
  one file before. Append tables and generations compact as ADR 0047 says: an Arrow file
  joins only files of its columns, and JSON lines of any columns share a file.
- A memory merge table holds a batch for each set of columns its rows hold, up to one a row,
  and its read-back hands a reader a reference a column for each.
- A JSON lines file the destination reads back holds one record a line. A read of published
  lines passes over each line twice, once for its keys.
- A widen of a column reads the table once before it is taken, without the table's lock.
- A SQLite database with a second hard link is refused.
- A merge gives its rows back by the columns they hold, not in the order they were published.
- The engine answers `schema_conflict` by resolving names again, which helps a clashing new
  column and not a widen of a column that exists. Amended 2026-10-03 (ADR 0041): a refused widen
  now routes the column's values to a variant column, or is refused typed where none may take
  them.
- A connector that flags its key column is refused at every destination, since the engine
  carries the flag through.
- Cost tests of a SQL plan count the steps SQLite's virtual machine takes, many operations
  against one, and depend on no clock. The reference merge's tests of truncates count the
  comparisons its search makes, through the one function that makes them. Those of flags in
  the reference merge and in staging, which have nothing to count, compare the least of three
  timings of many operations with one. Each is sized to tell a linear cost from a product in
  under a second.
