# ADR 0028: Portable SQL planning

Status: accepted, 2026-09-29.

## Context

`sqlgen` plans a SQL destination's statements once for every database, through a dialect. ADR
0027 made change merges portable from the start and split the rest of the portable work from M5b:
- the catalog writes rows whose key a row may hold with `INSERT … ON CONFLICT`, which SQLite and
  PostgreSQL know and SQL Server, Oracle, Snowflake and MySQL do not;
- a merge deleted the published rows of its staged keys, and a child table kept only the children
  of each root's winning row, by comparing row values (`(a, b) IN (SELECT …)`), which SQL Server
  and older databases lack;
- a commit publishing a child table created its root index, a schema change, and a commit
  finishing a replace generation renames and drops tables, both inside the commit's transaction;
- the root index was named `{target}__rdlt_root`, which a user table of that name collides with;
- a derived table's name is cut to the dialect's longest identifier and ends in a 32-bit hash of
  the whole, so two long table names can derive one staging table.

## Decision

- **A dialect says how it writes a row whose key a row may hold** (`SqlDialect::upserts`):
  `OnConflict`, which SQLite declares, or by default `Guarded`, standard SQL: an update of the row
  holding the key, then an insert where none holds it (`INSERT … SELECT … WHERE NOT EXISTS`). A
  transaction that writes a key another writes at the same time may then fail on the key's
  constraint, which the destination reports as it reports any write that failed; the catalog's
  keys are written by one pipeline's latest session, so the race is rare. A parameter selected without a table has no
  column to take its type from, which a database inferring parameter types may read as text; a
  dialect whose database has `ON CONFLICT`, PostgreSQL's too, declares it. Both styles write the
  catalog alike, which a test checks by running each against SQLite. A dialect whose database
  selects nothing without a table names the one it reads, as Oracle's `DUAL`
  (`SqlDialect::values_table`).
- **Keys are compared a column at a time**, in correlated `EXISTS`: a merge's delete of the
  published rows its staged keys replace, and a child table's children of each root's winning row.
  The merge's delete also takes the rows whose first key column is among the staged rows', which
  lets the database find them by the key's index rather than test every row of the table; a test
  checks SQLite's plan for it. Derived tables take their alias without `AS`, which Oracle refuses.
- **No commit changes a table's indexes.** Where a table's rows are staged, its writer indexes
  what a commit finds rows in (the SQLite writer runs both, and a test checks that a commit then
  changes no table or index):
  - every merge table, its staging and, for a change stream, its tombstones, by the key
    (`SqlPlanner::key_indexes`, `_rdlt_key__{table}`), where ADR 0027 indexed change tables only;
  - a child table by its root id (`SqlPlanner::root_index`, `_rdlt_root__{target}`).

  The prefix is one no user table takes, and each index is created through the dialect's
  `create_index`, whose default is `CREATE INDEX IF NOT EXISTS`.
- **A replace generation swaps in only where schema changes commit with their transaction.**
  Every dialect declares `transactional_ddl`, which has no default. The planner refuses a swap
  that renames or drops a generation table for one that does not, as `Unsupported`; a generation
  that wrote no table swaps in by emptying the base, which changes no schema, and is planned.
  `SqlPlanner::swaps_atomically` says which, so a destination leaves replace out of what it
  declares rather than fail at the commit. Renaming within the commit is what makes the swap
  atomic, and a copy instead would change the table's columns inside the commit too.
  - The renamed generation keeps its indexes' names, which derive from the generation table's;
    the swap drops them (the dialect's `drop_index`), and the next writer indexes the table under
    its own name, so a table never holds two indexes on one key.
- **A change stream's tombstones widen with its key.** A widen applies to the table, its staging
  and its tombstones, each that has the column, as ADR 0027 left for this milestone; a dialect
  widening in place would otherwise fail the commit that buries a widened key.
- **Tables whose derived tables would share a name are refused** when the second is created, as
  `table_name_clash`, a configuration error renaming either resolves (`SqlPlanner::distinct`):
  their staging, tombstones, root index or key indexes, and a generation table with its
  indexes, cut to the dialect's identifiers. A cut name is a SHA-256 of the whole name under a
  prefix no table may take (ADR 0049).

## Consequences

- A dialect for a database without `ON CONFLICT` or row values plans the writes, merges and
  change merges `sqlgen` makes; one without transactional schema changes loads every write mode
  but replace. Statements still written in one form for every dialect, as a swap's
  `ALTER TABLE … RENAME TO` and `DROP TABLE IF EXISTS`, are standard SQL that some databases
  spell otherwise; a dialect for one of those needs a hook for them.
- Implementing `SqlDialect` requires `transactional_ddl`.
- `SqlPlanner::claim` returns statements, as a guarded write takes two. Amended 2026-10-02: a
  claim is no call of its own; creating a table plans it (ADR 0049).
- A second table whose derived tables would take a first's name fails its first load, where it
  would have mixed rows with the first.
