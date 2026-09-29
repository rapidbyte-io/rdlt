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
  catalog alike, which a test checks by running each against SQLite.
- **Keys are compared a column at a time**, in correlated `EXISTS`: a merge's delete of the
  published rows its staged keys replace, and a child table's children of each root's winning row.
  Derived tables take their alias without `AS`, which Oracle refuses.
- **No commit changes a table's indexes.** A child table is indexed by its root id where its rows
  are staged (`SqlPlanner::root_index`, run by the SQLite writer), in an index named
  `_rdlt_root__{target}`, the prefix no user table takes, through the dialect's `create_index`,
  whose default is `CREATE INDEX IF NOT EXISTS`, as a change table's key indexes are (ADR 0027). A
  database indexed by an older engine keeps its old index beside the new one.
- **A replace generation swaps in only where schema changes commit with their transaction.**
  Every dialect declares `transactional_ddl`, which has no default, and the planner refuses a
  swap for one that does not, as `Unsupported`: its destination does not declare replace. Renaming
  within the commit is what makes the swap atomic, and a copy instead would change the table's
  columns inside the commit too.
- **A change stream's tombstones widen with its key.** A widen applies to the table, its staging
  and its tombstones, each that has the column, as ADR 0027 left for this milestone; a dialect
  widening in place would otherwise fail the commit that buries a widened key.
- **Tables whose derived tables would share a name are refused** when the second is created, as
  `table_name_clash`, a configuration error renaming either resolves (`SqlPlanner::distinct`):
  their staging, tombstones, root index or key indexes, cut to the dialect's identifiers. A generation table's
  name holds its generation, which two tables' generations rarely share, and is not checked.

## Consequences

- A dialect for a database without `ON CONFLICT` or row values plans every statement `sqlgen`
  writes, change merges included; one without transactional schema changes loads every write
  mode but replace.
- Implementing `SqlDialect` requires `transactional_ddl`.
- `SqlPlanner::claim` returns statements, as a guarded write takes two.
- A second table whose derived tables would take a first's name fails its first load, where it
  would have mixed rows with the first.
