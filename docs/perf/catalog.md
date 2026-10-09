# Catalog lookups

What finding, forgetting and discarding a commit's segments costs SQLite beside other pipelines'
segment rows in `_rdlt_segments`, which the index on (pipeline, epoch, segment) keeps constant.

## Method

- **Signal.** SQLite's virtual-machine steps (`StatementStatus::VmStep`) for each statement, as
  `sqlgen`'s test helper `planned()` runs it in a transaction it rolls back: exact, whatever the
  machine or its load.
- **Build.** rusqlite 0.40.2's bundled SQLite 3.53.2, which the reference destination ships.
- **Run.** `a_commit_finds_its_segments_in_as_many_steps_however_many_others_the_catalog_holds`
  (`crates/rdlt-connector/src/sqlgen/tests.rs`) fills the catalog with 10, then 10,000, rows of
  another pipeline's segments and asserts the steps are equal; turning its `assert_eq!` into
  `assert_ne!` prints both. `staged` returns rows, which `planned()` does not run, so its query
  plan stands for it.
- **Commit.** The commit that adds this record, on `d21a5914e44a`; `cargo nextest` of
  rdlt-connector's library tests, dev profile.

## Figures

| Statement | Beside 10 rows | Beside 10,000 rows |
|---|---|---|
| `forget` | 19 | 19 |
| `discard` (the catalog's statement) | 15 | 15 |
| `staged` | `SEARCH _rdlt_segments USING INDEX _rdlt_segments_staged` | the same |
