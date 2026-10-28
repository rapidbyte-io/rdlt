# ADR 0008: Lowering plans, constant metadata and the JSON fast path

Status: accepted, 2026-09-24; the per-value differential it defers is ADR 0011's.

## Context

M3 of the spec was split three ways: M3a shreds JSON (ADR 0007); M3b makes lowering a plan
computed once per table view and incoming schema, runs it off the runtime, dictionary-encodes
the constant metadata columns, and evaluates `arrow-json` as a fast path for flat JSON; M3c
brings `normalize`, child tables and lineage ids. Building M3b surfaced decisions the spec leaves
open.

## Decision

- **A lowering plan per view and incoming schema.** `LoweringPlan` records, for one table view
  and one incoming schema, where each of the table's columns takes its values, which incoming
  columns the schema policy discards, and the metadata columns' encoding. Tables keep up to eight
  plans per table for its current view, so a batch whose schema was seen before skips schema
  resolution; a schema change makes a new view and drops the old view's plans. Per batch, a plan
  discards, converts and lowers columns; a merge table's plan then keeps the rows its merge keeps,
  compared by the key as the table stores it (the last row of each key, or for a change stream
  the rows no later row of their key supersedes), and adds the metadata columns for those rows. A
  row's sequence is its position among the rows received, and a change stream's row's is its
  source's position.
- **Lowering that copies values runs on the compute pool.** A partition finds each batch's plan
  in order, since finding it may change the table, then lowers the flush's batches eight at a
  time, Arrow batches concatenated there too. Each window's lowered batches are charged to the
  memory budget as soon as it returns, then queued on their lane in order: a flush of the default
  size is one window, and a larger one never holds more than eight lowered batches uncharged.
  - A window runs on the compute pool where lowering any of its batches reads or writes a value,
    and on the partition's task where none does.
  - A batch is lowered copying nothing where its plan hands every column to the table's column
    of its own type, stored in that type, whose values need no check (no decimal, whose
    precision is checked value by value, no typed null and no nested value); keeps every row;
    adds only the load's constant columns, already built for as many rows of its load; and the
    batch is one push's, not several to concatenate, nor a change batch to split. Lowering it
    clones a handle a column and checks their lengths.
  - Such a lowering costs in proportion to the batch's columns, which the schema's limit bounds,
    and less than a trip to the pool, which wakes a thread there and the partition's task on its
    way back; every other lowering costs in proportion to its rows. The rule follows from the
    plan and the batch's types, so no threshold is tuned: a load's first piece, which builds the
    constant columns, goes to the pool, and its next pieces do not.
- **Constant metadata columns are dictionaries.** `_rdlt_load_epoch` and `_rdlt_loaded_at` hold
  one value per session, so each is a dictionary of one value with an `Int8` key per row. A plan
  builds them once and slices them per batch, building them again only for a new session or a
  batch of more rows than they hold. Their logical types stay `Int64` and `Timestamp`.
- **Destinations take dictionary-encoded columns.** The writer contract says a column may be
  dictionary-encoded and its values are what the column stores; clause `D-ENCODING` checks it.
  The SQLite destination decodes them; the memory and files destinations already store them.
- **No `arrow-json` fast path.** On flat JSON of a known schema the shredder is faster than
  `arrow-json`'s decoder, which also needs the schema up front. The spec allows the fast path only
  where it is faster, so the engine keeps one shredder; the `fast_path` bench group measures both,
  and [docs/perf/shred.md](../perf/shred.md) records the figures.
- **Passthrough is measured as a matrix, its paired cell against a bare loop.** The `passthrough`
  bench moves the same 5,120,000 rows of ten mixed columns through the engine into a destination
  that encodes each batch as Arrow IPC: in batches of 1,000, 8,000 and 80,000 rows, from 1, 4 and
  16 partitions of 1 and 4 streams, committing at the end or every sixteenth of the rows, within
  the default budget. Its paired cell, 64 batches of 80,000 rows from one partition of one
  stream within a 1 GiB budget through one writer, runs blocks of bare loop, engine, engine,
  bare loop through the same writer and keeps the ratio of their times; the overhead is the
  median of the samples' ratios with a bootstrap interval, so a machine that drifts during a run
  moves both sides of a ratio alike.
  [docs/perf/passthrough.md](../perf/passthrough.md) records the method and the numbers.

## Consequences

The metadata columns cost the destination 2 bytes a row instead of 24. How far passthrough is
over the bare loop, against its bound, is recorded in
[docs/perf/passthrough.md](../perf/passthrough.md). Destinations must decode dictionary-encoded
columns they cannot store as such.

The spec's lowering differential (§20.4) checks here that a plan reused across batches of any
size and load lowers each as a fresh plan does, value by value for the metadata columns. A
reference lowering each value on its own, independent of the conversions plans share, is left to
M3c, whose `normalize` changes what lowering produces.
