# ADR 0008: Lowering plans, constant metadata and the JSON fast path

Status: accepted, 2026-09-24; the per-value differential it defers is ADR 0011's.

## Context

M3 of the spec was split three ways: M3a shreds JSON (ADR 0007); M3b makes lowering a plan
computed once per table view and incoming schema, runs it on the compute pool, dictionary-encodes
the load id, and evaluates `arrow-json` as a fast path for flat JSON; M3c brings `normalize`,
child tables and lineage ids. Building M3b surfaced decisions the spec leaves open.

## Decision

- **A lowering plan per view and incoming schema.** `LoweringPlan` records, for one table view
  and one incoming schema, where each of the table's columns takes its values, which incoming
  columns the schema policy discards, and the metadata columns' encoding. Tables keep up to eight
  plans per table for its current view, so a batch whose schema was seen before skips schema
  resolution; a schema change makes a new view and drops the old view's plans. Per batch, a plan
  discards, converts and lowers columns and adds the metadata columns.
- **Lowering runs on the compute pool.** A partition finds each batch's plan in order, since
  finding it may change the table, then lowers the flush's batches on the pool eight at a time,
  Arrow batches concatenated there too. Each window's lowered batches are charged to the memory
  budget as soon as it returns, then queued on their lane in order: a flush of the default size
  is one window, and a larger one never holds more than eight lowered batches uncharged.
- **Constant metadata columns are dictionaries.** `_rdlt_load_id` and `_rdlt_loaded_at` hold one
  value per load, so each is a dictionary of one value with an `Int8` key per row (spec §8.5
  names the load id; the load start is constant alike). A plan builds them once and slices them
  per batch, building them again only for a batch larger than they are or smaller than half of
  them. Their logical types stay `Uuid` and `Timestamp`, and `Field::from_arrow` keeps an
  extension type under a dictionary or run-end encoding.
- **Destinations take dictionary-encoded columns.** The writer contract says a column may be
  dictionary-encoded and its values are what the column stores; clause `D-ENCODING` checks it.
  The SQLite destination decodes them; the memory and files destinations already store them.
- **No `arrow-json` fast path.** On flat JSON of a known schema, `arrow-json` decodes 429 MiB/s
  narrow rows and 279 MiB/s rows of 200 columns where the shredder takes 513 and 527 MiB/s, and it
  needs the schema up front. The spec allows the fast path only where it is faster, so the engine
  keeps one shredder; the `fast_path` bench group measures both for whoever looks again.
- **Passthrough is measured against a bare loop.** The `passthrough` bench replays 64 batches of
  about 7 MB into a destination that encodes each as Arrow IPC, through the engine and through a
  bare loop writing the same batches to the same writer.
  [docs/perf/passthrough.md](../perf/passthrough.md) records the method and the numbers.

## Consequences

Per batch, the engine spends about 13 µs on a batch of 80 000 rows; the metadata columns cost the
destination 2 bytes a row instead of 24. Arrow passthrough measured 5–13 % over the bare loop
across two runs on cores of each type, against the spec's 10 %: the rest is the destination
encoding the metadata columns and the hand-offs between the partition, the pool and the lane.
Destinations must decode dictionary-encoded columns they cannot store as such.

The spec's lowering differential (§20.4) checks here that a plan reused across batches of any
size and load lowers each as a fresh plan does, value by value for the metadata columns. A
reference lowering each value on its own, independent of the conversions plans share, is left to
M3c, whose `normalize` changes what lowering produces.
