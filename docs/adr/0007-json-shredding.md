# ADR 0007: JSON shredding and coalescing

Status: accepted, 2026-09-24.

## Context

M3 of the spec makes JSON pushes load. It was split: M3a shreds JSON pushes end to end,
coalescing pushes, shredding them in parallel on the compute pool and holding the shredder to a
reference implementation, fuzzing and the throughput gate; M3b brings `normalize` with child
tables and lineage ids, the `LoweringPlan`, the dictionary-encoded load id and the `arrow-json`
fast-path evaluation. Building M3a surfaced decisions the spec leaves open or gets wrong.

## Decision

- **One pass parses, observes and builds.** The spec parses each chunk into a tape, walks it to
  observe types, and builds against the joined types in a second walk. Walking sonic-rs's
  document costs as much as parsing it, which caps that design near 3.5 times the old engine.
  Instead each chunk is parsed once through serde seeds over sonic-rs, and every value goes
  straight into a column typed by the values the chunk held so far: a column starts as nulls and
  takes its first value's type, and converts itself when a later value widens it without loss (a
  float after integers exact as floats, a large unsigned integer after integers). Any other value
  makes the column `Json`, which it cannot build from the values it already took, so it stops
  building and only checks its later values' nesting. The chunks' shapes are joined in push
  order. A chunk whose columns fit the joined shape keeps them, fitted without its values:
  columns are matched by name, columns it lacks or held only nulls in become nulls of the joined
  type, and integers are cast to the floats or decimals the join widened them to. Only a chunk
  holding a column that stopped building, or one the join made `Json` or another kind, is parsed
  again and built against the joined shape. Batches therefore always have the joined shape, as
  the spec requires, and optional keys cost no second parse.
- **Pushes are split into records on the pool.** Each push's records are found on the compute
  pool before chunking: JSON lines by their line ends, a JSON array by its top-level commas,
  scanned without recursion however deep its values nest. Each record is then parsed on its own,
  so a line holds exactly one record and an array element exactly one value.
- **Values are typed by what they hold.** Integers that fit 64 signed bits are `Int64`; one
  beyond that range makes the column `Decimal(20, 0)`. Integers and floats together are `Float64`
  while every integer is exact as a float, and `Json` otherwise. Mixed kinds are `Json`, each
  value rendered as compact JSON text as it is parsed, keys in their order. Objects are structs
  and arrays lists, whatever the nesting policy: lowering applies the policy. sonic-rs, without
  its `arbitrary_precision` feature, reads integers beyond the unsigned 64-bit range as floats and
  negative zero as zero; the feature costs about a quarter of the throughput, so those are
  documented limits.
- **Hostile input is refused, typed.** A record that is not an object is `json_not_object`; an
  object repeating a key, however escaped and at any depth, `json_duplicate_key`; a value nested
  deeper than `MAX_NESTING_DEPTH` (64, the record counting as the first level) or more than
  `MAX_COLUMNS` top-level columns, `limit_exceeded`; anything else unparsable, `json_invalid`.
  Parsing recurses once per level and stops at the depth limit, so no input exhausts the stack:
  a value at the limit needs about 128 KiB in release builds, within a compute thread's 2 MiB, and
  nearly all of it unoptimized.
- **Partitions coalesce pushes.** Pushes gather until `BatchPolicy::target_bytes` or `max_rows`,
  or until the first has waited `max_latency` on the environment's clock. A checkpoint, a push
  of the other kind or an Arrow schema that differs, and the end of the read flush them first, so
  a segment never spans coalesced pushes. A JSON push's rows are only known once it is shredded,
  so JSON counts by bytes alone. Arrow batches of one schema are concatenated. The pushes'
  permits travel with an Arrow batch; JSON permits are released once the pushes are shredded, and
  each batch is charged as growth.
- **The reference shredder shares no code.** It parses with `serde_json` into a tree that keeps
  key order and repeated keys, reads each column's type off the whole set of its values, and
  builds the batch with `arrow-json`. A property test holds the shredder to it over generated
  records, pushes and chunk sizes; JSON text compares as parsed values. The `shred` fuzz target
  asserts the shredder never fails inside itself, and runs nightly.
- **Benchmarks build as shipped binaries.** The `bench` feature exposes the shredder to a
  criterion bench, and the bench profile uses fat LTO, as the old engine's harness does. The
  comparison, its method and the core scaling live in [docs/perf/shred.md](../perf/shred.md);
  scaling is measured on cores of one type. The base-against-head instruction-count gate of
  §21.3 lands with the other §20.14 gates in M9.
- **The simulation pushes JSON.** Streams drawn as JSON push their rows as JSON lines or arrays,
  and the oracle, which compares values, holds them to the same model as Arrow.

## Consequences

JSON pushes load, in parallel, at over five times the old engine on one core. Wide integers load
exactly up to the unsigned 64-bit range and as floats beyond it. A chunk whose shape drifts is
parsed twice, so drift costs throughput where it happens and nowhere else. The engine's
integration tests run compute jobs inline, since the paused test clock would otherwise advance
while a job runs on another thread.
