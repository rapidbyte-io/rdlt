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
  makes the column `Json`. A column of scalars but floats renders the values it took as JSON text,
  which holds them as they were read, and goes on building as JSON. Any other column cannot: a
  float's text as it was written, an object and an array are gone, so it stops building and only
  checks its later values' nesting. The chunks' shapes are joined in push order. A chunk whose
  columns fit the joined shape keeps them, fitted without its text: columns are matched by name,
  columns it lacks or held only nulls in become nulls of the joined type, integers are cast to
  the floats or decimals the join widened them to, and scalars but floats are rendered as JSON
  text where the join made their column `Json`. Only a chunk holding a column that stopped
  building, or one the join made another kind, or `Json` over floats, objects or arrays, is
  parsed again and built against the joined shape. Batches therefore always have the joined
  shape, as the spec requires, and optional keys cost no second parse. The pass is metered:
  builders charge what they are made with and grow by, within twice the chunk's text; a chunk
  that would pass that is read again observing kinds and counts, and what the batches take
  beyond the push's admission is reserved before any of them is built (ADR 0040).
- **Pushes are cut into chunks on the pool without walking their records.** JSON lines are cut
  at the first line end past each chunk's bytes, found by jumping past those bytes; a JSON
  array's top-level commas are found by one scan of its structure, without recursion however
  deep its values nest, which checks it is an array and counts each chunk's elements. Each
  chunk's job finds its own records, and counts a chunk of lines by its line ends, then parses
  each record on its own, so a line holds exactly one record and an array element exactly one
  value.
- **Values are typed by what they hold.** Integers are read exactly at any width: those that
  fit 64 signed bits are `Int64`; those above them that fit 64 unsigned bits, `Decimal(20, 0)`;
  within 38 digits `Decimal(38, 0)`; within 76 digits `Decimal(76, 0)`; and a column holding an
  integer beyond 76 digits is `Json`, each such integer kept as the text it was written as. A
  chunk whose fast parse read a float that may be a rounded integer, a whole float of 2⁶³ or more
  in a record holding 19 or more digits in a row, is parsed again with its numbers as text.
  Integers and floats together are `Float64` while every integer is exact as a float, and `Json`
  otherwise. Mixed kinds are `Json`, each value rendered as compact JSON text as it is parsed,
  keys in their order. Objects are structs and arrays lists, whatever the nesting policy:
  lowering applies the policy. Negative zero reads as zero.
- **Hostile input is refused, typed.** A record that is not an object is `json_not_object`; an
  object repeating a key, however escaped and at any depth, `json_duplicate_key`; a value nested
  deeper than `NESTING_DEPTH` (64, the record counting as the first level), more columns than
  the engine's derived `schema_columns` or more than `MAX_CELLS` (32 Mi) cells, `limit_exceeded`;
  anything else unparsable, `json_invalid`. Columns are counted for every column the records
  hold together, list items included, while a chunk is parsed and for the joined shape. A cell is
  a row under a column holding values: every row takes one in every column, so without the bound
  a small push of sparse, wide records builds gigabytes of nulls; cells are counted a level at a
  time, a list's items being the rows of their level, before anything is built. A refusal quotes
  a key or a number cut to 128 bytes. Only JSON's whitespace may surround a record, and an error
  names what broke and the record's place among the pushes without quoting the record.
  Parsing recurses once per level and stops at the depth limit, so no input exhausts the stack.
  Unoptimized builds use far more stack per level than release ones, so each level grows the
  stack on demand (with `stacker`), every shredding job is sure of 4 MiB before it starts, and
  compute threads have 8 MiB: values at the limit shred in every build, on any thread.
- **Partitions coalesce pushes.** Pushes gather until `BatchPolicy::target_bytes` or `max_rows`,
  or until the first has waited `max_latency` on the environment's clock. A checkpoint, a push
  of the other kind or an Arrow schema that differs, and the end of the read flush them first, so
  a segment never spans coalesced pushes. A JSON push's rows are only known once it is shredded,
  so JSON counts by bytes alone. Arrow batches of one schema are concatenated. Gathered pushes
  hold their permits, which the next push's admission may be waiting for, so the budget signals
  while any request waits and a partition then writes what it gathered at once: under memory
  pressure batches are smaller, never late. The pushes' permits travel with an Arrow batch; each
  batch shredded from JSON is charged its own bytes before the pushes' permits are released.
- **The reference shredder shares no code.** It parses with `serde_json` into a tree that keeps
  key order and repeated keys, reads each column's type off the whole set of its values, and
  builds the batch with `arrow-json`. A property test holds the shredder to it over generated
  records, pushes and chunk sizes; JSON text compares as parsed values. The `shred` fuzz target
  asserts the shredder never fails inside itself, and runs nightly.
- **Benchmarks build as shipped binaries.** The `bench` feature exposes the shredder to a
  criterion bench, which builds with the release profile's fat LTO and one codegen unit, as every
  shipped binary does. The comparison, its method and the core scaling live in
  [docs/perf/shred.md](../perf/shred.md); scaling is measured on cores of one type. Each pull
  request's instructions and allocations for shredding and normalizing, beside the engine's other
  hot paths, are counted against the base it merges into, built in the same CI job
  ([docs/perf/instructions.md](../perf/instructions.md)).
- **`Emitter::rows` pushes JSON lines, written by `serde_json`.** Each row is one line, so a push
  of rows is cut at its line ends like any JSON lines, never walked as an array. A raw value is
  written as it was given, and a line end in it, which valid JSON holds only between tokens, is
  written as a space; `serde_json` escapes every line end inside a string. `rows()` refuses a row
  that is not an object, `json_not_object`, before it pushes anything, since the engine reads a
  push that opens with an array as that array's elements. `serde_json` writes the rows, since
  sonic-rs writes `serde_json`'s private tokens (raw values, arbitrary-precision numbers) out as
  objects, so rows holding them would be pushed as different JSON.
- **The simulation pushes JSON.** Streams drawn as JSON push their rows as JSON lines or arrays,
  and the oracle, which compares values, holds them to the same model as Arrow.

## Consequences

JSON pushes load in parallel; [docs/perf/shred.md](../perf/shred.md) records their throughput
against the old engine on one core. Integers load exactly at any width, as decimals within 76
digits and as JSON text beyond. A chunk whose shape drifts to objects, arrays or floats under a
column of JSON is parsed twice, so that drift costs throughput where it happens and nowhere
else; drift between scalars costs no second parse. The engine's integration tests run compute
jobs inline, since the paused test clock would otherwise advance while a job runs on another
thread.
