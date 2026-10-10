# ADR 0026: JSON exactness

Status: accepted, 2026-09-29.

## Context

ADR 0025 split JSON exactness from H1b. Each of its findings needs values, not only types, to be
judged:
- A column of integers that later meets floats becomes JSON text, since the lattice joins `Int64`
  and a float as `Json` (spec §8.2): a 64-bit float rounds integers beyond 2⁵³. A column whose
  integers are all within 2⁵³ loses nothing to floats, yet takes them as text.
- An integer beyond 38 digits fails its push as `value_unrepresentable`, even in a column already
  of JSON text, which holds it.
- A merge key decimal the destination stores as text is refused a wider precision at the same
  scale, though its values render alike and keep matching.
- The simulation never pushes JSON integers beyond 64 bits, and the test kit writes a scaled
  decimal's JSON text without its point.

Drafting this found the exact parse reading a record of invalid UTF-8 unchecked, which aborted the
process; H1b fixed it (ADR 0025).

## Decision

- **JSON integers load at every width.**
  - An integer is read as the narrowest of `Int64`, `Decimal(20, 0)` (an unsigned 64-bit one),
    `Decimal(38, 0)` and `Decimal(76, 0)` that holds it, and one beyond 76 digits makes its column
    `Json`, which keeps its digits. No integer fails a push any more.
  - The fast parse refuses an integer beyond a float's range as it refuses invalid JSON, so any
    refusal sends the chunk through the exact parse, which tells them apart. The exact parse hands
    an integer beyond 38 digits to the shredder as its digits, through the one visit JSON never
    makes otherwise, `visit_bytes`.
  - The exact parse builds a record whole, so it first scans the record's brackets, outside its
    strings, and refuses one nested past the limit as `limit_exceeded`: a record the fast parse
    refused or found imprecise can no longer exhaust the stack.
- **A column of 64-bit integers is exact while a 64-bit float holds every value it stored.**
  - Exactness is judged from values, whatever the source: a batch's column of 64-bit integers
    rounds when a value its rows hold is beyond 2⁵³ either way. A column is read where it lies,
    in any encoding, a dictionary's values through the keys its rows hold and a run's through
    the runs they fall in, so only values rows hold count and nothing is decoded. A declared
    schema holds no values, so it creates exact columns.
  - The batch judged is a partition's flush, every table's rows of it judged together, before
    its policy drops any. A flush is shredded in chunks and lowered in slices whose sizes fall
    where they may, so where a flush is cut decides no column's type. A normalized flush is
    judged where its batches hold its rows, each column walked as normalizing places it, before
    any of it is split.
  - The table's model keeps its exact columns. A column added from a batch that rounds is not
    exact, and one that later takes such a batch stops being exact; nothing makes it exact again.
  - State records them with the schema, as `exact`; a record without them holds none, so a table
    created before this keeps taking floats as JSON text. Amended 2026-10-03 (ADR 0041): the
    record always holds them, and a record of the earlier format is refused. Losing exactness
    changes the model but not the table, so it advances the model's revision, which state
    compares, and not the schema's version, which destinations see. It is decided under the
    table's lock, before any of the rounding batch is written, and each commit records the
    table's current model, so no commit records a column exact after a rounding batch was
    planned.
  - A column of 64-bit floats takes a batch of exact integers cast, as it takes narrower floats.
    The conversion checks again, and refuses a batch whose integers a float would round as
    `value_unrepresentable`, so a plan used on other batches than it judged never rounds.
    So a declared column of 64-bit integers hinted as floats is no longer refused as a run plans:
    its batches are judged by their values, and one that rounds is incompatible with the hint.
  - Floats arriving at an exact column join its integers as 64-bit floats: they take a variant of
    floats, `__float64`, rather than JSON text. Such a join never widens the column in place,
    even where the destination declares it could: a partition's plan made before may still write
    the column integers. Only the lattice's joins widen a column.
  - Values go first to the column's own, as they are or cast; then to a variant that holds them as
    they are. A variant never takes a cast.
  - Amended 2026-10-03 (ADR 0040): a column arriving as JSON text whose own column is of another
    type sends there each value that column holds alone, and a column of integers it is read
    into stops being exact, since its integers are read only as the plan lowers them.
  - This amends spec §8.2: `Int64 ∨ FloatM` is `Json` in the lattice, and `Float64` in a variant
    for a column whose values a float holds exactly.
- **A merge key keeps matching where its values render alike.** A key widens where the destination
  stores both types by value, as before, or renders both into one type alike: integers and
  decimals of one scale render the same digits. A scale that grows still changes the key's text
  (`1.50` to `1.5000`) and is refused as `merge_key_changed`.
- **A column of JSON keeps its numbers as written** (amended 2026-10-03, ADR 0040): a chunk whose
  column of JSON holds a float is built by the exact parse, which renders each number as its
  text was written, never through a 64-bit float. Row identity hashes JSON numbers by their exact
  value.
- **The simulation models values.**
  - An arrival of 64-bit integers is exact where a float holds each of them, over the batch and
    over the batches the engine may gather with it, for JSON and Arrow alike. Exact integers join
    floats as floats, as the shredder joins them.
  - Each column's own column carries its exactness through every order of a phase's batches.
  - Draws of 64-bit integers mix values within 2⁵³, beyond it and at its edges; JSON pushes carry
    whole numbers of every width, past 76 digits too.
  - The model keeps a pushed value as the value drawn and reads it as its JSON text, so integers
    beyond 64 bits are checked exactly; the test kit writes a decimal's text with its scale and no
    leading zeros.
  - Its key model applies the same rule for keys rendered alike.
- Rejected:
  - Widening a column of exact integers to floats in place where the destination declares it:
    it races partitions still writing integers under an older plan, and no destination declares
    it.
  - Tracking exactness of integers nested in objects and lists stored whole: such values join as
    the lattice says, as before.
  - Refusing, rather than keeping as JSON text, integers beyond 76 digits: text holds them
    exactly, which is what a column of JSON is for.

## Consequences

- Tables take floats after exact integers in a `__float64` column. Amended 2026-10-03 (ADR 0041):
  state of the earlier format is refused, so no table takes them as JSON text for want of
  recorded exact columns.
- A push holding an integer beyond 38 digits loads instead of failing.
- State records always carry exact columns, none where a table has none; older engines' records
  are refused (ADR 0041).
- Spec §8.2's lattice table and JSON value inference are amended by this ADR.
