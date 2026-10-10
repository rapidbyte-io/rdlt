# ADR 0040: Shredding that is paid for before it is built, and JSON read exactly

Status: accepted, 2026-10-03.

## Context

ADR 0037 has the engine survive any connector: what a connector sends is charged to the budget
before it is held, or bounded by a limit with a typed refusal. JSON pushes, the JSON text of
Arrow columns and the rows normalizing makes of both escaped that in several ways:

- The shredder built a chunk's columns before it counted them. Its only bound on cells was
  checked once the chunks were joined, after every chunk had built its columns: empty records
  before one wide record took about 80 KB a row, and a push of a few hundred kilobytes ended the
  process.
  A list's items were not counted as rows of their level, its column limit held each object and
  not a record, and a megabyte of records of one key each, three hundred keys in turn, became
  241 MB under a budget that reserved 10.5 MB for it (ADR 0039 named that gap).
- Row identity walked JSON text recursively and read its numbers as 64-bit floats: text nested
  deep enough overflowed the stack, distinct integers beyond 2⁵³ shared one root id, and text that
  was not JSON was hashed as a string. A merge key stored as JSON on a normalized stream merged
  its rows by text while its child rows followed an id that read the values.
- An Arrow push's columns of JSON were spliced into lowered rows unchecked, so a source could
  write text that was not JSON into a destination's column of JSON.
- A column of JSON built from pushes rendered its floats through `f64`, so `0.123456789012345678`
  was stored as `0.12345678901234568`.
- One value of another kind made a flush's whole column JSON text, so every value of it took the
  variant or the schema policy: one string among a thousand integers discarded the thousand.
- The cost model charged a null converted to a type of no fixed width nothing, inside structs
  and lists, and a piece or row was bounded by the budget alone, past what a text array's 32-bit
  offsets reach.
- An array under a key no table can be named after failed internally, and errors quoted a
  record's keys and numbers whole.

## Decision

- **The shredder is metered.** It keeps its one pass (ADR 0007), and every builder it makes is
  paid for first. A JSON push is admitted for three times its text (ADR 0039): the text, and twice
  it for its batches. Each chunk may build within twice its own text: its builders charge a meter
  their presized capacity as they are made and what they grow by as they grow (values, offsets,
  validity bits, text and list items), and each column the fixed parts it takes a chunk, whatever
  its rows: its entry in the chunk's record (368 bytes and its name, and in a shape the mark of the
  object that last named it, enough for the vectors holding the entries just as they double, when
  they hold the old buffer and the new), and its builder (256 bytes, a struct's 1,536), its buffers'
  rounding included. A chunk that would pass its allowance stops building and is read again
  observing: kinds, counts of rows, items and text, no cells, nothing that grows with its rows. An
  observation charges each column's entry in its shape the same way, and an object its own shape
  (384 bytes). A chunk smaller than its records' columns, the last of a flush, may observe them past
  its allowance: what a flush's observations hold so is limited to one shape of every column a
  schema may hold, objects all (752 bytes a column, 5.6 MB at the default budget), reserved before
  the pushes are observed and refused past it, `limit_exceeded`.
  What building then takes is reserved without a wait while that is held, from it where it is
  enough; where it must wait, the observation and its reservation are let go first, so the
  partition waits holding nothing but its pushes, and the pushes are observed again once it is
  reserved.
- **What building takes beyond the admission is reserved before it is built.** Once every chunk
  is parsed or observed and the shapes joined, the bytes each chunk's batch takes against the
  joined shape are reckoned from the counts, with its columns' fixed parts (640 bytes an array, a
  struct's 1,536) and what its parse holds until then. A chunk that fits keeps its columns; one
  built again is built presized. What all of it and the joined shape take beyond the pushes'
  admission, reckoned over the flush, is reserved from the data
  share (`acquire_working`) before any of it is built and held with the batches; more than one
  request may take is refused, `json_exceeds_budget` (`ErrorKind::Source`). `shred` is now
  `observe` and `build`, and the partition reserves between them.
- **Columns and cells are counted where they are built.** The shredder's column limit is the
  engine's derived `schema_columns` (7,489 at the default budget, 943 at the least), counted as
  the emitter counts a schema: every field at any depth, and every list's items. It holds while a
  chunk is parsed, and again for the joined shape. Cells are counted a level at a time, the rows
  of the level times its leaves, a list's items being the rows of its items' level, and bounded
  by `MAX_CELLS` (32 Mi) before anything is built. Both refuse with `limit_exceeded`.
- **JSON text is read by one reader.** The engine's `json` module reads JSON text as tokens,
  iteratively, without recursion and without building a document; nesting past
  `NESTING_DEPTH` is refused, `limit_exceeded`. Numbers stay text. Row identity reads JSON
  text through it: text that is not JSON fails the write, `json_invalid` (`ErrorKind::Source`),
  never hashed as a string.
- **Numbers are hashed by their value.** A number's canonical text is its exact value in plain
  notation (`-12.5`, `0.001`, `1000`) while that is at most 400 bytes, which every value a 64-bit
  float or a decimal holds is, and in scientific notation beyond. It is a function of the value,
  so distinct values never share it, and no longer than the text it came from plus its exponent.
  A number whose exponent has more than 18 significant digits, whose place no 64-bit integer
  holds, is refused, `limit_exceeded`, as its push arrives, whatever the stream does with it: the
  check of an Arrow column of JSON refuses it, and so does the shredder's exact parse. The fast
  parse reads such a number as zero, so a record where it read a float of zero and whose text
  holds a digit, an `e` and more than 18 digits is parsed exactly. A float is hashed by the canonical text of its shortest
  JSON text, a tie going to the even digit as JSON writers break it (`ryu`), so a float and the
  JSON text a writer makes of it hash alike.
- **A JSON column's numbers are kept as written.** A chunk whose column of JSON holds a float is
  built by the exact parse, which renders each number as its text was written; rendering a float
  without its text is an internal error, never a rounding. Columns of floats keep spec §8.2's
  typing: a number with a fraction or exponent is a 64-bit float.
- **Every value of an Arrow push's columns of JSON is checked before it is used.** On the
  compute pool, at any depth and in every encoding (dictionaries, runs, views, structs, lists,
  list views and maps), only the values rows name, each once: text that is not JSON fails the
  write, `json_invalid`, and nesting past the limit `limit_exceeded`. Which rows are named is
  read as ranges, each level mapping its parent's (a list's rows to its items by their offsets, a
  run's rows to its values, a struct's through its nulls), so nothing is held a row or an item.
  Only a dictionary's named values (a bit a value, or the keys named where that is less) and the
  spans of a list view naming its items out of order are held, and what they take is reserved
  before the check runs. Lowering then only splices checked text. An object repeating a key is valid JSON there, and kept; only records of JSON pushes
  refuse one.
- **A merge key stored as JSON on a normalized stream is refused** when its table is created,
  `merge_key_json` (`ErrorKind::Schema`). Its rows would merge by the text the destination stores,
  where `1` and `1.0` differ, while their child rows follow the root id the values give, where
  they are one value. A stream that does not normalize keeps merging by the text. A key whose
  values are objects or arrays, which normalizing flattens into columns or moves to a table of
  its own, is refused as the batch is normalized, `merge_key_nested` (`ErrorKind::Schema`): the
  table would have no column for it.
- **A column of JSON is split value by value.** A column arriving as JSON text whose own column
  is of another type sends there each value that is a value of the column's type, read as the
  shredder reads that value alone, and converted: a null, of any column; an object whose fields
  the struct holds by name, in any order, those it lacks being null, which a field that may not
  be null may not be; an array whose items the list holds, which may be null; an integer of at
  most 2⁵³ in magnitude, in a column of 64-bit floats at any depth; and any other value whose
  type joins into the column's. Only the others take the variant, or the discard, the schema
  policy names. A value that would widen the column, an object with a field the struct lacks,
  goes with the others: the column's type is fixed once planned. An object repeating a key keeps
  its text, and so does a value the shredder would refuse alone, a number beyond a float's
  range. A frozen table still refuses, and a merge key still refuses to change. Row identity
  reads the batch as it arrived, and history hashes the columns as the table stores them. The
  values are read as a shredding job is, sure of its stack. A column of integers that JSON text is read
  into is no longer recorded exact (ADR 0026): its integers are read only as the plan lowers
  them. The cost model charges what reading takes: two null slots of the column's type a row,
  its null text where stored as text, and for each byte of text one more and two slots of the
  widest list item the type holds.
- **The cost model charges a null its target's width.** A column typed null converted to a type
  of no fixed width becomes nulls of that type, which take its null slot; inside structs and list
  items too, and an absent struct field its slot, its key and its null text.
- **A piece and a row are bounded by what offsets reach.** Lowering one piece, or one row, takes
  at most `MAX_PIECE_BYTES`, `i32::MAX`, whatever the budget, so no text array's 32-bit offsets
  overflow. A row that alone takes more is `row_exceeds_budget`.
- **An array no child table can be named after follows the stream's policy.** Its path is not a
  valid table path: the discard policies drop it, the others refuse, `table_path_invalid`
  (`ErrorKind::Schema`), on a first load too. No escaped or hashed name is made up for it.
- **Errors quote a record's content cut short.** A key or a number an error quotes is shown to
  `QUOTED_BYTES`, 128, as other text a connector sent is.
- Rejected:
  - Two passes for every chunk (observe, then build): the throughput gate of ADR 0007 needs the
    single pass, and only chunks that pass their allowance pay a second.
  - A second byte limit inside the shredder: the budget's request bound already says how much one
    write may take.
  - Canonicalising the numbers a column of JSON stores: the destination keeps what the source
    wrote; only identity reads values.
  - Moving text arrays to 64-bit offsets: destinations and the wire take 32-bit offsets, and a
    single value rendering beyond 2 GiB is not data a pipeline loads.
  - Naming an unusable key's child table by escaping or hashing it: any mapping either aliases a
    real key or renames tables that exist.

## Consequences

- No JSON push builds or observes what it was not admitted or reserved for, and no check of an
  Arrow column of JSON holds what was not, so ADR 0039's bound holds for JSON without exception;
  a JSON flush whose batches take more than a quarter of the budget beyond twice its text is
  refused. Thirteen records of 7,480 keys, each in a chunk of 2 MiB, are refused so; four load.
- Every JSON flush reserves the room its observations may hold beyond its text while it is
  observed, 5.6 MB at the default budget.
- JSON records that held up to 10,000 columns in an object are refused past the derived limit.
- Narrow records that widen a column past a chunk's allowance are parsed twice more; dense data
  still parses once. [docs/perf/shred.md](../perf/shred.md) records the meter's share of a
  profile and the shredder's throughput against the spec's bound.
- A run's report counts each stream's passes over the chunks of its committed flushes: those
  parsed, those whose meter tripped, those only observed, those built again, and those that read
  numbers exactly.
- Row ids of floats whose shortest text is a tie, and of JSON numbers a float rounded, differ from
  earlier runs.
- `1.50` and `1.5` stay distinct texts in a destination's column of JSON, one value to identity.
- Under `evolve`, a struct column gains a field only from a flush whose column is not JSON text.
