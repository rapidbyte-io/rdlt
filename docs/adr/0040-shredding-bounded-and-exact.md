# ADR 0040: Shredding that is paid for before it is built, and JSON read exactly

Status: accepted, 2026-10-03.

## Context

ADR 0037 has the engine survive any connector: what a connector sends is charged to the budget
before it is held, or bounded by a limit with a typed refusal. JSON pushes, the JSON text of
Arrow columns and the rows normalizing makes of both escape that unless each is bounded where it
is built and read as it was written:

- A shredder that builds a chunk's columns before counting them, and bounds cells only once the
  chunks are joined, lets empty records before one wide record take about 80 KB a row, so a push
  of a few hundred kilobytes ends the process. Unless a list's items count as rows of their
  level and the column limit holds a record rather than each object, a megabyte of records of
  one key each, three hundred keys in turn, becomes 241 MB under a budget that reserves 10.5 MB
  for it.
- Row identity that walks JSON text recursively and reads its numbers as 64-bit floats
  overflows the stack on text nested deep enough, gives distinct integers beyond 2⁵³ one root
  id, and hashes text that is not JSON as a string. A merge key stored as JSON on a normalized
  stream would merge its rows by text while its child rows follow an id that reads the values.
- An Arrow push's columns of JSON spliced into lowered rows unchecked let a source write text
  that is not JSON into a destination's column of JSON.
- A column of JSON whose floats are rendered through `f64` stores `0.123456789012345678` as
  `0.12345678901234568`.
- Making a flush's whole column JSON text for one value of another kind sends every value of it
  to the variant or the schema policy: one string among a thousand integers discards the
  thousand.
- A cost model that charges nothing for a null converted to a type of no fixed width, inside
  structs and lists, lets a batch hold what it never reserved, and a piece or row bounded by the
  budget alone can pass what a text array's 32-bit offsets reach.
- An array under a key no table can be named after needs a rule of its own, and an error that
  quotes a record's keys and numbers whole quotes whatever a connector sent.

## Decision

- **The shredder is metered.** It keeps its one pass (ADR 0007), and every builder it makes is paid
  for first. A JSON push is admitted for three times its text and a message's room (ADR 0039): the
  text and the message it arrives in, and twice the text for its batches. Each chunk may build
  within twice its own text: its builders charge a meter their presized capacity as they are made
  and what they grow by as they grow (values, offsets, validity bits, text and list items), and so
  does what a value of JSON is rendered into before its column takes it (its text, a 16-byte range a
  key of the objects open in it and an 8-byte mark an object), and each column the fixed parts it
  takes a chunk, whatever its rows: its entry in the chunk's record (368 bytes and its name, and in
  a shape the mark of the object that last named it, enough for the vectors holding the entries just
  as they double, when they hold the old buffer and the new), and its builder (256 bytes, a struct's
  1,536), its buffers' rounding included. A chunk that would pass its allowance stops building and
  is read again observing: kinds, counts of rows, items and text, no cells, nothing that grows with
  its rows. An observation charges each column's entry in its shape the same way, and an object its
  own shape (384 bytes). A chunk smaller than its records' columns, the last of a flush, may observe
  them past its allowance: what a flush's observations hold so is limited to one shape of every
  column a schema may hold, objects all (752 bytes a column, 5.6 MB at the default budget), reserved
  before the pushes are observed and refused past it, `limit_exceeded`. What building then takes is
  reserved without a wait while that is held, from it where it is enough; where it must wait, the
  observation and its reservation are let go first, so the partition waits holding nothing but its
  pushes, and the pushes are observed again once it is reserved.
- **What building takes beyond the admission is reserved before it is built.** Once every chunk is
  parsed or observed and the shapes joined, the bytes each chunk's batch takes against the joined
  shape are reckoned from the counts, with its columns' fixed parts (640 bytes an array, a struct's
  1,536) and what its parse holds until then. A chunk that fits keeps its columns, and renders its
  scalars in a column the join made `Json` as JSON text, reckoned at most the chunk's own text:
  compact JSON text is never longer than the valid JSON it was read from. One built again is built
  presized, beside what rendering its values of JSON holds: twice what its longest record's text, a
  16-byte range for every four bytes of it and an 8-byte mark for each level an object may nest to
  take. What all of it and the joined shape take beyond the pushes' admission, reckoned over the
  flush, is reserved from the data share (`acquire_working`) before any of it is built and held with
  the batches, which are freed before it is released; more than one request may take is refused,
  `json_exceeds_budget` (`ErrorKind::Source`). Shredding is two steps, `observe` and `build`, and
  the partition reserves between them.
- **Columns and cells are counted where they are built.** The shredder's column limit is the
  engine's derived `schema_columns` (6,241 at the default budget, 893 at the least), counted as
  the emitter counts a schema: every field at any depth, and every list's items. It holds while a
  chunk is parsed, and again for the joined shape. Cells are counted a level at a time, the rows
  of the level times its leaves, a list's items being the rows of its items' level, and bounded
  by `MAX_CELLS` (32 Mi) before anything is built. Both refuse with `limit_exceeded`.
- **JSON text is read by one reader.** The engine's `json` module reads JSON text as tokens,
  iteratively, without recursion and without building a document; nesting past
  `NESTING_DEPTH` is refused, `limit_exceeded`. Numbers stay text. Row identity reads JSON
  text through it: text that is not JSON fails the write, `json_invalid` (`ErrorKind::Source`),
  never hashed as a string. The shredder's exact parse walks a record through it a token at a
  time, into what the record builds, as the fast parse streams one: neither holds a document
  of the record, and each refuses what a record holds in the order it holds it.
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
  without its text is an internal error, never a rounding. A number with a fraction or exponent
  is typed a 64-bit float.
- **Every value of an Arrow push's columns of JSON is checked before it is used.** On the compute
  pool, at any depth and in every encoding (dictionaries, runs, views, structs, lists, list views
  and maps), only the values rows name, each once: text that is not JSON fails the write,
  `json_invalid`, and nesting past the limit `limit_exceeded`. Which rows are named is read as
  ranges, each level mapping its parent's (a list's rows to its items by their offsets, a run's rows
  to its values, a struct's through its nulls), so nothing is held a row or an item. Only a
  dictionary's named values (a bit a value, or the keys named where that is less) and the spans of a
  list view naming its items out of order are held, and what they take is reserved before the check
  runs. A push whose schema holds no column of JSON at any depth is not sent to the pool. Lowering
  then only splices checked text. An object repeating a key is valid JSON there, and kept; only
  records of JSON pushes refuse one.
- **A merge key stored as JSON on a normalized stream is refused** when its table is created,
  `merge_key_json` (`ErrorKind::Schema`). Its rows would merge by the text the destination stores,
  where `1` and `1.0` differ, while their child rows follow the root id the values give, where
  they are one value. A stream that does not normalize keeps merging by the text. A key that
  normalizing takes apart, an object it flattens into columns or an array it moves to a table of
  its own, is refused as the batch is normalized, `merge_key_nested` (`ErrorKind::Schema`): the
  table would have no column for it. A key normalizing keeps whole (named whole, deeper than
  `max_depth`, or held in a dictionary or runs) stays a column and is not refused.
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
  values are read as a shredding job is, sure of its stack. Each value is observed once to fit it,
  a token at a time with no document built, as the column's type guides: a part the type holds as
  JSON is observed as JSON with nothing below it, and a value no further than a key its struct
  lacks, or an object or array where its type holds neither, since it then fits no column. An
  object repeating a key is refused as the value is observed, and in a part held as JSON by a
  check that notes where each key of the objects open starts in the text, four bytes a key, and
  reads the keys again to compare them. A part held as JSON is read as the shredder reads a column
  of JSON: its text as written, an object with its own keys and a number with its digits. Reading
  walks each value a token at a time too, with no document built, and renders a part held as JSON
  noting no key, since fitting checked them, into room taken once for the longest value, which no
  part renders longer than: reading holds no more than its piece reserves. The join of the values
  that fit is kept to read them; where the policy drops rows, the kept values are observed again
  and joined alone, since a dropped value can change the type the others are read into. Fitting a
  value holds no more than twice its text and a hundred bytes for its nesting, beside an entry for
  each field of the column's type the value names, no more than reading the value builds for that
  field. The cost model charges what reading takes: two null slots of the column's type a row, its
  null text where stored as text, and for each byte of text one more and two slots of the widest
  list item the type holds.
- **The cost model charges a null its target's width.** A column typed null converted to a type
  of no fixed width becomes nulls of that type, which take its null slot; inside structs and list
  items too, and an absent struct field its slot, its key and its null text.
- **A piece and a row are bounded by what offsets reach.** Lowering one piece, or one row, takes
  at most `MAX_PIECE_BYTES`, `i32::MAX`, whatever the budget, so no text array's 32-bit offsets
  overflow. A row that alone takes more is `row_exceeds_budget`.
- **An array no child table can be named after follows the stream's policy.** Its path is not a
  valid table path: the discard policies drop it, the others refuse, `table_path_invalid`
  (`ErrorKind::Schema`), on a first load too. No escaped or hashed name is made up for it.
- **A normalized column is made by values.** A column of a child table, or of the stream's own
  table where the flush was shredded from JSON, is made only where a row its piece keeps, after
  dropped rows and their descendants go, holds a value in it: an Arrow child column holding only
  nulls makes no column and brings no type, as a JSON field no row holds makes none. The stream's
  own table of an Arrow push keeps every column its batch gives it, nulls typed included, in a
  piece any of whose rows it keeps, as a stream that does not normalize does. Whether a table is
  being created is judged once a flush, so a table the flush creates takes every column of every
  chunk and piece of it (ADR 0010). So the cut into chunks and pieces within a flush decides no
  column's existence or type, and how rows are grouped into flushes decides nothing for a child
  or JSON-root column beyond which flush first holds a value in it, which then meets the
  column's policy: a column holding values only in dropped rows is no column.
- **Errors quote a record's content cut short.** A key or a number an error quotes is shown to
  `QUOTED_BYTES`, 128, as other text a connector sent is.
- **A record the shredder refuses fails the run.** A JSON push holding a record that is not JSON
  (`json_invalid`), is not an object (`json_not_object`), repeats a key (`json_duplicate_key`) or
  passes one of the shredder's limits (`limit_exceeded`) fails the run with an `ErrorKind::Source`
  error naming its stream and coded for the refusal, which is not retried; `json_invalid` also
  numbers the record, from one, among the records shredded with it. No schema policy applies to
  such a record. Every later run resumes before it and fails at it again, so the only remedy is
  to fix or remove the record at the source. A source that cannot change its records, such as a
  topic, offers none until the record leaves its retention, when `RetentionLoss::Reset` reads on
  from the oldest record kept. A value its column cannot hold is the schema policy's: under
  `evolve` it fails its batch, and `discard_row` or `discard_value` on that column drop it and
  count it.
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
  - A table of the records the shredder refuses, chosen by a stream setting and published in the
    commit of the segment each came from, until a production source that pushes untrusted JSON,
    such as a webhook, a third-party API or a topic, is named. The first one brings it, which
    removes the outage one refused record causes.
  - A dead-letter queue outside the destination: it is a second destination, which needs
    exactly-once of its own, where a table published in the same commit needs none.

## Consequences

- No JSON push builds or observes what it was not admitted or reserved for, and no check of an
  Arrow column of JSON holds what was not, so ADR 0039's bound holds for JSON without exception;
  a JSON flush whose batches take more than a quarter of the budget beyond twice its text is
  refused. Thirteen records of 6,232 keys, each in a chunk of 2 MiB, are refused so; four load.
- A chunk built again with a column of JSON reckons about ten times its longest record beside
  its batch, what rendering a value of objects with one-byte keys could hold. A flush of
  records of megabytes whose column drifts to JSON takes that much more of a request, and may
  be refused, `json_exceeds_budget`.
- A struct's field of JSON in a split column stores its text as written, as a column of JSON
  does: an object keeps its own keys, without nulls for the fields other values gave it, and a
  number its digits, `1.50` as `1.50`.
- Every JSON flush reserves the room its observations may hold beyond its text while it is
  observed, 5.6 MB at the default budget.
- Narrow records that widen a column past a chunk's allowance are parsed twice more; dense data
  parses once. [docs/perf/shred.md](../perf/shred.md) records the meter's share of a profile and
  the shredder's throughput against its bounds.
- A run's report counts each stream's passes over the chunks of its committed flushes: those
  parsed, those whose meter tripped, those only observed, those built again, and those that read
  numbers exactly.
- `1.50` and `1.5` stay distinct texts in a destination's column of JSON, one value to identity.
- Under `evolve`, a struct column gains a field only from a flush whose column is not JSON text.
