# ADR 0026: JSON exactness

Status: accepted, 2026-09-29.

## Context

Some JSON values are judged exactly only by value, not by type:
- The lattice joins `Int64` and a float as `Json`, since a 64-bit float rounds integers beyond
  2⁵³. Judged by type alone, a column of integers that meets floats sends them to JSON text,
  though the floats lose nothing as floats.
- An integer beyond 38 digits fits no 128-bit decimal, though a column of JSON text holds it.
- A merge key decimal the destination stores as text renders alike at a wider precision of the
  same scale, so its values keep matching.
- The simulation must push JSON integers of every width, and the test kit write a scaled
  decimal's JSON text with its point.

## Decision

- **JSON integers load at every width.**
  - An integer is read as the narrowest of `Int64`, `Decimal(20, 0)` (an unsigned 64-bit one),
    `Decimal(38, 0)` and `Decimal(76, 0)` that holds it, and one beyond 76 digits makes its column
    `Json`, which keeps its digits. No integer fails a push.
  - The fast parse refuses an integer beyond a float's range as it refuses invalid JSON, so any
    refusal sends the chunk through the exact parse, which tells them apart. The exact parse hands
    an integer beyond 38 digits to the shredder as its digits, through the one visit JSON never
    makes otherwise, `visit_bytes`.
  - The exact parse walks a record a token at a time (ADR 0040), and its reader refuses a
    container opened past the nesting limit as `limit_exceeded`: a record the fast parse refused
    or found imprecise cannot exhaust the stack.
- **Floats meeting 64-bit integers take a variant of floats; integers meeting floats are cast
  only where a float holds each.**
  - A batch's column of 64-bit integers is judged from its values, whatever the source: it
    rounds when a value its rows hold is beyond 2⁵³ either way. A column is read where it lies,
    in any encoding, a dictionary's values through the keys its rows hold and a run's through
    the runs they fall in, so only values rows hold count and nothing is decoded. A declared
    schema holds no values, so it rounds nothing.
  - The batch judged is a partition's flush, every table's rows of it judged together, before
    its policy drops any. A flush is shredded in chunks and lowered in slices whose sizes fall
    where they may, so where a flush is cut decides no column's type. A normalized flush is
    judged where its batches hold its rows, each column walked as normalizing places it, before
    any of it is split.
  - A column of 64-bit floats takes a batch of 64-bit integers cast where none rounds, as it
    takes narrower floats. The conversion checks again, and refuses a batch whose integers a
    float would round as `value_unrepresentable`, so a plan used on other batches than it judged
    never rounds. A declared column of 64-bit integers hinted as floats is not refused as a run
    plans: its batches are judged by their values, and one that rounds is incompatible with the
    hint.
  - Floats arriving at a column of 64-bit integers join its integers as 64-bit floats, whatever
    integers it stored: they take a variant of floats, `__float64`, rather than JSON text. The
    variant holds only the floats, which are exact as floats, so nothing about the column's own
    values is kept to choose it. Such a join never widens the column in place, even where the
    destination declares it could: a float rounds integers beyond 2⁵³. Only the lattice's joins
    widen a column. A column that already has a variant holding floats, `__json` among them,
    keeps sending them there.
  - Values go first to the column's own, as they are or cast; then to a variant that holds them as
    they are. A variant never takes a cast.
  - A column arriving as JSON text whose own column is of another type sends there each value
    that column holds alone (ADR 0040).
  - The lattice joins `Int64` and a float as `Json`; a column of 64-bit integers picks its
    variant for floats as `Float64`. An integer is inferred as the narrowest type that holds it,
    as above.
- **A merge key keeps matching where its values render alike.** A key widens where the destination
  stores both types by value, or renders both into one type alike: integers and
  decimals of one scale render the same digits. A scale that grows still changes the key's text
  (`1.50` to `1.5000`) and is refused as `merge_key_changed`.
- **A column of JSON keeps its numbers as written**: a chunk whose column of JSON holds a float
  is built by the exact parse, which renders each number as its text was written, never through
  a 64-bit float. Row identity hashes JSON numbers by their exact value (ADR 0040).
- **The simulation models values.**
  - An arrival of 64-bit integers is exact where a float holds each of them, over the batch and
    over the batches the engine may gather with it, for JSON and Arrow alike. Exact integers join
    floats as floats, as the shredder joins them, and a column of floats takes them.
  - Each column's own column is followed through every order of a phase's batches: floats
    meeting one of 64-bit integers take a variant, whatever integers it holds.
  - Draws of 64-bit integers mix values within 2⁵³, beyond it and at its edges; JSON pushes carry
    whole numbers of every width, past 76 digits too.
  - The model keeps a pushed value as the value drawn and reads it as its JSON text, so integers
    beyond 64 bits are checked exactly; the test kit writes a decimal's text with its scale and no
    leading zeros.
  - Its key model applies the same rule for keys rendered alike.
- Rejected:
  - Widening a column of 64-bit integers to floats in place where the destination declares it:
    a float rounds integers beyond 2⁵³, a partition still writing integers under an older plan
    races it, and no destination declares it.
  - Recording which columns of 64-bit integers hold only integers a float holds exactly, to send
    floats beside any others to JSON text: the variant holds only the floats arriving, so the
    column's own integers decide nothing, and the record would need a revision of the model
    beside its schema version.
  - Tracking exactness of integers nested in objects and lists stored whole: such values join as
    the lattice says.
  - Refusing, rather than keeping as JSON text, integers beyond 76 digits: text holds them
    exactly, which is what a column of JSON is for.

## Consequences

- Tables take floats after 64-bit integers in a `__float64` column, whatever integers an
  earlier flush held.
- A push holding an integer beyond 38 digits loads.
