# ADR 0011: Exact lowering and normalizing, across every type and encoding

Status: accepted, 2026-09-25.

## Context

A `LoweringPlan` must store what each value means, exactly as lowering that value on its own
would, for every logical type in every Arrow encoding a source may send, and normalizing must not
depend on the encoding. Tests that draw a sample of types miss whole classes of defect, from
panics to values silently changed, so the tests draw every type and variant.

## Decision

- **The lowering differential checks meaning, not rendering.** Batches of every logical type,
  every encoding (unsigned, half-precision, `Decimal32`–`Decimal256`, large, view, dictionary,
  run-end, fixed-size, `Date64`, list views, maps) and nested two levels deep are lowered into a
  table they evolve, for any destination's native types, nested support and widenings, under
  every policy. Every stored cell is read back — natively, from its text, or from JSON with the
  type it was written from — and must mean exactly the source value: numbers as their shortest
  exact decimal, times in nanoseconds. A test asserts the generator draws every type in every
  encoding. The comparison is meaning-based because text formats are Arrow's contract; values
  are ours.
- **Lowering converts values itself where Arrow's kernels lose or refuse them.**
  - A `Null` column arriving at a `Json` column converts to nulls, which arrow-json panics on.
  - Maps, at any depth, become the list of key and value structs they hold before any cast, which
    a map column otherwise fails.
  - arrow-json encodes a list's items with the list's field, so JSON and UUID items would lose
    their extension types (JSON double-encoded): the engine encodes such lists itself.
  - The engine renders every temporal value itself, as Arrow renders those in range (a zero
    duration as `PT0S`, where Arrow writes `P0D`), exactly, with expanded years and signed clocks,
    and an instant chrono cannot place in its zone in UTC. Arrow fails a batch whose dates or
    timestamps lie beyond the years `chrono` holds, or whose times of day lie outside a day, and
    writes such durations as `<invalid>`.
  - Text loses a named zone's historical offset seconds (Asia/Kolkata before 1941), naming
    another instant, so such instants are rendered in UTC.
  - JSON has no non-finite numbers, and arrow-json writes them as `null`: they are the JSON
    strings `"NaN"`, `"Infinity"` and `"-Infinity"`, which a typed reader parses back.
  - Every null check on incoming arrays uses logical nulls: run-end and dictionary encodings hold
    their nulls in their values, so a physical check would count discards of, and drop under
    `discard_row`, rows that hold nothing, and miss a null merge key.
  - Dates and wall-clock times are placed in their columns' units with checked arithmetic: Arrow
    multiplies dates into timestamps unchecked, wrapping silently in release builds.
  - A widening never consults a zone (below), so no wall-clock timestamp is refused for a time its
    column's named zone skips or repeats, as Arrow's would be.
  - A `Date64` is the day it is within wherever it goes, a struct's and a list's included, and one
    a `Date32` cannot hold is refused; a decimal beyond its declared precision is refused.
  - Times are rescaled by the engine and refused where the finer type cannot hold them: Arrow
    widens times of day outside a day unchecked (`Time64` microseconds to nanoseconds overflows,
    panicking in debug builds).
  - Dictionary and run-end encodings are decoded, keeping only the values rows hold, before any
    conversion: filtering rows out of a dictionary leaves their values in it, and converting a
    dictionary converts every value, so a value only a dropped row held could refuse the batch.
- **Conversions that leave a unit's range are refused.** The lattice joins time units to the
  finer one, which holds every value only within the `i64` of that unit; a value beyond it is
  refused with `value_unrepresentable` rather than wrapped or rounded. The differential predicts
  each refusal independently.
- **Dates and wall-clock times in zoned columns.** A date in a timestamp column of any zone is its
  midnight in UTC, and a wall-clock time widened into a zoned column keeps its count from the
  epoch: a zone shows an instant and never moves it, so a widening changes no value, row id or
  history hash.
- **Normalizing does not depend on encodings.** A property test normalizes every drawn batch as
  drawn and with every encoding plain, and requires the same parts, ids, lineage and values; its
  dates stay within a `Date32`'s days, as a far `Date64` has no other encoding. Identity encodes
  temporal values as their kind and nanoseconds and decimals from their integer value, unwraps
  run-end encodings as it does dictionaries, and splits list views into child tables as it does
  lists, so no id depends on an Arrow type's name, a value's unit or its date type.
- **Correctness.**
  - A stream that appended to a table may merge into it: planning adds the sequence column,
    nullable, as it adds lineage columns.
  - Child parts are pruned of dropped parents' rows before they are planned, so a dropped row
    changes no schema and a frozen stream refuses a new array only if rows holding it remain.
  - SQL child merges qualify the root staging's columns, so a missing column is an error rather
    than the child table's column of that name.
  - A unit's normalized parts are charged to the memory budget before they wait on table changes.
- **Certification.** `D-REPLACE` swaps two tables' generations in one commit; the Vault fault
  `finish_one_generation` fails it alone.
- **Performance.**
  - SQL destinations index a child table of a merge table by its root id each time it merges,
    so one created before its stream merged is indexed too.
  - The files destination rewrites a child table only where its roots' rows drop children.
  - A lowering plan keeps its constant columns for every batch no larger than the largest seen.
  - Text is rendered into one reused buffer, not a string per value: as fast as Arrow's cast for
    decimals and faster for timestamps.
  - A plan made for a view already superseded is used but not cached over the current view's.
- **Dependencies.** The engine uses `chrono`, the version arrow already builds, for zone offsets.

## Consequences

Every value a source may send, in any encoding, lowers into a column that holds exactly it, or the
batch is refused by name; nothing is silently lost, rounded or wrapped. The differentials run
1 024 and 512 cases in CI and hundreds of thousands locally. The simulation draws every type and
encoding, nested arrays among them (ADR 0012), under column-level policies (ADR 0013).
