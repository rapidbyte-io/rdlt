# ADR 0011: Exact lowering and normalizing, across every type and encoding

Status: accepted, 2026-09-25.

## Context

Spec §20.4 compares `LoweringPlan` against direct per-value lowering; M3b deferred it (ADR 0008).
The owner asked, while M3e was drafted, that tests cover every data type and variant rather than a
sample. The first differential, drawing a handful of types, found a panic at once; drawing every
logical type, in every Arrow encoding a source may send, found eight more defects in lowering and
three in normalizing; mutation testing found one more in lowering, and the branch's review four
more. M3e also takes the minors M3c
and M3d deferred.

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
- **Found and fixed in lowering.**
  - A `Null` column arriving at a `Json` column panicked in arrow-json: it converts to nulls.
  - Map columns failed every batch: maps, at any depth, become the list of key and value structs
    they hold before any cast.
  - arrow-json encodes a list's items with the list's field, so JSON and UUID items lost their
    extension types (JSON came out double-encoded): the engine encodes such lists itself.
  - Temporal text beyond the years `chrono` holds failed the batch (dates, timestamps) or was
    written as `<invalid>` (durations), and times of day outside a day failed it too: durations
    are always rendered by the engine, as Arrow renders them in range (`PT1.5S`); other values
    Arrow cannot render are rendered exactly, in UTC, with expanded years and signed clocks.
  - A named zone's historical offset with seconds (Asia/Kolkata before 1941) lost them in text,
    naming another instant: such instants are rendered in UTC.
  - JSON has no non-finite numbers, and arrow-json wrote them as `null`: they are the JSON
    strings `"NaN"`, `"Infinity"` and `"-Infinity"`, which a typed reader parses back.
  - Run-end and dictionary encodings hold their nulls in their values, so discards counted, and
    `discard_row` dropped, rows that held nothing, and a null merge key went unseen: every null
    check on incoming arrays uses logical nulls.
  - Arrow multiplies dates into timestamps unchecked, wrapping silently in release builds: dates
    and wall-clock times are placed in their columns' units and zones with checked arithmetic.
  - Arrow refuses a wall-clock timestamp that its column's named zone skips or repeats, failing
    the batch: such times take the offset before the shift, or the earlier instant, as dates do.
  - The first fix read a skipped time as UTC to find its offset, which moved it backward in zones
    east of UTC (a date in Tehran landed on the day before): the offset is the one in force a day
    earlier, and the reference finds the shift by bisection instead.
  - A `Date64` beyond a `Date32`'s days became null in a timestamp column and was refused on its
    way to JSON or text: dates go to timestamps from their own days, and to JSON, and within
    structs and lists, as `Date64`s. A `Date` column still refuses them.
  - Arrow renders a `Date64` as a date and a time: it is rendered as a date.
  - Arrow widens times of day outside a day unchecked (`Time64` microseconds to nanoseconds
    overflowed, panicking in debug builds): times are rescaled here and refused where the finer
    type cannot hold them.
  - Filtering rows out of a dictionary leaves their values in it, and converting a dictionary
    converts every value, so a value only a dropped row held could refuse the batch: dictionary
    and run-end encodings are decoded, keeping only the values rows hold, before any conversion.
- **Conversions that leave a unit's range are refused.** The lattice joins time units to the
  finer one, which holds every value only within the `i64` of that unit; a value beyond it is
  refused with `value_unrepresentable` rather than wrapped or rounded. The differential predicts
  each refusal independently.
- **Dates and wall-clock times in zoned columns.** A date in a zoned timestamp column is its
  midnight there, as Arrow converts it, and a wall-clock time is that time there. Where a named
  zone's clocks show the time twice, the earlier instant; where they skip it, the offset in force
  before they did, so the time moves forward by the gap, as PostgreSQL and `java.time` move it.
  Beyond the years a named zone's offsets are known for, the value is refused; fixed offsets hold
  everywhere.
- **Normalizing does not depend on encodings.** A property test normalizes every drawn batch as
  drawn and with every encoding plain, and requires the same parts, ids, lineage and values; its
  dates stay within a `Date32`'s days, as a far `Date64` has no other encoding. It
  found that run-end values and list views were hashed by their Arrow type's name, list views
  were never split into child tables, and dates, times, timestamps and durations were hashed by
  their unit and date type. Identity now encodes temporal values as their kind and nanoseconds and
  decimals from their integer value, and unwraps run-end encodings as it does dictionaries. Ids
  of rows keyed by temporal values therefore differ from those M3c wrote, before 0.1.
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
  - SQL destinations index a child table of a merge table by its root id when they create it.
  - The files destination rewrites a child table only where its roots' rows drop children.
  - A lowering plan keeps its constant columns for every batch no larger than the largest seen.
  - Text is rendered into one reused buffer, not a string per value: as fast as Arrow's cast for
    decimals and faster for timestamps.
  - A plan made for a view already superseded is used but not cached over the current view's.
- **Dependencies.** The engine uses `chrono`, the version arrow already builds, for zone offsets.

## Consequences

Every value a source may send, in any encoding, lowers into a column that holds exactly it, or the
batch is refused by name; nothing is silently lost, rounded or wrapped. The differentials run
1 024 and 512 cases in CI and hundreds of thousands locally. The simulation does not yet draw
every type and encoding, nor column-level policies and nested arrays in its drift; that is M3f.
