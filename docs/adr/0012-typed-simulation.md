# ADR 0012: A typed simulation over every type and encoding

Status: accepted, 2026-09-25.

## Context

The owner asked that a green simulation mean the engine is solid. The simulation (spec §20.2)
checked exactly-once delivery through faults, crashes, fencing and concurrent runs, but its data
was six drift shapes of plain Arrow types and its oracle compared rows as JSON, blind to types.
Property tests drew every type and encoding (ADR 0011), but only against single components. The
work was split in three (owner's decision, 2026-09-25): M3f brings every type and encoding into the
simulation with a type-aware oracle; M3g every policy and setting and odd schema sequences; M3h
faults, concurrency, replay and coverage.

## Decision

- **A shared test kit.** `rdlt-testkit`, a crate outside spec §5's list, holds the drawn Arrow data,
  the exact value model (`Canon`) and the cell decoder that ADR 0011's differentials use. Property
  tests shrink its strategies; the simulation draws from the same strategies seeded by its own
  seed (`draw::draw`), so every draw is a pure function of the seed. It depends on
  `rdlt-connector` alone, and no production crate depends on it.
- **The simulation draws every type in every encoding.** Drift columns take any logical type in
  any encoding a source may send, nested up to three levels (objects in arrays, arrays in arrays),
  change to a type the lattice joins them with or to another, and hold edge values: full ranges,
  NaN and infinities, empty and Unicode text, empty lists, nulls at every level. Values stay
  within what every finer unit holds, so a phase converges rather than being refused; ADR 0011's
  differential covers the refusals. JSON streams draw the types JSON holds. Sources may send each
  batch as a slice of a larger one.
- **Swarm testing.** Each seed turns a random subset of features on: drift, nesting depth,
  encodings, JSON pushes, normalizing, sliced batches, faults, disruptions and narrow
  destinations; one seed in eight turns every feature on.
- **The oracle reads every cell by type.** The destination keeps each stored row's Arrow data. For
  every row the model expects, as often as the model expects it, the oracle requires each source
  value in exactly one of its column's own and variant columns, whose type holds the value's
  type, stored as the destination's capabilities say, and reading back to exactly the value sent,
  converted as the column's type converts it. A null must stay null. Identifiers follow the
  destination's rules, and every column a stored row has is named, or metadata. A column whose
  values only ever arrived at one type must be exactly that type, in its own column: a column's
  type holding its values is not enough, as `Json` holds every value. Every batch through one
  writer must have one schema, as a writer serves one version. Normalized streams' child tables
  are checked at any depth, each row matched to its parent through its lineage ids. Where no run
  was disrupted and no fault injected, the runs' reports must count exactly the rows and values
  the policy discarded, and on every other seed no more.
- **Found and fixed in the engine.**
  - A lane kept one writer per table for its whole attempt, so after a schema change the writer's
    `TableRef` named an older version than the batches it wrote, against the contract's "the
    schema version writes follow". Each write now goes through a writer of the version it was
    lowered for, and a flush reaches only the writers written since the last, so older versions'
    writers cost nothing more.
  - A destination could not tell what a lowered column holds: a text column of dates looked like
    any text column, and state records only a commit's newest schema. Written batches now name the
    logical type of each column stored as another type in the field metadata key
    `rdlt:logical_type` (`LOGICAL_TYPE_KEY`), as JSON; `Field::lowered_from` reads it.
  - A read whose rows after its last checkpoint were all discarded sealed nothing: its discards
    were never counted, and an incremental partition was never marked done, so the next run read
    and discarded its rows again. A partition's end is now decided by the rows it received.

## Consequences

- The simulation runs 10⁴ seeds in CI and 10⁵ locally in about ten minutes, as before.
- A failed run's report may miss a commit whose response its last attempt lost; a later attempt of
  the same run would have credited it. Discard counts are therefore checked exactly only on seeds
  without faults or disruptions, and bounded on the rest.
- JSON streams mostly push finite floats, so a column of them keeps one inferred type the oracle can
  check; one in four also pushes floats JSON cannot hold, by name.
- Draws fix the settings proptest otherwise reads from `PROPTEST_*` variables, so a seed replays
  alike wherever it runs.
- A child's id derives from its parent's and its position (spec §7.4), so two arrays of one row
  give their children the same ids; a child table's rows are read against their parent table.
- A text column widened in place keeps the text of its older type: dates stay `YYYY-MM-DD` in a
  column since widened to timestamps. Each row's `rdlt:logical_type` says which.
- Policies other than evolve, drop row and drop value, column-level settings, hints and odd schema
  sequences are M3g; faults beyond connector calls, multi-threaded runs, replay and the
  simulation's own coverage are M3h.
