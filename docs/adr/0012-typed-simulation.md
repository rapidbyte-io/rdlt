# ADR 0012: A typed simulation over every type and encoding

Status: accepted, 2026-09-25.

## Context

A green simulation must mean the engine is solid. A simulation that checks exactly-once delivery
through faults, crashes, fencing and concurrent runs over a few plain Arrow types, comparing rows
as JSON, is blind to types, and property tests that draw every type and encoding (ADR 0011) check
single components only. So the simulation draws every type and encoding, and its oracle reads
every stored cell by type.

## Decision

- **A shared test kit.** `rdlt-testkit` holds the drawn Arrow data, the exact value model (`Canon`)
  and the cell decoder that ADR 0011's differentials use. Property tests shrink its strategies; the
  simulation draws from the same strategies seeded by its own seed (`draw::draw`), so every draw is
  a pure function of the seed. It depends on `rdlt-connector`, and, for its `fake` feature, on
  `rdlt-wire`: a connector that answers the handshake and then breaks the protocol, served over any
  transport, which `rdlt-host`'s tests and the simulation meet. No production crate depends on it.
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
- **Each write goes through a writer of the schema version it was lowered for.** A writer's
  `TableRef` names the version of the batches it writes, as the contract's "the schema version
  writes follow" asks, even across a schema change within an attempt; a flush reaches only the
  writers written since the last, so older versions' writers cost nothing more.
- **Written batches name what a lowered column holds.** A text column of dates looks like any
  text column, and state records only a commit's newest schema, so each column stored as another
  type names its logical type in the field metadata key `rdlt:logical_type`
  (`LOGICAL_TYPE_KEY`), as JSON; `Field::lowered_from` reads it.
- **A partition's end is decided by the rows it received.** A read whose rows after its last
  checkpoint are all discarded still counts its discards, and an incremental partition so read is
  marked done, so the next run does not read and discard its rows again.

## Consequences

- The test job runs 20 seeds of each sweep, in the dev profile, on Linux and macOS, macOS's only
  simulation; the pull request's shards run 100 000 seeds of each in the simulation's profile and
  the nightly a million. How fast the simulation checks seeds is recorded in
  [docs/perf/sim.md](../perf/sim.md).
- A failed run's report may miss a commit whose response its last attempt lost; a later attempt of
  the same run would have credited it. Discard counts are therefore checked exactly only on seeds
  without faults or disruptions, and bounded on the rest.
- JSON streams mostly push finite floats, so a column of them keeps one inferred type the oracle can
  check; one in four also pushes floats JSON cannot hold, by name.
- Draws fix the settings proptest otherwise reads from `PROPTEST_*` variables, so a seed replays
  alike wherever it runs.
- A child's id derives from its parent's and its position, so two arrays of one row give their
  children the same ids; a child table's rows are read against their parent table.
- A text column widened in place keeps the text of its older type: dates stay `YYYY-MM-DD` in a
  column since widened to timestamps. Each row's `rdlt:logical_type` says which.
- Every policy and setting, hints and odd schema sequences are ADR 0013's; faults beyond
  connector calls, multi-threaded runs, replay and the simulation's own coverage ADR 0014's.
