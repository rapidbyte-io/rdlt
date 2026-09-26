# ADR 0013: Every schema policy and setting in the simulation

Status: accepted, 2026-09-26.

## Context

M3f made the simulation draw every type in every encoding (ADR 0012), but its streams only evolved,
dropped rows or dropped values, set once per stream. No run was ever refused. No drift column was
declared, hinted or set on its own, and merge keys were integers no two partitions shared. M3g is
the second of the three simulation milestones: every schema policy and setting, and odd schema
sequences.

## Decision

- **Settings at every level.** The pipeline, each stream and each drift column draw their own
  settings, each left to inherit or set:
  - policies: evolve, freeze, drop row and drop value;
  - `on_unsupported`: variant column or refuse;
  - nested values: native, JSON or normalized.

  The model resolves them with its own function. A test proves that the plan the simulation builds
  resolves alike under every combination.
- **Hints and declared columns.** Drift columns may be hinted, and the source may declare them.
  Either type is one of the column's types, their join, a neighbor of one, or another. JSON streams
  hint and declare only the types JSON values are inferred as, or `Json`. A normalized stream
  declares no column its policy discards.
- **The model decides by batch.** Schema resolution classifies a batch's column, not each value:
  - an Arrow column arrives as its shape's type, however many of its values are null;
  - a JSON push's column arrives as the join of every value it holds, so a value its column holds
    is discarded with the rest of a push whose join the column does not hold.

  The model follows the same batches, since a read resumes only at checkpoints, which fall between
  batches. The engine shreds a checkpoint's worth of pushes or more together and joins their
  types, which is the same as long as every push of a column in one partition and phase arrives
  alike; a test holds the workload to that. Non-finite floats pushed by name, which would break
  it, are M3h's, with a model of the pushes the engine shreds together.
- **Where each value sits.** A value whose type fits its column's fixed type must sit in the own
  column, written as exactly that type. A column's type is fixed when it is hinted, when it is
  declared and every batch fits it, or when its every batch arrived as one type. A value a hint
  does not hold must sit in a variant column.
- **Refusals are predicted.** Before each run, the oracle follows each column's own column through
  every order its batches may arrive in, from each type the earlier phases may have left:
  - the model predicts which refusals, by stream and code, some run **may** meet;
  - it predicts whether every run **must** meet one;
  - a normalized stream's refusals are only ever possible;
  - a merge key widens where the destination can, and is refused otherwise.

  A run that fails with a schema error the model does not predict, or that succeeds where every run
  must be refused, fails the seed.
- **An operator relaxes what refused.** After a refusal, the refused stream's frozen settings
  evolve, and its refused changes take variant columns. A destination that could not add columns
  is granted the right. Rows committed under the stricter settings are stored exactly as the
  relaxed ones store them, so the model stays the same, and the phase must still converge exactly
  once. A merge key refusal has no relaxation, so the seed stops there:
  - each table holds each row at most as often as the model says;
  - a merge table holds any row of a key delivered so far;
  - discards are bounded.
- **Merge keys.**
  - Keys change type: narrower integers, now and then a wider decimal, rarely text or a float.
  - Keys collide across partitions: a shared key's row is then the last of whichever partition
    delivered it last.
  - Keys span two columns: the key and a text tag.
- **Destinations.**
  - Commit kind is drawn.
  - Identifiers may use any characters, and some drift names are not ASCII words.
  - Reserved words include base and metadata column names.
  - Tables avoid reserved prefixes.
  - Some destinations cannot add columns.
- **Found and fixed in the engine.**
  - A normalized stream's column settings covered only the column's own path, so a policy on an
    object or array reached neither the columns it flattens into nor its child tables. A stream's
    column settings now apply to everything the column becomes.
  - A normalized stream ignored the hint on a column whose values were objects or arrays, and
    normalized them. A hinted column is now stored whole, as its hint.
  - A new array a column discards counted its items as discarded values even when another column
    dropped their row. Now only items whose rows load count, as a dropped row's other values never
    did.
  - A float written into JSON took the text arrow-json gives it, which for some values has more
    digits than reading it back needs: `2.71492836553152e19` became `2.7149283655315202e19`, the
    same double but another number to a destination that keeps JSON numbers exact. Floats are
    now written as the shortest text that reads back as them.
  - A merge key widened even where the schema was frozen, altering the destination's key column
    where freezing promises no change. A frozen key now refuses any type it does not hold with
    `schema_frozen`, which an operator can relax.
- **Found and fixed in the oracle.** Reserved words are compared after the destination's rules
  fold case. Before, the oracle compared them ignoring case, and so wrongly flagged `D0` where a
  case-preserving destination reserves `d0`.

## Consequences

- Rows that go with a dropped row count as discarded rows, as the engine has counted them since
  normalized streams arrived. The model counts them too.
- About one seed in twelve stops at a key refusal, about half of them in the first phase.
- A normalized stream's child tables are created by their first rows in whatever order partitions
  send them, so the oracle predicts the refusals of a column holding objects or arrays only as
  possible. A column of scalars stays one column of the stream's own table, and its refusals are
  predicted as exactly as any other stream's.
- A normalized stream's rows that a new array drops go before their batch's schema is resolved,
  taking with them the columns only they hold values in. Which columns arrive then depends on how
  the engine groups a flush's batches, so where rows go the oracle predicts refusals, keys' too,
  only as possible.
- Seeds that once found a defect are replayed on every run.
- The engine does not branch on commit kind. The simulated destination declares either, so a
  future difference is covered from the start.
- Faults beyond connector calls, multi-threaded runs, replay and the simulation's own coverage
  are M3h.
