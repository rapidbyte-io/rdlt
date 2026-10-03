# ADR 0046: Values held, identified and timed exactly, whatever a source sends

Status: accepted, 2026-10-03.

## Context

ADR 0037 treats everything a connector sends as hostile. The engine's handling of values had
trusted them in ways that broke data quietly or failed whole streams on one row:

- Row ids and history row hashes were the unseeded XXH3-128 of a row's encoding, which resists no
  chosen input: two binary values two bytes apart could share an id, so a history skipped the
  change between them and a child table took another root's rows. A Float32 hashed as its own
  shortest text, so widening it to Float64 rehashed every row; a history hashed what the
  destination stored, so JSON re-spelled, or a decimal whose scale grew, opened versions for
  values that had not changed.
- A source column could take the identifier of a metadata column its table did not have yet,
  such as `_rdlt_seq` while the stream appended; when the stream later merged or normalized, the
  engine named its own column the same, one column served for two, and creating a generation
  panicked.
- Arrow's temporal formatter panicked on a zoned instant whose local time chrono cannot hold,
  and wrote a Time64 of 2^32 seconds or more as an ordinary time of day. Dates held as Date64
  were divided toward zero where they became timestamps and floored where they became text; a
  history change time held as Date64 was multiplied into microseconds unchecked. A decimal
  beyond its declared precision was widened, rescaled and rendered as it was.
- One value its column could not hold, or one null or far change time, failed the whole batch
  whatever the stream's policy.
- A history took change times as sent and closed versions at them without comparing anything:
  a time earlier than the version it replaced made that version end before it began. A stream
  naming no change time read a wall clock that may step back. SQLite stored validity as text of
  varying width, whose order is not the instants'.
- Keys were refused when missing or null, but a NaN key, which equals nothing, reached the
  destination; a change could flag its key unchanged; a normalized stream identified rows lacking
  their key by one shared id.
- A declared schema that gained an array added a child table under a frozen policy, and rows a
  reset or replan abandoned stayed staged until the next session.

## Decision

- **Row identity is BLAKE3 of one canonical encoding.** An id is BLAKE3's whole 256-bit output;
  128 bits would give 64 bits of collision resistance, within reach of an attacker who writes
  both rows. Each kind of id hashes a tag of its own first: a root by its key or whole, a child
  by its parent's id and index, a history version by its data. Every name, text, byte string and
  number in the encoding carries its length in LEB128, so no encoding is a prefix of another.
  Numbers are one kind, by value: integers, decimals without trailing zeros, JSON numbers by
  their exact value and floats by the shortest text of the 64-bit float they are or widen to, so
  a widening changes no id while two floats never share one. A Date64 is the day it is within.
  Ids are stored, so they change once with this decision; nothing published reads the old ones.
  The engine writes a Float32 into JSON as the 64-bit float it widens to, so its text is that of
  the column it widens to.
- **A history version is hashed by the values its table holds**, each data column converted to
  its logical type before it is lowered: JSON by the values it renders, decimals by their value,
  floats as above, however the destination stores them.
- **Every metadata column has one identifier under a destination's rules**, assigned once in a
  fixed order whatever a table's mode, and no source column takes one. A source column whose
  name, folded and cleaned as the rules make it, is a metadata column's is refused,
  `column_name_reserved`, when it is named: at planning for a key, or at its first batch. A
  name map in state that names a source column as a metadata column is `state_invalid`, and a
  table view checks its columns are distinct when it is built. A table rdlt wrote is loaded as a
  source only once its `_rdlt_` columns are renamed or left out at the source.
- **Temporal values are rendered and converted exactly over every type's whole range.** Arrow
  renders a zoned instant only where chrono holds the instant and its time in the zone at an
  offset of whole minutes, and times of day never: the engine renders the rest exactly, in UTC.
  A Date64 is the day it is within everywhere, a struct's and a list's included, and one a
  Date32 cannot hold is refused. A change time becomes microseconds by a checked conversion of
  the engine's own: a date's midnight, and an instant between two microseconds the earlier. A
  decimal beyond its declared precision is no value of its type and is refused.
- **A value its column cannot hold follows the column's schema policy, row by row.** The
  policies that decide a value that would change the schema decide one no type of the column
  holds, and a change time no version can begin at: Evolve and Freeze refuse the batch with a
  typed error as before (`value_unrepresentable`, `change_time_invalid`, `change_time_null`);
  DiscardRow drops the row, its child rows with it in a normalized stream; DiscardValue nulls
  the value, and a version whose change time it nulls begins when its batch arrived. Each is
  counted. A value is found refused by converting its row alone, once its batch's conversion
  failed, in a column whose policy discards.
- **A version never ends before it begins.** A history destination begins a version when its
  change says, or at the latest instant its key's versions already hold where that is later: the
  latest start or end of the key's versions in the table, and of the versions the commit opened
  before it, in sequence order. A version whose successor said an earlier time spans no time and
  is kept, in sequence; a key opened again as of before its deletion begins where it was
  deleted. The SQL planner, the reference merge and the simulation follow the rule, and
  `D-HIST` requires it of every history destination. A stream naming no change time begins its
  versions by a load clock read from the engine's environment that never reads earlier than the
  load's start or an earlier reading, with times before the epoch exact and one beyond what
  microseconds hold refused. Where a destination stores no timestamps, validity is the
  microseconds since the epoch, which order as the instants do; a history stream into a
  destination storing neither timestamps nor 64-bit integers is refused,
  `history_validity_unsupported`.
- **No row is matched or identified by a key that cannot match.** Every keyed table, merge,
  history and a normalized stream's identity key alike, refuses before any destination sees
  the rows a key column the batch lacks (`merge_key_missing`), a null (`merge_key_null`) and a
  NaN at any depth and in any encoding (`merge_key_nan`), and a change flagging a key column
  unchanged (`merge_key_unchanged`). Identity has no encoding for a missing key.
- **A declared array is taken as given only while its table is created**, or where state records
  its table; after that a new one is a change its column's policy decides: a frozen stream
  refuses it when planned (`schema_frozen`), one that discards drops its rows.
- **A commit removes the staging of the segments the load abandoned since the last one**
  (`CommitMeta::abandoned`, field 10 of the wire's commit message), which is never among those it
  publishes; a replayed commit abandons nothing, its load's staging being gone with its session.
  `D-DISCARD` requires that a segment a commit abandons is never published after.
- **Identifier case is documented as folded**: `Lower` and `Upper` are Unicode's lower- and
  upper-case mappings and no further fold. A destination whose identifiers compare alike more
  widely declares narrower characters, and `D-NAMES` writes pairs of names the declared rules
  keep apart though they compare alike by ASCII case, case folding, normalization or
  compatibility, and requires both back. SQLite already refuses a name holding an ASCII
  upper-case letter and resolves every name without case before use (ADR 0049), so its owner
  and clash checks compare names as SQLite does.

## Consequences

- Stored ids, row hashes, the recorded identity vectors and the JSON text of Float32 values
  change, once. Ids take 32 bytes, which the cost of metadata and lineage columns counts.
- Source columns named exactly as a metadata column fail a stream that loaded them before, which
  needs its source column renamed or its state reset.
- A history destination, third parties' included, must begin versions no earlier than its key's
  latest instant to pass `D-HIST`, and remove abandoned staging to pass `D-DISCARD`. SQLite users
  read validity as integers.
- A discard policy discards values out of range as it discards type changes; a stream that
  relied on such a value failing must use Evolve or Freeze.
- Per-row discards convert a failing batch's rows one at a time, which costs time in proportion
  to the batch on that path only.
