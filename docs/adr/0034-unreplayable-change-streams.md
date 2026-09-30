# ADR 0034: Change streams whose source cannot read again

Status: accepted, 2026-09-30.

## Context

ADR 0029 refused a change stream whose source cannot read again what it acknowledged
(`change_read_unreplayable`): replay goes by partitions' positions, and a change stream's phases
move them other than forward. A commit that begins a phase deletes the previous phase's partition
positions and puts the new phase's start cursors. Four things kept the log from replaying such a
commit:

- A seal's `from`, the position its partition stood at before the commit, was taken before the
  commit's own phase transition, so a new phase's first seal named the previous phase's position.
- A replay that could not land the commit whole kept only partition positions, dropping the
  transition: the new phase's positions would be recorded under the old phase, with the old
  phase's positions left behind.
- The positions replay compared knew no phases, and partition ids may recur across phases, so
  `from` could not tell a new phase's partition from an old one of the same id.
- Nothing said which phase a seal belonged to.

ADR 0033 moved these streams to M5d4. The M5 exit gate asks for a simulation with change sources
and sources that cannot read again.

## Decision

- **Logs know phases.** A seal frame names its stream's phase (`phase`, 0 where an older frame
  has none). A commit that begins a phase logs the transition in a `Begun` frame (kind 8): the
  stream, the phase, and the state changes the commit's delta makes for it (the old phase's
  positions deleted, the new phase's start cursors put, the phase put). The `Begun` frames precede
  their commit frame in one append, and count only once that commit frame is read: a log torn
  between them holds no transition.
- **`from` follows the transition.** A seal's `from` is where its partition stands once its
  commit's own transition applies: the new phase's start cursor, or none.
- **Replay applies a logged transition.** Replaying a commit the destination missed, the engine
  applies a logged transition where the destination still stands before its phase and holds each
  position the transition deletes; otherwise the destination already moved past it. A seal then
  lands only where its phase is the stream's phase after the transitions applied and its `from`
  is where the destination stands. A reset stream's transitions from before the reset are skipped,
  as its seals are (ADR 0033).
- **`change_read_unreplayable` is lifted.** A change stream from a source that cannot read again
  loads through the log. It needs a log store (`wal_required`); a full read stays refused
  (`full_read_unreplayable`).
- **The reference change source can forget.** `replayable: false` makes it refuse, as transient,
  a read of its changes from before the position its slot acknowledged, as a replication slot
  does. It passes certification, `S-ACK` included.
- **The simulation.** Where pipelines keep logs, every other change stream's source forgets what it
  acknowledged, on every partition, and never sends changes again: the at-least-once changes are
  drawn and dropped, so every other draw falls as it did. The change oracle keeps the logs, crashes
  tear them, a round converges only once no log is left to replay, and every acknowledged
  position must have landed; a snapshot partition's position is gone once its stream reads its
  changes, which counts as landed.

## Consequences

- A source that cannot read again, a replication slot or a queue, can load a snapshot then its
  changes exactly once through crashes and failed commits.
- Such a stream still cannot be reset (ADR 0033): what its source acknowledged is gone.
