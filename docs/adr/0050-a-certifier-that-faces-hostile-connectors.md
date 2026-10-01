# ADR 0050: A certifier that faces hostile connectors

Status: accepted, 2026-10-01.

## Context

`rdlt-certify` and the clauses of `rdlt_connector::testing` are run against connectors nobody
trusts yet. ADR 0020 to 0022 wrote them for a connector that is trusted code with untrusted
output; ADR 0037 holds every connector that is not compiled into its host to be untrusted. Four
things did not hold to that:

- some waits had no deadline, and synchronous work could outlast a clause's bound;
- what a connector sent was bounded in wire bytes at most, then held, expanded and rendered
  whole, and quoted whole in reports;
- connector data could panic the certifier;
- a clause could pass without the behaviour it names having been seen, and a report passed, and
  the binary exited 0, with any number of clauses skipped.

## Decision

- **An outcome says what was seen.** A clause passes, fails, is *inapplicable* or is
  *unobserved*.
  - Inapplicable is by declaration only: the role a connector serves, its write and delete
    modes, its checkpointing, its identifiers' length, the limits and features it declares. Each
    clause names the declaration that makes it inapplicable, in the registry and in
    `docs/certify/clauses.md`. The engine uses of a connector only what it declares, so a clause
    left out this way is one the engine never relies on.
  - Unobserved is everything else that is not a pass or a failure: nothing reads back what a
    destination published, a source sent no checkpoint, a read ended within its first credit, no
    kill reached the connector, the source holds more than a clause reads. A connector decides
    these by how it behaves, so none counts toward a pass.
  - A report's verdict is passed, incomplete or failed. It passes when no clause failed, none
    was unobserved and one passed. The binary exits 0, 2 or 1; `--require partial` exits 0 for
    an incomplete run in which a clause passed, and the report says incomplete all the same.
  - `S-RESUME` is unobserved without a checkpoint, and resumes from checkpoints spread from the
    first to the last of a read.
- **Every wait has a deadline, and no clause's work is unbounded.** The raw handshakes and
  configurations of the read-back and acknowledged probes keep the connection deadline. The
  question of where a source stands, asked before the clauses, runs under a clause's bound. What
  a clause computes of connector data is bounded by limits on the data, below, and rendering
  yields between pieces. The binary bounds a whole run at an hour: an honest connector's
  clauses take seconds each and its kill clauses 300 s at most, while the clauses' own bounds,
  each for a connector that never answers, add up to hours. `--timeout` chooses another bound
  and `--no-timeout` lifts it; a role still certifying at the bound fails every clause. The
  binary prints each role's report as it ends.
- **What a connector sends is charged before it is held, expanded or rendered.** The limits live
  in `testing/limits.rs` and `rdlt-certify/src/limits.rs`.
  - A source clause holds 64 MiB and 2^20 rows of its reads, all of them together, charged as
    the engine charges a push, plus a fixed cost per event. Beyond it the read is stopped and the
    clause unobserved: a large table breaks no clause.
  - A read-back is admitted once: at most 10,000 rows (100,000 as the wire probe decodes), flat
    columns only, plain, dictionary or run-end encoded, never nested or of no width, and at most
    16 MiB once every row holds its own value. Beyond it the clause fails: a clause that wrote
    three rows read back thousands.
  - `K-SOURCE` loads 100,000 rows and 64 MiB at most, and is unobserved beyond.
  - Rows are rendered through one renderer, within 64 MiB of text a clause.
  - A reason is a type, cut at 2048 bytes with a mark, and shows the count of rows and the
    first eight.
- **No panic on connector data.** Read-back columns go through accessors that check a cast
  answered a value for each row, refuse a null where none may be, and never cast an instant to
  text. The renderer reads instants, dates, times and spans as integers, at any depth, so
  Arrow's calendar is never reached. Nothing is caught by unwinding: a typed failure does not
  depend on the build's panic strategy.
- **A kill clause passes on a kill that landed.** A spawned connector leads a process group, and
  stop and kill signal the group. The host's end of a connection counts a kill as landed when it
  sees the connection end after the kill: a socket ends only when no process holds its other
  end. The answer `K-DESTINATION` loses by itself is no evidence.
- **`P-CREDIT` accounts credit.** It sizes the first frame as the host does and grants a byte
  more, three times, each too little to restore the credit, watching a second after each.
- **A connector can tell a certification, and the documentation says so.** Certification names
  its pipelines and tables `certify_…`, offers the read-back and acknowledged features, makes
  calls no engine makes, and holds a connector to its limits on purpose. A connector built to
  pass can keep the clauses only while it is certified. The names stay, so an operator knows
  what certification left in a store; the one tell removed is the unknown feature every
  handshake offered, which only `P-HANDSHAKE`'s now does, under a name of its own each time.
  Certification finds the defects of a connector that does not hide them. What holds against one
  that does is the engine's own limits, deadlines and confinement.

## Consequences

- A certification with a fixture too small to show a behaviour exits 2 where it exited 0: a
  source needs data enough to checkpoint and for a kill to land, or `--require partial`.
- `Outcome`, `Clause` and `Report` changed shape: `Skipped` is gone, a reason is a `Reason`,
  a clause has `unless`, and JSON reports carry `verdict` and the outcomes `inapplicable` and
  `unobserved`.
- A destination that reads flat columns back nested, or encoded twice over, fails the clauses
  that read them.
- Silence is observed for a bounded time: a source that ignores credit and sends less than a
  frame every four seconds still passes `P-CREDIT`.
- A connector that leaves its process group and ends itself when its launcher is killed is
  counted as killed. A sandboxed spawn, with a process namespace of its own, closes that.
- A read-back remains the destination's own account of what it published.
- Until the engine's cost model charges a batch for all it pins, a source clause under-charges
  such a batch as the engine does.
