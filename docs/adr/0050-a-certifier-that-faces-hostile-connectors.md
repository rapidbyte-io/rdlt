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
    left out this way is one the engine never relies on. Where the engine relies on a
    behaviour in any mode a connector declares, the clause runs in a mode it declares: a kill
    clause loads a destination in the first of append, merge and replace it writes, and a
    source's streams each in the first of incremental, full and change reads it serves.
  - Unobserved is everything else that is not a pass or a failure: nothing reads back what a
    destination published, a source sent no checkpoint, a read ended within its first credit, no
    kill reached the connector, the source holds more than a clause reads, a destination keeps
    history alone or names shorter than `D-NAMES` writes. None counts toward a pass.
  - A report's verdict is passed, incomplete or failed. It passes when no clause failed, none
    was unobserved and one passed. The binary exits 0, 2 or 1; `--require partial` exits 0 for
    an incomplete run in which a clause passed, and the report says incomplete all the same.
  - `S-RESUME` is unobserved without a checkpoint. It resumes from every checkpoint of a read
    that sent five at most, and else from five spread from the first to the last, so a resume
    wrong only from a checkpoint between them is not caught. It compares a resumed read's pushes
    with the first read's only while their batches, both reads together, expand to 64 MiB at
    most, as the cost model measures them: comparing compares every value each row names.
    Beyond that the clause is unobserved.
- **Every wait has a deadline, and no clause's work is unbounded.** The raw handshakes and
  configurations of the read-back and acknowledged probes keep the connection deadline. The
  question of where a source stands, asked before the clauses, runs under a clause's bound. What
  a clause computes of connector data is bounded by limits on the data, below, and rendering
  yields between pieces. The binary bounds a whole run at an hour: the clauses of a
  connector that answers take seconds each and its kill clauses 300 s at most, while the
  clauses' own bounds, each for a connector that never answers, add up to hours. `--timeout`
  chooses another bound and `--no-timeout` lifts it; a bound further ahead than the clock holds
  is a usage error, not no bound. At the bound the clause being checked fails, the outcomes
  already found are kept, the clauses after are unobserved, and no connector is started for a
  role not yet begun. The binary prints each role's report as it ends.
- **What a connector sends is charged before it is held, expanded or rendered.** The limits live
  in `testing/limits.rs` and `rdlt-certify/src/limits.rs`.
  - A source clause holds 64 MiB and 2^20 rows of its reads, all of them together, charged as
    the engine charges a push, plus a fixed cost per event. Beyond it the read is stopped and the
    clause unobserved: a large table breaks no clause.
  - A JSON push is scanned, not parsed, to count its records, and charged a row for each before
    any is parsed. Records are then parsed one at a time, each 1 MiB of text at most, with a
    yield every 1 MiB, so a push never expands beyond what its rows were charged.
  - A read-back is admitted once: at most 10,000 rows (100,000 as the wire probe decodes), flat
    columns only, plain, dictionary or run-end encoded, never nested or of no width, and at most
    16 MiB once every row holds its own value, as the cost model measures what a batch expands
    to (ADR 0039). Beyond it the clause fails: a clause that wrote
    three rows read back thousands.
  - `K-SOURCE` loads 100,000 rows and 64 MiB at most, and is unobserved beyond.
  - Rows are rendered through one renderer, within 64 MiB of text a clause.
  - A reason is a type, cut at 2048 bytes with a mark, and shows the count of rows and the
    first eight.
- **A cast of a read-back column is tested not to panic, and a panic is never a pass.**
  - A read-back column is cast only along a short list: to the kind it was written as, an
    integer to a 64-bit one, text to text, bytes to bytes, an instant to and from its count. Any
    other kind fails the clause as a column of the wrong kind. A test casts the extreme values of
    every numeric and temporal kind, in each encoding admitted, to every kind a clause reads:
    each cast the list admits is exact or refused, and none panics. A date read where an
    instant was written, which overflowed in Arrow's cast, is refused.
  - The accessors check a cast answered a value for each row and refuse a null where none may
    be. `K-DESTINATION` reads its table through the same admission as every other clause.
  - The renderer reads instants, dates, times and spans as integers, at any depth, so Arrow's
    calendar is never reached.
  - Nothing is caught by unwinding: a typed failure does not depend on the build's panic
    strategy. A panic that remains ends the run, or fails the call whose task raised it, and
    is never a pass; the binary prints it as one line, cut and escaped as a reason is.
- **A host owns the process group of a connector it spawned, for its whole life.** The
  connector leads a group of its own, and a thread of the host, which outlives the runtime that
  spawned the connector, owns it.
  - The group is signalled only while its leader is seen to be the host's unreaped child
    (`waitid` with `WNOWAIT`), under a lock the thread reaps under: the group's id, the
    leader's, cannot belong to anything else then. Each signal goes to the leader's own process
    as well, which may have left the group it was started to lead. A leader something else
    reaped, as happens in a process that ignores `SIGCHLD` or waits for any child, is sent
    nothing and not waited for.
  - A stop closes the leader's input and sends `SIGTERM` before it returns, and the thread
    sends `SIGKILL` once the leader has exited or its grace has passed; a kill sends `SIGKILL`
    at once; a leader that exits by itself has its group killed. Only then is the leader
    reaped, its exit told, and the group asked whether a living member is left: one left after
    five seconds is reported (`rdlt_host::Lingering`). On Linux a member that has ended and
    that nothing reaped is no living member, read from `/proc`, so a host that is the first
    process of a container with no init reports nothing falsely; elsewhere the null signal
    answers.
  - A connector that started and could not be owned, its output not taken or its thread not
    started, is killed and reaped before the failure is returned. A process owns 1024 groups
    at most, and joins each thread once its group has ended.
  - `rdlt_host::stop_spawned` stops every group, kills what is still stopping when its
    patience ends, and waits to see it end. A host calls it before it exits, or holds
    `rdlt_host::StopsSpawned`, which does so when dropped, a panic's unwinding included.
    `rdlt-certify` holds one from before it spawns anything: it stops what it spawned when
    its run completes, is cut at its timeout, panics on its main thread, or hears `SIGINT`,
    `SIGTERM`, `SIGHUP` or `SIGQUIT` (`rdlt_host::Interrupts`; exit 130, 143, 129, 131). A
    second signal while its connectors stop kills them at once.
  - What is not reached, exactly:
    - a host killed outright (`SIGKILL`), ended by a signal it does not hear, or aborted: it
      runs no code. Its connectors end by the end of their input and, on Linux, the
      parent-death signal; what they started and left in their groups lives on;
    - a host that drops its connectors and exits without `stop_spawned`: each group was sent
      `SIGTERM`, and a member that ignores it is never killed;
    - a member that left the group, the leader excepted;
    - the members of a group whose leader something else reaped: they are reported, not
      signalled.

    A sandboxed spawn with a process namespace of its own closes the first three.
- **A kill clause passes on a kill that landed.** The host's end of a connection counts a kill
  as landed when it sees the connection end after the kill: a socket ends only when no process
  holds its other end. The answer `K-DESTINATION` loses by itself is no evidence. For a
  connector reached at an endpoint, or served in process, a kill is the host's cut of its
  connection, which lands by itself and shows no process stopped. The host counts cuts apart,
  and a kill clause that passes carries a note, `killed` or `cut`, that the report prints
  beside the pass and its JSON carries as `note`.
- **`P-CREDIT` accounts credit.** It sizes the first frame as the host does and grants a byte
  more, three times, each too little to restore the credit, watching a second after each. A
  first frame too small to leave room for three such bytes is granted nothing in their place, so
  the watch is four watches whatever the frame spent, four seconds unless one is chosen. A caller
  of the library chooses another watch with `Target::credit_watch`, and the binary with
  `--credit-watch <ms>`, from 1 to 1000; a pass under any watch but a second carries a note
  naming it, so a shortened certification shows in its report.
- **A connector can tell a certification, and the documentation says so.** Certification names
  its pipelines and tables `certify_…`, offers the read-back and acknowledged features, makes
  calls no engine makes, and holds a connector to its limits on purpose. A connector built to
  pass can keep the clauses only while it is certified. The names stay, so an operator knows
  what certification left in a store; the tell removed is the unknown feature every
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
  frame every four seconds still passes `P-CREDIT`, or every four watches where one is chosen.
- A connector that leaves its process group and ends itself when its launcher is killed is
  counted as killed. A sandboxed spawn, with a process namespace of its own, closes that.
- A destination that reads a column back as another kind than it was written as, beyond the
  casts listed, fails the clause: `K-DESTINATION` no longer takes ids read back as text.
- `ClauseResult` has a `note`, and `rdlt_host::Kills` counts the connections it cut.
- A read-back remains the destination's own account of what it published.
- A source clause charges a push as the engine's cost model does (ADR 0039): for what it keeps
  alive and for what it expands to, whichever is more.
