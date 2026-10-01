# ADR 0036: Real crashes, and M5's exit

Status: accepted, 2026-10-01.

## Context

Spec §20.6 asks for two tests of real crashes, and the M5 exit gate names them: a failpoint
sweep, crashing a run at every durability step and checking the run after it loads every row
once, and a kill matrix, killing the process running a pipeline or its spawned connectors as it
loads, then checking every row landed once and no connector outlived its run. ADR 0023 planned
them as M5f. The simulation models crashes with torn logs (ADR 0014, 0029); these crash real
processes, with real files, sockets and child processes.

## Decision

- **Crash points.** The engine's `failpoints` feature (the `fail` crate, never in a release)
  names a point at each durability step:
  - a log append;
  - either side of the lanes' flush and of the commit frame's fsync;
  - before a forgetting source hears of a logged commit;
  - either side of the destination's commit, and of the commit completing a stream, where a
    replace publishes its generation;
  - after the log appends the receipt, and after it removes each chunk it no longer needs;
  - either side of a source hearing of a landed commit;
  - either side of the closing frame's fsync, and after the log is removed;
  - either side of a replayed commit.

  A point configured through `FAILPOINTS` aborts the process there: no unwinding, no flush, no
  goodbye to a connector.
- **The harness stands for the CLI** until M7 has one: the engine's `crash_run` example runs one
  pipeline from a configuration file, its source and destination in its process or spawned, its
  log in a local directory where it keeps one, tells each read and commit as it happens, and
  exits 0 once the run succeeded. When the CLI exists, the sweep and the matrix run it instead.
- **State outlives a process only on disk.** The reference change source keeps its slot, and the
  log source its group, in a file where configured (`slot_path`, `group_path`), written whole
  beside it and renamed over it, so a crash leaves it as it was or as it is; a damaged file is
  refused at connect rather than read as empty. Amended 2026-10-01: the file is named from the
  root as `*.slot` or `*.group`, and a source with a stream that forgets names its slot or
  group (ADR 0049); how the file is opened and written is ADR 0047's. A source that forgets what it acknowledged can
  then be crashed, killed and spawned again as a replication slot or a consumer group would.
- **The sweep** crashes four pipelines with a log: a log that forgets appended to SQLite, a
  change stream that forgets merged into JSON-lines files, a full read replacing a SQLite table,
  and a timed change stream kept as history in SQLite; the full read and the history run again
  without a log, as a replayable source does by default, recovering by reading again from
  committed state.
  - Every point a pipeline's runs pass must crash them: points passed many times at their first
    hit and their third, those passed once at their only one.
  - The destination's commit crashes at every hit a run reaches, ending once a run makes fewer
    commits than the hit, since the commits a run makes vary with timing; the commits where a
    phase begins or a truncate lands crash with the rest.
  - A commit left by a crash is replayed through two more crashes, before and after it lands.

  The next run must load the table exactly as the reference models say, with no log left, or
  none written. A point that no longer crashes a run fails the sweep.
- **The kill matrix** runs the harness in a process group of its own, holding a kilobyte of
  batches so its source waits for commits, its reads in flight across them.
  - It kills the harness where it waits after a read or commit drawn from a seed
    (`RDLT_KILL_SEED`, else the clock) among those a clean run tells, a drawn delay after it began
    to wait, while its other reads and commits go on; the run is never done before its kill. A
    seed draws the same points and delays again; what the rest of the run did meanwhile varies.
  - It kills a spawned source through the host's `Kills` only while it reads, with more to send
    than its connection holds, and a spawned destination before a chosen commit or the one
    publishing a replace, waiting for it to die; the kill must fail an attempt, which a later
    attempt rides out.
  - A run the test gives up on is killed with its process group, so a failure leaves nothing
    running.

  After each, every row lands once and every process of the group is gone within ten seconds.
  Remote connectors are left to the `K` clauses, which sever their connections (ADR 0022).
- **Where they run.** A test target of the engine, `crashes`, needing `failpoints`: every
  `just test` runs it, on Linux and macOS, and `just crashes` rebuilds the harness first.
  Mutation testing and coverage leave it out: it takes minutes, its runs are processes of their
  own whose profiles coverage would merge by the hundred, and it catches nothing the other tests
  do not first.
- **M5 exits** with the simulation of change and forgetting sources (M5a–M5e), this sweep and this
  matrix green.

## Consequences

- A crash anywhere a commit is written, sent or heard of loses and doubles nothing, and a killed
  run leaves no connector behind, checked on real processes on every change.
- A killed process keeps what it wrote in the page cache, so real crashes cannot see a missing or
  reordered fsync, or an acknowledgement sent before the log holds its commit: the simulation's
  torn logs find those (ADR 0014, 0029).
- A spawned connector outlives its host only until it reads the end of its standard input or, on
  Linux, its parent's death signal; spawning happens on the runtime's worker threads, which live as
  long as the runtime, so that signal never fires early.
