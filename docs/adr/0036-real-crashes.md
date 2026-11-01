# ADR 0036: Real crashes

Status: accepted, 2026-10-01.

## Context

Two tests crash real processes: a failpoint sweep, crashing a run at every durability step and
checking the run after it loads every row once, and a kill matrix, killing the process running a
pipeline or its spawned connectors as it loads, then checking every row landed once and no
connector outlived its run. The simulation models crashes (ADR 0014); these crash real
processes, with real files, sockets and child processes.

## Decision

- **Crash points.** The engine's `failpoints` feature (the `fail` crate, never in a release)
  names a point at each durability step:
  - either side of the lanes' flush (`engine.flush.*`);
  - either side of the destination's commit (`engine.commit.*`), and of the commit completing a
    stream, where a replace publishes its generation (`engine.complete.*`);
  - either side of a source hearing of a landed commit (`engine.ack.before`, `engine.ack.after`).

  A point configured through `FAILPOINTS` aborts the process there: no unwinding, no flush, no
  goodbye to a connector. The harness turns its core dumps off before it runs, so an abort costs
  the system's dump handler nothing, and each process a test starts is killed with its group
  when the test's own process ends.
- **A harness stands for a CLI**, which the engine does not have: its `crash_run` example runs
  one pipeline from a configuration file, its source and destination in its process or spawned,
  tells each read and commit as it happens, and exits 0 once the run succeeded.
- **State outlives a process only on disk.** The reference change source keeps its slot, and the
  log source its group, in a file where configured (`slot_path`, `group_path`), written whole
  beside it and renamed over it, so a crash leaves it as it was or as it is; a damaged file is
  refused at connect rather than read as empty. The file is named from the root as `*.slot`
  or `*.group`, and a source with a stream that forgets names its slot or group (ADR 0049); how
  the file is opened and written is ADR 0047's. A source that forgets what it acknowledged can
  then be crashed, killed and spawned again as a replication slot or a consumer group would.
- **The sweep** crashes four pipelines: a log that forgets appended to SQLite, a change stream
  that forgets merged into JSON-lines files, a full read replacing a SQLite table, and a timed
  change stream kept as history in SQLite. Each recovers by reading from the destination's
  committed state.
  - Every point a pipeline's runs pass must crash them: points passed many times at their first
    hit and their third, those passed once at their only one.
  - The destination's commit crashes at every hit a run reaches, ending once a run makes fewer
    commits than the hit, since the commits a run makes vary with timing; the commits where a
    phase begins or a truncate lands crash with the rest.
  - A source is told a landed commit through a crash either side of the telling, at every commit
    a run reaches, for a log that forgets and one that reads again: the next run starts at the
    committed cursor, which a source that forgets serves from as one that reads again does.

  The next run must load the table exactly as the reference models say. A point that no longer
  crashes a run fails the sweep.
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
  - It kills a spawned destination once a chosen commit landed, before its answer reaches the
    engine: the later attempt's open finds that commit's key landed and credits it, so the run's
    report counts every commit that landed.
  - A run the test gives up on is killed with its process group, so a failure leaves nothing
    running.

  After each, every row lands once and every process of the group is gone within ten seconds.
  Remote connectors are left to the `K` clauses, which sever their connections (ADR 0022).
- **Where they run.** A test target of the engine, `crashes`, needing `failpoints`: every
  `just test` runs it, on Linux and macOS, and `just crashes` rebuilds the harness first.
  Mutation testing and coverage leave it out: it takes minutes, its runs are processes of their
  own whose profiles coverage would merge by the hundred, and it catches nothing the other tests
  do not first.

## Consequences

- A crash anywhere a commit is written, sent or heard of loses and doubles nothing, and a killed
  run leaves no connector behind, checked on real processes on every change.
- A killed process keeps what it wrote in the page cache, so real crashes cannot see a missing or
  reordered fsync. The engine writes nothing that must outlive it: the destination's commit and a
  source's slot or group file are theirs to sync, and no gate here sees a sync they miss.
- A spawned connector outlives its host only until it reads the end of its standard input or, on
  Linux, its parent's death signal; spawning happens on the runtime's worker threads, which live as
  long as the runtime, so that signal never fires early.
