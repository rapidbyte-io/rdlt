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
  names a point at each durability step: a log append, either side of the lanes' flush and of the
  commit frame's fsync, before a forgetting source hears of a logged commit, either side of the
  destination's commit, after the log records the receipt, either side of a source hearing of a
  landed commit, and either side of a replayed commit. A point configured through `FAILPOINTS`
  aborts the process there: no unwinding, no flush, no goodbye to a connector.
- **The harness stands for the CLI** until M7 has one: the engine's `crash_run` example runs one
  pipeline from a configuration file, its source and destination in its process or spawned, its
  log in a local directory, and exits 0 once the run succeeded. When the CLI exists, the sweep
  and the matrix run it instead.
- **State outlives a process only on disk.** The reference change source keeps its slot, and the
  log source its group, in a file where configured (`slot_path`, `group_path`), written whole
  beside it and renamed over it, so a crash leaves it as it was or as it is; a damaged file is
  refused at connect rather than read as empty. A source that forgets what it acknowledged can
  then be crashed, killed and spawned again as a replication slot or a consumer group would.
- **The sweep** crashes four pipelines, each with a log: a log that forgets appended to SQLite, a
  change stream that forgets merged into JSON-lines files, a full read replacing a SQLite table,
  and a timed change stream kept as history in SQLite. Every point must crash every pipeline, at
  its first hit and its third, and a commit left by a crash is replayed through two more crashes,
  before and after it lands; the next run must load the table exactly as the reference models
  say, with no log left. A point that no longer crashes a run fails the sweep.
- **The kill matrix** kills the harness at a moment drawn from a seed (`RDLT_KILL_SEED`, else the
  clock) in a process group of its own, and kills a spawned source or destination before a chosen
  commit through the host's `Kills`. After each, the pipeline runs again until it succeeds, every
  row lands once, and every process of the group is gone within ten seconds; most drawn kills
  must land as the pipeline loads. Remote connectors are left to the `K` clauses, which sever
  their connections (ADR 0022).
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
- A spawned connector outlives its host only until it reads the end of its standard input or, on
  Linux, its parent's death signal; spawning happens on the runtime's worker threads, which live as
  long as the runtime, so that signal never fires early.
