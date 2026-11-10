# ADR 0055: Python

Status: accepted, 2026-11-09.

## Context

The `rdlt` Python package runs its users' code inside the engine's process: resources and
sources read on threads of their own, a secret store answers a build, and transforms take the
engine's batches. CPython ends a thread that holds or wants the interpreter when the interpreter
finishes exiting. On 3.11, 3.12 and 3.13.0 to 3.13.7 it does so with `pthread_exit`, whose forced
unwind through Rust frames, `catch_unwind` among them, aborts the process; on later versions the
thread hangs instead. An exit with a generator or a transform running is the ordinary end of a
script that started a run and returned.

## Decision

- **User Python has only Python beneath it.** Every function of a user's that Rust calls, a
  generator's `next`, a source's `check` and `partitions`, a destination's methods and its sessions'
  and table writers', a secret store, a transform, runs on a daemon thread Python started, running
  `rdlt/_reading.py`, never on a thread with Rust frames beneath the call. A read's thread is one of
  these. An async call, made from a runtime's worker, runs on a daemon thread of its own, since that
  worker runs many tasks and one that never returns must not hold up the rest; a thread that waits
  for each answer in turn, as a compute thread does, keeps one daemon thread for its calls. The
  caller holds no GIL and touches nothing of Python's: it hands the call over as closures, the
  daemon thread builds its arguments and reads its answer, and the batches a transform or a table
  writer takes, a transform answers and a destination reads back are imported and exported in
  Python, since pyarrow releases the GIL in both. `rdlt._reading` is imported when the package
  loads, so no thread imports under Rust.
- **Three cases run under Rust, and are accepted.** Methods of what a generator yields run inside
  `push` with its Rust frames beneath them: a value's `isoformat` and a UUID's `__str__`, a `list`
  subclass's `__iter__`, `__getattr__` through `hasattr`, a producer's `__arrow_c_array__` and
  `__arrow_c_stream__`, and a Python-backed Arrow reader's next batch. So do a raised exception's
  `__str__`, traceback and declared kind, code and wait, read where a failure is turned into an
  error, and the finalizers of a connector instance and of a destination's sessions and table
  writers, which run where the native side lets go of what holds them. A slow one of these at the
  instant of exit can abort the process on CPython 3.11, 3.12 and 3.13.0 to 3.13.7, as above. Moving
  them means encoding every row, rendering every failure, or releasing every connection, in Python;
  a process exiting commits nothing further, so no committed data is at risk.
- **What the native side holds of Python, it reaches weakly.** What the native side holds is opaque
  to Python's collector, so a strong reference from it to a resource, a transform, a secret store or
  a connector's instance that holds the pipeline it serves could never be collected, and the
  pipeline's connector processes would live on with it. The native side holds a function that
  reaches the object through a weak reference, and what may still need the object holds it in the
  open: the host and the builder, the pipeline, its runs and their events, and a detached run until
  it ends. A connector's instances are kept by the placement of its class that the host or the
  builder holds, never by the class, which lives as long as its module.
- **A thread that goes away is told, not inferred.** A read's end is said by `finished` or
  `failed`, and a call's by its answer; a read whose channel closes without a word fails, so a
  thread that died never reads as a finished partition.

## Consequences

An async call costs a thread start, and a compute thread's call a hand-off to the daemon thread
it keeps. pyarrow's own C++ takes no such care as this ADR does: a daemon thread building arrays
from Python sequences when the interpreter exits can abort on CPython 3.11 and 3.12 with or
without `rdlt` in the process, so a transform doing so at the instant of exit can too. Exit is
the price of an interpreter's own daemon-thread rules, the same as for any daemon thread,
whatever the interpreter's version.

A transform that never returns keeps its compute thread until the process ends, since Python
code cannot be interrupted and a transform has no cancellation to pass in. A cancelled run ends
without it, but once stuck calls reach a host's compute threads, that host's other runs stop. A
user's function that can block, on a network or a lock, needs a timeout of its own.
