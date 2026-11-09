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
  generator's `next`, a source's `check` and `partitions`, a secret store, a transform, runs on a
  daemon thread Python started, running `rdlt/_reading.py`, never on a thread with Rust frames
  beneath the call. A read's thread is one of these; each other call starts one through
  `calls::start`, which answers what the function returned or raised on a oneshot channel. Rust
  starts the call and reads its answer. `rdlt._reading` is imported when the package loads, so
  no thread imports under Rust.
- **Two cases run under Rust, and are accepted.** Methods of what a generator yields run inside
  `push` with its Rust frames beneath them: a value's `isoformat` and a UUID's `__str__`, a
  `list` subclass's `__iter__`, `__getattr__` through `hasattr`, a producer's
  `__arrow_c_array__` and `__arrow_c_stream__`, and a Python-backed Arrow reader's next batch.
  So do a raised exception's `__str__` and traceback, rendered in `failed` and in `called`'s
  finish. A slow one of these at the instant of exit can abort the process on CPython 3.11, 3.12
  and 3.13.0 to 3.13.7, as above. Moving them means encoding every row, or rendering every
  failure, in Python; a process exiting commits nothing further, so no committed data is at risk.
- **A thread that goes away is told, not inferred.** A read's end is said by `finished` or
  `failed`, and a call's by its answer; a read whose channel closes without a word fails, so a
  thread that died never reads as a finished partition.

## Consequences

Calling Python from Rust costs a thread start. Exit is the price of an interpreter's own
daemon-thread rules, the same as for any daemon thread, whatever the interpreter's version.
