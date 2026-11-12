# ADR 0055: Python

Status: accepted, 2026-11-09.

## Context

Pipelines are described and run through one facade (ADR 0054), and code transforms run inside
the engine (ADR 0053). Python users want the same pipelines, with their own functions as sources
and transforms, and connector authors want to write connectors in Python without a second
implementation of the protocol ADRs 0038 to 0050 harden.

The `rdlt` Python package runs its users' code inside the engine's process: stream functions
and sources read on threads of their own, a secret store answers a build, and transforms take the
engine's batches. CPython ends a thread that holds or wants the interpreter when the interpreter
finishes exiting. On 3.11, 3.12 and 3.13.0 to 3.13.7 it does so with `pthread_exit`, whose forced
unwind through Rust frames, `catch_unwind` among them, aborts the process; on later versions the
thread hangs instead. An exit with a generator or a transform running is the ordinary end of a
script that started a run and returned.

## Decision

- **One wheel binds the facade.** `rdlt` is a mixed project: the extension `rdlt._native`
  (`crates/rdlt-python`, PyO3) and the typed package `python/rdlt`, which is the public API: the
  facade's words as keyword arguments, enums, frozen dataclasses for events, reports and
  statuses, and `rdlt.Error` with one subclass per kind. The extension passes JSON text, and
  Arrow through the C Data Interface; it holds no pipeline model of its own. A pipeline built in
  code is written as the document a file would hold, so code and documents meet one validation
  and one set of errors. The wheel is abi3, for CPython 3.11 and later, with a free-threaded
  3.14 wheel beside it, since free-threaded CPython loads no abi3 wheel, for Linux
  (manylinux_2_28, x86-64 and aarch64) and macOS (arm64 and x86-64). ADR 0048 records how the
  release builds and publishes them.
- **Python code in process is the embedder's.** A stream function, a transform, a secret store
  and a connector class placed in process run in the embedding process, trusted as its own code is
  (ADR 0037); a document names a transform or a connector class only once its host registered
  it.
- **The GIL is held for Python's calls alone.** A source reads each partition on a thread of its
  own, which holds the GIL while Python runs and while what it yielded becomes pushes: dicts are
  encoded to JSON in Rust, within the push's bytes and the nesting limit, and Arrow producers are
  imported without a copy. It waits for the engine's credit with the GIL released, and reads an
  async generator as one task of an event loop of its own. A transform runs on the compute pool,
  its call made on a daemon thread that holds the GIL for the call alone. No runtime worker waits
  for the GIL while a run goes on: an awaited answer is handed to asyncio from the runtime's
  blocking pool, and a worker takes the GIL only to raise `StoppedError` for an await whose run
  was cancelled. So a pipeline with no Python in it keeps committing while Python holds the GIL.
- **Exceptions cross whole.** A Python exception becomes the connector's or the transform's
  error: its type and message, shown and bounded as connector text, with its traceback as its
  cause. A connector says what kind of failure it met by raising `rdlt.connector.ConnectorError`
  or one of its kinds (`Transient`, `RateLimited` with its `retry_after`, `InvalidConfig`,
  `Unauthorized`, `InvalidData`, `Unsupported`), whose message is said as written, with no
  traceback; `TimeoutError` and `ConnectionError` are transient, and any other exception is
  internal.
- **The SDK is the Rust SDK.** A Python connector class, which `@source` or `@destination` makes,
  is wrapped as an `rdlt-connector` source or destination described at run time, so barriers,
  checkpoints, frame sizing, limits, credit, heartbeats and deadlines, and a destination's
  sessions, table writers and commits, are the Rust SDK's. A session's calls, its table writers'
  included, reach the class one at a time, as the Rust SDK orders them, so an author's destination
  needs no lock of its own. Its configuration is a pydantic model, whose refusals name the field,
  the fault and the kind of value, never the value.
- **One way to write a source, in the Rust SDK's words.** `rdlt.connector` names what the Rust SDK
  names, in Python's case: `ConnectorError` and its kinds, `RateLimited`'s `retry_after`, a
  `ReadStream` that `@stream` makes, its `Partition`s, `committed`, `acknowledged` and `replayable`,
  a source's `acknowledged`, `Cursor` and a stream's `cursor_version`, `checkpoint`, the
  `ConnectContext` a connector's `__init__` takes where it names one, a destination's
  `Capabilities`, `OpenContext`, `WriteStats` and `Fence`, the failure codes, such as
  `COMMIT_STALE`, and `serve`'s `args`. Where Python differs, the language makes it: decorators for
  attributes, exceptions for `Result`, named as exceptions are, so `InvalidConfig`, `Unauthorized`
  and `InvalidData` are the kinds Rust calls `Config`, `Auth` and `Data`, generators for an emitter,
  and a `StreamContext` for what a Rust read is told by its partition and asks its emitter. How a
  stream's rows land is the pipeline's to say, as for a Rust source. `@stream` makes a function a
  stream as it makes a method one, under the same rules, so a function moves into a `@source` class
  taking `self`, as its `partitions=` and `committed=` callables then do too. A pipeline reads its
  stream functions in process, as the streams of one source, `io.rapidbyte.functions`, which has no
  configuration, and is not served, installed or certified: a stream that needs those is a class's.
- **The engine ends a Python stream's read** as it ends any source's, since only the engine knows
  the read mode (ADR 0004, ADR 0024). A stream that declares no partitions reads
  `Partition.single()`, which never ends, as a `Partition(id, unbounded=True)` it declares does: a
  full read lands everything it yields, and an incremental read lands what it yields up to its last
  checkpoint, the next run reading the rest again from there, so it ends on
  `yield checkpoint(cursor)`; one that yields rows and no checkpoint after any fails
  `checkpoint_missing`, since none of them could land. Any other partition a stream declares ends: a
  read that ends on rows finishes it, and one that ends on a checkpoint leaves it there. A stream
  without a cursor is read whole every run.
- **Served mode is the Rust `serve`**, inside the extension, with a connector binary's flags and
  the GIL released while it serves. A served connector's code runs on Python's daemon threads,
  as an in-process one's does. SIGINT is ignored before serving: the server takes the signal
  itself and then calls the handler it found, and Python's would leave a `KeyboardInterrupt` to
  raise once the server returned. Spawned by a host, the server ignores Ctrl-C, since its host
  stops it; listening, it stops. Served factories answer no certification probe, though the
  wheel carries the certifier for its pytest plugin.
- **A blocking run answers Ctrl-C as the CLI does**: the first stops the run after its next
  commit and the call waits on; a second cancels the run and raises `KeyboardInterrupt`. A
  cancelled await of a run asks it to stop after its next commit and raises at once, as asyncio
  has a cancelled await do, and the run finishes in the background. Under `asyncio.run` the
  first Ctrl-C is that cancellation, so the process exits without waiting for the commit: what
  was committed stays, and the next run resumes from the last committed cursor.
- **Certification runs in pytest**: `certify_source` and `certify_destination` make one test per
  clause, over an in-process socket pair, a destination read back through its `published` and a
  source that says it tells where it stands asked through its streams' `acknowledged`, the kill
  clause spawning the installed launcher where one is given; a clause that applies and went
  unobserved fails its test.
- **User Python has only Python beneath it.** Every function of a user's that Rust calls, a
  generator's `next`, a source's `check`, a stream's `partitions`, `committed` and `acknowledged`, a
  destination's methods and its sessions' and table writers', a secret store, a transform, runs on a
  daemon thread Python started, running `rdlt/_reading.py`, never on a thread with Rust frames
  beneath the call. A read's thread is one of these. An async call, made from a runtime's worker,
  runs on a daemon thread of its own, since that worker runs many tasks and one that never returns
  must not hold up the rest; a thread that waits for each answer in turn, as a compute thread does,
  keeps one daemon thread for its calls. The caller holds no GIL and touches nothing of Python's: it
  hands the call over as closures, the daemon thread builds its arguments and reads its answer, and
  the batches a transform or a table writer takes, a transform answers and a destination reads back
  are imported and exported in Python, since pyarrow releases the GIL in both. `rdlt._reading` is
  imported when the package loads, so no thread imports under Rust.
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
  to Python's collector, so a strong reference from it to a stream function, a transform, a secret
  store or a connector's instance that holds the pipeline it serves could never be collected, and
  the pipeline's connector processes would live on with it. The native side holds a function that
  reaches the object through a weak reference, and what may still need the object holds it in the
  open: the host and the builder, the pipeline, its runs and their events, and a detached run until
  it ends. A connector's instances are kept by the placement of its class that the host or the
  builder holds, never by the class, which lives as long as its module.
- **A Python connector a host spawns is a directory connector with an environment of its own.**
  `rdlt connector install` makes one virtual environment per connector, with the interpreter the
  operator names, in the first connector directory, and writes the launcher,
  `bin/rdlt-connector`, which imports the wheel's one `rdlt.connectors` entry point and serves it,
  run by the environment's interpreter with `-I`, so no `PYTHON*` variable, user site-packages or
  script directory reaches it. No code of the connector's runs as the operator while it is
  installed: `-I` keeps `site`, which runs a `.pth` file's import lines whenever an interpreter of
  the environment starts, so the entry point is read from the wheel's archive by the base
  interpreter with `-S`, pip installs the wheel and what it needs in one run that starts before
  any of it is there, binaries only so no build backend runs, every package and the wheel itself
  hashed where a requirements file is given, and the check that nothing the wheel needs is
  missing runs with `-S`, its packages' directory put on the path by hand. The wheel is named to
  pip by its `file:` URL, so no index is asked for it; the requirements, which an index serves,
  keep one from being given for the run. The tree is made beside the connectors, one install at
  a time under a lock that also lets the next sweep what a killed one left, and renamed into
  place, its write taken away from group and others; it records its connector's id, and an
  install replaces only a tree that records its own (`name_taken` otherwise). The host hashes the
  launcher, whose first line names the interpreter, and holds the interpreter by its path, as
  any script's; the installed packages are the operator's files, as a native binary's shared
  libraries are. The environment's interpreter is a link to its base installation, which a
  sandbox shows only in the system's directories, so on Linux an install takes an interpreter
  there alone, whether the host sandboxes its connectors or trusts them, since a tree serves
  either host. A connector the host spawns needs nothing written but its scratch directory and
  `/dev/shm`.
- **A thread that goes away is told, not inferred.** A read's end is said by `finished` or
  `failed`, and a call's by its answer; a read whose channel closes without a word fails, so a
  thread that died never reads as a finished partition.

Rejected:
- **A kind of source of its own for functions**, with its own words and rules: a function moved
  into a connector class would read differently, and would be rewritten to become a connector.
- **A Python implementation of the protocol.** It would be a second implementation of what ADRs
  0038 to 0050 harden.
- **Two packages, the certifier apart.** One wheel is what a user installs, and served factories
  answer no probe by construction.
- **orjson for rows.** Encoding in Rust keeps the limits where the push is made, and adds no
  dependency.

## Consequences

On Linux a connector cannot be installed with an interpreter outside the system's directories,
such as uv, pyenv and mise install beneath a home directory: the sandbox would not show the base
installation its environment links to, and `rdlt connector install` refuses it
(`interpreter_unreachable`). Such a connector is served without an install, `--listen` from an
environment holding its wheel. An interpreter installed in the connector's own tree, a standalone
install, would lift that refusal.

A directory connector's digest pins its launcher, not what the launcher runs: the interpreter
its first line names is held by its path, and the packages beside it are the operator's files.
An interpreter upgraded in place, or a package the operator changes in the tree, changes what
runs under the same digest, as a native binary's shared libraries do.

A process forked after the runtime started is refused in the child with `rdlt.InternalError`,
which names multiprocessing's spawn and forkserver methods. The wheel carries rdlt-certify's
code, and a served connector never serves its probes. The nightly workflow tests the wheel on
CPython 3.11 to 3.15 and on free-threaded 3.14, on Linux and macOS.

A commit's barrier waits, up to its limit, for every partition still reading that checkpoints
when asked to, as the generator does, so within one pipeline a transform that runs long on such a
partition's flush, in Python or not, holds back the pipeline's commits until it returns or the
barrier's wait ends; the GIL is not what holds them.

A stream that never checkpoints is read as one segment, so its run holds more of it at once.
On Linux, glibc gives each thread that allocates, every read's and every host's compute threads,
an arena of its own, and an arena keeps what it freed: across such runs, each on a host of its
own, resident memory grows while what is in use stays flat. Over six runs of 10,000 batches,
malloc's bytes in use stayed between 10.5 and 10.8 MiB while its free bytes grew from 163 to
330 MiB; with `MALLOC_ARENA_MAX=2` they stayed under 93 MiB, and checkpointing every hundred
batches kept them under 41 MiB. `MALLOC_ARENA_MAX` bounds it where a process makes many such
runs.

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
