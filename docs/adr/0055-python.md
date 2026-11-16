# ADR 0055: Python

Status: accepted, 2026-11-09.

## Context

Pipelines are described and run through one facade (ADR 0054), and code transforms run inside
the engine (ADR 0053). Python users want the same pipelines, with their own functions as sources
and transforms, and connector authors want to write connectors in Python without a second
implementation of the protocol ADRs 0038 to 0050 harden.

The `rdlt` Python package runs its users' code inside the engine's process: stream functions and
sources read on threads they hold while they read, a secret store answers a build, and transforms
take the engine's batches. CPython ends a thread that holds or wants the interpreter when the
interpreter finishes exiting. On 3.11, 3.12 and 3.13.0 to 3.13.7 it does so with `pthread_exit`,
whose forced unwind through Python running beneath Rust frames, `catch_unwind` among them, aborts
the process; PyO3 parks a thread whose own wait or attach is unwound, so that thread hangs until the
process ends, as every such thread does on later versions. An exit with a generator or a transform
running is the ordinary end of a script that started a run and returned. Exit has another half: a
thread that ends runs its libraries' per-thread teardown, OpenSSL's thread state and malloc's caches
among them, after its last Python instruction, and while the process exits that teardown races
`exit()`'s handlers and corrupts the heap. An embedder that never finalizes the interpreter, as
rdlt's own tests are, reaches those handlers moments after its last call.

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
- **The GIL is held for Python's calls alone.** A source reads each partition on a daemon thread
  it holds while it reads, which holds the GIL while Python runs and while what it yielded becomes
  pushes: dicts are encoded to JSON in Rust, within the push's bytes and the nesting limit, and
  Arrow producers are imported without a copy. A sync read waits for the engine's credit with the
  GIL released. An async read runs as one task of an event loop of its own, whose thread never
  waits for the engine: an item's push that finds no room is sent by a task of the runtime, a
  notice that finds none waits its turn, and the runtime then writes to a socket the loop
  watches, so the loop runs the read's other tasks meanwhile and wakes with no other thread taking
  the GIL. A transform runs on the compute pool, its call made on a daemon thread that holds the
  GIL for the call alone. No runtime worker waits for the GIL while a run goes on, and none takes
  it: a daemon thread hands an awaited answer to asyncio through the event loop's
  `call_soon_threadsafe`, a `StoppedError` made beforehand on the awaiting thread, and none to an
  await cancelled first. So a pipeline with no Python in it keeps committing while Python holds
  the GIL. An await whose answer no daemon thread can take waits until one can, or until it is
  cancelled, as `asyncio.wait_for` does at its timeout.
- **A row lands as its JSON says, as a Rust read's `rows` do.** A dict a read yields is written as
  JSON in Rust, and the engine types it as it types any source's JSON: `None` is null, the one
  missing value; a bool, text and a finite float are themselves; an integer is exact at any width;
  a date, datetime or time is the text its `isoformat()` answers, and a UUID its hyphenated text.
  A `Decimal` is the text of its exact digits, read through `Decimal` itself, since JSON has no
  decimal and the engine reads a number with a fraction as a 64-bit float. A whole one is text
  too, though an `Int64` or a `Decimal(p, 0)` would hold it, so a column's type never follows a
  value's exponent; `5`, `5.0` and `5E+0` are three texts, and so three merge keys. A decimal
  type a stream declares or a hint gives does not read that text: the text goes where any value
  its column cannot hold goes. A float or `Decimal` that is not finite, a date or time that is
  not equal to itself by its own `__eq__`, as pandas' `NaT` is, and a value of any other type
  fail the read, each with a code of its own, so no value lands as another. A typed decimal,
  temporal or binary column is yielded as Arrow, which lands as its types say, as a Rust read
  pushes a batch for one; Arrow is held where it lies, so the author must not write again into a
  buffer a read yielded, which the engine may still hold. A pyarrow chunked array of structs holding
  a null entry fails the read, as a batch's struct array holding one does. Another library's
  stream of struct arrays is read as batches whose own nulls arrow-rs's reader drops, and the
  extension reads no stream itself (ADR 0048), so a null entry of one lands as a row of what its
  fields hold there. pyarrow's reader refuses such a stream, but reading every stream through it
  would import every batch twice, and hold every library's stream to what the installed pyarrow
  can import.
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
  sessions, table writers and commits, are the Rust SDK's. A session's calls reach the class one
  at a time, in the order the Rust SDK gives them, and so do its table writers' where the
  destination runs one writer, as `Capabilities.minimal()` does, so such a destination needs no
  lock of its own. One whose `capabilities()` says it runs more says its table writers may be
  called at once, as the Rust SDK's served session calls them: each writer's calls come one at a
  time, in order, beside the other writers' and the session's `apply_schema`, `writer` and
  `discard_staged`, and what its writers share, with each other and with the session, is the
  author's to lock. No writer's call runs while `commit` or `close` runs, a call the engine
  stopped waiting for included, and none begins until they return. What an older session's
  abandoned calls stage is the destination's to keep from a commit, by its fence and by
  publishing only its session's own epoch. A session opened before `capabilities()` answered is
  called one at a time. A destination merges as a Rust one does, with `rdlt.connector.merge`,
  `rdlt_connector::merge` run in Rust over Arrow handed over through the C Data Interface, the
  GIL released while it merges. Its configuration is a pydantic model, whose refusals name the
  field, the fault and the kind of value, never the value.
- **One way to write a source, in the Rust SDK's words.** `rdlt.connector` names what the Rust SDK
  names, in Python's case: `ConnectorError` and its kinds, `RateLimited`'s `retry_after`, a source's
  `streams()`, answering the `ReadStream`s its connection offers, and `discover()`, answering its
  catalog's `StreamSpec`s, each stream's spec with its `Checkpointing`, a `schema`, `read_modes`, a
  `change_time` and a name within a namespace, a `StreamName`, its `Partition`s, planned from its
  `StreamState` and `PartitionState`s, or a `PartitionPlan` of phases, `committed`, `acknowledged`
  and `replayable`, a source's `acknowledged`, `Cursor` and a stream's `cursor_version`,
  `checkpoint`, `changes` and the `ChangeOp`, `OP_COLUMN`, `SEQ_COLUMN` and `UNCHANGED_COLUMN` of a
  change batch, the `ConnectContext` a connector's `__init__` takes where it names one, a
  destination's `capabilities()` answering its `Capabilities`, `OpenContext`, `WriteStats` and
  `Fence`, a merge table's `MergeKey`, `ChangeColumns`, `RootKey` and `HistoryColumns`, `to_arrow`
  of a table schema, the merge kit `rdlt.connector.merge` with `Merged`, the failure codes, such as
  `COMMIT_STALE`, and `serve`'s `args`. Where Python differs,
  the language makes it: decorators for attributes, so `stream(...)` is called at run time to make a
  stream `streams()` answers, keywords for a builder's methods, a `ChangeOp` that is its code,
  exceptions for `Result`, named as exceptions are, so `InvalidConfig`, `Unauthorized` and
  `InvalidData` are the kinds Rust calls `Config`, `Auth` and `Data`, generators for an emitter, and
  a `StreamContext`, with its `Exclusion`, `Limits` and `Headroom`, for what a Rust read is told by
  its partition and asks its emitter, given where a read names `ctx`, as a `partitions` or `plan` is
  given the `StreamState` where it names `state`. A stream without a cursor is read in full alone,
  since Python's cursor is optional and Rust's is not. A stream's schema is any Arrow schema, read
  through the C Data Interface, so no one Arrow library is required, and refused where it is
  declared where Rust has no table schema for it. Rust checks what a source answers as it checks a
  Rust source's, so a catalog, a plan or a change batch is refused with Rust's own error and code.
  How a stream's rows land is the pipeline's to say, as for a Rust source. `@stream` makes a
  function a stream as it makes a method one, under the same rules, so a function moves into a
  `@source` class taking `self`, as its `partitions=`, `plan=` and `committed=` callables then do
  too. A pipeline reads its stream functions in process, as the streams of one source,
  `io.rapidbyte.functions`, which has no configuration, and is not served, installed or certified: a
  stream that needs those is a class's.
- **A class's streams are one answer.** Its `@stream` methods are its `streams()` written as
  declarations, so a class has one or the other, and one with both is refused where it is
  declared. `streams()` is asked once a connection, as the Rust SDK assembles a connection's
  streams, and a stream it answers is called as it is, its bound methods and closures holding what
  they call; the placement keeps those streams beside the instance, and the native side reaches
  them weakly, as it reaches the instance.
- **The engine ends a Python stream's read** as it ends any source's, since only the engine knows
  the read mode (ADR 0004, ADR 0024). A stream that declares no partitions reads
  `Partition.single()`, which never ends, as a `Partition(id, unbounded=True)` it declares does: a
  full read lands everything it yields, and an incremental read lands what it yields up to its last
  checkpoint, the next run reading the rest again from there, so it ends on
  `yield checkpoint(cursor)`; one that yields rows and no checkpoint after any fails
  `checkpoint_missing`, since none of them could land. Any other partition a stream declares ends: a
  read that ends on rows finishes it, and one that ends on a checkpoint leaves it there. A stream
  without a cursor is read whole every run.
- **Served mode is the Rust `serve`**, inside the extension, with a connector binary's flags and the
  GIL released while it serves: `serve` takes a source class, a destination class or one of each,
  whose roles one process serves, as `Served` does. A served connector's code runs on Python's
  daemon threads, as an in-process one's does. SIGINT is ignored before serving: the server takes
  the signal itself and then calls the handler it found, and Python's would leave a
  `KeyboardInterrupt` to raise once the server returned. Spawned by a host, the server ignores
  Ctrl-C, since its host stops it; listening, it stops. A class `serve` is handed answers no
  certification probe, as a Rust binary's plain factory answers none (ADR 0044); handed as
  `acknowledging_source_factory(C)` or `readable_destination_factory(C)`, it answers those its class
  can, as a Rust binary's `main` serving those factories does. The installed launcher refuses one,
  alone or in a pair, as a Rust release serves no probe.
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
  unobserved fails its test. Given a connector's id and endpoint and the host's TLS, they certify
  the connector listening there over mutual TLS, as rdlt-certify's listening target does.
- **User Python has only Python beneath it.** Every function of a user's that Rust calls, a
  generator's `next`, a source's `check`, `streams` and `discover`, a stream's `partitions`, `plan`,
  `committed` and `acknowledged`, a destination's methods and its sessions' and table writers', a
  secret store, a transform, runs on a daemon thread Python started, running `rdlt/_reading.py`,
  never on a thread with Rust frames beneath the call. Each call, a partition's read among them, is
  made on one of the process's daemon threads: the one that went idle last, which has the call to
  itself, since a worker runs many tasks and one that never returns must not hold up the rest. Each
  call starts in a context of its own, as on a fresh thread. Thread-local state, by contrast, is the
  thread's, as in any pool. A call that takes the last idle one has it start another before it
  calls, so no caller waits for a thread to start. A daemon thread lets go of what a call held, goes
  back on the pool and then hands the answer over, and none ever ends, so none is tearing down when
  the process exits: a user's `SystemExit` or `KeyboardInterrupt` is its call's answer, and a thread
  a bug of rdlt's own escapes answers the call it held and waits for good. Only threads Python owns
  start them: the thread that first asks for the runtime or serves, and the daemon threads
  themselves, each saying as text where one did not start. A child forked from a process that had
  started its own starts afresh. The caller holds no GIL and touches nothing of Python's: it hands
  the call over as closures, the daemon thread builds its arguments and reads its answer, and the
  batches a transform or a table writer takes, a transform answers and a destination reads back are
  imported and exported in Python, since pyarrow releases the GIL in both. The merge kit's
  arguments are exported in Python, each by its own `__arrow_c_array__`, `__arrow_c_stream__` or
  `__arrow_c_schema__`, so no method of the argument itself runs beneath Rust there, though a
  Python-backed stream's reader does. `rdlt._reading` is imported when the package loads, so no
  thread imports under Rust.
- **Five cases run under Rust, and are accepted.** Methods of what a generator yields run inside
  `push` with its Rust frames beneath them: a date's or a time's `__eq__` and `isoformat` and a
  UUID's `__str__`, a `list` subclass's `__iter__`, `__getattr__` through `hasattr`, a producer's
  `__arrow_c_array__` and `__arrow_c_stream__`, a pyarrow chunked array's `type` and `null_count`
  and pyarrow's `types.is_struct`, and a Python-backed Arrow reader's next batch. So do a
  Python-backed stream's next batch, its schema and its release, read in a merge kit's call. So do a
  raised exception's `__str__`, traceback and declared kind, code and wait, read where a failure is
  turned into an error, and the finalizers of a connector instance and of a destination's sessions
  and table writers, which run where the native side lets go of what holds them. So does a user's
  audit hook, which `_thread.start_new_thread`'s audit event calls as a daemon thread starts the
  next. A slow one of these at the instant of exit can abort the process on CPython 3.11, 3.12 and
  3.13.0 to 3.13.7, as above. Moving them means encoding every row, reading every stream into
  batches, rendering every failure, releasing every connection, or starting every thread, in Python;
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
  `rdlt connector install` makes one virtual environment per connector, its tree, with the
  interpreter the operator names, in the first connector directory, and writes the launcher,
  `bin/rdlt-connector`, which imports the wheel's one `rdlt.connectors` entry point and serves it,
  run by the environment's interpreter with `-I`, so no `PYTHON*` variable, user site-packages or
  script directory reaches it. The tree holds the connector's packages and no pip. It is made
  beside the connectors, one install at a time under a lock that also lets the next sweep what a
  killed one left, held until what the install made beside the tree is removed, and exchanged
  with the tree it replaces in one step, so one of the two is in place throughout; a file system
  that cannot exchange two names is found before anything is made. Its write is taken away from
  group and others, it records its connector's id, and an install replaces only a tree that
  records its own (`name_taken` otherwise). The host
  hashes the launcher, whose first line names the interpreter, and holds the interpreter by its
  path, as any script's; the installed packages are the operator's files, as a native binary's
  shared libraries are. The environment's interpreter is a link to its base installation, which a
  sandbox shows only in the system's directories, so on Linux an install takes an interpreter
  there alone, whether the host sandboxes its connectors or trusts them, since a tree serves
  either host. A connector the host spawns needs nothing written but its scratch directory and
  `/dev/shm`. An entry point names a class, or a tuple of a source class and a destination class,
  whose roles the launcher serves in one process.
- **No interpreter an install starts imports from the tree.** No code of the connector's, nor of
  a package it needs, runs as the operator while it is installed, and no list of what not to
  import could keep it so: an interpreter of the environment runs each `.pth` file's import lines
  and imports `sitecustomize` as it starts, with `-I` as without, and any interpreter whose path
  holds the tree imports from it whatever it asks for by name, pip's own modules or one pip
  imports. The install starts these processes, each as the operator:
  - the base interpreter with `-I -S`, which says what it is, then reads the wheel's entry point
    and hash from its archive as data;
  - the base interpreter's `venv`, with `-I`, which makes the tree with no pip, and beside it, in
    the install's own directory, the environment pip runs from, whose interpreter then runs
    `ensurepip` to install the pip the base installation carries;
  - that environment's pip, with `-I`, which installs into the tree as its prefix, the tree never
    on its path, and starts nothing from it.

  venv and pip run in the install's own directory and are given of the operator's environment
  only where programs, the home and temporary files are, the locale, the proxies and the
  certificate authorities an index may need, and `PIP_CONFIG_FILE` naming `/dev/null`. Nothing in
  the tree is started, its interpreter included, and after pip the tree is read as data alone: a
  package that replaced a link of its `bin/`, or the `pyvenv.cfg` its interpreter reads, the
  tree's, or one beside it in `bin/`, which it would read first, or that added a `._pth` file
  there, which would set its path, is refused (`replaces_environment`), so the interpreter stays
  venv's link to the base installation. An interpreter named by a relative path is found from
  where the install was asked, since venv and pip run in a directory of their own. pip
  reads none of its configuration, neither its variables (`--isolated`) nor its files, as
  `ensurepip` reads none: what is installed, and where, is what the install's command line and
  its requirements say. The wheel is copied once into a directory of the install's own, and the
  copy is what is read, hashed and installed, named to pip by its `file:` URL, so no index is
  asked for it. What pip and venv print reaches standard error a line at a time, each shown as
  connector text is, since it holds what a wheel and an index say.
- **Nothing pip installs is built from its source.** A build backend is a package's own code,
  run as whoever builds it. pip's `--only-binary=:all:` holds the files it finds in an index or a
  directory of links, which it judges by their names, but not a requirement it reads as a URL or
  a path, which it builds whatever it is, and it reads a requirement's text as a path from its
  working directory where that holds a `/`, and as an archive's name where it ends as one does.
  So every way such a requirement could reach pip is closed:
  - the operator's wheel is taken only where its file's name is a wheel's,
    `{name}-{version}(-{build})?-{python}-{abi}-{platform}.whl`, the name and tags of ASCII
    letters, digits, `.` and `_`, the name starting with no `.`, the version PEP 440's, since pip
    reads any other as a source to build. Its copy is named to pip by its `file:` URL, which rdlt
    writes from the copy's absolute path, with no `..` in it, quoting every byte but letters,
    digits and `-._~`; the same check names it on pip's command line and in the requirements
    file. That line is the one requirement holding a `/`: pip finds nothing of its text as a
    path in the install's own directory, where it runs, and reads it as that URL;
  - the wheel's metadata, read as data, is refused where it needs a package by a URL (an `@`
    before a requirement's markers), and what it needs comes from an index, whose packages pip
    refuses where they need one by a URL;
  - a requirements file is read in rdlt's own grammar and written again as the file pip reads. A
    line names a wheel by its version, `name[extras]==version`, with its hash,
    `--hash=sha256:...` (or `sha384`, `sha512`), and markers where it needs them; or it names
    where wheels are found, `--index-url` (`-i`), `--extra-index-url`, `--find-links` (`-f`) or
    `--no-index`, a relative path taken from the file's directory. A wheel at a URL is named so,
    by `--find-links` and its version: pip judges what it finds there as it judges an index's
    files, and takes a source from neither, nor a wheel the file does not hash. A direct
    reference, `name @ URL`, is refused. Lines join and comments end as pip has them. Anything
    else is refused, naming its line (`requirements`): another file (`-r`, `-c`), an editable
    (`-e`), `--no-binary`, `--only-binary`, `--trusted-host`, which would have pip trust a host
    without checking its certificate, `--pre` and every other option, and `${...}`, since no
    variable is expanded, an index's credentials coming from `.netrc`. The file holds printable
    ASCII, spaces and tabs alone, its lines ending in `\n` or `\r\n`: pip breaks lines where
    Python's `splitlines` does, at `\r`, `\x1c` and `U+2028` among others, so any other
    character is refused, naming it, lest one line rdlt checked be two pip reads. Each part is
    read in its own grammar: PEP 508's for names and extras, PEP 440's for versions, RFC 3986's
    for an option's URL, a SHA-2 digest of its length in hex for hashes, and PEP 508's for
    markers, a variable of the environment compared with a quoted value, joined by `and` and
    `or` and nested in parentheses, a value holding letters, digits and `._+*,!~<>=-` alone and
    starting with no `-`. The file pip reads is written from what was read. No requirement rdlt
    writes but the wheel's own holds a `/` or a `\`, nor ends, as pip's `splitext` reads it with
    a `.tar` before, in any case, in an archive's extension, so pip reads none as a path or an
    archive; each requirement's line splits, at its spaces as pip splits it, into the
    requirement and the hashes checked, no other word starting with `-`, and each option's line,
    as a shell splits it, into the option and the value checked, neither holding a `#` that
    starts a comment or a `${` pip would expand. A test sends every line rdlt's tests write, and
    a few hundred drawn from its grammar, through pip's own parser and its reading of each
    requirement, and holds it to what rdlt checked;
  - with a requirements file, pip installs only the packages it hashes, the wheel among them, and
    refuses, before it builds or installs anything, a dependency the file does not hash, one a
    package found there needs by a URL among them, so every package in the tree is one the
    operator hashed.
- **A thread that goes away is told, not inferred.** A read's end is said by `finished` or
  `failed`, and a call's by its answer; a read whose channel closes without a word fails, so a
  thread that died never reads as a finished partition.

Rejected:
- **A kind of source of its own for functions**, with its own words and rules: a function moved
  into a connector class would read differently, and would be rewritten to become a connector.
- **A Python implementation of the protocol.** It would be a second implementation of what ADRs
  0038 to 0050 harden.
- **Two packages, the certifier apart.** One wheel is what a user installs, and a served class
  answers no probe unless its launcher's own code asks for it.
- **orjson for rows.** Encoding in Rust keeps the limits where the push is made, and adds no
  dependency.
- **A `Decimal` as a JSON number**, which the engine reads as a 64-bit float, rounding what a
  double does not hold and typing the column by how the value is written. Refusing one with a
  fraction instead fails the reads of every database's numeric column, where its text keeps every
  digit. Typing a decimal from a JSON push would change, for every source, how JSON's numbers land
  (ADRs 0007 and 0026).
- **A whole `Decimal` as an integer and any other as text**, which keeps a whole one's integer
  type: a column's type would follow each value's exponent, so a column of `Decimal("5")` and
  `Decimal("5.5")` would become JSON, or split into a variant across loads.
- **A declared or hinted decimal column reading a `Decimal`'s text.** A column takes a value of
  another type only where its type holds that type as it is (the resolver's `place` and
  `splits`), so it would need fitting by value, and an exact parser of decimal text, since
  Arrow's cast rounds digits past the scale and refuses an exponent. It is the engine's to add, if
  ever, for every value JSON carries as text, dates, times and UUIDs among them, not this
  binding's.
- **Waking an async read through a daemon thread and `call_soon_threadsafe`**, as an awaited
  answer is handed over: every wait would take a daemon thread and the GIL, which a busy loop
  holds, so the read would fall behind the engine it waits for.
- **pyo3-async-runtimes for awaitables.** It hands an awaited answer over by taking the GIL on the
  runtime's blocking pool, and given a runtime whose blocking pool were a daemon thread, the
  closure it hands over would still call asyncio beneath that thread's Rust frames.
- **Running the tree's pip, or any interpreter of the tree, while it is installed**, with `-S` or
  its packages' directory put on the path by hand: the tree is what the wheel wrote, so its
  interpreter, its `pyvenv.cfg`, its pip and every module on its path are the wheel's to replace.
- **Checking the installed packages with `pip check`.** It needs pip to import from the tree;
  given every dependency to resolve and the requirements' hashes, pip refuses one that is missing
  or unhashed before it installs anything.
- **Reading pip's index settings and ignoring the rest.** What pip reads of its configuration
  changes with its version, so a list of what to ignore would miss some; an index is named in the
  requirements file instead.
- **Giving pip the operator's requirements file as written**, or a list of its options to
  refuse: pip's own grammar reads further files, expands variables and takes options that let it
  build from source, so a connector's own requirements file would be a way to run its code.
- **Running pip in the sandbox.** Only Linux has one, and the install is the same on macOS.
- **A wheel named in a requirements file by its URL**, a URL rdlt would check: pip reads such a
  requirement's text as a path from its working directory, and decides a wheel by its own
  reading of the URL's last segment, so a URL rdlt took could have pip build a source.
  `--find-links` leaves the choice to pip's own judgement of what it finds, which takes no
  source.
- **Replacing a tree with two renames.** An install ended between them, by a signal or a crash,
  leaves no tree, and the next install would sweep away the old one.

## Consequences

On Linux a connector cannot be installed with an interpreter outside the system's directories,
such as uv, pyenv and mise install beneath a home directory: the sandbox would not show the base
installation its environment links to, and `rdlt connector install` refuses it
(`interpreter_unreachable`). Such a connector is served without an install, `--listen` from an
environment holding its wheel. An interpreter installed in the connector's own tree, a standalone
install, would lift that refusal.

An installed tree holds no pip, and its packages' scripts name the environment pip ran from,
which is gone: its packages are read as files, `lib/python3.N/site-packages/*.dist-info`, and
anything run from it by hand, its interpreter included, runs the connector's code with the
operator's access, outside any sandbox. Any index but PyPI, and any links, are named in a
requirements file, whose every package is then hashed, a connector's dependencies being wheels
alone. An index's credentials come from `.netrc`, and pip sends its `default` entry, where it has
one, to every index host the file names. Replacing a tree needs a file system that exchanges two
names in one step, as ext4, XFS, Btrfs, tmpfs and APFS do; on one that cannot, such as vfat,
exFAT or OpenZFS before 2.2, which answer the exchange `EINVAL`, `EOPNOTSUPP` or `ENOSYS`, the
install says so before it makes anything, and the tree is removed by hand before its connector
is installed again; any other failure of the exchange is told as I/O.

A directory connector's digest pins its launcher, not what the launcher runs: the interpreter
its first line names is held by its path, and the packages beside it are the operator's files.
An interpreter upgraded in place, or a package the operator changes in the tree, changes what
runs under the same digest, as a native binary's shared libraries do.

A process forked after the runtime started is refused in the child with `rdlt.InternalError`, which
names multiprocessing's spawn and forkserver methods. A caller waits for a call's answer through
`parking_lot`, since std's waits park on a dispatch semaphore on macOS, which a forked child cannot
use where its thread made one before the fork. The wheel carries rdlt-certify's code, and a served
connector serves a probe only where its launcher's own code asks for it. The nightly workflow tests
the wheel on CPython 3.11 to 3.15 and on free-threaded 3.14, on Linux and macOS.

A commit's barrier waits, up to its limit, for every partition still reading that checkpoints
when asked to, as the generator does, so within one pipeline a transform that runs long on such a
partition's flush, in Python or not, holds back the pipeline's commits until it returns or the
barrier's wait ends; the GIL is not what holds them.

A stream that never checkpoints is read as one segment, so its run holds more of it at once.
On Linux, glibc gives each thread that allocates, every daemon thread and every host's compute
threads, an arena of its own, and an arena keeps what it freed: across such runs, each on a host
of its own, resident memory grows while what is in use stays flat. Over six runs of 10,000 batches,
malloc's bytes in use stayed between 10.5 and 10.8 MiB while its free bytes grew from 163 to
330 MiB; with `MALLOC_ARENA_MAX=2` they stayed under 93 MiB, and checkpointing every hundred
batches kept them under 41 MiB. `MALLOC_ARENA_MAX` bounds it where a process makes many such
runs.

An async read holds a socket pair while it reads, and needs an event loop that watches a socket,
through `add_reader`, as asyncio's own loops and uvloop do; on one that cannot, the read fails.

A call costs a hand-off to an idle daemon thread. The process keeps as many daemon threads as
calls it ever made at once, and one more. pyarrow's own C++ takes no such care as this ADR does:
a daemon thread building arrays from Python sequences when the interpreter exits can abort on
CPython 3.11 and 3.12 with or without `rdlt` in the process, so a transform doing so at the
instant of exit can too. Exit is the price of an interpreter's own daemon-thread rules, the same
as for any daemon thread, whatever the interpreter's version: on 3.11 to 3.13.7, a daemon thread
running Python as the interpreter finalizes is ended by it, and its teardown runs then.

A transform that never returns keeps its compute thread and its daemon thread until the process
ends, since Python code cannot be interrupted and a transform has no cancellation to pass in. A
cancelled run ends without it, but once stuck calls reach a host's compute threads, that host's
other runs stop. A user's function that can block, on a network or a lock, needs a timeout of its
own.
