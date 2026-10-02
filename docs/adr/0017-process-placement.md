# ADR 0017: Placing connectors in processes of their own

Status: accepted, 2026-09-26; where the audited `unsafe` lives and what forbids the rest are
amended by ADR 0048; lookup, trust, descriptors and output are amended by ADR 0043.

## Context

ADR 0015 split M4 in four. Its third part, M4c, held all of `rdlt-host` (§13):
- the `Provider` and `Registry`;
- process placement, supervision and respawn;
- remote placement over TCP with mTLS;
- the placement matrix, and ledger items P1–P4.

That is two transports, each with its own lifecycle and its own tests. One plan would review both
only at its end. The owner also asked for the host to be simulated over a network, which
`rdlt-sim` does not do yet.

## Decision

- **M4c is split again.**
  - **M4c** (this record): the `Provider` and `Registry`, `serve::<C>()`, process placement,
    supervision and respawn, the placement matrix's in-process and spawned legs, and P1–P4.
  - **M4d**: remote placement over TCP with mTLS, `--listen` and TLS on the served end, redialing,
    the matrix's remote leg, and a network simulation of the host under turmoil. It settles
    whether the host's timers go through `Env`.
  - **M4e** is what ADR 0015 called M4d: `rdlt-certify`, and the `K` kill matrix.
- **`serve::<C>()` is a connector binary's whole `main`** (§11.7).
  - It takes the host's socket with `--rdlt-fd N`, and serves it with its own runtime, so a
    connector's manifest lists `rdlt-connector` alone.
  - It shuts down gracefully once its standard input ends or it receives `SIGTERM`.
  - It ignores `SIGINT`: the host stops its connectors itself. Amended 2026-10-01 (ADR 0050):
    a spawned connector leads a process group of its own, which the host owns, so a terminal's
    Ctrl-C no longer reaches it, and neither does its hanging up. A host listens for `SIGINT`,
    `SIGTERM`, `SIGHUP` and `SIGQUIT` (`rdlt_host::Interrupts`) and stops what it spawned
    (`rdlt_host::stop_spawned`, or the guard `rdlt_host::StopsSpawned`) before it exits;
    `rdlt-certify` does.
  - On Linux, it asks for `SIGTERM` when its parent dies (`PR_SET_PDEATHSIG`). Standard input
    ending covers a host that died before it asked. A host killed outright runs no code: these
    two end the connector, and nothing ends what the connector started and left in its group.
  - The `#[source]` and `#[destination]` attributes implement `Serve` for the connector, so the
    binary names only its type. A binary serving two types, or a type with both roles, uses
    `serve::Served::serve()`.
- **The workspace's one `unsafe` is in one audited module** (§20.14).
  - The module is `rdlt_connector::serve::inherited`. Turning file descriptor 3 into an owned
    socket needs `OwnedFd::from_raw_fd`.
  - `adopt` makes it sound:
    - it refuses the standard streams, which the standard library owns;
    - it checks that the descriptor is an open socket;
    - it refuses a close-on-exec descriptor. The standard library opens everything
      close-on-exec, and `dup2`, through which the host passes the socket, clears the flag, so a
      descriptor with the flag set is one this process opened and owns;
    - it adopts once per process.
  - Miri runs the module's test (`just miri`, in CI's coverage job).
  - The workspace denies `unsafe` code, and every crate root but `rdlt-connector`'s forbids it.
    `cargo xtask lint` fails on `unsafe` anywhere but that module, and on a crate root that does
    not forbid it. Amended 2026-10-01: the module is a crate of its own, `rdlt-adopt`, and every
    other target's root forbids `unsafe` code, `rdlt-connector`'s included (ADR 0048).
  - The host passes the descriptor with `command-fds`, whose API is safe.
- **Providers** (§13.1).
  - A `Provider` places the source or destination a `ConnectorRef` names. A `ConnectorRef` has an
    id, the versions it accepts (a semver requirement), and a path or endpoint. The result is a
    `Placed`: the connector, its spec, its `Placement`, and the SHA-256 digest of its binary when
    it has one.
  - The trait returns boxed futures, as every engine-facing trait does, so `Registry::fallback`
    can hold any provider.
  - `ProviderError` is `NotFound`, `VersionMismatch`, `SpawnFailed` or `HandshakeFailed`, each
    with its source. `Unreachable` and `Tls` come with remote placement.
  - A version that is not semver matches only a reference that accepts any version.
- **The registry** places connectors linked into the binary in process, and every other with its
  fallback, else `NotFound` (§13.2).
- **`Local` places connectors in processes** (§13.3).
  - Resolution takes the reference's path first. Without one, it looks for
    `rdlt-connector-<the id's last segment>` in the connector directories, then on `PATH`. The
    path is made absolute, so spawning never searches `PATH` again, and a respawn runs the same
    file. Amended 2026-10-02 (ADR 0043): `PATH` is never searched, the binary is opened once
    and executed from that open file, and `Local` spawns into a sandbox unless its binaries are
    stated to be trusted.
  - The connector starts with its socket on file descriptor 3 (`--rdlt-fd=3`) and a piped
    standard input. Its environment is cleared, keeping only the variables `env_passthrough`
    names; the host sets no `RDLT_*` variables yet.
  - Standard output is drained into `tracing` as warnings, and standard error as information,
    with the connector's id and process id. A line goes in pieces of at most 8 KiB, so a
    connector that never ends a line cannot grow the host's memory.
  - The last 8 KiB of standard error, and the exit status, are kept. A transport failure of that
    connector (`connector_lost` or `transport`) carries them as its source (`LastWords`), from
    its source's and destination's calls and from its sessions' and writers'.
  - Dropping a placed connector stops it: before the drop returns its standard input closes
    and its process group receives `SIGTERM`, and after the grace period (10 s by default) the
    thread that owns the group sends `SIGKILL`. Amended 2026-10-02 (ADR 0050): the reaper is a
    thread, not a task, and the kill after the grace needs a host that is still running, so a
    host calls `rdlt_host::stop_spawned` before it exits.
  - A connector whose binary serves another id, or another version than accepted, is refused.
- **Supervision** (§13.5).
  - A connector is lost when its transport fails, it misses heartbeats, or it exits. An exit loses
    it at once.
  - The next call respawns it and handshakes again. The engine's retry of the attempt reaches the
    new process and resumes from committed state.
  - A respawned destination must declare the capabilities it first declared.
- **The placement matrix** (§20.9) runs the engine's destination-facing scenarios in two legs.
  - In process, against every reference destination, on the paused clock, as before.
  - Spawned, against the SQLite and files destinations, on the real clock (the `placement`
    module). A spawned connector's heartbeat and deadlines run on the real clock, and a paused
    clock expires them while the connector's process works.
  - The memory destination keeps its store in its process's memory, so it has no spawned leg.
  - A spawned generator feeds one scenario.
  - The spawned connectors are examples of the tests' crates, which the test build builds. The
    reference crate ships the real binaries, `rdlt-connector-{memory,generator,files,sqlite}`.
- **P1–P4** are `rdlt-host` tests against a scripted connector:
  `sigint_leaves_no_orphaned_connectors`, `connector_writing_stdout_keeps_running`,
  `connector_crash_error_carries_stderr_tail` and `env_passthrough_reaches_connector`.

## Consequences

- The output drains are tasks that outlive the call that spawned them, and each ends when its
  process does. Amended 2026-10-02 (ADR 0050): the reaper is a thread of the host, which
  outlives the runtime that spawned the connector, and stops a process whose runtime ends
  first; nothing relies on `kill_on_drop`. The groups a process owns are one list for the
  whole process: `stop_spawned` stops the connectors of every `Local` in it.
- A debug build's binaries are hundreds of megabytes, and hashing one took seconds, so `sha2` is
  optimised in the dev profile.
- The spawned leg adds about a minute of wall-clock time to the tests. Mutation testing gives it,
  and the host's process tests, 30 s a test.
- Connectors written in Python (§13.3's `python:` references) are resolved with M8.
