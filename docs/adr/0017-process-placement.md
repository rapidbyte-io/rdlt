# ADR 0017: Placing connectors in processes of their own

Status: accepted, 2026-09-26.

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
  - It leads a process group of its own, which the host owns, so neither a terminal's Ctrl-C nor
    its hanging up reaches it. A host listens for `SIGINT`, `SIGTERM`, `SIGHUP` and `SIGQUIT`
    (`rdlt_host::Interrupts`) and stops what it spawned (`rdlt_host::stop_spawned`, or the guard
    `rdlt_host::StopsSpawned`) before it exits; `rdlt-certify` does (ADR 0050).
  - On Linux, it asks for `SIGTERM` when its parent dies (`PR_SET_PDEATHSIG`). Standard input
    ending covers a host that died before it asked. A host killed outright runs no code: these
    two end the connector, and nothing ends what the connector started and left in its group.
  - The `#[source]` and `#[destination]` attributes implement `Serve` for the connector, so the
    binary names only its type. A binary serving two types, or a type with both roles, uses
    `serve::Served::serve`, given a closure that builds the roles it serves.
- **The workspace's `unsafe` code is one audited crate, `rdlt-adopt`** (§20.14; ADR 0048).
  - `serve::<C>()`, before it builds the connector's factory, turns file descriptor 3 into an
    owned socket through `rdlt_adopt::adopt`, which needs `OwnedFd::from_raw_fd` and checks what
    it can, leaving to its caller that nothing else in the process owns the descriptor (ADR
    0048's ruling):
    - it refuses the standard streams, which the standard library owns;
    - it checks that the descriptor is an open socket;
    - it refuses a close-on-exec descriptor. The standard library opens everything
      close-on-exec, and `dup2`, through which the host passes the socket, clears the flag, so a
      descriptor with the flag set is one this process opened and owns;
    - it adopts once per process.
  - Miri runs the test of `adopt` (`just miri`, in CI's coverage job).
  - Every target's root forbids `unsafe` code, and `cargo xtask lint` fails on `unsafe` anywhere
    outside `rdlt-adopt` and on a target root that does not forbid it.
  - The host passes the descriptor with `command-fds`, whose API is safe, and the child
    inherits no descriptor but its standard streams and its socket: `rdlt_adopt::inheriting_only`
    marks every other close-on-exec between `fork` and `exec` (ADR 0043).
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
- **`Local` places connectors in processes** (§13.3), in a sandbox unless its binaries are
  stated to be trusted (`Local::sandboxed`, `Local::trusting_binaries`; ADR 0043).
  - Resolution takes the reference's path first. Without one, it looks for
    `rdlt-connector-<the id's last segment>` in the connector directories alone; `PATH` and the
    working directory are never searched. The binary is opened once and hashed from that open
    file, at placement and before every spawn; on Linux it is executed from it too, so a respawn
    runs the same file. macOS cannot execute an open file: there a digest is refused and a
    trusted binary is executed by its path. A sandboxed connector whose binary was renamed over or
    removed is refused (`binary_replaced`) (ADR 0043).
  - A spawn, the binary's hash and any sandbox's attempt included, ends within the connect
    deadline. Past it the placement fails as a spawn that failed (`SpawnFailed`), and a
    respawn as a lost connector; what the spawn starts afterwards is stopped as it ends.
  - Each step of a start has a connect deadline of its own: the hash at placement, the spawn,
    the connection's setup with its handshake, the resolution of its configuration's secrets,
    and its configuration. A start whose every step is slow takes up to that many connect
    deadlines in all.
  - The connector starts with its socket on file descriptor 3 (`--rdlt-fd=3`) and a piped standard
    input. Its environment is cleared, keeping only the variables `env_passthrough` names. The host
    sets no `RDLT_*` variables; it sets glibc's malloc tunables,
    `glibc.malloc.trim_threshold=268435456` and `glibc.malloc.mmap_threshold=33554432`, in
    `GLIBC_TUNABLES`, keeping any the operator passes (in a passed `GLIBC_TUNABLES`, or glibc's
    `MALLOC_TRIM_THRESHOLD_` and `MALLOC_MMAP_THRESHOLD_`). They are a no-op where glibc is not the
    connector's allocator. The host's own process is unchanged: an operator running the host, or a
    listening connector it does not spawn, may set the same `GLIBC_TUNABLES` for that process. A
    spawned connector keeps the memory it frees (a heap is trimmed only past 256 MiB free at its
    top) and peaks higher, by up to 67 MiB (about 12 %) in the bench; an operator bounding a
    connector's memory (ADR 0043) can pass its own `GLIBC_TUNABLES`, which overrides these. The
    figures are in `docs/perf/served.md`, in "Malloc tunables for spawned connectors".
  - Standard output is drained into `tracing` as warnings, and standard error as information,
    with the connector's id and process id, and bounded: each stream is read at a megabyte a
    second after a burst of four, a line is cut at a kilobyte, and the log takes 32 lines a
    second after a burst of 256, the lines beyond counted (ADR 0043).
  - The last 8 KiB of standard error, and the exit status, are kept. A transport failure of that
    connector (`connector_lost` or `transport`) carries them as its source (`LastWords`), from
    its source's and destination's calls and from its sessions' and writers'.
  - Dropping a placed connector stops it: before the drop returns its standard input closes
    and its process group receives `SIGTERM`, and after the grace period (10 s by default) the
    thread that owns the group sends `SIGKILL`. That kill needs a host still running, so a host
    calls `rdlt_host::stop_spawned` before it exits (ADR 0050).
  - A connector whose binary serves another id, or another version than accepted, is refused.
- **Supervision** (§13.5).
  - A connector is lost when its transport fails, it misses heartbeats, or it exits. An exit loses
    it at once.
  - The next call respawns it and handshakes again. The engine's retry of the attempt reaches the
    new process and resumes from committed state.
  - A respawned destination must declare the capabilities it first declared.
- **The placement matrix** (§20.9) runs the engine's destination-facing scenarios in two legs.
  - In process, against every reference destination, on the paused clock.
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

- The output drains are tasks that end when their process does. The reaper is a thread of the
  host, which outlives the runtime that spawned the connector and stops a process whose runtime
  ends first; the groups a process owns are one list, so `stop_spawned` stops the connectors of
  every `Local` in it (ADR 0050).
- A debug build's binaries are hundreds of megabytes, and hashing one took seconds, so `sha2` is
  optimised in the dev profile.
- The spawned leg adds about a minute of wall-clock time to the tests. Mutation testing gives it,
  and the host's process tests, 30 s a test.
- Connectors written in Python (§13.3's `python:` references) are resolved with M8.
