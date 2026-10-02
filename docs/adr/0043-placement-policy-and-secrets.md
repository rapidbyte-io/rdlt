# ADR 0043: Placement as policy, a sandbox for local connectors, and secrets

Status: accepted, 2026-10-02.

## Context

ADR 0037 holds a connector to be untrusted code, and says where one may run and what it is
given. The host did not enforce it:

- A reference's digest, endpoint and path were ignored by a provider that did not implement
  them, and a raw wire spawned a binary without comparing its digest.
- A local connector ran with its host's own access. It was looked up on `PATH`, the working
  directory included when `PATH` was unset or held an empty entry; its binary was hashed by
  path and then executed by path; it inherited every descriptor of the host that was not
  closed on exec; and its output became one log event a line, raw and without a bound.
- A configuration was plain JSON that the host kept and copied, a secret was a literal in it,
  configuration errors quoted the values they refused, and whatever a connector said, its
  secrets included, reached errors, logs and reports as it wrote it.

## Decision

- **A reference states requirements, and a provider honours each or refuses.** A
  `ConnectorRef` names an id and may name a version, a path, an endpoint, a digest and an
  `Isolation` (`Process`, `Sandbox`, `Remote`). `ProviderError::Unsupported`
  (`placement_unsupported`) refuses a requirement the placement does not implement, before
  anything is spawned, dialed or connected, on a raw wire too.
  - In process honours none of them, and its names say what it is: `Registry::trusted`,
    `trusted_source`, `trusted_destination`.
  - Remote honours an endpoint and `Isolation::Remote`. The name a connector's certificate
    must carry is the endpoint's host: a reference has no second name for it.
  - Local honours a path, a digest where the platform executes an open file, and
    `Isolation::Process`; `Isolation::Sandbox` only when it was built with a sandbox.
  - A stream a function opens honours none.
  - Every `ProviderError` has a stable `code()`.
- **A local connector runs in a sandbox, or its operator says its binaries are trusted.**
  `Local::sandboxed(sandbox)` and `Local::trusting_binaries()` are the only constructors.
  - `Sandbox` is a trait: given the descriptor the program is open at, its arguments, its
    whole environment, the descriptor of its socket and its `Grants` (paths to read, paths to
    write, the network or not), it answers the command to run and how the connector is asked
    to stop.
  - `Bubblewrap` is the sandbox rdlt ships, for Linux. The connector sees the host's system
    directories read only, a `/tmp`, `/proc` and `/dev` of its own, and what it was granted;
    no home directory, no network unless granted, its own user, process, IPC, host-name and
    control-group namespaces; the environment the host states and the `PWD` the launcher
    sets; a session of its own; and it ends with its host. The launcher is found at an
    absolute path, `/usr/bin/bwrap` or one configured, never by name. Whether it makes a
    sandbox is tried once, by confining the launcher itself: a launcher that is missing
    (`sandbox_missing`) or makes none, as where unprivileged user namespaces are off
    (`sandbox_unavailable`), refuses the placement.
  - Outside Linux there is no sandbox (`sandbox_unsupported`): untrusted connectors run
    remotely there, and only binaries stated to be trusted run locally.
  - Bubblewrap ends at `SIGTERM` and its sandbox with it, so a sandboxed connector is asked
    to stop by the end of its input alone and killed, after its grace, through the launcher:
    the process namespace ends with its first process, which ends with the launcher. What a
    connector started is therefore killed with it even where it left its process group.
  - The thread that owns a connector's process group spawns the connector, so a
    parent-death signal, the launcher's or the connector's own, follows the thread that lives
    as long as the group and no other.
- **A binary is found only where its operator said, opened once, and known by that open
  file.**
  - A connector named without a path is looked up in the connector directories alone, each an
    absolute path, by name, following no link. `PATH` and the working directory are never
    searched.
  - The file must be a regular, executable file that belongs to the host's user or the
    superuser and that no other user may write, and so must a directory it was looked up in
    (`binary_shared`).
  - The open file is what is hashed, at placement and before every spawn, and what is
    executed: through `/proc/self/fd`, or handed to the sandbox, which binds it. A file that
    takes the binary's name later is never run; one whose bytes change is refused.
  - A script's interpreter opens the script by the name it was executed by, so a trusted
    script is also given at descriptor 4, which its interpreter holds.
  - macOS cannot execute an open file without `unsafe` code: there a digest is refused as
    unsupported, none is reported, and a trusted binary is executed by its path.
- **A connector is given its standard streams and its socket.** Everything the standard
  library opens is closed on exec. Each descriptor the host holds that is not, as one it was
  started with, is covered in the child with the null device: the workspace has no `unsafe`
  code to close it there, and the host does not change a descriptor it does not own. Spawns
  hold one lock from before a socket is made until the connector has it, for the platforms
  where a descriptor is marked in a second step.
- **A connector's output is bounded.** Each stream is read at a megabyte a second after a
  burst of four, so a connector that floods waits to write; a line is cut at a kilobyte; the
  log is given 32 lines a second after a burst of 256, as a field of an event the host words,
  and the lines beyond are counted and said twice: once as they start being dropped, once
  with their count as the stream ends. Lines of nothing are no events.
- **Connector text is shown and bounded where the host receives it.** One function,
  `rdlt_connector::text::shown`, escapes every character a terminal obeys or a reader cannot
  see, makes a run of spaces one, and cuts at a limit with a mark; showing twice changes
  nothing. It is applied to an error's message, code and causes as the wire delivers them,
  to last words, to output lines, to an engine report's texts, and to every reason of a
  certification report, whose plain form, `Display` and JSON are the same text.
- **A configuration is secret material, and its secrets are references.**
  - `Secret<T>` is not `Clone` (`duplicate` copies on purpose), is wiped when dropped, and
    compares text and bytes in constant time. A configuration error names the field, the
    fault and the kind of value found, and never reads serde's message, which quotes values.
  - The host holds a configuration as `Config`: zeroized text with no `Debug` that shows it.
  - A text value may hold `${env:NAME}`, `${file:/absolute/path}` and `${secret:name}`;
    `$${` is a literal `${`. A `SecretResolver` resolves them when the configuration is sent:
    after the handshake showed the connector to be the one the reference names, and again at
    each respawn or redial. `Secrets` resolves `env` from the host's environment, `file`
    from a regular file that is the host's user's alone and reached through no link at its
    name, and `secret` through the embedder's resolver, or `RDLT_SECRET_<NAME>` without one.
  - Every resolved value is kept in the `Redactions` of that connector, and replaced by
    `***` in its errors, its last words and its output, as it is written and as JSON,
    `Debug` and `shown` write it, and where a text was cut within it. An in-process
    connector's errors are flattened to text and scrubbed the same way.
  - `rdlt-certify` reads a configuration from a file or standard input, never the command
    line, resolves its references, scrubs what it prints, and certifies a binary inside the
    sandbox unless told it is trusted.

Rejected:
- **Closing the host's other descriptors in the child.** It needs code between `fork` and
  `exec`, which is `unsafe`. Covering them gives the connector nothing of the host's.
- **Marking the host's descriptors close-on-exec in the host.** They are not the library's
  to change, and an embedder may pass one to a child of its own.
- **A private copy of the binary to execute.** It breaks a binary that finds its files beside
  itself, and costs a copy for every spawn. An open file is the same guarantee.
- **Scrubbing every text value of a configuration.** A path or a host name in an error is
  what an operator needs to read. What is scrubbed is what was referred to as a secret.
- **Escaping the backslash in shown text.** Shown text would then change when shown again,
  and every boundary would have to know whether another had been there.

## Consequences

- `Local::new`, `Registry::new`, `Registry::source` and `destination`, the search of `PATH`,
  and `rdlt-certify --config` are gone; `Local::wire` is async. ADR 0017's lookup and ADR
  0025's check-then-execute are replaced, and ADR 0020's `--config` is withdrawn.
- A binary upgraded by a rename is not picked up by a running host: its connector is spawned
  again from the file it was placed from.
- A connector that writes more than a megabyte a second to its standard streams is slowed to
  that, and one slowed past a call's deadline fails that call.
- A sandboxed connector that does not end at the end of its input is killed after its grace.
- What is not guaranteed, and is the operator's to bound:
  - the processor time, memory and disk a local connector's own process takes: the sandbox's
    job, or control groups';
  - a descriptor another thread of an embedder opens between the host's look and its fork;
  - on macOS, a descriptor the host inherited, and any confinement of a local connector;
  - a secret a connector transforms before it says it, or one an in-process connector
    panics with: an in-process connector is trusted code;
  - the buffers of the transport a configuration is sent through, which are not wiped.
- A reference with a requirement its placement cannot honour now fails where it was placed
  before. A host that holds an inheritable descriptor spawns connectors that find the null
  device at its number.
