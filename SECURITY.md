# Security

## Trust model

- The engine and the host are the trusted computing base. A connector is untrusted code, unless
  it is compiled into the embedder's binary and registered in process.
- Everything a connector sends is untrusted: data, control messages, state, receipts, errors and
  timing. The engine validates it, bounds it, charges it to a budget and puts a deadline on every
  wait, so that no connector can crash, hang or exhaust the engine, or reach another pipeline.
- Untrusted connectors run outside the engine's process: remotely over mutual TLS, or in a local
  process inside a sandbox. In-process placement is for trusted connectors.
- A connector can still send wrong data, and a destination can lose the data it is given: rdlt
  confines a connector, it does not vouch for it.

ADR 0037 records this model. It is being put in place by a hardening program, and until that is
complete, run only connectors you trust. The mechanisms in place today:
- connectors listening on the network are reached over TLS 1.3 with mutual authentication, in a
  full handshake each time. A connector accepts only the hosts named to it, by a name in their
  certificates, and a private key is read only from a file of its user's alone;
- a listening connector holds a bounded number of connections at each stage, so peers that never
  authenticate take no file descriptor of a session and hold up no accept. Once they fill the
  places they are given, each further connection closes the oldest of the address that holds
  the most, so a flood from few addresses closes its own. A flood from very many addresses, or
  from behind the host's own, can still close a host's handshake, which the host tries again:
  keep a firewall in front of a connector. Refused connections are reported in one line an
  interval;
- a connector's own binary serves no certification probe, and a source hears a host report
  committed only the checkpoints it sent that host and where that host's reads started, once
  the source accepted them;
- an endpoint that is refused is never repeated in an error;
- where a connector runs is policy, a local one runs in a sandbox or as a binary stated to be
  trusted, and its configuration's secrets are resolved only for the connector the host has
  verified and scrubbed from what it says back (Placement, and Secrets and connector text,
  below);
- frames from a connector are checked against size limits before they are decoded.

## Placement

A reference to a connector states what it requires: an id, and optionally a version, a path,
an endpoint, a digest and an isolation. Every provider honours each requirement it is given or
refuses the reference with a typed error before anything runs; none is ignored.

- **In process** (`Registry::trusted`): for connectors the embedder trusts as its own code.
  They share the engine's address space and access.
- **Remote** (`Remote`): over TLS 1.3 with mutual authentication, the connector's certificate
  checked against the CA bundle and the endpoint's host name. The machine or container the
  connector runs in is its isolation.
- **Local** (`Local`): built with a sandbox, or with the statement that its binaries are
  trusted, and in no other way.
  - `Bubblewrap`, on Linux: a connector sees the system directories read only and what it was
    granted, nothing of a home directory, no network unless granted, no process of the
    host's, only the environment stated, and it ends with its host. Where bubblewrap is
    missing, or unprivileged user namespaces are off, the placement is refused.
  - On macOS there is no sandbox: a local connector runs only as a trusted binary, with the
    host's access. Run untrusted connectors remotely there.
  - A binary named without a path is found only in the directories configured, never on
    `PATH` or in the working directory. It is opened once; it and the directory must be
    writable by no other user; the open file is what is hashed and what is executed, so no
    file can be swapped in between. On macOS an open file cannot be executed: a digest is
    refused there rather than checked against a path.
  - A connector is given its standard streams and its socket. A descriptor the host holds
    that a child would inherit is covered with the null device in the connector.
  - Its process group is the host's for its whole life, and is stopped and killed as a
    group; a sandboxed connector is killed with its whole process namespace.

## Secrets and connector text

- A configuration value may refer to a secret: `${env:NAME}`, `${file:/absolute/path}`,
  `${secret:name}`. References are resolved when the configuration is sent to a connector
  whose identity the host has checked: its id and version, and its binary's digest or its
  certificate's name. A connector receives only its own configuration.
- A resolved secret is replaced by `***` in that connector's errors, its last words and its
  logged output, in an engine's report of them, and in `rdlt-certify`'s reports. It is not
  written to the write-ahead log or to state by the engine. A literal value in a
  configuration is not a secret and is not scrubbed.
- A configuration error names the field and what is wrong with it, never the value.
  `Secret<T>` redacts `Debug`, `Display` and `Serialize`, is wiped when dropped and is not
  `Clone`.
- Text from a connector, its errors, last words, output and every reason in a certification
  report, is shown with each character a terminal obeys or a reader cannot see escaped, and
  cut at a limit, where the host receives it.
- A connector's output is read at a bounded rate and logged at a bounded rate of lines.

Not guaranteed:
- **The data.** A source can send wrong rows and a destination can lose or corrupt what it is
  given; the engine confines a connector, it does not vouch for it.
- **A connector's own resources.** The processor time, memory and disk of a local
  connector's process are the sandbox's or the operator's to limit, with control groups or a
  container.
- **A secret a connector transforms** before it prints it, or that trusted in-process code
  panics with; and the memory of the transport a configuration crosses.
- **macOS local placement**, beyond the checks on the binary: no sandbox, no digest, and a
  descriptor the host itself inherited is not covered.

ADR 0043 records these.

## Build and supply chain

- `unsafe` code is in one audited crate, `rdlt-adopt`. Every other crate, test, example, bench
  and fuzz target forbids it, which the compiler enforces for the code it compiles there; the
  lint rejects the keyword everywhere else it is a token, macro bodies included, which the
  compiler does not check.
- Dependencies are locked, and every lockfile is checked against the RustSec advisory database
  on each change and each night.
- Development and CI tools are locked to a release archive and its checksum; GitHub Actions are
  pinned to commits that are checked against the versions they claim.
- CI runs with a read-only token and no secrets. One job has the token in its environment, and
  it builds nothing.

ADR 0048 records these.

## Certification

`rdlt-certify` treats the connector it certifies as hostile: it spawns a binary inside the
sandbox unless told the binary is trusted, reads the configuration from a file or standard input
and never its command line, and scrubs the secrets the configuration refers to from what it
prints. Every wait has a deadline, and
what a connector sends is charged against a limit before it is held, expanded or rendered, by its
rows, its bytes and, for JSON, its records. A column read back is cast only between kinds a test
shows cannot panic. A clause passes only when the behaviour it names was seen, in a mode the
connector declares. A panic is not caught: it ends the run with a failure, never a pass. The
certifier stops the process group of each connector it spawned before it exits, unless it is killed
outright or aborted; a member that left its group is beyond that. It is no control against a
connector built to pass: a connector can tell a certification from an engine's load, and a
destination's read-back is its own account of what it published (`docs/certify/clauses.md`). What
holds against an untrusted connector is the trust model above: the engine's limits, deadlines and
confinement.

ADR 0050 records this.

## Listening connectors

- The hosts named to one listening connector are one trust domain: each can open any pipeline
  through it, and two that use one pipeline id fence each other. Hosts that must not trust each
  other get a connector each.
- A lost host key is bounded by its certificate's lifetime: issue short-lived certificates. A
  connector can also be given revocation lists, which it reads when it starts.
- The reference connectors are examples and test connectors. Their generator and memory binaries
  are built only on request, and none is hardened for hosts it does not trust.

ADR 0044 records these.

## What a frame may hold

A connector's Arrow frames are checked as a whole before they are decoded (ADR 0038): a frame's
buffers are disjoint and long enough for its values, its values and the bytes its views name are
within the limits, and a schema is measured before it is built. A frame that is not is refused
with a typed error, `limit_exceeded` or `malformed_frame`.

## Building rdlt

rdlt turns a panic, of the Arrow library on a corrupt frame or of a connector's task, into a
typed error by unwinding. Build every binary that embeds rdlt with `panic = "unwind"`, Rust's
default: `rdlt-wire`, `rdlt-engine` and a served `rdlt-connector` refuse to compile with
`panic = "abort"`, under which one such panic would end the whole process.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose
**Report a vulnerability**. Do not open a public issue.
