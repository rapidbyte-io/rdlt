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
- frames from a connector are checked against size limits before they are decoded, and every
  other message within the limit of what its call carries (The control plane, below).

## Placement

A reference to a connector states what it requires: an id, and optionally a version, a path,
an endpoint, a digest, an isolation, and what its pipeline grants its connector. Every provider
honours each requirement it is given or refuses the reference with a typed error before
anything runs; none is ignored.

- **In process** (`Registry::trusted`): for connectors the embedder trusts as its own code.
  They share the engine's address space and access.
- **Remote** (`Remote`): over TLS 1.3 with mutual authentication, the connector's certificate
  checked against the CA bundle and the endpoint's host name. The machine or container the
  connector runs in is its isolation.
- **Local** (`Local`): built with a sandbox, or with the statement that its binaries are
  trusted, and in no other way.
  - `Bubblewrap`, on Linux: a connector sees the system directories read only, a private
    `/tmp`, and what it was granted; nothing of a home directory, no process of the host's,
    only the environment stated, no network unless granted, and no user namespace of its own
    making; it ends with its host. Its arguments and environment reach the launcher through a
    descriptor, never a command line another user could read. The launcher is checked as a
    connector's binary is, and run from the file opened. Where bubblewrap is missing, older
    than 0.8, or cannot make user namespaces, the placement is refused.
  - **Grants belong to a pipeline, within the operator's roots**: a provider grants every
    connector only paths to read, as system directories; what a connector may write, or read
    beyond those, is asked for on the reference that places it, which a pipeline's author may
    write, and granted only within the roots the operator names on the provider
    (`grantable_read`, `grantable_write`). By default none is named and nothing is granted.
  - A root that may be written may hold nothing that decides what the host runs or keeps: no
    connector directory or binary, no script's interpreter, no launcher, not the host's
    executable's directory, a directory the host keeps its log, state or secrets in, or one a
    secret resolver reads. The placement is refused otherwise.
  - Each path granted is opened once, checked by what was opened, and bound by its descriptor
    at every spawn of the placement, so a link changed after the check changes nothing. No
    other connector of the process may hold a path within, above or equal to it while its
    connector lives, until reaped, unless both state their grants are shared; no grant may
    write a program a placement runs, and no placement runs a program a held grant may write.
  - **The network grant** shares the host's network namespace: every interface and route, the
    host's loopback services, and the abstract Unix sockets of that namespace, such as a
    session's buses. Grant it only to a connector you would let reach those.
  - On macOS there is no sandbox: a local connector runs only as a trusted binary, with the
    host's access. Run untrusted connectors remotely there.
  - A binary named without a path is found only in the directories configured, never on
    `PATH` or in the working directory. The binary, every directory above it and above a
    connector directory must be writable by no other user, a sticky directory such as `/tmp`
    excepted for entries the user owns. It is opened once, and the open file is what is
    hashed and what is executed, so no file can be swapped in between. A sandboxed connector
    whose binary was replaced since it was placed is refused until it is placed again. On
    macOS an open file cannot be executed: a digest is refused there rather than checked
    against a path.
  - A connector is given its standard streams and its socket, and no other descriptor of the
    host's: after the descriptors it is given are in place, the child marks every other
    close-on-exec, whatever another thread of the host opened meanwhile. A sandboxed connector
    is spawned only on a kernel that marks them all in one call, Linux 5.11 and later; for a
    trusted one elsewhere each is marked in turn up to 65,536, a measure of hygiene.
  - Its process group is the host's for its whole life, and is stopped and killed as a
    group; a sandboxed connector is killed with its whole process namespace.

## Secrets and connector text

- **Whoever writes a pipeline's configuration is not trusted with the host's secrets.** A
  configuration value may refer to a secret, `${env:NAME}`, `${file:/absolute/path}` or
  `${secret:name}`, and reaches only what the operator lists: the variables an `EnvSecrets`
  names, files beneath the private directories a `FileSecrets` names, and the named secrets of
  the store the operator gives. By default no reference resolves.
- References are resolved when the configuration is sent to a connector whose identity the
  host has checked: its id and version, and its binary's digest or its certificate's name. A
  connector receives only its own configuration.
- A resolved secret is replaced by `***` in that connector's errors, its last words and its
  logged output, whichever start of it said it, in an engine's report of them, and in
  `rdlt-certify`'s reports. It is not written to the write-ahead log or to state by the
  engine. A literal value in a configuration is not a secret and is not scrubbed.
- A configuration error names the field and what is wrong with it, never the value.
  `Secret<T>` redacts `Debug`, `Display` and `Serialize`, is wiped when dropped and is not
  `Clone`.
- Text from a connector, its errors, last words, output, the names it gives streams and
  tables in an engine's errors, and every reason in a certification report, is shown with
  each character a terminal obeys or a reader cannot see escaped, and cut at a limit, where
  the host receives it.
- A connector's output is read at a bounded rate and logged at a bounded rate of lines.

Not guaranteed:
- **The data.** A source can send wrong rows and a destination can lose or corrupt what it is
  given; the engine confines a connector, it does not vouch for it.
- **A connector's own resources.** The processor time, memory and disk of a local
  connector's process are the sandbox's or the operator's to limit, with control groups or a
  container; its private `/tmp` is memory.
- **What a network grant reaches**: the host's network namespace, as above.
- **What a read grant reaches beyond reading**: a connector may connect to a Unix socket
  beneath a path it may only read, such as a session bus or an agent's socket under
  `/run/user`. Grant no such directory to a connector you would not let reach those.
- **A secret a connector transforms** before it prints it, or that trusted in-process code
  panics with; and the memory of the transport a configuration crosses.
- **macOS local placement**, beyond the checks on the binary: no sandbox and no digest.

ADR 0043 records these.

## Build and supply chain

- `unsafe` code is in one audited crate, `rdlt-adopt`: adopting a host's socket, and the hook
  that keeps a spawned connector from inheriting the host's descriptors. Every other crate,
  test, example, bench
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

## The control plane

What a connector says beside its data is bounded, checked and given a deadline as its frames
are (ADR 0042):
- each call's messages, frames among them, are passed to the decoder only once whole, within
  the limit of what the call carries, and counted before they are decoded, for what decoding them
  would hold, within a bound of their own; a message that cannot be counted is refused, and a
  served connection holds the messages still arriving within a window, reading no more until it
  has room; what decoding a remote connector's answers holds is charged to the run's memory
  budget before it is decoded, within limits the budget derives, and the engine commits no
  state an open could not answer; catalogs, plans and identifier
  rules are bounded in count and checked in linear time;
- stream names, partition ids, table paths and destination identifiers refuse characters that
  hide or reorder text, and an error's code must be a token that is none of the host's own;
- every call into a connector has a deadline in every placement, a read asked to stop is
  dropped after its wait, and a failed attempt closes its session;
- a receipt must answer its own commit, a table keeps the merge key and change time it was
  loaded by, and opened state must hold each key once and only names its destination's rules
  could have given; a reset recovers a pipeline whatever names its state records, and drops no
  table under a name its destination reserves;
- a connector's error neither sets a wait outside the retry policy nor claims a fence or a stop
  the engine did not see, and a read that keeps losing its place spends the retry budget.

Not guaranteed: a read's credit is kept by the served end and HTTP/2's flow control; a host
cannot tell an overrun from a read that kept to its credit (ADR 0016).

## What a frame may hold

A connector's Arrow frames are checked as a whole before they are decoded (ADR 0038): a frame's
buffers are disjoint and long enough for its values, its values and the bytes its views name are
within the limits, and a schema is measured before it is built. A frame that is not is refused
with a typed error, `limit_exceeded` or `malformed_frame`.

## What the engine holds

Whatever a connector sends is reserved from the memory budget before the engine holds it, or
bounded by a limit with a typed refusal (ADR 0039):

- The budget is never passed. It is divided into shares, for the cursors of checkpoints, the
  log's frames, what commits record of tables, what reads keep, and data, and a reservation is
  made only where it fits its share. One that could never fit is refused:
  `push_exceeds_budget`, `row_exceeds_budget`, `log_frame_exceeds_budget`,
  `table_exceeds_budget`, and `limit_exceeded` naming `cursor bytes` or `read kept bytes`.
- A push reserves what it keeps alive, its schema included; JSON text reserves three times
  itself, for the batches it becomes, and what its batches take beyond that is reckoned and
  reserved before any of it is built, or refused, `json_exceeds_budget` (ADR 0040). The
  shredder counts every column its records hold together against the schema's column limit,
  and its cells a level at a time, before it builds them.
- JSON text is read without recursion, nesting past 64 levels refused, and its numbers by their
  exact value. Every value of an Arrow push's columns of JSON is checked before it is used:
  text that is not JSON fails the write, `json_invalid`. A refusal quotes a record's keys and
  numbers cut to 128 bytes.
- A batch is lowered a piece at a time, through plans that normalize too. Each piece reserves
  what lowering it holds, as its table stores it and with the nulls of the columns it lacks,
  before it is lowered. A row that alone takes more than a quarter of the budget is refused.
- A checkpoint never waits behind data, and a read keeps no more than its part of a quarter of
  the budget, so reads cannot starve checkpoints or pushes. Every read holds a slot, and the
  part is the share divided by the slots; a following run reads fewer unbounded partitions than
  it has slots, or is refused with `partitions_too_few`.
- What a connector is told at the handshake it may send, a frame, a JSON push, a cursor, its
  dictionaries, its schema and its columns, is what the budget admits, each limit derived so
  the worst case it admits, all at once, fits its share: a connector that keeps to them is
  refused nothing for the budget's sake and waits on it for nothing but commits. An engine
  whose memory admits less than the protocol's least frame or dictionary limit is refused when
  it is configured, and a peer that sets a dictionary limit below the protocol's least is
  refused at the handshake.
- No wait on the budget is for ever: at its deadline, an hour by default, the attempt fails with
  `memory_budget_wait_exceeded`, saying what held the budget. No connector's error can claim
  that kind or code.
- A batch pushed in process meets the limits a frame meets on the wire: its nesting, its values,
  the bytes its views name and the bytes it keeps alive.
- A checkpoint's cursor is charged until its commit lands, and checkpoints that seal no rows keep
  one cursor a partition. Signals are kept as state, not queued.
- A decoder's dictionaries and what a served write stages between flushes have limits of their
  own, `dictionary bytes` and `staged bytes`.

How much one push may expand to in total is not bounded: it costs time and destination storage in
proportion, within the budget's memory.

## What a pipeline keeps

What a pipeline keeps from one push and one run to the next is bounded as a whole, and stored
in forms its readers read back (ADR 0041):

- A stored type nests no deeper however deep the type, and a schema nested beyond the nesting
  limit cannot be built: state, the write-ahead log and the files destination's catalog read
  back every schema written. Stored records name their format and refuse fields their reader
  does not know.
- A table's columns, nested fields counted, are held to the schema columns the budget admits
  (`table_columns_exceeded`), a stream's child tables to `GrowthLimits` and the stored state
  limit (`child_tables_exceeded`), and a schema version to what it counts
  (`schema_version_exhausted`).
- Stored state, as an open's answer carrying it holds it decoded, and a commit's request are
  held to the advertised state limit: a commit beyond either is refused before it is
  logged or acknowledged (`state_bytes_exceeded`), unless state was past the limit already and
  the commit does not grow it. A plan never forgets a partition's
  position; a commit that would pass the limit deletes the done markers of partitions no plan
  names, oldest first, each reported, and a reset of the stream clears the rest.
- State that records two tables under one identifier is refused (`state_invalid`); a reset never
  drops such a table.
- A lane holds a bounded share of open destination writers and retires superseded ones; the log
  forgets superseded schema frames, and an open segment keeps at most as much of the log as it
  holds itself.

## The write-ahead log

A load's log holds rows its source may have been told were committed, and replay applies it with
the engine's authority, so it is bound, private and checked (ADR 0045):

- The local store reaches everything beneath its base one name at a time, never through a link.
  The base must be the engine's user's and writable by no other; every directory and file below
  it must be the user's alone, of the kind expected, on the base's file system, checked at every
  open (`wal_not_private`). A name the store never writes is refused, never read (`wal_stray`).
  Each pipeline's directory is its own, also where a file system folds case.
- A chunk is published whole, never appended to, so any damage, a chunk of another format, a
  chunk missing, or a chunk naming another pipeline or load is refused (`wal_unreadable`,
  `wal_foreign`) rather than read in part. Frames are bounded before they are read, and batches
  decoded within the memory budget's limits.
- A logged commit applies only whole and only as the engine writes one: older than the
  replaying session, its seals, batches and rows all there, recording no reset, no receipt but
  its own and no position its seals did not set, dropping no table.
- What replay stages is reserved from the memory budget (`replay_exceeds_budget`). A load's log
  holds at most `GrowthLimits::log_bytes` on disk, 4 GiB by default (`log_bytes_exceeded`), and
  a full disk is retried (`wal_storage_full`).
- A replay fences a running load by publishing its log's next chunk, so the two never both write.

A log is not signed: whoever can write as the engine's user holds its authority already. A log
that cannot be read stops its pipeline's runs until an operator removes it, since setting it
aside would drop rows its source was told were committed.

## Building rdlt

rdlt turns a panic, of the Arrow library on a corrupt frame or of a connector's task, into a
typed error by unwinding. Build every binary that embeds rdlt with `panic = "unwind"`, Rust's
default: `rdlt-wire`, `rdlt-engine` and a served `rdlt-connector` refuse to compile with
`panic = "abort"`, under which one such panic would end the whole process.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose
**Report a vulnerability**. Do not open a public issue.
