# ADR 0020: Certifying connectors over the wire

Status: accepted, 2026-09-27.

## Context

A connector is certified by clause families: source (`S`), destination (`D`), protocol (`P`)
and kill (`K`) clauses. Certifying one over a real socket needs a client of the protocol, which
lives in `rdlt-host`, where the connector crate cannot reach it. So `rdlt-certify` holds a
library and a binary that certifies any connector binary or endpoint over the wire; the `K`
clauses are ADR 0022's.

## Decision

- **A target is how a connector is reached**, each connection a fresh one:
  - served in this process over a socket pair (`Target::served`): the certification through the
    full protocol;
  - spawned from its binary (`Target::spawned`), or listening at an endpoint over mutual TLS
    (`Target::listening`);
  - or opened by any function (`Target::connected`), for another transport, or a connector that
    is no `rdlt-connector` binary.
- **Nothing stands between a clause and the protocol.** A clause's connections are the host's
  `Connection`s with no supervision: nothing redials or retries what the connector did.
  - When a spawned connector's transport fails, the failure carries what it left on its standard
    error and how it exited, as supervision's errors do: a connector that cannot start says why.
- **The source and destination clauses are `rdlt_connector::testing`'s**, run over the host's
  `RemoteSource` and `RemoteDestination`.
  - A destination whose published data nothing can read is certified with the `Unprobed` probe:
    the eleven clauses that read what it published are unobserved (ADR 0050), and `D-CHECK`,
    `D-EPOCH` and `D-STATE` run. The binary certifies destinations so.
- **The protocol's clauses** are checked by a client that speaks the protocol raw:
  - `P-HANDSHAKE`: the handshake answers the protocol's major version with the spec and limits,
    and refuses another as unsupported;
  - `P-ORDER`: a call before the handshake, and a second handshake, are refused, typed;
  - `P-ROLE`: a role the connector does not serve is refused as unsupported;
  - `P-LIMITS`: a configuration beyond the connector's limit is refused with `limit_exceeded`.
    A limit beyond the largest configuration this host sends is never met, and is not exceeded:
    the clause is inapplicable, rather than let a connector size this process's memory;
  - `P-HEARTBEAT`: each heartbeat is answered with its sequence number, in order;
  - `P-MALFORMED`: a read or write that does not begin with its start is refused, typed, and the
    connection serves on;
  - `P-CREDIT`: a read sends nothing more once its credit is spent, until more is granted. One
    frame after the grant shows it went on; the read is then stopped, so a partition of any
    length is certified in bounded time.
  - Per-operation deadlines are the host's to keep, and its own tests pin them
    (`slow_commit_within_deadline_succeeds`); no clause checks them of a connector.
- **`rdlt-host` has raw wires** for clients that speak the protocol themselves: `Local::wire`,
  `Remote::wire`, `remote::client`, and `Connection::connector_spec`.
- **The binary**: `rdlt-certify <binary | grpcs://host:port>` certifies every role the connector
  serves, or `--role`'s, with one configuration, read from a file or standard input
  (`--config-file`), never the command line, and certifies a binary inside a sandbox unless
  `--trusted` (ADR 0043); an endpoint needs `--tls-cert`, `--tls-key` and `--tls-ca`. It prints
  plain text or JSON (`--output`), and exits 0 when every report passed, 2 when one is
  incomplete and none failed, 1 on findings or when the connector serves none of the roles
  asked, 64 on a wrong command line and 74 when the connector or a file named cannot be read
  (ADR 0050); `--require partial` exits 0 for an incomplete run in which a clause passed.
  `--clauses` prints the registry.
  - A clause passes, fails, is inapplicable by the connector's declaration, or is unobserved.
    A report passes when no clause failed, none was unobserved and one passed; otherwise it is
    incomplete, in the library as in the binary, and its JSON says so (ADR 0050).
  - No clause of a role the connector does not serve applies. Without `--role`, its report is
    left out; with `--role` naming such a role, nothing was certified, and it exits 1 saying so.
  - Plain output shows what the connector said, not obeys it: its controls, line breaks among them,
    and the marks that reorder text are escaped. JSON output carries it verbatim. A reason is shown
    where it is made, so the plain report, its `Display` and the JSON carry the same escaped text
    (ADR 0043). A closed standard output ends quietly; one that cannot be written exits 74.
  - A malformed endpoint is a wrong command line, and exits 64.
  - Its help and its reports are snapshots, with `insta`.
- **The clauses' documentation is generated from the registry**: `docs/certify/clauses.md`, which
  a test holds equal to `rdlt-certify --clauses`.
- **A read starts with the barrier pending.** `ReadStart` carries the barrier pending at the
  start (`barrier`, field 5), and the host also sends it as a control, before the read's credit,
  so a read too short to see a later control still answers it, as it does in the engine's
  process. A connector that does not know the field reads 0 and answers the control; one that
  knows both answers the barrier once.

## Consequences

- `rdlt_connector::testing::certify_*` stays in process; `rdlt_certify::certify_*` with
  `Target::served` runs the same clauses through the protocol.
- A destination that keeps its store in its process fails `D-EPOCH` and `D-STATE` when spawned,
  as each connection is a process of its own; certify it served, or listening.
- A connector binary serving both roles is certified with one configuration unless `--role`
  picks one.
