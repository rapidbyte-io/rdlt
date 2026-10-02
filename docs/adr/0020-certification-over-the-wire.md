# ADR 0020: Certifying connectors over the wire

Status: accepted, 2026-09-27; outcomes, the pass rule and exit codes are amended by ADR 0050
(2026-10-01): a skipped clause is inapplicable or unobserved, and an unobserved one does not pass.
Where the configuration is read from, the sandbox a binary is certified in and how a
connector's text is shown are amended by ADR 0043 (2026-10-02).

## Context

ADR 0018 left `rdlt-certify` and the `K` kill clauses to M4f. ADR 0016 left certification over a
real socket to the same crate: the connector crate cannot hold a client of the protocol, since the
client lives in `rdlt-host`. §20.8 names the clause families and the entry points: a library, and
a binary that certifies any connector binary or endpoint and also runs `K`.

## Decision

- **M4f is split.**
  - M4f (this record) is `rdlt-certify`'s library and binary, with the source (`S`), destination
    (`D`) and protocol (`P`) clauses over the wire.
  - M4g is the `K` clauses: a connector killed at random points while an engine converges
    exactly once. Converging needs an engine, and the spec's crate map (§4) gives `rdlt-certify`
    none; M4g settles that.
- **A target is how a connector is reached**, each connection a fresh one:
  - served in this process over a socket pair (`Target::served`): the certification through the
    full protocol ADR 0016 deferred;
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
    the eleven clauses that read what it published are skipped, and `D-CHECK`, `D-EPOCH` and
    `D-STATE` run. The binary certifies destinations so.
- **The protocol's clauses** are checked by a client that speaks the protocol raw:
  - `P-HANDSHAKE`: the handshake answers the protocol's major version with the spec and limits,
    and refuses another as unsupported;
  - `P-ORDER`: a call before the handshake, and a second handshake, are refused, typed;
  - `P-ROLE`: a role the connector does not serve is refused as unsupported;
  - `P-LIMITS`: a configuration beyond the connector's limit is refused with `limit_exceeded`.
    A limit beyond the largest configuration this host sends is never met, and is not exceeded:
    the clause is skipped, rather than let a connector size this process's memory;
  - `P-HEARTBEAT`: each heartbeat is answered with its sequence number, in order;
  - `P-MALFORMED`: a read or write that does not begin with its start is refused, typed, and the
    connection serves on;
  - `P-CREDIT`: a read sends nothing more once its credit is spent, until more is granted. One
    frame after the grant shows it went on; the read is then stopped, so a partition of any
    length is certified in bounded time.
  - Per-operation deadlines (§12.6) are the host's to keep, and its own tests pin them (ledger
    item L3); no clause checks them of a connector.
- **`rdlt-host` gains raw wires** for clients that speak the protocol themselves: `Local::wire`,
  `Remote::wire`, `remote::client`, and `Connection::connector_spec`.
- **The binary**: `rdlt-certify <binary | grpcs://host:port>` certifies every role the connector
  serves, or `--role`'s, with one configuration (`--config`, `--config-file`; amended
  2026-10-02 by ADR 0043: from a file or standard input only, and a binary is certified inside
  a sandbox unless `--trusted`); an endpoint needs
  `--tls-cert`, `--tls-key` and `--tls-ca`. It prints plain text or JSON (`--output`), and exits
  0 when every report passed, 1 on findings, 64 on a wrong command line and 74 when the
  connector or a file named cannot be read (§19). `--clauses` prints the registry.
  - A report passes when no clause failed and one passed at least: one whose every clause was
    skipped certified nothing, in the library as in the binary, and its JSON says so.
  - A role the connector does not serve skips each of its clauses. Without `--role`, its report
    is left out; with `--role` naming such a role, nothing was certified, and it exits 1 saying
    so.
  - Plain output shows what the connector said, not obeys it (§19): its controls, line breaks
    among them, and the marks that reorder text are escaped. JSON output carries it verbatim.
    Amended 2026-10-02 (ADR 0043): a reason is shown where it is made, so the plain report,
    its `Display` and the JSON carry the same escaped text.
    A closed standard output ends quietly; one that cannot be written exits 74.
  - A malformed endpoint is a wrong command line, and exits 64.
  - Its help and its reports are snapshots (§20.11), with `insta`.
- **The clauses' documentation is generated from the registry**: `docs/certify/clauses.md`, which
  a test holds equal to `rdlt-certify --clauses`.
- **What certification found**: a barrier pending when a read starts was forwarded after the
  read's start, so a read too short to see it never answered it, where in the engine's process it
  always does. `ReadStart` now carries the barrier pending at the start (`barrier`, field 5),
  and the host still sends it as a control too, before the read's credit. A connector that does not know the field reads 0
  and answers the control, as before; one that knows both answers the barrier once.

## Consequences

- `rdlt_connector::testing::certify_*` stays in process; `rdlt_certify::certify_*` with
  `Target::served` runs the same clauses through the protocol.
- A destination that keeps its store in its process fails `D-EPOCH` and `D-STATE` when spawned,
  as each connection is a process of its own; certify it served, or listening.
- A connector binary serving both roles is certified with one configuration unless `--role`
  picks one.
- M4's exit criterion (`P` and `K` clauses green) completes with M4g.
