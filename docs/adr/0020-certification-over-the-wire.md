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
  - The destination clauses send the identifiers the destination's rules make of their names,
    within the rules as the engine's names are: each folded as the rules fold, a column's
    lengthened with `_` until it is no reserved word, and a table's, where it is longer than the
    rules allow, cut to `certify_` and as much of a digest of its name as fits. `D-NAMES` alone
    writes names at the rules' edges. Where the rules reserve every form of a column's name, or a
    prefix or word a table's name beginning `certify_` may start with, every clause that names a
    table or column is unobserved: all but `D-CHECK` and `D-STATE`.
- **The protocol's clauses** are checked by a client that speaks the protocol raw:
  - `P-HANDSHAKE`: the handshake answers the protocol's major version with the spec and limits,
    and refuses another as unsupported;
  - `P-ORDER`: a call before the handshake, and a second handshake, are refused, typed;
  - `P-ROLE`: a role the connector does not serve is refused as unsupported;
  - `P-LIMITS`: a configuration beyond the connector's limit is refused with `limit_exceeded`.
    A limit beyond the largest configuration this host sends is never met, and is not exceeded:
    the clause is inapplicable, rather than let a connector size this process's memory;
  - `P-HEARTBEAT`: each heartbeat is answered with its sequence number, in order, while the
    stream of heartbeats stays open. Each goes once the heartbeat before it was answered, within
    the host's own heartbeat interval, as a host's do, which never ends the stream: a connector
    that answers only once the stream ends is lost by every host;
  - `P-MALFORMED`: a read or write that does not begin with its start is refused, typed, and the
    connection serves on;
  - `P-CREDIT`: a read sends nothing more once its credit is spent, until more is granted. One
    frame after the grant shows it went on; the read is then stopped, so a partition of any
    length is certified in bounded time.
  - Per-operation deadlines are the host's to keep, and its own tests pin them
    (`slow_commit_within_deadline_succeeds`); no clause checks them of a connector.
- **`rdlt-host` has raw wires** for clients that speak the protocol themselves: `Local::wire`,
  `Remote::wire`, `remote::client`, and `Connection::connector_spec`.
- **The command**: `rdlt certify <binary | grpcs://host:port>` certifies every role the
  connector serves, or `--role`'s, with one configuration, read from a file or standard input
  (`--config-file`), never the command line, and certifies a binary inside a sandbox unless
  `--trust-binaries` (ADR 0043); an endpoint needs `--tls-cert`, `--tls-key` and `--tls-ca`. It
  prints plain text or JSON (`--output`), and exits 0 when every report passed, 1 on findings,
  when one is incomplete and none failed or when the connector serves none of the roles asked,
  64 on a wrong command line and 74 when the connector or a file named cannot be read
  (ADR 0050); `--require partial` exits 0 for an incomplete run in which a clause passed.
  `--clauses` prints the registry.
  - A clause passes, fails, is inapplicable by the connector's declaration, or is unobserved.
    A report passes when no clause failed, none was unobserved and one passed; otherwise it is
    incomplete, in the library as in the command, and its JSON says so (ADR 0050).
  - No clause of a role the connector does not serve applies. Without `--role`, its report is
    left out; with `--role` naming such a role, nothing was certified, and it exits 1 saying so.
  - Plain output shows what the connector said, not obeys it: its controls, line breaks among them,
    and the marks that reorder text are escaped. JSON output carries it verbatim. A reason is shown
    where it is made, so the plain report, its `Display` and the JSON carry the same escaped text
    (ADR 0043). A closed standard output ends quietly; one that cannot be written exits 74, and
    so does a pipe or file that has not taken the report within a few seconds, since the signals'
    handlers are installed by then and none is heard while a write blocks. A terminal that stopped
    reading is left as it is.
  - A malformed endpoint is a wrong command line, and exits 64.
  - Its help and its reports are snapshots, with `insta`.
- **The clauses' documentation is generated from the registry**: `docs/certify/clauses.md`, which
  a test holds equal to `rdlt certify --clauses`.
- **A read starts with the barrier pending.** `ReadStart` carries the barrier pending at the
  start (`barrier`, field 4), and the host also sends it as a control, before the read's credit,
  so a read too short to see a later control still answers it, as it does in the engine's
  process. A connector that does not know the field reads 0 and answers the control; one that
  knows both answers the barrier once. `S-BARRIER` raises its first barrier so, before each read
  of an on-demand stream: wherever the connector runs, the read sees it before its first push.
  It raises a second once the read has answered the first and pushed, and so holds the read to
  the contract for a barrier the engine raises during a read too. The clause reads through a
  channel of one event, over a connection of its own whose window of credit stays at a byte
  (`ReadWindow::Fixed`): each frame is granted back as it is taken and no more, so one frame is
  in flight whatever its size. Every other source clause reads as an engine does. A source then
  sends at most `BARRIER_SLACK` events before it sees the second barrier, what rdlt's serving
  end and host hold of a read, the events the serving end queues ahead of its credit and a
  Python source's bridge, besides the pieces the serving end cuts its batches into. Those are
  bounded on each read, not counted: by the rows of its batches after the barrier over the row
  limit its connection agreed, and by twice their values, their views' bytes and their frames'
  bytes over the limit of each. A frame's bytes are weighed with a validity bit for every value,
  as the serving end weighs a batch holding a null, against the frame limit less what a frame
  takes beside its rows, which is what the serving end fills a piece to. The clause judges the
  second barrier only against the pushes after `BARRIER_SLACK` and that bound, which can be twice
  the pieces cut and more, and leaves it unjudged on a shorter read, and on every read of a schema
  so wide that a frame has no room for rows; a connector served by another end may hold more. Over
  a slow link the fixed window can make the clause's read pass its bound, which fails it. A read
  answers each barrier it is judged on with a checkpoint before its last push: a stream that
  checkpoints only at its end fails, and so does one that answers the first with an opening
  checkpoint and then checkpoints only at its end. A read that pushes nothing, or pushes once and
  answers after it, shows nothing, and where no read shows an answer, the clause is unobserved.

## Consequences

- `rdlt_connector::testing::certify_*` stays in process; `rdlt_certify::certify_*` with
  `Target::served` runs the same clauses through the protocol.
- A destination that keeps its store in its process fails `D-EPOCH` and `D-STATE` when spawned,
  as each connection is a process of its own; certify it served, or listening.
- A connector binary serving both roles is certified with one configuration unless `--role`
  picks one.
