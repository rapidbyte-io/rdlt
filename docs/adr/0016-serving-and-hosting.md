# ADR 0016: Serving the protocol, and the engine's remote source and destination

Status: accepted, 2026-09-26.

## Context

M4a (ADR 0015) defined the wire protocol, and nothing spoke it yet. M4b makes both ends speak
it:

- A connector serves it, from `rdlt-connector`'s `serve` feature (§11.7).
- The engine drives a served connector through `RemoteSource` and `RemoteDestination` in a new
  crate, `rdlt-host` (§4.2, §13).

Placements are M4c's: spawning processes, `--rdlt-fd`, `--listen` and remote mTLS. M4b's tests
serve connectors over in-process socket pairs, which is what a spawned connector's pre-connected
socket will be.

## Decision

- **One connection is one session of the protocol.**
  - It opens with a handshake that names the role and carries the configuration. The connector
    then connects for that role, once.
  - Every later call works on that connection. A call before the handshake is refused
    (`no_handshake`), and so is a second handshake (`handshake_repeated`) or a call for the other
    role (`role`).
  - A binary serves every role it has a factory for (`Served`), and the handshake's spec lists
    them.
  - A host of another major version is refused (`protocol_version`, `Unsupported`).
  - The configuration document is admitted against the connector's limit. Each end enforces its
    own limits on what it receives, and the host also refuses, typed, a frame beyond the limit
    the connector's handshake declares, before sending it.
  - `serve_connection` drives the connection's HTTP/2 itself, with hyper, until the host closes
    it. tonic's own server returns as soon as its incoming stream ends, and it cannot tie state to
    one connection.
- **Errors travel whole.**
  - A failed call is a gRPC status whose code follows the error's kind, and whose details carry
    the `v1::Error`. The host reads back the same kind, code, message, retry and limit.
  - A status without an error in its details is a transport failure: transient where retrying
    may help, internal otherwise, with code `transport`.
  - A read that fails ends with its error's status. A write that fails answers with a `WriteAck`
    error, and the write ends.
  - A frame the codec refuses, or a message that does not decode, is an internal error:
    `malformed_frame` or `invalid_message`.
  - A status's own message is cut to 1 KiB, and the error in its details to the control string
    limit, so its trailers fit the host's header limit (`HEADER_LIST_BYTES`, 256 KiB).
  - A write the connector ended answers with the error its last answers carry, not a transport
    failure.
- **Credit** (§12.5).
  - A sender sends while its credit is above zero, and each frame spends its encoded size, which
    may leave the credit below zero until more is granted. A small window then bounds how far a
    sender runs ahead, and no frame is too large for any window.
  - Both ends default to 4 MiB (`CREDIT_WINDOW`). The host's read window is an option. A served
    write grants the smaller of the default window and the connector's frame limit.
  - The host grants a read's credit back as it hands each frame to the engine, whose memory
    budget admits it.
  - A host cannot tell a connector that overran its credit from one that did not, because it
    grants credit back as it consumes. So the protection is the served end keeping to its credit,
    plus HTTP/2's own flow control, not a check on the host.
  - Both ends grant HTTP/2's largest connection window (`CONNECTION_WINDOW`). Credit and each
    stream's window bound what a peer sends, so frames the engine has not taken yet, on reads it
    is behind on, never starve the connection's other streams. The heartbeat is one of them: with
    HTTP/2's default window, four backpressured reads silenced it and lost a live connector.
- **Liveness and deadlines** (§12.6).
  - The host sends a heartbeat every interval (5 s by default). Once as many as it allows (6) are
    unanswered when the next is due, the connector is lost, and every call on the connection
    fails with a transient `connector_lost` error. That lets the engine retry the attempt, and
    M4c's supervision respawn the connector.
  - Each call has its deadline from §12.6, and fails past it with a transient
    `deadline_exceeded` error. Reporting committed cursors, which §12.6 does not list, takes the
    commit's deadline.
  - A started read has no deadline: silence on a data stream is never fatal.
  - Each frame of a write is sent within the write-ack deadline, as the transport's windows may
    fill before the connector's credit is spent: a destination whose writer never returns fails
    the write with `deadline_exceeded` rather than hanging it.
  - After this end stalls, the next heartbeat waits its interval rather than catching up, so a
    stalled host does not lose a live connector. An echo of a heartbeat never sent answers
    nothing.
  - A commit longer than the heartbeat's patience succeeds while the connector answers
    heartbeats, which is ledger item L3 (`slow_commit_within_deadline_succeeds`).
- **Reads.**
  - The host forwards the engine's barriers and stops to the connector through two new public
    items of the partition sink: `send`, and `requested`, which waits for the engine's next
    request.
  - The served read feeds the source's partition channel and sends its events as frames.
  - Arrow batches open a new schema epoch when their schema changes, and the host requires
    epochs to grow.
  - JSON pushes, cursors, log lines and metric names are admitted against the host's limits.
- **The protocol changes in two places.** Neither field could be filled truthfully:
  - `ReadStart.mode` is gone: the contract's read carries no mode.
  - `Done.reason` is gone. A read ends with `Done` when the source's read returns, and with its
    error's status when it fails, as the in-process adapter returns. A read the engine stopped
    ends however the source's read returned.
- **`rdlt-wire` generates the service** with tonic-prost-build: the client always, and the server
  under a `serve` feature. It re-exports tonic and prost, and states the protocol's version as
  `PROTOCOL_MAJOR` 1 and `PROTOCOL_MINOR` 0.
- **The gates follow the code.**
  - The served end is tested from `rdlt-host`, where a client exists. So mutation testing runs the
    engine's, connector's, wire's and host's tests for every mutant (`--test-package`), and
    mutates `rdlt-host` too.
  - Coverage runs `rdlt-host`'s tests with the other three.

## Consequences

- Any gRPC implementation can serve a connector: the protocol's behaviour is in its messages,
  its statuses and this record.
- Mutation testing takes longer per mutant, as it runs four packages' tests: about five minutes
  for this milestone's diff.
- `serve::<C>()`, a connector binary's whole `main` (§11.7), with `--rdlt-fd` and `--listen`, lands
  with M4c's placements. Taking a pre-connected socket from file descriptor 3 needs the audited
  `unsafe` §20.14 allows.
- `rdlt_connector::testing::certify` over a real socket (§11.8) needs a client, and a client
  needs `rdlt-host`, which the connector crate cannot depend on. It lands with `rdlt-certify` in
  M4d.
