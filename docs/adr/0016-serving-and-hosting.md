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
    (`no_handshake`), and so is a second handshake (`handshake_repeated`), before it connects
    anything, or a call for the other role (`role`).
  - A binary serves every role it has a factory for (`Served`), and the handshake's spec lists
    them.
  - A host of another major version is refused (`protocol_version`, `Unsupported`).
  - The configuration document is admitted against the connector's limit. Each end enforces its
    own limits on what it receives, and also refuses, typed, to send what the other end's limits
    from the handshake would refuse: a served read fails with the limit's error, and so does a
    remote write.
  - `serve_connection` drives the connection's HTTP/2 itself, with hyper, until the host closes
    it. tonic's own server returns as soon as its incoming stream ends, and it cannot tie state to
    one connection.
- **Errors travel whole.**
  - A failed call is a gRPC status whose code follows the error's kind, and whose details carry
    the `v1::Error`. The host reads back the same kind, code, message, retry and limit.
  - A status without an error in its details is a transport failure: transient where retrying
    may help, internal otherwise, with code `transport`.
  - A data call refused as it opens, or that fails, answers with a frame carrying its error, a
    `ReadFrame`'s or a `WriteAck`'s, and the call ends.
  - A frame the codec refuses, or a message that does not decode, is an internal error:
    `malformed_frame` or `invalid_message`.
  - A status's own message is cut to 1 KiB, and the error in its details to the control string
    limit, so its trailers fit the host's header limit (`HEADER_LIST_BYTES`, 256 KiB).
  - A write the connector ended answers with the error its last answers carry, not a transport
    failure.
- **Each data call is a stream of its own** (§12.5). A read, a write and certification's
  read-back are no gRPC calls: each is a byte stream of frames, and only the control calls ride
  gRPC on the control connection.
  - A frame is a 16-byte head, its body's length (`u32`, little-endian), its kind, its flags, two
    zero bytes and a tag (`u64`, little-endian), then its body. A `Batch` frame is one Arrow IPC
    encapsulated message, a dictionary or a record batch, its tag the schema epoch on a read and
    the segment on a write, its flags whether it holds change rows and whether it is a
    dictionary. A `Json` frame is a push's text. A `Message` frame is one protobuf message of the
    call's, its flags naming the call where it opens one.
  - A call opens with a `Message` frame naming it and holding its start. The connector answers
    with an empty `Message` frame naming the call, or with the error it refuses it with; the host
    waits for that answer within the connect deadline for a read, and within the write-ack
    deadline for a write, whose writer the connector opens once the session's schema changes and
    commits let it.
  - A receiver checks each head against what its sender may send before anything is held for
    its body: a kind the call does not carry, flags it does not know, a nonzero reserved field
    or a length beyond the kind's bound is refused there. A head refused for its length fails
    the call with the limit's error; any other is `malformed_frame`. A message frame is scanned
    against its message's form before it is decoded (ADR 0042).
  - A spawned connector's streams are sockets: its file descriptor 3 is an opener over which
    the host passes it one end of a fresh socket pair a stream, its control connection first
    (ADR 0017). A connector reached over the network carries each stream on one `Data` call of
    its control connection, the call's request and answer bodies the stream's bytes each way.
  - A data call lives no longer than its connection: once the control connection ends, the host
    gone or the connection failed, the connector ends every data call it still serves, a writer
    waiting in its destination among them. A stop lets the data calls in flight end, within the
    connector's drain, before it closes the control connection, and takes no new ones. A stream
    that ends before the answer a host awaits fails the call as `connector_lost`: a connector
    that crashed ends it as one that closes it.
  - There is no credit. On a socket the kernel's buffer paces a sender: a write blocks once its
    reader stops, and the host's reader stops when the engine's budget has no room for the next
    frame. A sender that ignores this parks only its socket's buffer, in the kernel. On a `Data`
    call, HTTP/2's windows pace and bound the sender: each call's stream window is
    `STREAM_WINDOW`, a frame at the least frame limit a peer may set (4 MiB), and the connection's
    window holds every call's and one more (`MAX_CALLS`, 200, the heartbeat among them), so a
    peer parks at most a stream window a call, and never starves the connection's other calls,
    the heartbeat among them. Frames that carry no event are bounded by count between events
    (ADR 0042).
  - Both ends take transport frames of up to `TRANSPORT_FRAME_BYTES` (1 MiB): a frame of the
    protocol crosses in few reads and writes, and a ping waits for at most one transport frame.
    Neither end lets HTTP/2 size its windows itself, which would override both.
- **Liveness and deadlines** (§12.6).
  - The host sends a heartbeat every interval (5 s by default). Once as many as it allows (6) are
    unanswered when the next is due, or the heartbeat's stream has not opened within as many
    intervals, the connector is lost, and every call on the connection fails with a transient
    `connector_lost` error. A heartbeat its queue has no room for counts as unanswered: the
    heartbeat never waits on its transport. That lets the engine retry the attempt, and M4c's
    supervision respawn the connector.
  - An echo is a frame of the control connection's. A spawned connector's data calls are sockets
    of their own, which it never waits behind. A dialed connector's ride that connection as `Data`
    calls, and there it waits its turn behind a transport frame of each call with data queued: a
    patience shorter than the link takes to carry those loses a connector that answers every
    beat; the default's 30 s carries a transport frame of all 199 calls on any link of 7 MiB a
    second or more.
  - Each call has its deadline from §12.6, and fails past it with a transient
    `deadline_exceeded` error. The connect deadline bounds a connection's setup, HTTP/2's
    preface and settings, and the handshake's answer together; the configuration has a connect
    deadline of its own. Reporting committed cursors, which §12.6 does not list, takes the
    commit's deadline.
  - A started read has no deadline: silence on a data stream is never fatal. A read asked to
    stop is dropped once the engine's stop wait passes (ADR 0042).
  - Each frame of a write is written within the write-ack deadline, as a stream fills when a
    connector reads nothing: such a write fails with `deadline_exceeded` rather than hanging, and
    so does one to a destination whose writer never returns, once its stages and its stream are
    full (ADR 0042). A frame cut before any of it was written is dropped, and the next write
    sends its schema, and every dictionary after it, again; a frame cut part way leaves the
    stream torn, and its writer refusing every later call with `deadline_exceeded`. A flush sent
    whose stats did not arrive leaves its writer refusing every later call too: the protocol has
    no flush id, and its stats would be taken for the next flush's.
  - After this end stalls, the next heartbeat waits its interval rather than catching up, so a
    stalled host does not lose a live connector. An echo of a heartbeat never sent answers
    nothing.
  - The host's HTTP/2 pings, which end a connection the network dropped silently, wait twice
    the heartbeat's patience. After a stall, HTTP/2 checks a ping's deadline before it reads
    the pong waiting, where the heartbeat reads its echoes first: with the longer patience, the
    heartbeat alone judges whether a connector is lost.
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
- **A read ends with `Done` when the source's read returns**, and with a frame carrying its error
  when it fails, as the in-process adapter returns. A read the engine stopped ends however the
  source's read returned.
- **`rdlt-wire` generates the control calls with tonic-prost-build**: the client and server
  stubs; `serve` adds tonic's server support. The data calls' frames are `rdlt_wire::stream`'s:
  a reader that reads each body into an allocation of exactly its length from a 64-byte address,
  so a batch's buffers and a push's text are read where they arrive, and a writer that writes a
  frame from the batch's own buffers. A served connection's control calls go through
  `route::Router`, which holds each request to its bounds, serves a `Data` call's stream as a
  passed one is served, and sends every other call to the generated server. It re-exports tonic
  and prost, and states the protocol's version as `PROTOCOL_MAJOR` (6), which both ends must
  match.
- **The gates follow the code.** The served end is tested from `rdlt-host`, where a client
  exists, so `rdlt-host`'s tests run against `rdlt-wire`'s mutants (ADR 0003), and coverage runs
  `rdlt-host`'s tests with the engine's, connector's and wire's.

## Consequences

- A connector in another language serves the control calls with any gRPC implementation, and its
  data calls' frames as this record states them: the protocol's behaviour is in its messages,
  its statuses, its frames and this record.
- `serve::<C>()`, a connector binary's whole `main` (§11.7), with `--rdlt-fd` and `--listen`, lands
  with M4c's placements. Taking a pre-connected socket from file descriptor 3 needs the audited
  `unsafe` §20.14 allows.
- `rdlt_connector::testing::certify` over a real socket (§11.8) needs a client, and a client
  needs `rdlt-host`, which the connector crate cannot depend on. It lands with `rdlt-certify` in
  M4d.
