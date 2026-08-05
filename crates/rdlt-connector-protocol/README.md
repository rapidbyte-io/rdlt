# rdlt-connector-protocol

The versioned gRPC contract for out-of-process rdlt connectors: what a
spawned connector process prints on stdout before it serves anything,
and the `.proto` (compiled hermetically at build time — `build.rs`
vendors its own `protoc`, no system install required) that governs
everything after.

This crate is deliberately thin: a handshake-line parser/renderer
([`handshake::Line`]) and the generated protobuf/gRPC types
([`proto`]). It carries no server and no client — the sdk's `serve`
module (`rdlt-connector-sdk`, feature `serve`) turns an sdk connector
into a server over this protocol; a provider that DIALS a connector
(feature 039) is the client. This crate is the one thing both sides
depend on.

## EXPERIMENTAL status

Governed by `docs/adr/0001-out-of-process-connectors.md`, decision D8.
**Versioned, not frozen.** Field numbers are pinned from day one —
evolution is additive only, never a renumbering — but the surface
itself (message shapes, RPC names, the handshake choreography) may
still move. It freezes once BOTH of the following have exercised it,
not before:

- the protocol conformance kit (feature 040): a standalone certifier
  that drives any connector executable, in any language, against the
  same clauses first-party connectors answer to, including a
  process-kill matrix at every message boundary — a real `SIGKILL`
  mid-publish is a truer crash than an injected failpoint;
- at least one non-Rust implementation (feature 040's Python proof
  connector, deliberately small).

Until both exist, treat every message shape here as provisional. The
proto file's own header comment carries the same banner — this README
and `src/lib.rs` both mirror it rather than being the one place it's
recorded.

## Trust model (owner decision D-038-1)

Config documents — which may carry credentials — cross the Unix domain
socket **in the clear**. There is no protocol-level encryption or
authentication in v0:

- the socket is created owner-only (mode `0600`, enforced by the sdk's
  `serve::common::bind_uds` — not by anything in this crate, since
  this crate has no server);
- a spawned connector process inherits its operator's trust exactly
  like any other child process — the same boundary a locally-installed
  CLI plugin or a `sudo` child crosses;
- nothing in this protocol redacts or encrypts a `*_json` payload —
  never log `config_json`, `table_schema_json`, or any other `*_json`
  field verbatim, since it may carry a `Secret`'s revealed value.

`Secret` *references* (a config field naming WHERE a credential lives —
an environment variable, a secret-manager path — rather than carrying
the credential's value) are the recorded direction for a future network
transport, not built in v0. Network transports (TCP+mTLS for
provider-managed remote fleets — ADR 0001 D3) are a future binding of
this SAME proto; a different trust model belongs to that binding when
it's built, not retrofitted onto UDS today.

## The handshake line

Before any gRPC exists, a spawned connector process needs to tell its
parent WHERE to dial and WHICH protocol versions it accepts. It does
that with exactly one line on stdout, then falls silent (stderr stays
the human log channel):

```
rdlt-connector|1|<proto_min>|<proto_max>|<socket_path>
```

Five pipe-separated fields, FROZEN format (not versioned the same way
the RPC protocol is — see below):

| field | meaning |
|---|---|
| `rdlt-connector` | leading token; anything else and the line is not one of ours |
| `1` | the LINE FORMAT's own version — independent of `PROTOCOL_VERSION` below; the line could reach format `2` while the RPC protocol stays at `0` |
| `proto_min` | lowest `PROTOCOL_VERSION` this connector process will accept over `Handshake` |
| `proto_max` | highest `PROTOCOL_VERSION` this connector process will accept |
| `socket_path` | the Unix domain socket to dial `Connector`/`SourceService`/`DestinationService` on |

Parsing splits on the FIRST FOUR pipes only (`splitn(5, '|')`) — the
socket path is the one field never re-split, so a path containing `|`
survives (`handshake::Line::parse`'s own test pins this). [`PROTOCOL_VERSION`]
is the value this crate's generated code actually implements (`0` for
v0); it is distinct from the line format's `1`.

## The three services

```protobuf
service Connector {
  rpc Handshake(HandshakeRequest) returns (HandshakeReply);
  rpc Check(CheckRequest) returns (CheckReply);
  rpc Spec(SpecRequest) returns (SpecReply);
}
service SourceService {
  rpc Streams(StreamsRequest) returns (StreamsReply);
  rpc Read(ReadRequest) returns (stream ReadFrame);
}
service DestinationService {
  rpc OpenSession(stream SessionRequest) returns (stream SessionReply);
}
```

**`Connector`** is answered by every connector regardless of role.
`Handshake` carries the config document (`config_json`), the requested
`protocol_version`, and `expected_role` ("source" | "destination"); a
successful reply carries the connector's spec, its `ConnectorSpec`
and (for a destination) `DestinationCapabilities`, both pre-serialized
JSON. A refused handshake — wrong role, an out-of-range protocol
version, undecodable or invalid config, or a second `Handshake` on an
already-populated session — answers `HandshakeReply.error`, an
`ErrorFrame`, always classified `FATAL`. The config-free `Spec` RPC
answers before the handshake — it carries no session state, only the
connector's static identity: `SpecReply.spec_json` is `ConnectorSpec`
JSON (name, version, config_schema), served from the connector's
statics alone, so a provider can ask a spawned connector what it IS
before deciding what config to hand it. `state_format_versions` on
`HandshakeOk` is a **v0 HOLE, not an oversight**: nothing on either
side negotiates a resume-format version to put there yet, because
nothing dials this protocol end-to-end today. Feature 039's adapter is
where that negotiation gets designed; it ships empty until then.

**`SourceService`** is a straightforward discover-then-stream shape:
`Streams` lists what's available, `Read` streams `ReadFrame`s (one of
`raw_json`, `arrow_ipc`, `checkpoint_cursor_json`, or a terminal
`error`) for one requested stream, optionally resuming from a cursor.

**`DestinationService` — the amended session semantics.** `OpenSession`
is ONE long-lived bidirectional stream, and that stream **is** the
session: every frame (`Open`/`Ensure`/`Write`/`ExistingReceipt`/
`Replay`/`Publish`/`ReadState`/`Close`) maps 1:1 onto its own method on
the connector's raw `Backend` — the wire speaks the real exactly-once
grammar directly, not a collapsed `commit` call. This is an AMENDMENT
from the design's original shape (038 Task 5 review, ADR D5): an
earlier version wrapped the sdk's own `LoadSession`, which made
`ExistingReceipt`/`Replay` inert stubs instead of real answers.

**The one-batch `Write` rule.** `Write.arrow_ipc` carries EXACTLY ONE
record batch as an Arrow IPC *stream* (one schema message, one
record-batch message). A second batch message in the same `Write` frame
refuses FATAL rather than being silently accepted with only the first
batch written — that was measured as silent row loss during 038 Task 5's
review, not a hypothetical. A multi-batch write is several `Write`
frames, one batch each; the proto's own comment on `Write` states this
rule verbatim. The same one-batch rule governs the read direction:
`ReadFrame.arrow_ipc` carries exactly one batch per frame, but that
direction is server-streamed, so enforcement sits with conforming
CLIENTS — a frame carrying a second batch message is refused, not
silently truncated to its first batch (the client-side posture is
feature 039's decode contract; the proto's comment on
`ReadFrame.arrow_ipc` states the rule).

The amendment has a consequence a wire client MUST understand: **this
service does not sequence commit frames for you.** Driving
`ExistingReceipt` → `Replay-or-Publish` in the right order, and never
sending `Publish` twice for one `(load_id, commit_seq)` without asking
`ExistingReceipt` first, is the CALLER's job — the same job the sdk's
in-process `Session<B>` type already does for an embedder, and the
SAME generic 039's remote adapter will reuse rather than reimplement.
A foreign client that gets this wrong is not refereed by this server.
**The only thing that actually saves exactly-once here is the
destination's OWN durable receipt guard inside `Backend::publish`** —
a shipped `Backend` that doesn't keep one is wire-reachably
double-publishable. Nothing in this codebase currently proves a
shipped `Backend` keeps that guard when reached over the wire (the
sdk's conformance kit today only exercises the in-process
`Session<B>` path, where the choreography IS enforced by the caller);
feature 040's conformance kit owns closing that gap.

The other non-obvious rule: **the client MUST read replies
concurrently with sending requests**, not write-everything-then-read.
The server's reply channel is bounded and interleaves `PartClosedEvent`
notifications with request replies; a client that defers reading until
every request is sent can wedge itself against a full HTTP/2
flow-control window once enough unread replies queue up server-side.
The proto's own comment on `DestinationService` carries this same
warning in substance.

**The `PartClosedEvent` ordering promise — narrowed, on purpose (038 T5
review).** Copied verbatim from the sdk's `serve::destination` module
doc, the one place this rule is authored (this README quotes it rather
than re-deriving it, so the two can't drift):

> `OpenContext::part_events` is the other place this server departs
> from a plain request/reply shape: the listener is a SYNC callback, so
> any part it reports while a `Backend` call is in flight is already
> sitting in the unbounded channel by the time that call's `await`
> returns. Draining that channel immediately BEFORE sending the reply
> for the call that (may have) produced it is what the ordering promise
> actually covers: every part already queued when a call returns
> precedes that call's own reply. An asynchronously emitted part — one a
> buffering backend fires from a task this server never directly
> awaited — carries no such promise; it simply arrives as its own
> `PartClosedEvent` reply as soon as the request loop next turns (the
> `biased` `select!` in `drive_session`), which may land before, after,
> or interleaved with any particular request's reply.

In short: `part_closed` before the reply of the call that (synchronously)
caused it — a promise about ONE call's own emissions, not a global
ordering across the whole session.

## Frame size ceiling

Served connectors raise tonic's default 4 MiB per-message receive cap
to a hard ceiling of **64 MiB** — THIS crate's [`MAX_FRAME_BYTES`],
which both sides of the wire import (the sdk's `serve` module installs
it on every served service; a dialing client sets its decode cap from
the same constant, so the ceiling can never skew between the two). The
SPI's byte-budget channels run
8-64 MiB, so a single legitimate Arrow batch in a `Write` frame (or a
`ReadFrame`) may exceed 4 MiB — and the one-batch-per-frame rule means
such a batch has no conforming way to be delivered smaller. HTTP/2
flow-control windows remain the PACING mechanism within the ceiling;
the cap is the hard refusal line, deliberately above any in-tree
budget. A foreign integrator (any client not importing this crate)
MUST set its own decode cap to match: a dialing side left at tonic's
4 MiB default kills the stream with an opaque transport error on the
first over-4 MiB frame a server legally sends.

## `Status` vs `ErrorFrame`: two refusal shapes, on purpose

A caller has to know both exist and check the right one for a given
refusal — this rule is recorded exactly twice: here, and once more on
the sdk's `serve` module doc (`rdlt-connector-sdk::serve`), never a
third time.

A **protocol-state violation** — any RPC other than `Handshake` arriving
before a handshake has completed (the config-free `Spec` RPC is the one
other exemption: it answers before the handshake — it carries no
session state, only the connector's static identity, so arriving early
is not a violation at all), or a second concurrent `OpenSession`
while one is already active on the same served connector process —
answers as a raw gRPC `Status` (`Code::FailedPrecondition`,
`"handshake has not completed"` / `"one session per connector
process"`), ending the RPC outright. There was never a valid session for
a payload-shaped outcome to be reported INTO, so there is nothing to
carry one.

A **connector outcome** — everything else, including a `Handshake` RPC's
OWN refusals (bad role, out-of-range version, undecodable or invalid
config) and every refusal reachable once a `DestinationService` session
is open (write-before-`Open`, write-before-`Ensure`, a second `Open`
frame, an empty request frame, a connector's own classified failure) —
answers as an `ErrorFrame` carried as reply-payload state
(`Classification::{Transient,RateLimited,Fatal}`), inside an RPC/stream
that itself completes normally. A `Read` request whose
`stream_spec_json`/`since_cursor_json` fails to decode rides this shape
too: the refusal is the response stream's first and only frame, a
terminal `error` — never a `Status`. This is DATA a caller is meant to
inspect uniformly, not a protocol bug — the RPC layer has no reason to
reject the call that reported it.

## Gotchas, named so they're searchable

**grpc-python over a Unix domain socket needs an explicit authority.**
Measured live (research spike, `specs/038-connector-protocol/research.md`
§4): grpc-python's C-core UDS resolver synthesizes an HTTP/2
`:authority` pseudo-header from the socket path itself, which tonic's
`h2` layer rejects outright — `RST_STREAM (error code 1)` on EVERY RPC,
including a bare unary call, with the server-side log reading
`malformed headers: malformed authority ... invalid authority`. The
fix is one channel option on the Python side:

```python
channel = grpc.insecure_channel(
    target, options=(("grpc.default_authority", "localhost"),)
)
```

With that option set, both unary and streaming RPCs succeed cleanly.
Without it, the failure gives no hint that the fix is a channel option
rather than a broken server — expect this to be the first thing a new
non-Rust integrator hits, and diagnose it by that exact
`RST_STREAM ... error code 1` signature before assuming the server is
at fault.

**tonic's default CLIENT flow-control window is ~2 MiB, not HTTP/2's
64 KiB.** Measured live (same spike, §2): a `tonic::transport::Endpoint`
that never calls `initial_stream_window_size`/
`initial_connection_window_size` explicitly gets a much larger window
than the raw HTTP/2 spec default — generous, not a problem by itself,
but it means "just don't configure it" silently means "up to ~2 MiB of
in-flight bytes per stream," not "64 KiB." If a memory ceiling on
in-flight streamed bytes actually matters to a caller (a provider
holding many concurrent connector sessions, say), set both window
sizes explicitly rather than relying on the default. This crate has no
opinion on the number — it's a client `Endpoint` concern, which is
039's adapter, not this crate or the sdk's `serve` listener (the accept
side of the same connection has no `Endpoint` to configure).

**`state_format_versions` is empty in v0, on purpose — see above.** A
provider reading `HandshakeOk` today gets an empty map, not an omission
bug; feature 039 is where the resume-format negotiation this field
exists for actually gets designed and populated.
