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

## FROZEN contract — "v1", frozen 2026-08-07

Governed by `docs/adr/0001-out-of-process-connectors.md`, decision D8,
whose experimental period this freeze CLOSES; the ADR carries the
amendment and the evidence. In short, three things beat on this wire
before it froze: a standalone certifier (`rdlt-certify`) that drives
any connector executable, in any language, against 29 conformance
clauses — each one proven capable of FAILING against a deliberately
broken connector, and including a `SIGKILL` matrix at every message
boundary, which is a truer crash than an injected failpoint; a
non-Rust connector (a small Python one) certified by that same binary
against those same clauses; and a recorded benchmark session in which
every throughput bar the project holds itself to still passed with the
connectors OUT OF PROCESS, wire and all.

**"v1" names the contract, not a number on the wire.** The negotiated
`PROTOCOL_VERSION` stays `0`, and the file stays
`rdlt_connector_v0.proto`. That number is the identifier both sides
compare during the handshake; bumping it for a freeze that moves no
byte would break every shipped handshake and buy nothing. A `1` on this
wire is reserved for a genuinely incompatible protocol, if one is ever
needed. What froze is the CONTRACT — the rules below — not the
identifier.

### The compatibility rules, binding from here

**1. Field numbers are frozen.** No number is renumbered, repurposed,
or recycled for a new meaning; a retired field's number is `reserved`,
never handed to something else. A renumber is invisible to almost
every net a repo can have — the Rust structs still compile (names
don't change), and every end-to-end test still passes, because both
in-tree sides regenerate from the same `.proto` and so agree with each
other while disagreeing with the contract. The only party that breaks
is a third party codegen'd from an earlier copy, which is exactly the
failure this rule exists to prevent. So the rule carries TWO nets,
covering different things:

- **numbering, exhaustively** — `tests/cases/test_field_numbers.rs`
  reads this crate's `.proto` as text and checks EVERY
  `(message, field, number)` triple, plus every enum value and the set
  of declared messages, against a frozen table. Every message is
  covered, not a sample. A renumber, a reused number, a deleted field,
  or a vanished message fails that test by name;
- **encoding, end to end, on five representative messages** —
  `tests/cases/test_frames.rs` encodes `HandshakeRequest`, a `Write`
  session request, a `part_closed` session reply, an `arrow_ipc` read
  frame and a `SpecReply` with fixed field values and compares the
  result against hardcoded hex, alongside a pin that
  `PROTOCOL_VERSION` is `0`. This proves the whole prost/tonic path
  actually puts those numbers on the wire — a sample, deliberately,
  since its subject is the generated encoder rather than the contract
  text.

Post-freeze both are COMPATIBILITY pins, not only correctness pins: a
red one means the change under it breaks connectors already shipped,
and the answer is to revise the change, never the pin. Adding a field
is legal and updates the number table deliberately; changing or
removing a row is not.

**2. Evolution is additive only.** New fields take fresh numbers on
existing messages; new messages, new RPCs, new `oneof` arms, and new
enum values may be added. Nothing is removed, narrowed, made required,
or given a second meaning. The other half of additive-only is that a
receiver must TOLERATE what it does not know — an unrecognized enum
value or `oneof` arm from a newer peer is neither a crash nor a silent
accept. The shipped posture is safe-loud: the client normalizes an
`Unspecified` or unrecognized `Classification` to `Fatal` rather than
guessing that an unknown refusal is retryable, since guessing
"transient" would retry a permanent failure forever.

**3. The `#[non_exhaustive]` discipline is what makes rule 2
non-breaking in Rust.** The SPI and client types the wire maps onto —
error classifications, the source/destination context and request
structs, destination capabilities, the client's own failure and
requirement types — are all `#[non_exhaustive]`, so a wire addition can
grow its Rust counterpart without a semver break, and out-of-crate
matches already carry the wildcard arm that a new variant needs. An
addition that would force removing that attribute is not additive; it
is a new protocol.

**4. The handshake line grammar is frozen** — the five pipe-separated
fields spelled out below, parsed by splitting on the first four pipes
only. The line carries its OWN format version (`1`), independent of
`PROTOCOL_VERSION`, and THAT is the escape hatch for the line itself: a
different line shape takes format `2`, and a parser refusing a format
it does not know is behaving correctly.

**5. The named clauses below are frozen behavior, each already pinned
by a test in this workspace; each one that ALSO carries a certifier
clause a third-party connector answers to names that clause's id
below.** The freeze makes those pins load-bearing for COMPATIBILITY.
Where a bullet names no clause id — or names one whose certifier scope
is NARROWER than the bullet's own claim — the rule is frozen and pinned
by this workspace's tests, but a third party's own connector is not yet
measured against the uncovered part; read the gap as owed work, never
as licence. TWO such gaps exist today. Both are on the WRITE direction,
both are named in place below, and each names the clause that would
close it: the write half of the one-batch rule (owed clause P11) and
the write half of the cause-text rule (owed clause P12). Each clause's
substance is stated here; the certifier ships every clause id with its
full definition (`rdlt-certify --explain`).

This list is a HIGHLIGHT, not the whole contract: it names the
wire-shape rules a client author most easily gets wrong. The
certifier's FULL clause set — the source resume and cancellation laws,
the destination's exactly-once laws, all ten protocol clauses, and the
`SIGKILL` matrix — is the behavioral contract a connector must pass,
and none of it is less frozen for going unlisted here.

- **the two refusal shapes** — a protocol-state violation answers a raw
  gRPC `Status`, a connector outcome answers an `ErrorFrame` inside a
  normally-completing RPC (see "`Status` vs `ErrorFrame`" below). Three
  clauses cover this between them: the `Status` half by clause P8,
  which drives the one protocol-state violation v0 defines — a second
  concurrent `OpenSession`, refused `FailedPrecondition` (`OpenSession`
  is the destination service's RPC; the source service runs no session
  state machine, so there is no source-side counterpart to certify);
  and the `ErrorFrame` half in both directions — on the source by
  clause P6, which requires an induced read refusal to arrive as a
  terminal error frame rather than a clean end of stream, and on the
  destination by clause P10, which requires a `write` to a
  never-ensured table, and a `publish` for an already-receipted load,
  to be refused with a typed error frame inside a session that goes on
  completing normally. What is NOT covered is the destination frame's
  own shape: P10 judges only that the refusal IS an error frame and
  discards its contents, so the classification enum and bare cause text
  go unjudged in the write direction. That is the same gap the
  cause-text bullet below discloses (owed clause P12), not a third one;
- **one Arrow batch per frame**, in both directions — the write side
  refuses a second batch FATAL, the read side's refusal seat is the
  client. **The certifier pins the READ half only**: clause P5 judges
  read frames on the wire bytes and says nothing about `Write`, and the
  certifier's own write frames are single-batch BY CONSTRUCTION
  (`rdlt-certify` builds each one from a single fixture batch), so a
  third-party DESTINATION that silently accepted a multi-batch `Write`
  and kept the first batch would certify all-Pass today — the shape of
  the 038 T5 defect this rule exists to forbid. A write-side clause
  (P11: a multi-batch `Write` frame must be refused FATAL, never
  partially accepted) is the certifier's NEXT clause and is owed;
  until it ships, the write half is frozen by this rule and pinned by
  this workspace's tests alone;
- **the error-frame cause-text contract** — `ErrorFrame.message` is
  CAUSE text only; classification travels solely as the enum, and no
  server writes a rendered classification into `message`. **The
  certifier pins the READ half only**: clause P6 induces a refusal by
  reading a reserved nonexistent stream name and then judges the
  resulting frame — terminal, a real classification enum value, and a
  message that does not begin with one of the four client renderings
  (`transient|fatal source|destination error: `) — but P6 is listed
  only in the certifier's SOURCE wire clause set; the DESTINATION set
  is P3 and P7 alone, and every destination probe that induces a
  refusal accepts the error frame and discards it without reading its
  message. So a third-party DESTINATION that rendered
  `"fatal destination error: …"` into `ErrorFrame.message` — the 026
  double-frame class, where the receiving client renders the frame a
  second time — would certify all-Pass today. A write-side clause
  (P12: a destination's error-frame message must carry bare cause text,
  never a rendered classification) is owed beside P11; until it ships,
  the write half is frozen by this rule and pinned by this workspace's
  tests alone;
- **the one-session-per-process ceiling** — a second concurrent
  `OpenSession` on a live socket is refused `FailedPrecondition`
  (clause P8);
- **the handshake identity rules** — the identity a handshake reports
  must agree with the connector's own `Spec` document and with the id
  the operator named; a mismatch is refused, never worked around
  (clause P3), and an unknown config field is refused at the handshake
  with a typed, classified refusal (clause P2).

**What is NOT foreclosed.** Three doors are deliberately left open, and
all three are additive:

- **backpressure credits.** The hatch this proto's own header names:
  `Read` rides HTTP/2 flow control and declares no byte-budget,
  credit, or window field at all, so if a future measurement finds
  that bound insufficient, a `ReadCredit` message is an addition the
  frozen rules permit;
- **state-format negotiation.** `state_format_versions` on
  `HandshakeOk` ships EMPTY in v0 and is threaded through unread (see
  the note at the end of this README) — with one format version per
  state kind there is nothing to negotiate yet. The field exists,
  frozen at its number; the negotiation SEMANTICS belong to the
  feature that adds a second format version, and defining them then
  breaks nothing now;
- **network transports.** TCP+mTLS for provider-managed remote fleets
  is a future binding of this SAME proto, with its own trust model.

Freezing the contract froze the rules of change, not the surface's
growth.

The proto file's own header comment and `src/lib.rs` both mirror this
status rather than being the one place it is recorded.

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
`HandshakeOk` is a **v0 HOLE, not an oversight**: v0 servers send an
empty map, and 039's client (`rdlt-connector-client`, surfaced through
`rdlt-runtime`) threads it through to embedders UNREAD
(`ManagedSource`/`ManagedDestination::state_format_versions`) — with
one format version per state kind there is nothing to negotiate yet.
Negotiation semantics are owned by the feature that adds a second
format version; the map ships empty until then.

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
SAME generic 039's remote adapter reuses rather than reimplements.
A foreign client that gets this wrong is not refereed by this server.
**The only thing that actually saves exactly-once here is the
destination's OWN durable receipt guard inside `Backend::publish`** —
a shipped `Backend` that doesn't keep one is wire-reachably
double-publishable. That guard is now PROVEN over the wire rather than
assumed: the standalone certifier drives a real spawned connector
frame by frame and asserts that re-committing the same
`(load_id, commit_seq)` returns the prior receipt and re-publishes
nothing, and its kill matrix `SIGKILL`s the connector at every commit
boundary and requires a fresh process re-driving the same load to
leave exactly the fixture rows — nothing lost, nothing doubled.

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

`ErrorFrame.message` is the CAUSE text; classification travels only as
the enum; the receiving client renders the classification frame exactly
once on reconstruction. A server never puts a rendered classification
frame (rdlt's SPI `Display` spellings, or any equivalent of its own)
into `message` — a third-party server authors its own cause text and
cannot know the receiving side's spellings, so the wire carries causes,
never frames.

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
bug; 039's client threads it through to embedders unread, and the
resume-format negotiation this field exists for is owned by the feature
that adds a second format version.
