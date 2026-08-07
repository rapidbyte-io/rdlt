# ADR 0001 — Out-of-process connectors and the connector protocol

Status: ACCEPTED (2026-08-05). Governs the connector-runtime program
(features 038+). Amendments are recorded here, never applied
silently.

## Context

rdlt's connectors compile into the engine binary today. That shape
built the engine's identity — the in-process byte-bounded channel,
Arrow batches without a serialization boundary, crash sweeps arming
failpoints inside connector code, one workspace gated as a whole —
but it couples every connector's release to the engine's, makes the
engine binary carry every connector's dependency tree (duckdb's C++,
ODPI-C, the snowflake fork-pinned dependency that blocks publishing
rdlt/rdlt-cli at all), and forecloses third-party connectors.

The owner's direction (2026-08-05): decoupled-first. Connectors are
to be independently developed, versioned, released, upgraded and
rolled back — including by third parties — without rebuilding rdlt or
the applications embedding it.

The opportunity beyond decoupling: no existing connector protocol
carries correctness as its own grammar. Singer's was too weak (JSON
lines, no backpressure, no exactly-once). Airbyte's keeps correctness
platform-side. rdlt's protocol makes the exactly-once choreography,
byte-bounded backpressure, typed error taxonomy, and version-gated
state formats the WIRE CONTRACT itself — the property 020-037 built
in-process, made legible to any connector in any language.

## Decisions

**D1 — Decoupled-first; the protocol is the product contract.**
Connectors run as separate long-lived processes speaking a versioned
protocol. *(AMENDED 2026-08-05, owner decision:)* the in-process
connector wiring is TRANSITIONAL, not permanent — once the
out-of-process path is ESTABLISHED, the facade's in-process
connector compilation (its per-connector features and dependencies)
is REMOVED, per the house coexist→certify→delete swap discipline
(the 025-031 second-generation precedent). "Established" means: the
reference ports done, the conformance kit certifying them, AND the
remote-mode benchmarks still meeting the recorded bars — the removal
go/no-go sits at that stage, because deleting in-process makes the
remote numbers the benchmark identity. What survives removal: the
SPI traits (`Source`/`Destination`/`LoadSession` — the engine's
consumption surface, which the remote adapters implement) and the
sdk (the connector-authoring framework, used inside connector
binaries). What dies: connectors as library dependencies of the
facade/CLI.

**D2 — The engine's ignorance is preserved.** The engine continues to
accept already-constructed `Source`/`Destination` objects (true
today) and never learns about connector registries, artifact
downloads, Docker, Kubernetes, process spawning, version resolution,
or repositories. A `ConnectorProvider` abstraction at a HIGHER layer
(`rdlt-runtime`) turns a `connector: {id, version}` requirement into
protocol-backed objects. `ManagedSource`/`ManagedDestination` carry:
the dyn object, connector identity, resolved version, immutable
artifact digest, negotiated protocol version, and an optional
lifecycle guard. Resolution of `id + requested version` into
`exact version + digest + endpoint` belongs to the embedding
application (rapidbyte's business), never to rdlt.

**D3 — gRPC is the contract; gRPC-over-UDS is the v1 transport.**
The protocol is defined as a versioned `.proto` (the machine-readable
contract third parties codegen from). Process model follows the
HashiCorp go-plugin precedent: the engine (via a provider) spawns the
connector; the child prints ONE handshake line on stdout (unix socket
path + protocol version range) and then stdout falls silent; the
engine connects gRPC over that socket; stderr is the human log
channel. Network transports (TCP+mTLS for provider-managed remote
fleets) are a future binding of the SAME proto, provider-side —
recorded, not built.

Why gRPC over the also-considered hand-rolled framing (the decisive
arguments, recorded so the trade is auditable): the engine reads
MULTIPLE streams from one source concurrently — per-stream gRPC
server-streams give multiplexing that a single pipe would force us to
reinvent by hand; first-class cancellation/deadline propagation maps
onto `ControlFlow` cancellation; proto codegen is the polyglot
contract JSON Schema alone is not; and the long-term network story is
gRPC's home turf. The cost accepted: tonic/prost in the engine client
and every Rust connector binary, and grpcurl-grade (not jq-grade)
debuggability.

**D4 — Proto is the envelope; serde documents are the payload.** The
proto owns RPC shape and evolution (field-number discipline). Every
already-document-shaped thing crosses as an opaque payload INSIDE
proto messages: connector configs, cursors (already
`serde_json::Value`), `StateDoc`, the capability sheet — all
serde-defined with the house `format_version` gates remaining the
source of truth. Record batches cross as raw Arrow IPC bytes
(Flight-style layout without adopting Flight). This keeps ONE
evolution system per concern and shrinks proto/serde drift to the
envelope.

**D5 — Exactly-once is the wire grammar.** The destination session
protocol mirrors the sdk `Backend` choreography EXACTLY — ensure →
write → existing_receipt → replay → publish → read_state → close
(strict on success, best-effort on abandonment) — not a generic
open/write/commit. The source protocol carries discovery, start-read
with since-cursors, batches with schema epochs (a schema change
precedes the first batch at its version — the 036 causal guarantee,
on the wire), checkpoints, discards, part-closed telemetry events,
progress, completion, and cancellation. The handshake verifies:
connector id, version, artifact digest, role, protocol version
range, and SUPPORTED STATE/CURSOR FORMAT VERSIONS (a connector must
refuse state it cannot resume BEFORE extraction begins — the 037
version-gate discipline, negotiated up front). Connector identity is
persisted in run metadata, reports, WAL metadata, and committed
state, so rollback is an artifact swap, never a rebuild. Scoping, as
shipped by 038: v0 defines the read-stream vocabulary as
batch/checkpoint frames only — the discards, progress, and
source-side part-closed telemetry named here are forward intent,
arriving with the features that need them.

**D6 — Backpressure stays byte-bounded, engine-owned.** The engine's
one byte-budget channel (020 D17) remains the authority. First
measurement (the 038 spike): whether gRPC/h2 suspension-based flow
control (stop polling → windows fill → sender blocks) bounds bytes
tightly enough to honor the budget. If not: explicit protocol-level
byte-credit messages, granted only as bytes leave the bounded
channel, with max-one-unacknowledged-batch as the degenerate v1
mode. Either way the guarantee is the engine's, never an accident of
transport defaults.

**D7 — `serve()` makes connector authorship nearly free.** The sdk
grows a serve module that turns any existing sdk `Shell` into a
protocol server. First-party connectors become thin binaries (a
~10-line `main`), changing essentially no connector logic — the
payoff of 027's inversion of control.

**D8 — The conformance binary is the third-party seam.** The testkit
kits (S-clauses, D-clauses, crash matrices) gain a protocol-driving
twin packaged as a standalone certifier: point it at any connector
executable, in any language, and it certifies against the same
clauses first-party connectors answer to — INCLUDING a process-kill
matrix at every message boundary (a real SIGKILL mid-publish is a
truer crash than an injected failpoint; in-process failpoints remain
for first-party interiors via the `FAILPOINTS` env var at spawn).
The protocol stays EXPERIMENTAL — versioned, but not frozen — until
the conformance kit and at least one non-Rust implementation (a
deliberately small Python connector) have beaten on it.

*(AMENDED 2026-08-07 — THE EXPERIMENTAL PERIOD IS CLOSED. The wire
contract is FROZEN, and "v1" is this decision's label for it.)*

The conditions above were met, and one more was demanded before the
freeze was taken: that the out-of-process shape still be worth
shipping under measurement. The evidence, in the order it landed:

- **The conformance kit exists and can fail.** `rdlt-certify` spawns
  any connector executable — resolved by path or by the discovery
  convention — and certifies it over the real wire against 29 named
  clauses: the source resume and cancellation laws, the destination's
  staging invisibility / atomic state-with-data / idempotent receipts /
  dead-predecessor teardown / idempotent ensure / no-state-when-fresh /
  merge-upsert laws, ten protocol clauses (one stdout handshake line
  and nothing after it; typed refusal of an unknown config field;
  spec/handshake identity agreement; a complete config-free `Spec`
  reply; one Arrow batch per read frame judged on the wire bytes; a
  terminal error frame carrying a real classification and bare cause
  text; a tolerated state-format-version map; the one-session-per-
  process ceiling; abandoned-session reclaim; and a Backend-direct
  order book driven frame by frame with no client-side manners in
  between), and a nine-arm `SIGKILL` matrix. Every clause was proven
  capable of FAILING against a deliberately broken connector — a green
  suite that cannot go red certifies nothing.
- **The kill matrix is a real process kill, not an injected
  failpoint.** It `SIGKILL`s a live connector at every message
  boundary and holds the wire to two promises: a typed error within
  ten seconds (a dead connector must fail the wire, never hang it),
  and — for the destination arms — exactly-once convergence proven by
  re-run, where a fresh process re-drives the same load and the table
  must hold exactly the fixture rows. That is what turned the
  destination's own durable receipt guard from an assumption into a
  measured property of a shipped connector reached over the wire.
- **A non-Rust implementation is certified.** A deliberately small
  Python connector, written against the `.proto` alone, passes the
  same certifier binary and the same clauses first-party connectors
  answer to. The polyglot claim is demonstrated, not asserted.
- **A first-party destination is certified live.** The snowflake
  connector passes the destination clauses against the real service,
  out of process.
- **The throughput bars hold out of process** (recorded session
  2026-08-07, five in-process cells beside their spawned-connector
  twins, every arm rowcount-verified): 9.50x against a 4.0x bar,
  60.00x against 40.0x, 52.30x against 45.0x, 2.42x against 2.0x. The
  wire costs +114 ms to +463 ms per cell (x1.10 to x1.54). The session
  carried asymmetries in BOTH directions and `benches/RESULTS.md`
  names all three: two cut FOR the wire — the rdlt arms ran 9-20%
  slow against their own baseline while the competitor moved 1-4%, so
  every ratio was divided by a high wall and is deflated rather than
  flattered, and the throttled backpressure window below means the
  remote arms ran narrower than configured — while one cuts AGAINST
  it: baseline-first ordering put each remote arm on the quieter
  machine in four of the five pairs (`loadavg_at_start` fell across
  the pair), so the measured overhead is if anything an
  under-statement. None of the three is quantified, so none is netted
  against another or used to restate a figure, and the verdict is
  unmoved either way — all four bars still clear when each remote
  cell's SLOWEST of five runs replaces its median (9.0x / 42.1x /
  50.3x / 2.3x). Every bar tolerates the wire costing at least 1.43x MORE
  than it measured before that bar would bind; the tightest is the
  `s3jsonl-to-pg-200k` cell, and the figure is derived, not measured —
  reproduce it as: the competitor took 64,480 ms and the bar demands
  40x, so rdlt's budget is 1,612 ms; the cell spent 697.9 ms of
  in-process work plus 376.7 ms of wire, leaving 537.4 ms of further
  wire cost available, which is 1.43x the wire cost it actually paid.
  Spawn is NOT the cost — spawn through
  handshake-complete measured 1.63 / 1.81 / 2.06 ms (min / median /
  p90) — so the frozen contract needs no process-pooling or daemon
  mechanism, and none was added.

**What "v1" means, precisely.** The wire's version NUMBER stays `0`
and the file stays `rdlt_connector_v0.proto`. That number is the
identifier the handshake negotiates; bumping it for a freeze that
changes no byte would break every connector already shipped and buy
nothing. A `1` on the wire is reserved for a genuinely incompatible
protocol, should one ever be needed. "v1" is this ADR's label for the
FIRST FROZEN CONTRACT, and the freeze consists of: removing the
EXPERIMENTAL markers, and making these rules binding —

1. field numbers are never renumbered, repurposed, or recycled (a
   retired number is `reserved`). A renumber is silent at the Rust
   type level AND invisible end-to-end — both in-tree sides regenerate
   from the same file, so they agree with each other while disagreeing
   with the contract, and only a third party breaks — so the rule
   carries two nets: a test that reads the `.proto` as text and pins
   EVERY message's every field number against a frozen table
   (exhaustive, the contract's own text), and golden frame pins that
   encode five representative messages and compare the bytes
   (a sample, proving the generated encoder really emits them);
2. evolution is ADDITIVE ONLY — new fields take fresh numbers; new
   messages, RPCs, `oneof` arms and enum values may be added; nothing
   is removed, narrowed, made required, or given a second meaning;
3. a receiver tolerates what a newer peer sends without knowing it,
   safe-loud (an unrecognized classification normalizes to FATAL
   rather than being guessed retryable), with the `#[non_exhaustive]`
   discipline on the Rust types the wire maps onto keeping such
   additions from being semver breaks;
4. the handshake line grammar is frozen, carrying its own independent
   format version as the escape hatch for the line itself;
5. the named clauses are frozen behavior: the two refusal shapes
   (protocol-state violations answer a raw gRPC `Status`, connector
   outcomes answer an `ErrorFrame` inside a normally-completing RPC),
   one Arrow batch per frame in both directions, `ErrorFrame.message`
   as cause text only with classification travelling solely as the
   enum, the one-session-per-process ceiling, and the handshake
   identity rules. That enumeration HIGHLIGHTS the wire-shape rules a
   client author most easily gets wrong; the certifier's full clause
   set — the source laws, the destination's exactly-once laws, all ten
   protocol clauses, and the kill matrix — is the behavioral contract,
   and none of it is less frozen for going unlisted. TWO of those
   rules are stated for both directions but certified on the READ
   direction only, and the README names both gaps in place rather than
   letting the enumeration read as a guarantee: the one-batch rule's
   write half (the certifier's own `Write` frames are single-batch by
   construction, so a destination that quietly kept the first batch of
   a multi-batch `Write` certifies clean — owed clause P11) and the
   cause-text rule's write half (its clause judges an induced READ
   refusal and is listed in the source wire set alone, so a
   destination rendering a classification into `ErrorFrame.message`
   certifies clean — owed clause P12). Both are frozen behavior
   pinned by this workspace's tests; what is owed is a third party's
   measurement against them.

The publish posture did NOT move with the freeze: the protocol,
client, runtime and certifier crates remain `publish = false`, and
the publish wave is separate, owner-scheduled work.

**What the freeze does NOT foreclose, and one live defect that does
not reopen it.** Additive growth is exactly what rules 1-3 preserve.
Three doors stay open: network transports (TCP+mTLS, D3) as a future
binding of this same proto; `state_format_versions` negotiation, whose
field is frozen at its number but ships empty because there is nothing
to negotiate until a second format version exists; and a `ReadCredit`
message as the documented escape hatch for backpressure. That hatch matters, because a real defect was
measured during the benchmark session and it is ENGINE-SIDE, not
wire-side: the engine's byte accounting sums each Arrow buffer's
allocated CAPACITY rather than the slice it actually uses, and an
IPC-decoded batch is a set of zero-copy slices of ONE allocation, so
such a batch meters at roughly its buffer count times its true size
(~17x on the measured table; ~12x more than the locally-built batches
the in-process arm charges, which over-report ~1.4x themselves through
builder doubling). The same expression meters source backpressure, so
a remote Arrow source runs with a far smaller effective in-flight
window than configured — which makes the recorded wire overhead
likely an upper bound, though only likely: no figure has been
restated on the strength of an unmeasured fix, and a wider window also
raises resident bytes in a constellation whose peak RSS already runs
x1.49 to x2.48 the in-process arm's (measured across the five twin
pairs; largest on `pg-to-pg-1m`, no pair reaching 3x, and two of the
five below 2x). This does not reopen the frozen contract: the
proto declares no byte-budget, credit, or window field at all —
`Read` rides HTTP/2 flow control by design (D6) with the engine's own
byte-budget channel as the authority — so both the defect and its fix
live entirely in engine/SPI code that no wire byte describes. Should a
measurement later show flow control insufficient on its own,
`ReadCredit` is the additive addition the frozen rules already permit.

The sequencing rule below that gates the repo split on this freeze is
therefore satisfied: independent connector versioning now has a
standing contract to version against.

**D9 — Sequencing.** Sequential features under this ADR, each
spec'd/planned/gated/reviewed on its own (the 025-031 program shape;
no up-front multi-feature specs — later features learn from earlier
ones). Intent, not spec:

*(AMENDED 2026-08-05: the originally-separate "SPI
protocol-representability" feature collapsed into 038 — exploration
showed the SPI is already dyn-safe (compile-time pinned since 027),
the engine already holds only trait objects, and the facade already
feature-gates connectors with `default = []`; the remaining SPI
polish rides 038, and the spike belongs inside the feature whose
design it referees, per the 023 probe-before-design-freeze pattern.)*

- 038: `rdlt-connector-protocol` (the proto + handshake) + sdk
  `serve()`, with the WALKING-SKELETON SPIKE as its research phase —
  a throwaway echo connector over tonic/UDS measuring (a)
  suspension-based backpressure against the byte budget (the D6
  referee), (b) spawn→UDS→handshake latency, (c) a Python stub
  driving the same proto — plus the small SPI polish (Serialize on
  the part-event payloads, `#[non_exhaustive]` hedges on
  OpenContext/ReadRequest, documented Send-only LoadSession and
  plaintext-secrets-over-UDS trust model).
- 039: remote adapters (`RemoteSource`/`RemoteDestination`/
  `RemoteLoadSession`) + `LocalBinaryConnectorProvider` +
  CLI integration (`connector:` requirement in pipeline YAML;
  `schema`/`validate` ask a spawned connector). Reference port: the
  FILE connector (both halves, no external service, richest crash
  surface); snowflake second (dissolves the fork-dependency publish
  blocker).
- 040: the remote conformance kit + process-kill matrix + the Python
  proof connector.
- 041: benchmarks in remote mode published honestly beside
  in-process, remaining ports, repo-split decision, protocol v1
  freeze.

Standing sequencing rules: the 0.3.0 publish is DEFERRED to the
protocol era (the still-open window is where 038's SPI changes
belong); the house-style refactor (REFACTORING.md) follows the
program; repos split (independent connector versioning becomes real)
only AFTER protocol v1 freezes — versioning independently against a
moving wire contract is fiction.

Repo-split direction (owner decision, 2026-08-06, recorded so 041's
decision arrives pre-shaped rather than reopened): when the split
happens it is ONE `rdlt-connectors` monorepo for first-party
connectors — one house style, one gate, one clause vocabulary,
per-connector RELEASES via crate versions and tags — not
per-connector repos (repo sprawl, gate drift). The engine repo keeps
the contract surface: SPI, sdk, protocol, client, runtime, and the
conformance certifier. The connectors repo is seeded AFTER the D1
swap, so only sdk-born, kit-certified connector code crosses — the
certifier is the boundary (a connector enters when `rdlt-certify`
passes it, the same bar third parties answer to), which is what
keeps the new repo free of legacy by construction.

Merge policy (owner decision, 2026-08-06): interim program features
merge to main INDIVIDUALLY as they complete — they are additive and
inert by construction, per-feature gates stay truthful against real
main, and single-feature merge commits stay revertable. The ONE
atomic-branch moment is the D1 SWAP (deleting the facade's
in-process connector wiring and flipping the default): its own
branch, landing only after the conformance kit certifies the ports
and the remote benchmarks hold the recorded bars, tagged beforehand
for a clean rollback point.

## Consequences accepted

- A serialization boundary costs performance. The benchmark matrix
  gains remote-mode cells, re-recorded and PUBLISHED next to the
  in-process numbers — never hidden. The engine cold-start bar
  (≤40 ms) is unchanged and engine-only; the spawn path gets its own
  measured bar; providers may pool long-lived connector processes
  across runs.
- The testkit grows remote twins of its kits; the gate eventually
  splits (engine gate, per-connector gates, conformance
  certification between them) — designed deliberately at repo-split
  time, not improvised.
- tonic/prost enter the dependency tree of the engine client and
  every Rust connector binary.
- This is a multi-feature program (five 037-sized features is the
  honest estimate), with the protocol's experimental period as the
  guard against enshrining first-draft accidents. *(That guard ran its
  course and did its job: the period closed 2026-08-07 with the
  amendment recorded in D8 above, and several first-draft accidents —
  a silently-truncating multi-batch write, a session shape that made
  the exactly-once frames inert stubs — were caught and corrected
  inside it rather than shipped frozen.)*

## Explicitly out of scope for rdlt (forever, per the vision)

Connector registries and marketplaces, artifact repositories and
downloads, Docker/Kubernetes orchestration, fleet management,
scheduled re-certification. These are rapidbyte's layer, built on
`ConnectorProvider`.
