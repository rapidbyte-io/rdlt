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
  guard against enshrining first-draft accidents.

## Explicitly out of scope for rdlt (forever, per the vision)

Connector registries and marketplaces, artifact repositories and
downloads, Docker/Kubernetes orchestration, fleet management,
scheduled re-certification. These are rapidbyte's layer, built on
`ConnectorProvider`.
