# rdlt-connector

The connector SPI: the PROTOCOL layer of rdlt's connector SDK, and
nothing that moves fast. The `source::Source` and
`destination::Destination`/`destination::LoadSession` trait contract,
the classified error taxonomy, the byte-budgeted record channel, the
trust-boundary gate, and `Secret`. The engine and every connector build
against this crate, and a connector can be written against it alone.

The crate is deliberately protocol-only: everything in it is vocabulary
BOTH sides of the contract speak — hosts and connectors alike.
Storage-, format-, and driver-specific vocabulary (parquet write
options, part-rolling policy, PEM material, an object store's
recoverability rule) belongs to the individual connector that needs it,
not to this crate and not to `rdlt-connector-sdk`.

## The modules — the crate's table of contents

Every item lives on its canonical module path; the crate root holds no
item re-exports.

| module | what it holds |
|---|---|
| `source` | `source::Source`, its per-read `ReadRequest`, and the `StreamSpec` declarations a source offers |
| `destination` | `destination::Destination`, its per-run `destination::LoadSession`, the `OpenContext` a session opens under, the `destination::Capabilities` declaration, and the part-closed telemetry events (`PartClosed`, `PartCloseReason`) |
| `error` | the classified taxonomy: `SourceError` / `DestinationError`, plus `BoxError` |
| `spec` | `ConnectorSpec` — connector self-description |
| `channel` | the byte-budgeted channel: the generic byte core (`channel::bytes`) and the records layer (`channel::records`) sources push through |
| `gate` | the trust-boundary toolbox: the size ceilings (`MAX_DOCUMENT_BYTES`, `MAX_CURSOR_BYTES`, `MAX_WIRE_IDENTIFIER_BYTES`) and the refusal helpers every seat that handles connector-authored bytes installs before acting on them |
| `secret` | `Secret` — a credential that cannot be printed by accident |

Two module re-exports complete the surface. `rdlt_connector::core` is
`rdlt_core`: connectors take vocabulary types from there, never from
`rdlt_core` directly, so a connector crate has exactly one foundation
dependency and single-version identity across a workspace.
`rdlt_connector::arrow` is `arrow_array`, re-exported for the same
single-version reason — `arrow::RecordBatch` is the canonical spelling.

## The contract in one page

- **Classification is the connector's whole retry story**: `Transient`
  and `RateLimited` (with the server's `Retry-After` forwarded) are
  retried by the host with backoff; `Fatal` aborts the run. Connectors
  never loop.
- **Backpressure is bytes, not messages**: awaiting a push IS the flow
  control; the budget counts queued-and-unprocessed bytes (permits ride
  with values), zero-byte checkpoints are never gated, an oversized item
  drains the budget rather than deadlocking, and closing the channel
  wakes a parked producer.
- **Sources resume**: given `since`, never re-emit rows that cursor
  covers. A closed channel is cancellation, not an error.
- **Destinations stage invisibly and publish atomically** with pipeline
  state; re-committing a `(load_id, commit_seq)` returns the prior
  receipt and republishes nothing; a new session makes a crashed
  predecessor's staging invisible.
- **Connector-authored bytes are gated before they are acted on**: the
  `gate` ceilings bound untyped documents at 8 MiB and cursors at 4 MiB
  (a cursor is also recorded in the engine's WAL, so its bound is
  deliberately tighter), and the gate's refusal helpers are the one
  implementation shared by every decode seat — in-process, serve-side,
  and client-side — so the seats cannot drift.
- **Secrets never render**: `Secret` masks both `Debug` and `Display`;
  `reveal()` is the sole, grep-able accessor.

## Second generation

This crate is the second-generation rewrite of the original SPI, which
it replaced wholesale at workspace version 0.3.0: behavior is
contract-identical — the six error classification frames, the channel's
byte-budget semantics, secret redaction, and every serde spelling
carried over verbatim — while the Rust API gained the additions below.
The rewrite was re-derived from the generation-1 contract under a
no-copying rule, and each addition was a recorded decision, not drift.

- **`check()`** on both traits: a cheap connectivity probe distinct from
  moving data — "are the credentials right" answered in seconds. Default
  body reports success without probing, documented as such.
- **`SourceError::context` / `DestinationError::context`**: attach
  context around the variant's INNER cause, preserving classification and
  `retry_after`. Double-framing ("transient source error: … transient
  source error: …") — a defect found independently in two connectors —
  is now inexpressible.
- **Extendable capabilities**: `destination::Capabilities` is
  `#[non_exhaustive]` with `with_*` declaration builders over a
  conservative `Default`, so a future capability is one new method, not a
  breaking change for every out-of-tree destination.
- **`OpenContext`** (was `OpenCtx`), and modules named by their nouns —
  the paths teach the structure.

Features: `failpoints` (crash-sweep seam, routed to `rdlt-core`),
`schema` (`schemars::JsonSchema` for `Secret`).

Behavioral conformance lives in `rdlt-testkit`: certified = passes the
conformance suites there.
