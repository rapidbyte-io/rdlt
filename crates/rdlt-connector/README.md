# rdlt-connector

The in-process connector SPI, second generation: the PROTOCOL layer of
rdlt's connector SDK. `Source`/`Destination`/`LoadSession` traits, the
byte-budgeted record channel, the capability declaration, the classified
error taxonomy, and the shared config vocabulary (`Secret`, `PemSource`,
parquet options) — and nothing that moves fast.

The engine and every connector build against this crate. It is the
second-generation rewrite of the original SPI, which it replaced
wholesale at workspace version 0.3.0: behavior is contract-identical —
the six error classification frames, the channel's byte-budget
semantics, secret redaction, and every serde spelling carried over
verbatim — while the Rust API gained the additions below. The design
record is `specs/027-sdk-trio/plan.md`.

## What is new over generation 1

- **`check()`** on both traits: a cheap connectivity probe distinct from
  moving data — "are the credentials right" answered in seconds. Default
  body reports success without probing, documented as such.
- **`SourceError::context` / `DestinationError::context`**: attach
  context around the variant's INNER cause, preserving classification and
  `retry_after`. Double-framing ("transient source error: … transient
  source error: …") — a defect found independently in two connectors —
  is now inexpressible.
- **Extendable capabilities**: `DestinationCapabilities` is
  `#[non_exhaustive]` with `with_*` declaration builders over a
  conservative `Default`, so a future capability is one new method, not a
  breaking change for every out-of-tree destination.
- **`OpenContext`** (was `OpenCtx`), and modules named by their nouns:
  the traits live in `source`/`destination`, parquet intentions in
  `parquet` (was `output`), the object-store rule in `store` (was
  `objects`).

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
- **Secrets never render**: `Secret` masks both `Debug` and `Display`;
  `reveal()` is the sole, grep-able accessor. `PemSource` hides inline
  material and shows paths.

Features: `failpoints` (crash-sweep seam, routed to `rdlt-core`),
`schema` (schemars for the shared config types), `object-store` (the one
recoverability rule).

Behavioral conformance lives in `rdlt-testkit`: certified = passes the
conformance suites there.
