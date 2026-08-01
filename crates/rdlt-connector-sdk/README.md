# rdlt-connector-sdk

The connector-builder framework — the AUTHORING layer of rdlt's
connector SDK. The protocol lives in `rdlt-connector` (re-exported here
as `spi`); the conformance suites live in `rdlt-testkit`; this crate is
what a connector is **built on**: the SDK owns the protocol choreography
and plumbing, the author fills in the system-specific holes. Hosts never
depend on this crate.

```rust
use rdlt_connector_sdk::prelude::*;
```

## The three seams

**`config::Document`** — the validated config-document contract. Every
text entry point (`from_yaml`/`from_json`/`from_value`) parses THEN
validates, by construction; the associated error type absorbs the parser
errors through the connector's own `From` impls, so every frozen,
connector-specific message spelling survives adoption unchanged. The
seam renders no text — that is what the duplication evidence demanded
(the connectors' frozen prefixes disagree in spelling, and one
deliberately leaves parse errors unprefixed).

**`source`** — implement `SourceConnector` (name/version constants, a
`Document` config, `assemble`, `streams`, `read_stream`) and
`source::shell` provides the SPI: `spec()` assembled from the constants
and schema, `check`/`streams` delegated, every read handed a `Feed`.
The `Feed` makes closed-channel-is-cancellation a property of the type —
each push returns `ControlFlow`, and `Break` means the host hung up.

**`destination`** — implement `DestinationConnector` plus a `Backend`
(the system IO: ensure, write, publish, receipts, state) and the SDK's
session choreography enforces the conformance clauses by construction:
a write to a never-ensured table is refused, and a re-committed
`(load_id, commit_seq)` returns its existing receipt before anything
republishes. Atomicity and staging invisibility remain the backend's
storage contract — properties no wrapper can add — and the kits verify
them.

## Proof discipline

The crate's own suite certifies a complete in-memory example connector
(`tests/cases/example.rs` — also the reference an author reads) against
the **same `rdlt-testkit` conformance kits every shipping connector
answers to**. "The framework satisfies the SPI contract" is a test
result here, not a claim.

## What deliberately stays in the connector

Every frozen message spelling, error-classification keys (six
connectors classify on six different, load-bearing keys), cursor
machinery, and SQL planning. The extraction evidence behind those
boundaries is recorded in `specs/027-sdk-trio/plan.md` (Wave 2).

Features: `schema` (`config::schema_of` — JSON Schema generated from
the config structs themselves).
