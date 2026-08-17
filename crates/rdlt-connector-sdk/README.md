# rdlt-connector-sdk

The connector-builder framework: the AUTHORING layer of rdlt's
connector SDK. The protocol lives in `rdlt-connector` (re-exported here
as `rdlt_connector_sdk::spi`), the conformance suites in
`rdlt-testkit`; this crate is what a connector is **built on**. The sdk
owns the protocol choreography and plumbing, the author fills in the
system-specific holes. Hosts never depend on this crate.

## Scope

The sdk contains only what is true of every connector by virtue of the
protocol. If an item's justification names a storage system, a wire
format, or a driver, it is not sdk: it belongs to the one connector
that needs it, sized to that connector's use. There are no shared
"batteries" here and no prelude; every item lives on exactly one
canonical module path.

```rust
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
use rdlt_connector_sdk::spi::{SourceError, StreamSpec};
```

`spi` is the crate root's only re-export: a connector depends on the sdk
alone and reaches the SPI through it, so a connector crate has one
foundation dependency and one version of every protocol type. The
SPI's `schema` and `failpoints` features forward under the same names.

## The three seams

**`config::Document`** is the validated config-document contract. The
provided entry points `from_yaml`, `from_json`, and `from_value` parse
THEN validate, by construction; the associated error type absorbs the
two parser errors through the connector's own `From` impls, so every
connector-specific message spelling stays the connector's. The seam
renders no validation text of its own. The YAML entry is also a
text-security seat:
`config::reject_graph_syntax` refuses anchors and aliases at the raw
text boundary, before the parser can expand them.

**`source`**: implement `source::SourceConnector` (name and version
constants, a `Document` config, `assemble`, `streams`, `read_stream`)
and `source::Shell` is the SPI face `serve` runs over: `spec()`
assembled from the constants and schema, `check`/`streams` delegated,
every read handed a `source::Feed`. The `Feed` makes closed-channel-is-cancellation a
property of the type: each push returns `ControlFlow`, and `Break`
means the host hung up.

**`destination`**: implement `destination::DestinationConnector` plus a
`destination::Backend` (the system IO: ensure, write, publish,
receipts, state) and `destination::Shell` is the SPI face `serve` runs
over, with the session choreography enforced by construction. A write to a
never-ensured table is refused, and a re-committed
`(load_id, commit_seq)` runs the backend's replay housekeeping and
returns its existing receipt; the publish is never reached. Atomicity
and staging invisibility remain the backend's storage contract,
properties no wrapper can add, and the conformance kits verify them.

## `serve`: the connector as a process

Production connectors run as processes. Behind the `serve` feature (OFF
by default; a crate that uses only the framework pays nothing for the
server, not even tonic in its dependency tree), `serve::source::run` and
`serve::destination::run` serve the connector's shell over gRPC on a
Unix socket to the host that spawned it, and `run_on` is the seam under
each for tests. Building the shell in-process (`Shell::from_value` and
its text siblings) is what `serve` does with the handshake's config
document and what a connector's own tests do to drive it directly. `serve::wire`
carries what both roles share: the socket, `serve::Error`, the
handshake, and the two refusal shapes. `serve_main!` is a connector
binary's whole `main`:

```rust
rdlt_connector_sdk::serve_main! {
    about: "rdlt example connector — a protocol server",
    roles: {
        Source => rdlt_connector_sdk::serve::source::run::<MySource>(),
        Destination => rdlt_connector_sdk::serve::destination::run::<MyDestination>(),
    }
}
```

## Proof discipline

The crate's own suite certifies a complete in-memory example connector
(`tests/cases/example.rs`, also the reference an author reads) against
the **same `rdlt-testkit` conformance kits every shipping connector
answers to**. "The framework satisfies the SPI contract" is a test
result here, not a claim.

## What stays in the connector

Every message spelling, every error-classification key, cursor
machinery, SQL planning, and every format, storage, or driver concern.
The sdk renders no operator-facing text of its own beyond the refusals
it owns.

## Features

`schema` (`config::schema_of`, JSON Schema generated from the config
structs themselves), `failpoints` (the crash-sweep seam, forwarded to
the SPI), `serve` (the out-of-process protocol server).
