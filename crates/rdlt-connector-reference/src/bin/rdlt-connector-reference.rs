//! `rdlt-connector-reference` — the reference connector as an
//! out-of-process protocol server (ADR 0001). D-039-1's discovery
//! convention resolves the connector id `io.rapidbyte.reference` to
//! THIS binary name on PATH; a provider spawns it with
//! `--role=<source|destination>`, reads the one stdout handshake line,
//! and everything after is the wire protocol.
//!
//! Behavior contract (the serve_main expansion the spawn suites pin):
//! missing/invalid args → clap's stderr + exit 2; `--version` prints
//! the crate version; a serve error → one stderr line + exit 1.

use rdlt_connector_reference::destination::Reference as ReferenceDestination;
use rdlt_connector_reference::source::Reference as ReferenceSource;

rdlt_connector_sdk::serve_main! {
    about: "rdlt reference connector — a protocol server (ADR 0001)",
    roles: {
        Source => rdlt_connector_sdk::serve::source::run::<ReferenceSource>(),
        Destination => rdlt_connector_sdk::serve::destination::run::<ReferenceDestination>(),
    }
}
