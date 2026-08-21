//! `rdlt-connector-reference` — the reference connector as an
//! out-of-process protocol server. A provider resolves the connector id
//! `io.rapidbyte.reference` to THIS binary name on PATH, spawns it with
//! `--role=<source|destination>`, reads the one stdout handshake line,
//! and everything after is the wire protocol.
//!
//! Behavior contract (the serve_main expansion the spawn suites pin):
//! missing/invalid args → clap's stderr + exit 2; `--version` prints
//! the crate version; a serve error → one stderr line + exit 1.

use rdlt_connector_reference::destination::connector::Reference as ReferenceDestination;
use rdlt_connector_reference::source::connector::Reference as ReferenceSource;

rdlt_connector_sdk::serve_main! {
    about: "rdlt reference connector — one jsonl file in, jsonl parts and receipts out",
    roles: {
        Source => rdlt_connector_sdk::serve::source::run::<ReferenceSource>(),
        Destination => rdlt_connector_sdk::serve::destination::run::<ReferenceDestination>(),
    }
}
