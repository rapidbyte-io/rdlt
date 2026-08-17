//! # rdlt-connector-sdk — the connector-builder framework
//!
//! The AUTHORING layer of the connector SDK. The protocol lives in
//! `rdlt-connector` (re-exported here as [`spi`]), the conformance
//! suites in `rdlt-testkit`; this crate is the framework a connector is
//! BUILT ON — the SDK owns the protocol choreography and plumbing, the
//! author fills in the system-specific holes. Hosts never depend on it.
//!
//! - [`config::Document`] — the validated config-document contract:
//!   every text entry point parses THEN validates, by construction.
//! - [`source`] — implement [`source::SourceConnector`] (declare
//!   streams, read one stream into a [`source::Feed`]); [`source::shell`]
//!   is the SPI implementation, and closed-channel-is-cancellation is a
//!   property of the `Feed` type.
//! - [`destination`] — implement [`destination::DestinationConnector`]
//!   and a [`destination::Backend`] (the system IO); the session
//!   choreography enforces the conformance clauses by construction —
//!   write-before-ensure refused, an already-published
//!   `(load_id, commit_seq)` replayed from its receipt, state
//!   read-through.
//! - `serve` (behind the `serve` feature, OFF by default) — the same
//!   connector dialed over gRPC as an out-of-process protocol server
//!   instead of called in-process; `serve_main!` is a connector
//!   binary's whole `main`.
//!
//! The sdk contains only what is true of every connector by virtue of
//! the protocol: nothing storage-, format-, or driver-specific lives
//! here, and every frozen message spelling, error classification key,
//! cursor rule and SQL plan stays in the connector. The framework
//! renders no operator-facing text of its own beyond the refusals it
//! owns: write-before-ensure, the receipt-identity checks at commit,
//! and the raw-YAML gates ahead of the parser.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build; the docs build holds the
// published surface to -D warnings.
#![warn(missing_docs)]

pub mod config;
pub mod destination;
#[cfg(feature = "serve")]
pub mod serve;
pub mod source;

/// The connector SPI, re-exported: one dependency authors a connector.
pub use rdlt_connector as spi;
