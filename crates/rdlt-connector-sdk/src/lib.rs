//! # rdlt-connector-sdk — the connector-builder framework
//!
//! The AUTHORING layer of the connector SDK: the protocol lives in
//! `rdlt-connector` (re-exported here as [`spi`]), the conformance
//! suites live in `rdlt-testkit`, and this crate is the framework a
//! connector is BUILT ON — an inversion of control where the SDK owns
//! the protocol choreography and plumbing, and the author fills in the
//! system-specific holes. Hosts never depend on this crate.
//!
//! Three seams:
//!
//! - [`config::Document`] — the validated config-document contract:
//!   every text entry point parses THEN validates, by construction.
//! - [`source`] — the source framework: implement
//!   [`source::SourceConnector`] (declare streams, read one stream into
//!   a [`source::Feed`]) and [`source::shell`] provides the SPI
//!   implementation; closed-channel-is-cancellation is a property of
//!   the `Feed` type.
//! - [`destination`] — the destination framework: implement
//!   [`destination::DestinationConnector`] and a
//!   [`destination::Backend`] (the system IO), and the SDK's session
//!   choreography enforces the conformance clauses by construction —
//!   write-before-ensure refused, an already-published
//!   `(load_id, commit_seq)` replayed from its receipt before anything
//!   republishes, state read-through.
//!
//! WHAT STAYS IN THE CONNECTOR, deliberately: every frozen message
//! spelling, error classification keys, cursor machinery, and SQL
//! planning. The framework is choreography and plumbing — it renders no
//! operator-facing text of its own beyond the two refusals it owns
//! (unknown stream, write-before-ensure).

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

pub mod config;
pub mod destination;
pub mod source;

/// The connector SPI, re-exported: one dependency authors a connector.
pub use rdlt_connector as spi;

/// The one-import authoring surface.
pub mod prelude {
    pub use crate::config::Document;
    pub use crate::destination::{Backend, DestinationConnector};
    pub use crate::source::{Feed, SourceConnector};
    pub use rdlt_connector::{
        ConnectorSpec, Cursor, Destination, DestinationCapabilities, DestinationError, LoadSession,
        OpenContext, ReadRequest, RecordBatch, Source, SourceError, StreamSpec,
    };
}
