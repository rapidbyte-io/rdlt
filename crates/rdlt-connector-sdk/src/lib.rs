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
//! operator-facing text of its own beyond the one refusal it owns
//! (write-before-ensure); even the unknown-stream refusal is the
//! connector's, worded where the config's shape is known.
//!
//! A fourth, optional seam: behind the `serve` feature (OFF by
//! default), `serve` turns a framework connector into an out-of-process
//! protocol server (038) — the same `SourceConnector`/
//! `DestinationConnector` impl, dialed over gRPC instead of called
//! in-process. A connector that never runs out-of-process pays nothing
//! for it, not even tonic in its dependency tree.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

pub mod config;
pub mod destination;
#[cfg(feature = "serve")]
pub mod serve;
pub mod source;
pub mod yaml;

/// The connector SPI, re-exported: one dependency authors a connector.
pub use rdlt_connector as spi;

/// The one-import AUTHORING surface: the traits you implement and the
/// vocabulary your signatures name — and nothing the framework
/// implements FOR you (`Source`, `Destination`, `LoadSession` stay off
/// it deliberately; an author never touches those, and having both
/// halves in one namespace invites `impl Source for MyThing` where
/// `impl SourceConnector` was meant). Anything else is reached by its
/// canonical module path ([`spi`], [`config`], [`source`],
/// [`destination`]).
///
/// ```
/// use rdlt_connector_sdk::prelude::*;
///
/// // Every name an author's impl blocks and signatures need is in
/// // scope — this signature is spelled entirely from the prelude:
/// async fn _streams<S: SourceConnector>(source: &S) -> Result<Vec<StreamSpec>, SourceError> {
///     source.streams().await
/// }
/// fn _capabilities<D: DestinationConnector>(destination: &D) -> DestinationCapabilities {
///     destination.capabilities()
/// }
/// # fn main() {}
/// ```
pub mod prelude {
    pub use crate::config::Document;
    // `Session` sits beside `Backend` deliberately (038 T5 review round
    // 2, F-2): it is not authoring vocabulary (an author never
    // constructs one — `Destination::open` does, in-process), but 039's
    // remote-backend adapter is a SECOND caller that composes it
    // directly over a `Backend` reached out of process, and needs it
    // nameable from the same one-import surface everything else here
    // comes from.
    pub use crate::destination::{Backend, DestinationConnector, Session};
    pub use crate::source::{Feed, SourceConnector};
    pub use rdlt_connector::{
        Cursor, DestinationCapabilities, DestinationError, OpenContext, RecordBatch, SourceError,
        StreamSpec,
    };
}
