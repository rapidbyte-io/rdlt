//! # rdlt-connector-client — the wire client an adapter drives a served
//! connector through (039)
//!
//! The out-of-process counterpart to the sdk's `serve` half: [`dial`]
//! connects to the Unix domain socket a spawned connector's handshake
//! line advertised, [`handshake`] verifies the connector is the one the
//! provider resolved (D-039-2) and decodes its self-description, and
//! the error module maps wire [`proto::ErrorFrame`]s back to the SPI's
//! own classifications so the engine's retry machinery never learns the
//! wire exists. Tasks 3/4's `RemoteSource`/`RemoteBackend` adapters
//! build on exactly these seams; `rdlt-runtime` re-exports
//! [`ConnectorRequirement`] (the client verifies, the runtime resolves).
//!
//! **EXPERIMENTAL**, exactly as far as the protocol crate is (ADR 0001
//! D8): the wire is versioned but unfrozen, so this crate stays
//! unpublished alongside it.
//!
//! The modules are private and the surface below is the one canonical
//! path to every name — the flat interface Tasks 3-5 consume.
//!
//! [`proto::ErrorFrame`]: rdlt_connector_protocol::proto::ErrorFrame

mod dial;
mod error;
mod handshake;

pub use dial::{connector_client, destination_client, dial, source_client};
pub use error::{Classification, ClientError};
pub use handshake::{ConnectorRequirement, HandshakeOutcome, Role, handshake};

// `error::source_error_from_frame`/`error::dest_error_from_frame` stay
// crate-internal at their defining paths — Tasks 3/4's adapters reach
// them as `crate::error::…`.
