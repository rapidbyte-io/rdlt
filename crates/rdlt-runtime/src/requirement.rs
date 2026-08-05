//! The requirement vocabulary, re-exported from the client crate.
//!
//! [`ConnectorRequirement`] is DEFINED in `rdlt-connector-client` and
//! re-exported here because the two crates split one job: the CLIENT
//! verifies (its handshake checks the reported id, and the exact
//! version when one is pinned — D-039-2), the RUNTIME resolves (turning
//! an id into a spawnable path is the provider's job, so `path` rides
//! the requirement for it). Consumers above this layer (the facade,
//! embedders) name everything through THIS crate — the client crate
//! stays an implementation detail of the wire.
//!
//! [`ClientError`], [`HandshakeOutcome`] and [`Role`] ride along for
//! the same reason: [`crate::ProviderError`] carries a `ClientError`
//! arm, and a caller matching on it should not need a second
//! dependency to spell the type.

pub use rdlt_connector_client::{ClientError, ConnectorRequirement, HandshakeOutcome, Role};
