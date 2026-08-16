//! The runtime's names for the client's types; the client's own paths
//! are canonical. The two crates split one job — the client VERIFIES
//! the reported identity, the runtime RESOLVES an id into a spawnable
//! path — so consumers above this layer name everything through this
//! crate.

pub use rdlt_connector_client::error::{Classification, Error as ClientError};
pub use rdlt_connector_client::handshake::{
    Outcome as HandshakeOutcome, Requirement as ConnectorRequirement, Role,
};
