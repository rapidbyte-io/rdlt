//! Hosting rdlt connectors: providers that place a pipeline's connectors in process or in processes
//! of their own, and the engine's source and destination over the wire protocol to a connector
//! served on the other end of a connection.

#![forbid(unsafe_code)]

pub mod provider;
pub mod registry;
pub mod remote;

pub use provider::{ConnectorRef, Digest, Placed, Placement, Provider, ProviderError};
pub use registry::Registry;
pub use remote::{
    CONNECTOR_LOST, Connection, DEADLINE_EXCEEDED, Deadlines, Options, RemoteDestination,
    RemoteSource,
};
