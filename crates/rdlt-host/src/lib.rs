//! Hosting rdlt connectors: providers that place a pipeline's connectors in process or in processes
//! of their own, and the engine's source and destination over the wire protocol to a connector
//! served on the other end of a connection.

#![forbid(unsafe_code)]

mod connect;
mod kills;
pub mod local;
pub mod network;
pub mod provider;
pub mod registry;
pub mod remote;
#[cfg(test)]
mod sink;
mod supervise;
mod wire;

pub use connect::{Connect, Open};
pub use kills::Kills;
pub use local::{Interrupts, LastWords, Lingering, Local, Witness, spawned, stop_spawned};
pub use network::{Endpoint, Network, Remote, Stream, Tcp};
pub use provider::{ConnectorRef, Digest, Placed, Placement, Provider, ProviderError};
pub use rdlt_wire::tls::Identity;
pub use registry::Registry;
pub use remote::{
    CONNECTOR_LOST, Connection, DEADLINE_EXCEEDED, Deadlines, Handshaken, Options,
    RemoteDestination, RemoteSource,
};
pub use supervise::TLS;
pub use wire::Wire;
