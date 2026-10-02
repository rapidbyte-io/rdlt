//! Hosting rdlt connectors: providers that place a pipeline's connectors in process or in processes
//! of their own, and the engine's source and destination over the wire protocol to a connector
//! served on the other end of a connection.

#![forbid(unsafe_code)]

mod connect;
mod guard;
mod kills;
pub mod limits;
pub mod local;
pub mod network;
pub mod provider;
pub mod registry;
pub mod remote;
pub mod secrets;
#[cfg(test)]
mod sink;
mod supervise;
mod wire;

pub use connect::{Connect, Open};
pub use kills::Kills;
pub use local::{
    Bubblewrap, Confined, Grants, Interrupts, LastWords, Launcher, Lingering, Local, NetworkGrant,
    Sandbox, SandboxError, Stops, StopsSpawned, Witness, spawned, stop_spawned,
};
pub use network::{Endpoint, EndpointError, Network, Remote, Stream, Tcp};
pub use provider::{ConnectorRef, Digest, Isolation, Placed, Placement, Provider, ProviderError};
pub use rdlt_wire::tls::Identity;
pub use registry::Registry;
pub use remote::{
    CONNECTOR_LOST, Connection, DEADLINE_EXCEEDED, Deadlines, Handshaken, Options,
    RemoteDestination, RemoteSource,
};
pub use secrets::{
    Config, EnvSecrets, FileSecrets, Redactions, ReferenceFault, SecretError, SecretFault,
    SecretKind, SecretReference, SecretResolver, Secrets,
};
pub use supervise::TLS;
pub use wire::Wire;
