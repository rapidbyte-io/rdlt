//! Hosting rdlt connectors: providers that place a pipeline's connectors in process or in processes
//! of their own, and the engine's source and destination over the wire protocol to a connector
//! served on the other end of a connection.

#![forbid(unsafe_code)]

// Kani compiles every crate aborting, and runs nothing it compiles.
#[cfg(all(panic = "abort", not(kani)))]
compile_error!(
    "rdlt-host scrubs an in-process connector's panic as it unwinds: build with panic = \"unwind\""
);

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
pub mod trace;
mod wire;

pub use connect::{Connect, Open};
pub use kills::Kills;
pub use local::{
    Access, Bubblewrap, Confined, Interrupts, LastWords, Launcher, Lingering, Local, NetworkAccess,
    OsError, Sandbox, SandboxError, Stops, StopsSpawned, Witness, spawned, stop_spawned,
};
#[cfg(feature = "sim")]
pub use local::{Ended, Played, Playing};
pub use network::{Endpoint, EndpointError, Network, Remote, Stream, Tcp};
pub use provider::{ConnectorRef, Digest, Isolation, Placed, Placement, Provider, ProviderError};
pub use rdlt_wire::tls::Identity;
pub use registry::Registry;
pub use remote::{
    CONNECTOR_LOST, Connection, DEADLINE_EXCEEDED, Deadlines, Handshaken, MAX_FREE_FRAMES, Options,
    RemoteDestination, RemoteSource,
};
pub use secrets::{
    Config, EnvSecrets, Redactions, ReferenceFault, SecretError, SecretFault, SecretKind,
    SecretReference, SecretResolver, Secrets,
};
pub use supervise::TLS;
pub use trace::TraceParent;
pub use wire::Wire;
