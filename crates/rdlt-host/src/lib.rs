//! Hosting rdlt connectors out of process: the engine's source and destination, over the wire
//! protocol to a connector served on the other end of a connection.

pub mod remote;

pub use remote::{
    CONNECTOR_LOST, Connection, DEADLINE_EXCEEDED, Deadlines, Options, RemoteDestination,
    RemoteSource,
};
