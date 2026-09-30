//! The wire protocol between rdlt and connectors that run out of process: the messages of
//! package `rdlt.connector.v1`, the codec carrying Arrow batches in them, and the limits each
//! end enforces on what it receives.

#![forbid(unsafe_code)]

pub mod codec;
pub mod error;
pub mod limits;
#[cfg(feature = "tls")]
pub mod tls;

/// The messages of package `rdlt.connector.v1`, and its `Connector` service's client
/// (`connector_client`) and server (`connector_server`), generated from the `.proto` files under
/// `proto/` by `cargo xtask codegen`.
#[expect(
    clippy::doc_markdown,
    clippy::large_enum_variant,
    clippy::struct_excessive_bools,
    reason = "generated code keeps the .proto files' comments and messages as written"
)]
#[expect(
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::default_trait_access,
    clippy::too_many_lines,
    unused_qualifications,
    reason = "tonic's generated client and server are written in its style, not this workspace's"
)]
#[path = "generated/rdlt.connector.v1.rs"]
pub mod v1;

pub use codec::{Decoder, Encoder, IpcFrame};
pub use error::{Frame, Problem, WireError};
pub use limits::{Limits, Refusal};
pub use prost;
pub use tonic;

/// The protocol's major version: a peer of another major version is refused at the handshake.
///
/// Version 2 configures the connector in `Configure`, after the handshake, where version 1 sent
/// the configuration in the handshake itself.
pub const PROTOCOL_MAJOR: u32 = 2;

/// The protocol's minor version: a peer of another minor version is served, with the features
/// both ends know.
pub const PROTOCOL_MINOR: u32 = 0;

/// The handshake's feature a host offers to read back what a destination published, and a
/// destination that can accepts: then it serves `ReadPublished`.
pub const PUBLISHED: &str = "published";

/// The handshake's feature a host offers to ask where a source stands outside the engine, and a
/// source that tells accepts: then it serves `ReadAcknowledged`.
pub const ACKNOWLEDGED: &str = "acknowledged";
