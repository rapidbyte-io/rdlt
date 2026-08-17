//! [`ConnectorProvider`] — the seam between "I need connector X" and a
//! ready SPI object — and [`ProviderError`], its typed failure surface.

use async_trait::async_trait;
use rdlt_connector_protocol::handshake;

use crate::managed::{ManagedDestination, ManagedSource};
use crate::requirement::{ClientError, ConnectorRequirement};

/// Turns a [`ConnectorRequirement`] plus the connector's own config
/// document into a managed SPI object. The facade holds a default
/// [`crate::LocalBinaryConnectorProvider`]; embedders supply their own
/// implementation (a pooled provider, a remote scheduler) through the
/// same trait — the engine never learns which.
///
/// `config` is the connector's OWN document, opaque here: it crosses
/// the wire in the handshake and the connector's config gate is the
/// thing that validates it (a refusal comes back as
/// [`ClientError::Handshake`] inside [`ProviderError::Client`]).
#[async_trait]
pub trait ConnectorProvider: Send + Sync {
    /// Provide `requirement` as a source, configured by `config`.
    async fn source(
        &self,
        requirement: &ConnectorRequirement,
        config: &serde_json::Value,
    ) -> Result<ManagedSource, ProviderError>;

    /// Provide `requirement` as a destination, configured by `config`.
    async fn destination(
        &self,
        requirement: &ConnectorRequirement,
        config: &serde_json::Value,
    ) -> Result<ManagedDestination, ProviderError>;
}

/// What providing a connector can report.
///
/// `#[non_exhaustive]`: the provider surface can grow (a checksum
/// mismatch, a pool-exhausted arm) without a breaking change — match
/// with a wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProviderError {
    /// Discovery found nothing: no binary by the D-039-1 convention on
    /// PATH, and the requirement carried no explicit path. The spelling
    /// is FROZEN — it names both the convention and the override, and
    /// tests pin it full-string.
    #[error(
        "connector `{id}`: no binary `{binary}` on PATH and no explicit path was given — install it (e.g. cargo install {binary}) or set path: in the connector requirement"
    )]
    NotFound {
        /// The requirement's connector id.
        id: String,
        /// The conventional binary name the id resolved to.
        binary: String,
    },
    /// The spawned process wrote a first stdout line that is not a
    /// handshake line — the typed parse refusal rides as the cause.
    #[error("connector `{binary}` wrote an invalid handshake line: {source}")]
    HandshakeLine {
        /// The binary that misbehaved (the conventional name, or the
        /// override path as given).
        binary: String,
        /// Why the line refused to parse.
        #[source]
        source: handshake::Error,
    },
    /// The spawned process flooded stdout past the handshake-line cap
    /// without a line terminator — not a connector, refused as soon as
    /// the cap fills rather than buffering the flood until the timeout.
    #[error(
        "connector `{binary}` wrote {limit} bytes of stdout without completing a handshake line"
    )]
    HandshakeLineOverflow {
        /// The binary that flooded.
        binary: String,
        /// The byte cap the flood filled.
        limit: u64,
    },
    /// The spawned process wrote no handshake line before the
    /// provider's line timeout — a binary that is not a connector at
    /// all, or one wedged before serving.
    #[error("connector `{binary}` wrote no handshake line before the provider's timeout")]
    Timeout {
        /// The binary that stayed silent.
        binary: String,
    },
    /// The handshake line advertised a protocol range this host's
    /// version sits outside — refused AT THE LINE, before any dial, so
    /// the mismatch surfaces as one typed error instead of whatever a
    /// version-skewed gRPC exchange happens to produce.
    #[error(
        "connector `{binary}` accepts protocol versions {min}..={max}, but this host speaks protocol {ours} — upgrade whichever side is behind"
    )]
    ProtocolRange {
        /// The binary whose line advertised the range.
        binary: String,
        /// The connector's lowest accepted protocol version.
        min: u32,
        /// The connector's highest accepted protocol version.
        max: u32,
        /// This host's protocol version.
        ours: u32,
    },
    /// The spawned process exited unsuccessfully before writing any
    /// handshake bytes. Exit code 2 is the connector binaries' usage
    /// refusal when the requested role is unsupported.
    #[error(
        "connector `{binary}` exited before writing a handshake line for role `{role}` ({status})"
    )]
    ExitedBeforeHandshake {
        /// The binary that exited.
        binary: String,
        /// The role passed to the binary.
        role: String,
        /// The process status distinguishing usage refusal from a
        /// signal or another failure code.
        status: std::process::ExitStatus,
    },
    /// Everything past the handshake line is the client crate's to
    /// classify: dialing the advertised socket, the handshake RPC,
    /// identity/version verification (D-039-2).
    #[error(transparent)]
    Client(#[from] ClientError),
    /// The OS refused the spawn, or the child's stdout pipe failed
    /// while the line was being read.
    #[error("spawning connector `{binary}`: {source}")]
    Spawn {
        /// The binary that failed to spawn or feed its pipe.
        binary: String,
        /// The io-level cause.
        #[source]
        source: std::io::Error,
    },
}
