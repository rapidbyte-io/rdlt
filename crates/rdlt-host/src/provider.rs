//! Placing connectors: a provider finds the connector a reference names, places it in process or
//! out, and hands the engine a source or destination that runs it.

use std::fmt;
use std::path::PathBuf;

#[cfg(test)]
mod tests;

use rdlt_connector::{BoxFuture, ConnectorError, ConnectorId, ConnectorSpec, Destination, Source};

/// What a pipeline names as its source or destination: a connector's id, the versions it accepts,
/// and where to find it when that is not the provider's choice.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectorRef {
    /// The connector's id.
    pub id: ConnectorId,
    /// The versions accepted; any version when absent.
    pub version_req: Option<semver::VersionReq>,
    /// The connector's binary, for a process placement.
    pub path: Option<PathBuf>,
    /// The connector's address, for a remote placement.
    pub endpoint: Option<String>,
    /// The digest its binary must have, for a process placement; any binary when absent.
    pub digest: Option<Digest>,
}

impl fmt::Debug for ConnectorRef {
    /// Shows the endpoint by its host and port, and not at all where it is none: what makes an
    /// endpoint wrong may be a credential written into it.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let endpoint = self.endpoint.as_deref().map(|endpoint| {
            crate::network::Endpoint::parse(endpoint).map_or_else(
                |_| "<no endpoint>".to_owned(),
                |endpoint| endpoint.to_string(),
            )
        });
        formatter
            .debug_struct("ConnectorRef")
            .field("id", &self.id)
            .field("version_req", &self.version_req)
            .field("path", &self.path)
            .field("endpoint", &endpoint)
            .field("digest", &self.digest)
            .finish()
    }
}

impl ConnectorRef {
    /// A reference to `id`, in any version, wherever the provider finds it.
    pub fn new(id: ConnectorId) -> Self {
        Self {
            id,
            version_req: None,
            path: None,
            endpoint: None,
            digest: None,
        }
    }

    /// Accepts only a binary of digest `digest`, for a process placement.
    #[must_use]
    pub fn digest(mut self, digest: Digest) -> Self {
        self.digest = Some(digest);
        self
    }

    /// Accepts only the versions `version_req` matches.
    #[must_use]
    pub fn version(mut self, version_req: semver::VersionReq) -> Self {
        self.version_req = Some(version_req);
        self
    }

    /// Runs the connector from the binary at `path`.
    #[must_use]
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Reaches the connector listening at `endpoint`, `grpcs://host:port`.
    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }
}

/// Where a placed connector runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Placement {
    /// In the engine's process.
    InProcess,
    /// In a process of its own, spawned from the binary at `path`.
    Process {
        /// The binary.
        path: PathBuf,
    },
    /// Reached through streams a function opens, as a connector served in the host's own process
    /// for certification is.
    Connected,
    /// Elsewhere on the network, listening at `endpoint`.
    Remote {
        /// The endpoint, `grpcs://host:port`.
        endpoint: String,
    },
}

/// The SHA-256 digest of a connector's binary, which says exactly what ran.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Digest(pub [u8; 32]);

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0
            .iter()
            .try_for_each(|byte| write!(formatter, "{byte:02x}"))
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Digest({self})")
    }
}

/// A placed connector: the source or destination the engine drives, the connector's spec, where
/// it runs, and the digest of its binary when it runs from one.
#[derive(Debug)]
pub struct Placed<T> {
    /// The source or destination.
    pub connector: T,
    /// The connector's spec, as it declared it.
    pub spec: ConnectorSpec,
    /// Where it runs.
    pub placement: Placement,
    /// The digest of its binary, for a process placement.
    pub digest: Option<Digest>,
}

/// Why a provider could not place a connector.
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// No connector answers to the reference.
    #[error("no connector `{id}` was found")]
    NotFound {
        /// The connector's id.
        id: ConnectorId,
        /// Why the lookup failed, when a lookup ran.
        #[source]
        source: Option<std::io::Error>,
    },
    /// The connector found is of a version the reference does not accept.
    #[error("connector `{id}` is version {found}, and `{required}` is required")]
    VersionMismatch {
        /// The connector's id.
        id: ConnectorId,
        /// The versions the reference accepts.
        required: semver::VersionReq,
        /// The version found.
        found: String,
    },
    /// The connector's binary could not be started.
    #[error("starting connector `{id}` from {} failed", path.display())]
    SpawnFailed {
        /// The connector's id.
        id: ConnectorId,
        /// The binary.
        path: PathBuf,
        /// Why it could not start.
        #[source]
        source: std::io::Error,
    },
    /// The reference's endpoint is not one; the error does not repeat it.
    #[error("connector `{id}` has no usable endpoint: an endpoint is `grpcs://host:port`")]
    Endpoint {
        /// The connector's id.
        id: ConnectorId,
        /// What is wrong with it.
        #[source]
        source: crate::network::EndpointError,
    },
    /// The connector's endpoint could not be reached.
    #[error("connector `{id}` at `{endpoint}` could not be reached")]
    Unreachable {
        /// The connector's id.
        id: ConnectorId,
        /// The endpoint's host and port.
        endpoint: String,
        /// Why.
        #[source]
        source: std::io::Error,
    },
    /// The TLS with the connector failed: the configuration, or the handshake, as when either end
    /// refuses the other's certificate.
    #[error("the TLS with connector `{id}` at `{endpoint}` failed")]
    Tls {
        /// The connector's id.
        id: ConnectorId,
        /// The endpoint's host and port.
        endpoint: String,
        /// Why.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The connector's binary has another digest than the reference requires, or changed since
    /// it was placed.
    #[error("connector `{id}` at {} has digest {found}, not {expected}", path.display())]
    DigestMismatch {
        /// The connector's id.
        id: ConnectorId,
        /// The binary.
        path: PathBuf,
        /// The digest required.
        expected: Digest,
        /// The digest found.
        found: Digest,
    },
    /// The connector started, but did not connect: its handshake, or its own connect, failed.
    #[error("connector `{id}` did not connect")]
    HandshakeFailed {
        /// The connector's id.
        id: ConnectorId,
        /// The connector's error.
        #[source]
        source: Box<ConnectorError>,
    },
}

/// Finds connectors by reference and places them.
pub trait Provider: Send + Sync {
    /// The source `reference` names, connected with `config`.
    fn source<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Source>>, ProviderError>>;

    /// The destination `reference` names, connected with `config`.
    fn destination<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Destination>>, ProviderError>>;
}

/// Whether `version` is one `reference` accepts; a version that is not semver matches only when
/// the reference accepts any.
pub(crate) fn accepts(reference: &ConnectorRef, version: &str) -> Result<(), ProviderError> {
    let Some(required) = &reference.version_req else {
        return Ok(());
    };
    match semver::Version::parse(version) {
        Ok(found) if required.matches(&found) => Ok(()),
        _ => Err(ProviderError::VersionMismatch {
            id: reference.id.clone(),
            required: required.clone(),
            found: version.to_owned(),
        }),
    }
}
