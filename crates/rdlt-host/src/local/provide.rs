//! How a [`Local`] answers as a [`Provider`], and the errors it gives for a connector it did
//! not spawn.

use std::path::Path;
use std::sync::Arc;

use rdlt_connector::{BoxFuture, ConnectorError, ConnectorId, Destination, Role, Source};

use super::Local;
use super::process::{Launch, Unspawned};
use crate::provider::{ConnectorRef, Placed, Placement, Provider, ProviderError};
use crate::supervise::{SupervisedDestination, SupervisedSource};

/// The provider's error for a connector that was not spawned.
pub(crate) fn refused(launch: &Launch, unspawned: Unspawned) -> ProviderError {
    let (id, path) = (launch.id.clone(), launch.binary.path().to_owned());
    match unspawned {
        Unspawned::Sandbox(source) => ProviderError::Sandbox { id, source },
        Unspawned::Changed { expected, found } => ProviderError::DigestMismatch {
            id,
            path,
            expected,
            found,
        },
        Unspawned::Replaced => ProviderError::Replaced { id, path },
        Unspawned::Io(source) => ProviderError::SpawnFailed { id, path, source },
    }
}

pub(super) fn spawn_failed(
    reference: &ConnectorRef,
    path: &Path,
    source: std::io::Error,
) -> ProviderError {
    ProviderError::SpawnFailed {
        id: reference.id.clone(),
        path: path.to_owned(),
        source,
    }
}

pub(super) fn handshake_failed(reference: &ConnectorRef, source: ConnectorError) -> ProviderError {
    ProviderError::HandshakeFailed {
        id: reference.id.clone(),
        source: Box::new(source),
    }
}

/// `rdlt-connector-` and the last segment of `id`.
pub(super) fn binary_name(id: &ConnectorId) -> String {
    let last = id.as_str().rsplit('.').next().unwrap_or(id.as_str());
    format!("rdlt-connector-{last}")
}

impl Provider for Local {
    fn source<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Source>>, ProviderError>> {
        Box::pin(async move {
            let (supervisor, spec, path, digest) =
                self.start(reference, Role::Source, config).await?;
            Ok(Placed {
                connector: Box::new(SupervisedSource(Arc::new(supervisor))) as Box<dyn Source>,
                spec,
                placement: Placement::Process { path },
                digest,
            })
        })
    }

    fn destination<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Destination>>, ProviderError>> {
        Box::pin(async move {
            let (supervisor, spec, path, digest) =
                self.start(reference, Role::Destination, config).await?;
            let capabilities = supervisor
                .capabilities()
                .await
                .map_err(|source| handshake_failed(reference, source))?;
            let destination = SupervisedDestination {
                supervisor: Arc::new(supervisor),
                capabilities,
            };
            Ok(Placed {
                connector: Box::new(destination) as Box<dyn Destination>,
                spec,
                placement: Placement::Process { path },
                digest,
            })
        })
    }
}
