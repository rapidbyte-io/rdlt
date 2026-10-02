//! The registry: connectors linked into the engine's binary, placed in process, and a fallback
//! provider for every other.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorId, Destination, DestinationConnector, DestinationFactory,
    Source, SourceConnector, SourceFactory, destination_factory, source_factory,
};

#[cfg(test)]
mod tests;

use crate::guard::{Guarded, received};
use crate::provider::{ConnectorRef, Honours, Placed, Placement, Provider, ProviderError, accepts};
use crate::secrets::{Config, Redactions, SecretResolver, Secrets};

/// Resolves an id to an in-process connector first, then to the fallback provider.
///
/// `Registry::trusted().trusted_source::<Tickets>().fallback(Local::sandboxed(Bubblewrap::new()))`.
/// A connector placed in process shares the engine's address space and all its access: only
/// code its embedder trusts as its own is registered, which the names say. In-process
/// connectors are first-class, and pay no serialization.
pub struct Registry {
    sources: HashMap<ConnectorId, Box<dyn SourceFactory>>,
    destinations: HashMap<ConnectorId, Box<dyn DestinationFactory>>,
    fallback: Option<Box<dyn Provider>>,
    secrets: Arc<dyn SecretResolver>,
}

/// What in-process placement honours of a reference: its id and version, which the registry
/// itself knows, and no isolation.
const HONOURS: Honours = Honours {
    placement: "in-process",
    path: false,
    endpoint: false,
    digest: false,
    grants: false,
    isolation: &[],
};

impl fmt::Debug for Registry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sources: Vec<_> = self.sources.keys().map(ConnectorId::as_str).collect();
        let mut destinations: Vec<_> = self.destinations.keys().map(ConnectorId::as_str).collect();
        sources.sort_unstable();
        destinations.sort_unstable();
        formatter
            .debug_struct("Registry")
            .field("sources", &sources)
            .field("destinations", &destinations)
            .field("fallback", &self.fallback.is_some())
            .finish_non_exhaustive()
    }
}

impl Registry {
    /// A registry of the connectors its embedder trusts as its own code: none yet, and no
    /// fallback.
    pub fn trusted() -> Self {
        Self {
            sources: HashMap::new(),
            destinations: HashMap::new(),
            fallback: None,
            secrets: Arc::new(Secrets::new()),
        }
    }

    /// Places source `C`, which its embedder trusts, in process.
    #[must_use]
    pub fn trusted_source<C: SourceConnector>(mut self) -> Self {
        let factory = source_factory::<C>();
        self.sources.insert(factory.spec().id.clone(), factory);
        self
    }

    /// Places destination `C`, which its embedder trusts, in process.
    #[must_use]
    pub fn trusted_destination<C: DestinationConnector>(mut self) -> Self {
        let factory = destination_factory::<C>();
        self.destinations.insert(factory.spec().id.clone(), factory);
        self
    }

    /// Places every other connector with `provider`.
    #[must_use]
    pub fn fallback(mut self, provider: impl Provider + 'static) -> Self {
        self.fallback = Some(Box::new(provider));
        self
    }

    /// Resolves the secret references of each in-process connector's configuration with
    /// `secrets`.
    #[must_use]
    pub fn secrets(mut self, secrets: impl SecretResolver + 'static) -> Self {
        self.secrets = Arc::new(secrets);
        self
    }

    fn not_found(reference: &ConnectorRef) -> ProviderError {
        ProviderError::NotFound {
            id: reference.id.clone(),
            source: None,
        }
    }

    /// The configuration an in-process connector named by `reference`, of `version`, is
    /// connected with: `config`, its secret references resolved once the reference is seen to
    /// require nothing in-process placement does not honour and to accept the version.
    async fn configured(
        &self,
        reference: &ConnectorRef,
        version: &str,
        config: &serde_json::Value,
    ) -> Result<(serde_json::Value, Redactions), ProviderError> {
        HONOURS.admit(reference)?;
        accepts(reference, version)?;
        let redactions = Redactions::new();
        let unprepared = |source| ProviderError::Secret {
            id: reference.id.clone(),
            source,
        };
        let held = Config::from(config);
        let resolving = held.resolved(&*self.secrets, &redactions);
        let json = resolving.await.map_err(unprepared)?;
        let resolved = serde_json::from_str(&json)
            .map_err(|_| unprepared(crate::secrets::SecretError::NotJson))?;
        Ok((resolved, redactions))
    }
}

/// The provider's error for an in-process connector whose own connect failed.
fn unconnected(
    reference: &ConnectorRef,
    error: &rdlt_connector::ConnectorError,
    redactions: &Redactions,
) -> ProviderError {
    ProviderError::HandshakeFailed {
        id: reference.id.clone(),
        source: Box::new(received(error, redactions)),
    }
}

impl Provider for Registry {
    fn source<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Source>>, ProviderError>> {
        Box::pin(async move {
            let Some(factory) = self.sources.get(&reference.id) else {
                return match &self.fallback {
                    Some(fallback) => fallback.source(reference, config).await,
                    None => Err(Self::not_found(reference)),
                };
            };
            let version = &factory.spec().version;
            let (config, redactions) = self.configured(reference, version, config).await?;
            let inner = factory
                .connect(config, ConnectContext::new())
                .await
                .map_err(|error| unconnected(reference, &error, &redactions))?;
            Ok(Placed {
                connector: Box::new(Guarded { inner, redactions }) as Box<dyn Source>,
                spec: factory.spec().clone(),
                placement: Placement::InProcess,
                digest: None,
            })
        })
    }

    fn destination<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Destination>>, ProviderError>> {
        Box::pin(async move {
            let Some(factory) = self.destinations.get(&reference.id) else {
                return match &self.fallback {
                    Some(fallback) => fallback.destination(reference, config).await,
                    None => Err(Self::not_found(reference)),
                };
            };
            let version = &factory.spec().version;
            let (config, redactions) = self.configured(reference, version, config).await?;
            let inner = factory
                .connect(config, ConnectContext::new())
                .await
                .map_err(|error| unconnected(reference, &error, &redactions))?;
            Ok(Placed {
                connector: Box::new(Guarded { inner, redactions }) as Box<dyn Destination>,
                spec: factory.spec().clone(),
                placement: Placement::InProcess,
                digest: None,
            })
        })
    }
}
