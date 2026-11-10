//! The registry: connectors linked into the engine's binary, placed in process, and the
//! placements every other reference names.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorId, ConnectorSpec, Destination, DestinationConnector,
    DestinationFactory, Source, SourceConnector, SourceFactory, destination_factory,
    source_factory,
};

#[cfg(test)]
mod tests;

use crate::guard::{Guarded, received, unwound};
use crate::provider::{ConnectorRef, Honours, Placed, Placement, Provider, ProviderError, accepts};
use crate::secrets::{Config, Redactions, SecretResolver, Secrets};

/// Places each connector as its reference says: in process where its id is registered, remotely
/// where the reference names an endpoint, and as a local binary otherwise.
///
/// `Registry::trusted().trusted_source::<Tickets>().local(Local::sandboxed(Bubblewrap::new()))`.
/// A connector placed in process shares the engine's address space and all its access: only
/// code its embedder trusts as its own is registered, which the names say. A reference whose
/// placement the registry was not given is refused, never placed another way.
pub struct Registry {
    sources: HashMap<ConnectorId, Arc<dyn SourceFactory>>,
    destinations: HashMap<ConnectorId, Arc<dyn DestinationFactory>>,
    remote: Option<Box<dyn Provider>>,
    local: Option<Box<dyn Provider>>,
    secrets: Arc<dyn SecretResolver>,
}

/// What in-process placement honours of a reference: its id and version, which the registry
/// itself knows, and no isolation.
const HONOURS: Honours = Honours {
    placement: "in-process",
    path: false,
    endpoint: false,
    digest: false,
    access: false,
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
            .field("remote", &self.remote.is_some())
            .field("local", &self.local.is_some())
            .finish_non_exhaustive()
    }
}

impl Registry {
    /// A registry of the connectors its embedder trusts as its own code: none yet, and no
    /// other placement.
    pub fn trusted() -> Self {
        Self {
            sources: HashMap::new(),
            destinations: HashMap::new(),
            remote: None,
            local: None,
            secrets: Arc::new(Secrets::new()),
        }
    }

    /// Places source `C`, which its embedder trusts, in process.
    #[must_use]
    pub fn trusted_source<C: SourceConnector>(self) -> Self {
        self.trusted_source_factory(Arc::from(source_factory::<C>()))
    }

    /// Places the source `factory` connects, which its embedder trusts, in process.
    #[must_use]
    pub fn trusted_source_factory(mut self, factory: Arc<dyn SourceFactory>) -> Self {
        self.sources.insert(factory.spec().id.clone(), factory);
        self
    }

    /// Places destination `C`, which its embedder trusts, in process.
    #[must_use]
    pub fn trusted_destination<C: DestinationConnector>(self) -> Self {
        self.trusted_destination_factory(Arc::from(destination_factory::<C>()))
    }

    /// Places the destination `factory` connects, which its embedder trusts, in process.
    #[must_use]
    pub fn trusted_destination_factory(mut self, factory: Arc<dyn DestinationFactory>) -> Self {
        self.destinations.insert(factory.spec().id.clone(), factory);
        self
    }

    /// Places every reference that names an endpoint, and no registered id, with `provider`.
    #[must_use]
    pub fn remote(mut self, provider: impl Provider + 'static) -> Self {
        self.remote = Some(Box::new(provider));
        self
    }

    /// Places every reference that names no endpoint and no registered id with `provider`.
    #[must_use]
    pub fn local(mut self, provider: impl Provider + 'static) -> Self {
        self.local = Some(Box::new(provider));
        self
    }

    /// Resolves the secret references of each in-process connector's configuration with
    /// `secrets`.
    #[must_use]
    pub fn secrets(mut self, secrets: impl SecretResolver + 'static) -> Self {
        self.secrets = Arc::new(secrets);
        self
    }

    /// The placement `reference`, which no registered connector answers to, names: remote for
    /// an endpoint, a local process otherwise; refused where this registry was given none.
    fn placing(&self, reference: &ConnectorRef) -> Result<&dyn Provider, ProviderError> {
        let (provider, placement) = match reference.endpoint {
            Some(_) => (&self.remote, "remote"),
            None => (&self.local, "process"),
        };
        provider.as_deref().ok_or_else(|| ProviderError::Refused {
            id: reference.id.clone(),
            placement,
        })
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
                return self.placing(reference)?.source(reference, config).await;
            };
            let version = &factory.spec().version;
            let (config, redactions) = self.configured(reference, version, config).await?;
            // A connector parses what it was sent as it connects: a panic there is scrubbed too.
            let connecting = async { factory.connect(config, ConnectContext::new()).await };
            let inner = unwound(connecting, &redactions)
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
                return self
                    .placing(reference)?
                    .destination(reference, config)
                    .await;
            };
            let version = &factory.spec().version;
            let (config, redactions) = self.configured(reference, version, config).await?;
            let connecting = async { factory.connect(config, ConnectContext::new()).await };
            let inner = unwound(connecting, &redactions)
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

    fn spec<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        role: rdlt_connector::Role,
    ) -> BoxFuture<'a, Result<ConnectorSpec, ProviderError>> {
        Box::pin(async move {
            let registered = match role {
                rdlt_connector::Role::Source => self
                    .sources
                    .get(&reference.id)
                    .map(|factory| factory.spec()),
                rdlt_connector::Role::Destination => self
                    .destinations
                    .get(&reference.id)
                    .map(|factory| factory.spec()),
            };
            let Some(spec) = registered else {
                return self.placing(reference)?.spec(reference, role).await;
            };
            HONOURS.admit(reference)?;
            accepts(reference, &spec.version)?;
            Ok(spec.clone())
        })
    }
}
