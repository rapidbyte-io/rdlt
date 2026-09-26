//! The registry: connectors linked into the engine's binary, placed in process, and a fallback
//! provider for every other.

use std::collections::HashMap;
use std::fmt;

use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorId, Destination, DestinationConnector, DestinationFactory,
    Source, SourceConnector, SourceFactory, destination_factory, source_factory,
};

#[cfg(test)]
mod tests;

use crate::provider::{ConnectorRef, Placed, Placement, Provider, ProviderError, accepts};

/// Resolves an id to an in-process connector first, then to the fallback provider.
///
/// `Registry::new().source::<Tickets>().destination::<Sqlite>().fallback(Local::new())`. In-process
/// connectors are first-class, and pay no serialization.
#[derive(Default)]
pub struct Registry {
    sources: HashMap<ConnectorId, Box<dyn SourceFactory>>,
    destinations: HashMap<ConnectorId, Box<dyn DestinationFactory>>,
    fallback: Option<Box<dyn Provider>>,
}

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
            .finish()
    }
}

impl Registry {
    /// A registry of no connectors, and no fallback.
    pub fn new() -> Self {
        Self::default()
    }

    /// Places source `C` in process.
    #[must_use]
    pub fn source<C: SourceConnector>(mut self) -> Self {
        let factory = source_factory::<C>();
        self.sources.insert(factory.spec().id.clone(), factory);
        self
    }

    /// Places destination `C` in process.
    #[must_use]
    pub fn destination<C: DestinationConnector>(mut self) -> Self {
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

    fn not_found(reference: &ConnectorRef) -> ProviderError {
        ProviderError::NotFound {
            id: reference.id.clone(),
            source: None,
        }
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
            accepts(reference, &factory.spec().version)?;
            let connector = factory
                .connect(config.clone(), ConnectContext::new())
                .await
                .map_err(|source| ProviderError::HandshakeFailed {
                    id: reference.id.clone(),
                    source: Box::new(source),
                })?;
            Ok(Placed {
                connector,
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
            accepts(reference, &factory.spec().version)?;
            let connector = factory
                .connect(config.clone(), ConnectContext::new())
                .await
                .map_err(|source| ProviderError::HandshakeFailed {
                    id: reference.id.clone(),
                    source: Box::new(source),
                })?;
            Ok(Placed {
                connector,
                spec: factory.spec().clone(),
                placement: Placement::InProcess,
                digest: None,
            })
        })
    }
}
