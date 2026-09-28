//! Connectors reached through streams a function opens, as a certification opens them to one
//! served in its own process: supervised as any other, so a lost one is reached again.

use std::sync::Arc;

use rdlt_connector::{BoxFuture, Destination, Role, Source};

use crate::kills::Kills;
use crate::network::Stream;
use crate::provider::{ConnectorRef, Placed, Placement, Provider, ProviderError};
use crate::remote::Options;
use crate::supervise::{Spawned, Start, SupervisedDestination, SupervisedSource, Supervisor};

/// Opens a stream to a connector.
pub type Open = Arc<dyn Fn() -> BoxFuture<'static, std::io::Result<Box<dyn Stream>>> + Send + Sync>;

/// Places the connector every stream `open` opens reaches, whatever the reference names, and
/// reaches it again through a new one once it is lost.
#[derive(Clone)]
pub struct Connect {
    open: Open,
    options: Options,
}

impl std::fmt::Debug for Connect {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Connect")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl Connect {
    /// Reaches connectors through the streams `open` opens.
    pub fn new<F>(open: F) -> Self
    where
        F: Fn() -> BoxFuture<'static, std::io::Result<Box<dyn Stream>>> + Send + Sync + 'static,
    {
        Self {
            open: Arc::new(open),
            options: Options::default(),
        }
    }

    /// Runs each connection with `options`.
    #[must_use]
    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// Reaches each connector so that `kills` cuts every connection to it.
    #[must_use]
    pub fn kills(mut self, kills: &Kills) -> Self {
        let (open, kills) = (self.open, kills.clone());
        self.open = Arc::new(move || {
            let opening = open();
            let kills = kills.clone();
            Box::pin(async move { opening.await.map(|stream| kills.sever(stream)) })
        });
        self
    }

    /// Reaches the connector as `role` with `config`.
    async fn start(
        &self,
        reference: &ConnectorRef,
        role: Role,
        config: &serde_json::Value,
    ) -> Result<Supervisor, ProviderError> {
        let start = Start::Connect(Arc::clone(&self.open));
        let supervisor = Supervisor::start(start, role, config.clone(), self.options)
            .await
            .map_err(|spawned| match spawned {
                Spawned::Connect(source) => ProviderError::HandshakeFailed {
                    id: reference.id.clone(),
                    source: Box::new(source),
                },
                Spawned::Io(source) | Spawned::Unreachable(source) | Spawned::Tls(source) => {
                    ProviderError::Unreachable {
                        id: reference.id.clone(),
                        endpoint: CONNECTED.to_owned(),
                        source,
                    }
                }
            })?;
        Ok(supervisor)
    }
}

/// Where a connector reached through a function is, as errors and specs name it.
const CONNECTED: &str = "a stream a function opens";

impl Provider for Connect {
    fn source<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Source>>, ProviderError>> {
        Box::pin(async move {
            let supervisor = self.start(reference, Role::Source, config).await?;
            let spec = supervisor.checked_spec(reference, CONNECTED).await?;
            Ok(Placed {
                connector: Box::new(SupervisedSource(Arc::new(supervisor))) as Box<dyn Source>,
                spec,
                placement: Placement::Connected,
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
            let supervisor = self.start(reference, Role::Destination, config).await?;
            let spec = supervisor.checked_spec(reference, CONNECTED).await?;
            let capabilities = supervisor.capabilities().await.map_err(|source| {
                ProviderError::HandshakeFailed {
                    id: reference.id.clone(),
                    source: Box::new(source),
                }
            })?;
            let destination = SupervisedDestination {
                supervisor: Arc::new(supervisor),
                capabilities,
            };
            Ok(Placed {
                connector: Box::new(destination) as Box<dyn Destination>,
                spec,
                placement: Placement::Connected,
                digest: None,
            })
        })
    }
}
