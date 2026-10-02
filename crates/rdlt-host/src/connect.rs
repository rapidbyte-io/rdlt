//! Connectors reached through streams a function opens, as a certification opens them to one
//! served in its own process: supervised as any other, so a lost one is reached again.

use std::sync::Arc;

use rdlt_connector::{BoxFuture, Destination, Role, Source};

use crate::kills::Kills;
use crate::network::Stream;
use crate::provider::{ConnectorRef, Honours, Placed, Placement, Provider, ProviderError};
use crate::remote::Options;
use crate::secrets::{Config, SecretResolver, Secrets};
use crate::supervise::{
    Configured, Gate, Spawned, Start, SupervisedDestination, SupervisedSource, Supervisor,
};

/// Opens a stream to a connector.
pub type Open = Arc<dyn Fn() -> BoxFuture<'static, std::io::Result<Box<dyn Stream>>> + Send + Sync>;

/// Places the connector every stream `open` opens reaches, when it is the connector the
/// reference names, and reaches it again through a new one once it is lost.
///
/// A reference that requires a path, an endpoint, a digest or an isolation is refused: a
/// stream a function opens shows none of them.
#[derive(Clone)]
pub struct Connect {
    open: Open,
    options: Options,
    secrets: Arc<dyn SecretResolver>,
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
            secrets: Arc::new(Secrets::new()),
        }
    }

    /// Resolves the secret references of each configuration with `secrets`.
    #[must_use]
    pub fn secrets(mut self, secrets: impl SecretResolver + 'static) -> Self {
        self.secrets = Arc::new(secrets);
        self
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
        HONOURS.admit(reference)?;
        let start = Start::Connect(Arc::clone(&self.open));
        let gate = Gate {
            reference,
            found_at: CONNECTED,
        };
        let configured = Configured {
            config: Config::from(config),
            secrets: Arc::clone(&self.secrets),
        };
        let supervisor = Supervisor::start(start, role, configured, self.options, &gate)
            .await
            .map_err(|spawned| match spawned {
                Spawned::Refused(refused) => refused,
                Spawned::Secret(source) => ProviderError::Secret {
                    id: reference.id.clone(),
                    source,
                },
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

/// What a stream a function opens honours of a reference: its id and version, which the
/// handshake shows, and nothing else.
const HONOURS: Honours = Honours {
    placement: "connected",
    path: false,
    endpoint: false,
    digest: false,
    grants: false,
    isolation: &[],
};

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
            let spec = supervisor.spec();
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
            let spec = supervisor.spec();
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
