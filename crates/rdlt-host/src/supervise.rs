//! Supervision: a connector that is lost, by its transport failing, missing heartbeats or exiting,
//! which closes its socket, is started again for the next call, respawned or redialed, and the
//! engine's retry of the attempt reaches it.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::wire::TRANSPORT;
use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, ConnectorError, ConnectorErrorKind, ConnectorSpec, Cursor,
    Destination, OpenContext, OpenedSession, Partition, PartitionId, PartitionSink, ReadRequest,
    Role, Source, StreamName, StreamState,
};
use tokio::sync::Mutex;

mod session;

use session::SupervisedSession;

use crate::local::process::{Launch, Process};
use crate::network::Dial;
use crate::provider::{ConnectorRef, ProviderError, accepts};
use crate::remote::{CONNECTOR_LOST, Connection, Options, RemoteDestination, RemoteSource};

/// How long the errors of a lost connector wait for its standard error to close.
const LAST_WORDS: Duration = Duration::from_secs(1);

/// How a connector starts: spawned in a process of its own, or dialed where it listens.
pub(crate) enum Start {
    /// Spawned from its binary.
    Spawn(Launch),
    /// Dialed over mutual TLS.
    Dial(Dial),
}

/// A connector and the connection to it: its process, when this process spawned it.
pub(crate) struct Running {
    pub(crate) connection: Arc<Connection>,
    pub(crate) process: Option<Process>,
}

/// Starts a connector, and starts it again, respawned or redialed, once it is lost.
pub(crate) struct Supervisor {
    start: Start,
    role: Role,
    config: serde_json::Value,
    options: Options,
    running: Mutex<Running>,
    /// The spec the connector was checked to serve: whatever is started again must serve it.
    checked: std::sync::OnceLock<ConnectorSpec>,
}

impl Supervisor {
    /// Starts the connector `start` describes, as `role`, with `config`.
    pub(crate) async fn start(
        start: Start,
        role: Role,
        config: serde_json::Value,
        options: Options,
    ) -> Result<Self, Spawned> {
        let running = begin(&start, role, &config, options).await?;
        Ok(Self {
            start,
            role,
            config,
            options,
            running: Mutex::new(running),
            checked: std::sync::OnceLock::new(),
        })
    }

    /// The connection to the connector last started, lost or not.
    pub(crate) async fn live(&self) -> Arc<Connection> {
        Arc::clone(&self.running.lock().await.connection)
    }

    /// The connection to a live connector, starting it again if it was lost.
    async fn connection(&self) -> Result<Arc<Connection>, ConnectorError> {
        let mut running = self.running.lock().await;
        if running.connection.is_spent() {
            let started = begin(&self.start, self.role, &self.config, self.options)
                .await
                .map_err(Spawned::into_error)?;
            self.same(&started.connection)?;
            *running = started;
        }
        Ok(Arc::clone(&running.connection))
    }

    /// Whether `connection`'s connector serves the spec the first was checked to serve: a redial
    /// may reach whatever listens at the endpoint now.
    fn same(&self, connection: &Connection) -> Result<(), ConnectorError> {
        let Some(checked) = self.checked.get() else {
            return Ok(());
        };
        let spec = crate::remote::contract_spec(connection.spec(), self.role)?;
        if spec == *checked {
            return Ok(());
        }
        Err(ConnectorError::config(format!(
            "the connector started again serves `{}` {}, not `{}` {} as it was placed",
            spec.id, spec.version, checked.id, checked.version
        )))
    }

    /// `result`, its error carrying the connector's last words when its transport failed.
    pub(crate) async fn explain<T>(
        &self,
        result: rdlt_connector::Result<T>,
    ) -> rdlt_connector::Result<T> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => Err(self.explained(error).await),
        }
    }

    /// `error`, carrying a spawned connector's last words when its transport failed.
    async fn explained(&self, error: ConnectorError) -> ConnectorError {
        let transport = matches!(error.code(), Some(CONNECTOR_LOST | TRANSPORT));
        if !transport || std::error::Error::source(&error).is_some() {
            return error;
        }
        let running = self.running.lock().await;
        match &running.process {
            Some(process) => error.with_source(process.last_words(LAST_WORDS).await),
            None => error,
        }
    }

    /// The live connector's spec, for its role, once it is checked to be the connector
    /// `reference` names, of a version it accepts.
    pub(crate) async fn checked_spec(
        &self,
        reference: &ConnectorRef,
        found_at: &str,
    ) -> Result<ConnectorSpec, ProviderError> {
        let handshake_failed = |source| ProviderError::HandshakeFailed {
            id: reference.id.clone(),
            source: Box::new(source),
        };
        let spec = crate::remote::contract_spec(self.live().await.spec(), self.role)
            .map_err(handshake_failed)?;
        if spec.id != reference.id {
            let message = format!("{found_at} serves `{}`", spec.id);
            return Err(handshake_failed(ConnectorError::config(message)));
        }
        accepts(reference, &spec.version)?;
        self.checked.set(spec.clone()).ok();
        Ok(spec)
    }

    /// The capabilities the live destination declares.
    pub(crate) async fn capabilities(&self) -> Result<Capabilities, ConnectorError> {
        let destination = RemoteDestination::new(self.live().await)?;
        Ok(destination.capabilities().clone())
    }
}

/// Why a connector did not start.
pub(crate) enum Spawned {
    /// Its process did not spawn.
    Io(std::io::Error),
    /// Its address could not be reached.
    Unreachable(std::io::Error),
    /// Its TLS handshake failed.
    Tls(std::io::Error),
    /// It did not connect: its handshake, or its own connect, failed.
    Connect(ConnectorError),
}

impl Spawned {
    fn into_error(self) -> ConnectorError {
        match self {
            Self::Io(error) | Self::Unreachable(error) => ConnectorError::new(
                ConnectorErrorKind::Transient,
                "starting the lost connector again failed",
            )
            .with_code(CONNECTOR_LOST)
            .with_source(error),
            // A refused certificate is refused again on a retry; a connection lost in the
            // handshake is not.
            Self::Tls(error) => {
                let refused = error
                    .get_ref()
                    .is_some_and(<dyn std::error::Error + Send + Sync>::is::<rustls::Error>);
                let kind = if refused {
                    ConnectorErrorKind::Auth
                } else {
                    ConnectorErrorKind::Transient
                };
                ConnectorError::new(kind, "the TLS handshake with the lost connector failed")
                    .with_code(TLS)
                    .with_source(error)
            }
            Self::Connect(error) => error,
        }
    }
}

/// The code of the error a TLS handshake with a connector fails with.
pub const TLS: &str = "tls";

/// Starts the connector as `start` says, and handshakes with it.
async fn begin(
    start: &Start,
    role: Role,
    config: &serde_json::Value,
    options: Options,
) -> Result<Running, Spawned> {
    match start {
        Start::Spawn(launch) => spawn(launch, role, config, options).await,
        Start::Dial(dial) => {
            let io = crate::network::dial(dial, options.deadlines.connect).await?;
            let connection = Connection::connect(io, role, config, options)
                .await
                .map_err(Spawned::Connect)?;
            Ok(Running {
                connection,
                process: None,
            })
        }
    }
}

/// Spawns the connector and handshakes with it.
async fn spawn(
    launch: &Launch,
    role: Role,
    config: &serde_json::Value,
    options: Options,
) -> Result<Running, Spawned> {
    let (host, connector) = std::os::unix::net::UnixStream::pair().map_err(Spawned::Io)?;
    let process = Process::spawn(launch, connector.into()).map_err(Spawned::Io)?;
    host.set_nonblocking(true).map_err(Spawned::Io)?;
    let io = tokio::net::UnixStream::from_std(host).map_err(Spawned::Io)?;
    let connection = match Connection::connect(io, role, config, options).await {
        Ok(connection) => connection,
        Err(error) => {
            let words = process.last_words(LAST_WORDS).await;
            let error = if std::error::Error::source(&error).is_none()
                && matches!(error.code(), Some(CONNECTOR_LOST | TRANSPORT))
            {
                error.with_source(words)
            } else {
                error
            };
            return Err(Spawned::Connect(error));
        }
    };
    Ok(Running {
        connection,
        process: Some(process),
    })
}

/// A spawned source, respawned when lost.
pub(crate) struct SupervisedSource(pub(crate) Arc<Supervisor>);

impl SupervisedSource {
    async fn source(&self) -> Result<RemoteSource, ConnectorError> {
        Ok(RemoteSource::new(self.0.connection().await?))
    }
}

impl Source for SupervisedSource {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let result = self.source().await?.check().await;
            self.0.explain(result).await
        })
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        Box::pin(async move {
            let result = self.source().await?.discover().await;
            self.0.explain(result).await
        })
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<Vec<Partition>>> {
        Box::pin(async move {
            let result = self.source().await?.plan(stream, state).await;
            self.0.explain(result).await
        })
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let result = self.source().await?.read(request, sink).await;
            self.0.explain(result).await
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let result = self.source().await?.committed(stream, cursors).await;
            self.0.explain(result).await
        })
    }
}

/// A spawned destination, respawned when lost; its capabilities are those it first declared.
pub(crate) struct SupervisedDestination {
    pub(crate) supervisor: Arc<Supervisor>,
    pub(crate) capabilities: Capabilities,
}

impl SupervisedDestination {
    async fn destination(&self) -> Result<RemoteDestination, ConnectorError> {
        let destination = RemoteDestination::new(self.supervisor.connection().await?)?;
        if *destination.capabilities() != self.capabilities {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "the respawned connector declares other capabilities than it first did",
            ));
        }
        Ok(destination)
    }
}

impl Destination for SupervisedDestination {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let result = self.destination().await?.check().await;
            self.supervisor.explain(result).await
        })
    }

    fn open<'a>(
        &'a self,
        context: &'a OpenContext,
    ) -> BoxFuture<'a, rdlt_connector::Result<OpenedSession>> {
        Box::pin(async move {
            let result = self.destination().await?.open(context).await;
            let opened = self.supervisor.explain(result).await?;
            Ok(OpenedSession {
                session: Box::new(SupervisedSession {
                    inner: opened.session,
                    supervisor: Arc::clone(&self.supervisor),
                }),
                ..opened
            })
        })
    }
}
