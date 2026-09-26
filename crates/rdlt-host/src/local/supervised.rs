//! Supervision: a spawned connector that is lost, by its transport failing, missing heartbeats or
//! exiting, which closes its socket, is respawned for the next call, and the engine's retry of the
//! attempt reaches it.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::wire::TRANSPORT;
use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, ConnectorError, ConnectorErrorKind, Cursor, Destination,
    OpenContext, OpenedSession, Partition, PartitionId, PartitionSink, ReadRequest, Role, Source,
    StreamName, StreamState,
};
use tokio::sync::Mutex;

use super::process::{Launch, Process};
use super::session::SupervisedSession;
use crate::remote::{CONNECTOR_LOST, Connection, Options, RemoteDestination, RemoteSource};

/// How long the errors of a lost connector wait for its standard error to close.
const LAST_WORDS: Duration = Duration::from_secs(1);

/// A connector process and the connection to it.
pub(crate) struct Running {
    pub(crate) connection: Arc<Connection>,
    pub(crate) process: Process,
}

/// Spawns a connector, and respawns it once it is lost.
pub(crate) struct Supervisor {
    launch: Launch,
    role: Role,
    config: serde_json::Value,
    options: Options,
    running: Mutex<Running>,
}

impl Supervisor {
    /// Starts the connector `launch` describes, as `role`, with `config`.
    pub(crate) async fn start(
        launch: Launch,
        role: Role,
        config: serde_json::Value,
        options: Options,
    ) -> Result<Self, Spawned> {
        let running = spawn(&launch, role, &config, options).await?;
        Ok(Self {
            launch,
            role,
            config,
            options,
            running: Mutex::new(running),
        })
    }

    /// The connection to the connector last spawned, lost or not.
    pub(crate) async fn live(&self) -> Arc<Connection> {
        Arc::clone(&self.running.lock().await.connection)
    }

    /// The connection to a live connector, respawning it if it was lost.
    async fn connection(&self) -> Result<Arc<Connection>, ConnectorError> {
        let mut running = self.running.lock().await;
        if running.connection.is_lost() {
            *running = spawn(&self.launch, self.role, &self.config, self.options)
                .await
                .map_err(Spawned::into_error)?;
        }
        Ok(Arc::clone(&running.connection))
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

    /// `error`, carrying the connector's last words when its transport failed.
    async fn explained(&self, error: ConnectorError) -> ConnectorError {
        let transport = matches!(error.code(), Some(CONNECTOR_LOST | TRANSPORT));
        if !transport || std::error::Error::source(&error).is_some() {
            return error;
        }
        let running = self.running.lock().await;
        let words = running.process.last_words(LAST_WORDS).await;
        error.with_source(words)
    }
}

/// Why a connector did not start: its process did not spawn, or it did not connect.
pub(crate) enum Spawned {
    Io(std::io::Error),
    Connect(ConnectorError),
}

impl Spawned {
    fn into_error(self) -> ConnectorError {
        match self {
            Self::Io(error) => ConnectorError::new(
                ConnectorErrorKind::Transient,
                "respawning the lost connector failed",
            )
            .with_code(CONNECTOR_LOST)
            .with_source(error),
            Self::Connect(error) => error,
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
        process,
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
