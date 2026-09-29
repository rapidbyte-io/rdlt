//! Supervision: a connector that is lost, by its transport failing, missing heartbeats or exiting,
//! which closes its socket, is started again for the next call, respawned or redialed, and the
//! engine's retry of the attempt reaches it.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::wire::TRANSPORT;
use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, ConnectorError, ConnectorErrorKind, ConnectorSpec, Cursor,
    Destination, OpenContext, OpenedSession, PartitionId, PartitionPlan, PartitionSink,
    ReadRequest, Role, Source, StreamName, StreamState,
};
use tokio::sync::Mutex;

mod session;

use session::SupervisedSession;

use crate::connect::Open;
use crate::local::process::{Launch, Process};
use crate::network::Dial;
use crate::provider::{ConnectorRef, ProviderError, accepts};
use crate::remote::{CONNECTOR_LOST, Connection, Options, RemoteDestination, RemoteSource};
use rdlt_connector::wire::v1;

/// How long the errors of a lost connector wait for its standard error to close.
const LAST_WORDS: Duration = Duration::from_secs(1);

/// How a connector starts: spawned in a process of its own, dialed where it listens, or reached
/// through a stream a function opens.
pub(crate) enum Start {
    /// Spawned from its binary.
    Spawn(Launch),
    /// Dialed over mutual TLS.
    Dial(Dial),
    /// Reached through a stream the function opens.
    Connect(Open),
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
    checked: ConnectorSpec,
}

/// What a connector must be before it sees its configuration: the connector `reference` names,
/// of a version it accepts, found at `found_at`.
pub(crate) struct Gate<'a> {
    pub(crate) reference: &'a ConnectorRef,
    pub(crate) found_at: &'a str,
}

impl Gate<'_> {
    /// Whether the handshake's `spec` is the connector the reference names, of a version it
    /// accepts.
    fn admit(&self, spec: &v1::ConnectorSpec) -> Result<(), ProviderError> {
        if spec.id != self.reference.id.as_str() {
            let message = format!("{} serves `{}`", self.found_at, spec.id);
            return Err(self.refused(ConnectorError::config(message)));
        }
        accepts(self.reference, &spec.version)
    }

    fn refused(&self, source: ConnectorError) -> ProviderError {
        ProviderError::HandshakeFailed {
            id: self.reference.id.clone(),
            source: Box::new(source),
        }
    }
}

impl Supervisor {
    /// Starts the connector `start` describes, as `role`, and configures it with `config` once
    /// its handshake shows it is the connector `gate` admits.
    pub(crate) async fn start(
        start: Start,
        role: Role,
        config: serde_json::Value,
        options: Options,
        gate: &Gate<'_>,
    ) -> Result<Self, Spawned> {
        let admit = |spec: &v1::ConnectorSpec| gate.admit(spec).map_err(Spawned::Refused);
        let running = begin(&start, role, &config, options, &admit).await?;
        let checked = crate::remote::contract_spec(running.connection.spec(), role)
            .map_err(|error| Spawned::Refused(gate.refused(error)))?;
        Ok(Self {
            start,
            role,
            config,
            options,
            running: Mutex::new(running),
            checked,
        })
    }

    /// The spec the connector was checked to serve, for its role.
    pub(crate) fn spec(&self) -> ConnectorSpec {
        self.checked.clone()
    }

    /// The connection to the connector last started, lost or not.
    pub(crate) async fn live(&self) -> Arc<Connection> {
        Arc::clone(&self.running.lock().await.connection)
    }

    /// The connection to a live connector, starting it again if it was lost.
    async fn connection(&self) -> Result<Arc<Connection>, ConnectorError> {
        let mut running = self.running.lock().await;
        if running.connection.is_spent() {
            let admit = |spec: &v1::ConnectorSpec| self.same_identity(spec);
            let started = begin(&self.start, self.role, &self.config, self.options, &admit)
                .await
                .map_err(Spawned::into_error)?;
            self.same(&started.connection)?;
            *running = started;
        }
        Ok(Arc::clone(&running.connection))
    }

    /// Whether a connector started again handshook with the id and version first checked, before
    /// it sees the configuration: a redial may reach whatever listens at the endpoint now.
    fn same_identity(&self, spec: &v1::ConnectorSpec) -> Result<(), Spawned> {
        let checked = &self.checked;
        if spec.id == checked.id.as_str() && spec.version == checked.version {
            return Ok(());
        }
        Err(Spawned::Connect(changed(&spec.id, &spec.version, checked)))
    }

    /// Whether `connection`'s connector serves the spec the first was checked to serve, with what
    /// its configuration declares.
    fn same(&self, connection: &Connection) -> Result<(), ConnectorError> {
        let checked = &self.checked;
        let spec = crate::remote::contract_spec(connection.spec(), self.role)?;
        if spec == *checked {
            return Ok(());
        }
        Err(changed(spec.id.as_str(), &spec.version, checked))
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

    /// The capabilities the live destination declares.
    pub(crate) async fn capabilities(&self) -> Result<Capabilities, ConnectorError> {
        let destination = RemoteDestination::new(self.live().await)?;
        Ok(destination.capabilities().clone())
    }
}

/// The error of a connector started again that serves `id` `version`, not the connector
/// `checked` as it was placed; coded `connector_changed`.
fn changed(id: &str, version: &str, checked: &ConnectorSpec) -> ConnectorError {
    ConnectorError::config(format!(
        "the connector started again serves `{id}` {version}, not `{}` {} as it was placed",
        checked.id, checked.version
    ))
    .with_code("connector_changed")
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
    /// It is not the connector placed: another binary, id or version, refused before it saw its
    /// configuration.
    Refused(ProviderError),
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
            Self::Refused(refused) => {
                ConnectorError::config("the connector started again is not the connector placed")
                    .with_code("connector_changed")
                    .with_source(refused)
            }
        }
    }
}

/// The code of the error a TLS handshake with a connector fails with.
pub const TLS: &str = "tls";

/// Checks the spec a connector handshook with, before it sees its configuration.
type Admit<'a> = dyn Fn(&v1::ConnectorSpec) -> Result<(), Spawned> + Sync + 'a;

/// Starts the connector as `start` says, handshakes with it, and configures it with `config` once
/// `admit` accepts its spec.
async fn begin(
    start: &Start,
    role: Role,
    config: &serde_json::Value,
    options: Options,
    admit: &Admit<'_>,
) -> Result<Running, Spawned> {
    match start {
        Start::Spawn(launch) => spawn(launch, role, config, options, admit).await,
        Start::Connect(open) => {
            let deadline = options.deadlines.connect;
            let io = tokio::time::timeout(deadline, open())
                .await
                .map_err(|_| {
                    Spawned::Unreachable(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("the connector's stream did not open within {deadline:?}"),
                    ))
                })?
                .map_err(Spawned::Unreachable)?;
            let connection = configured(io, role, config, options, admit).await?;
            Ok(Running {
                connection,
                process: None,
            })
        }
        Start::Dial(dial) => {
            let io = crate::network::dial(dial, options.deadlines.connect).await?;
            let connection = configured(io, role, config, options, admit).await?;
            Ok(Running {
                connection,
                process: None,
            })
        }
    }
}

/// Handshakes over `io`, and configures the connector with `config` once `admit` accepts its spec.
async fn configured<IO>(
    io: IO,
    role: Role,
    config: &serde_json::Value,
    options: Options,
    admit: &Admit<'_>,
) -> Result<Arc<Connection>, Spawned>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let handshaken = Connection::handshake(io, role, options)
        .await
        .map_err(Spawned::Connect)?;
    admit(handshaken.spec())?;
    handshaken.configure(config).await.map_err(Spawned::Connect)
}

/// Spawns the connector, once its binary is unchanged since it was placed, and handshakes with it.
async fn spawn(
    launch: &Launch,
    role: Role,
    config: &serde_json::Value,
    options: Options,
    admit: &Admit<'_>,
) -> Result<Running, Spawned> {
    if let Some(expected) = launch.digest {
        let found = crate::local::digest(&launch.path)
            .await
            .map_err(Spawned::Io)?;
        if found != expected {
            return Err(Spawned::Refused(ProviderError::DigestMismatch {
                id: launch.id.clone(),
                path: launch.path.clone(),
                expected,
                found,
            }));
        }
    }
    let (io, process) = Process::launched(launch).map_err(Spawned::Io)?;
    let connection = match configured(io, role, config, options, admit).await {
        Ok(connection) => connection,
        Err(Spawned::Connect(error)) => {
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
        // A connector refused never sees its configuration, and stops with its process.
        Err(refused) => return Err(refused),
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
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
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
