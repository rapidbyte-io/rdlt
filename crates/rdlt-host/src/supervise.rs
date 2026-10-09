//! Supervision: a connector that is lost, by its transport failing, missing heartbeats or exiting,
//! which closes its socket, is started again for the next call, respawned or redialed, and the
//! engine's retry of the attempt reaches it.

use std::sync::Arc;

use rdlt_connector::wire::TRANSPORT;
use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, ConnectorError, ConnectorErrorKind, ConnectorSpec, Cursor,
    Destination, OpenContext, OpenedSession, PartitionId, PartitionPlan, PartitionSink,
    ReadRequest, Role, Source, StreamName, StreamState,
};
use tokio::sync::Mutex;

mod session;
#[cfg(test)]
mod tests;

use session::SupervisedSession;

use crate::connect::Open;
use crate::local::Witness;
use crate::local::process::{Launch, Process, Unspawned};
use crate::network::Dial;
use crate::provider::{ConnectorRef, ProviderError, accepts};
use crate::remote::{CONNECTOR_LOST, Connection, Options, RemoteDestination, RemoteSource};
use crate::secrets::{Config, Redactions, SecretError, SecretResolver};
use rdlt_connector::wire::v1;

use crate::limits::LAST_WORDS;
use rdlt_connector::limits::MAX_ERROR_CODE_BYTES;

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

/// What explains the errors of calls made on one start of a connector: the secrets every
/// start was sent, and a witness to that start's process, when this process spawned it.
#[derive(Clone)]
pub(crate) struct Words {
    redactions: Redactions,
    witness: Option<Witness>,
}

impl Words {
    /// `result`, its error as the host keeps a connector's: scrubbed of every secret any start
    /// of the connector was sent, shown, and carrying the last words of the process the call
    /// went to when its transport failed.
    pub(crate) async fn explain<T>(
        &self,
        result: rdlt_connector::Result<T>,
    ) -> rdlt_connector::Result<T> {
        let Err(error) = result else {
            return result;
        };
        let transport = matches!(error.code(), Some(CONNECTOR_LOST | TRANSPORT));
        let explained = std::error::Error::source(&error).is_some();
        let error = error.received(&|text| self.redactions.scrubbed(text));
        Err(match &self.witness {
            Some(witness) if transport && !explained => {
                error.with_source(witness.last_words(LAST_WORDS).await)
            }
            _ => error,
        })
    }
}

/// Starts a connector, and starts it again, respawned or redialed, once it is lost.
pub(crate) struct Supervisor {
    start: Start,
    role: Role,
    /// What the connector is configured with, each time it is started.
    configured: Configured,
    options: Options,
    running: Mutex<Running>,
    /// Every secret any start of the connector was sent: a connector may say one again after
    /// it was started anew.
    redactions: Redactions,
    /// The spec the connector was checked to serve: whatever is started again must serve it.
    checked: ConnectorSpec,
}

/// A connector's configuration, and what resolves its secret references at each start.
pub(crate) struct Configured {
    pub(crate) config: Config,
    pub(crate) secrets: Arc<dyn SecretResolver>,
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
            let serves = rdlt_connector::text::shown(&spec.id, MAX_ERROR_CODE_BYTES);
            let message = format!("{} serves `{serves}`", self.found_at);
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
        configured: Configured,
        options: &Options,
        gate: &Gate<'_>,
    ) -> Result<Self, Spawned> {
        let admit = |spec: &v1::ConnectorSpec| gate.admit(spec).map_err(Spawned::Refused);
        let redactions = Redactions::new();
        let running = begin(&start, role, &configured, options, &admit, &redactions).await?;
        let checked = crate::remote::contract_spec(running.connection.spec())
            .map_err(|error| Spawned::Refused(gate.refused(error)))?;
        Ok(Self {
            start,
            role,
            configured,
            options: *options,
            running: Mutex::new(running),
            redactions,
            checked,
        })
    }

    /// The spec the connector was checked to serve.
    pub(crate) fn spec(&self) -> ConnectorSpec {
        self.checked.clone()
    }

    /// The connection to the connector last started, lost or not.
    pub(crate) async fn live(&self) -> Arc<Connection> {
        Arc::clone(&self.running.lock().await.connection)
    }

    /// The connection to a live connector, starting it again if it was lost, and what
    /// explains the errors of calls made on it.
    async fn connection(&self) -> Result<(Arc<Connection>, Words), ConnectorError> {
        let mut running = self.running.lock().await;
        if running.connection.is_spent() {
            let admit = |spec: &v1::ConnectorSpec| self.same_identity(spec);
            let (start, configured) = (&self.start, &self.configured);
            let starting = begin(
                start,
                self.role,
                configured,
                &self.options,
                &admit,
                &self.redactions,
            );
            let started = starting.await.map_err(Spawned::into_error)?;
            self.same(&started.connection)?;
            *running = started;
        }
        let words = Words {
            redactions: self.redactions.clone(),
            witness: running.process.as_ref().map(Process::witness),
        };
        Ok((Arc::clone(&running.connection), words))
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
        let spec = crate::remote::contract_spec(connection.spec())?;
        if spec == *checked {
            return Ok(());
        }
        Err(changed(spec.id.as_str(), &spec.version, checked))
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
    /// It is the connector placed, and a secret its configuration refers to did not resolve.
    Secret(SecretError),
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
            Self::Secret(error) => {
                ConnectorError::config("the connector's configuration could not be prepared")
                    .with_code(error.code())
                    .with_source(error)
            }
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

/// Starts the connector as `start` says, handshakes with it, and configures it once `admit`
/// accepts its spec.
async fn begin(
    start: &Start,
    role: Role,
    configured: &Configured,
    options: &Options,
    admit: &Admit<'_>,
    redactions: &Redactions,
) -> Result<Running, Spawned> {
    let io: Box<dyn crate::network::Stream> = match start {
        Start::Spawn(launch) => {
            return spawn(launch, role, configured, options, admit, redactions).await;
        }
        Start::Connect(open) => {
            let deadline = options.deadlines.connect;
            tokio::time::timeout(deadline, open())
                .await
                .map_err(|_| {
                    Spawned::Unreachable(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("the connector's stream did not open within {deadline:?}"),
                    ))
                })?
                .map_err(Spawned::Unreachable)?
        }
        Start::Dial(dial) => Box::new(crate::network::dial(dial, options.deadlines.connect).await?),
    };
    let connection = self::configured(io, role, configured, redactions, options, admit).await?;
    Ok(Running {
        connection,
        process: None,
    })
}

/// Handshakes over `io`, and once `admit` accepts the connector's spec resolves the secrets
/// of its configuration and configures it: a connector other than was placed is sent no
/// configuration, and no secret is resolved for it.
async fn configured<IO>(
    io: IO,
    role: Role,
    configured: &Configured,
    redactions: &Redactions,
    options: &Options,
    admit: &Admit<'_>,
) -> Result<Arc<Connection>, Spawned>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let scrubbed =
        |error: ConnectorError| Spawned::Connect(error.received(&|text| redactions.scrubbed(text)));
    let handshaken = Connection::handshake(io, role, *options)
        .await
        .map_err(scrubbed)?;
    admit(handshaken.spec())?;
    let resolving = configured.config.resolved(&*configured.secrets, redactions);
    let config_json = resolving.await.map_err(Spawned::Secret)?;
    // The request owns its text, and the transport its bytes: neither is wiped.
    let sent = handshaken.configure_json(config_json.as_str().to_owned());
    sent.await.map_err(scrubbed)
}

/// Spawns the connector, once its binary is unchanged since it was placed, and handshakes
/// with it.
async fn spawn(
    launch: &Launch,
    role: Role,
    configured: &Configured,
    options: &Options,
    admit: &Admit<'_>,
    redactions: &Redactions,
) -> Result<Running, Spawned> {
    let launched = Process::launching(launch.clone(), redactions.clone()).await;
    let (io, process) = launched.map_err(|unspawned| match *unspawned {
        (_, Unspawned::Io(error)) => Spawned::Io(error),
        (launch, refused) => Spawned::Refused(crate::local::refused(&launch, refused)),
    })?;
    let connecting = self::configured(io, role, configured, redactions, options, admit);
    let connection = match connecting.await {
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
    async fn source(&self) -> Result<(RemoteSource, Words), ConnectorError> {
        let (connection, words) = self.0.connection().await?;
        Ok((RemoteSource::new(connection), words))
    }
}

impl Source for SupervisedSource {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.check().await).await
        })
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.discover().await).await
        })
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.plan(stream, state).await).await
        })
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.read(request, sink).await).await
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (source, words) = self.source().await?;
            words.explain(source.committed(stream, cursors).await).await
        })
    }
}

/// A spawned destination, respawned when lost; its capabilities are those it first declared.
pub(crate) struct SupervisedDestination {
    pub(crate) supervisor: Arc<Supervisor>,
    pub(crate) capabilities: Capabilities,
}

impl SupervisedDestination {
    async fn destination(&self) -> Result<(RemoteDestination, Words), ConnectorError> {
        let (connection, words) = self.supervisor.connection().await?;
        let destination = RemoteDestination::new(connection)?;
        if *destination.capabilities() != self.capabilities {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "the respawned connector declares other capabilities than it first did",
            ));
        }
        Ok((destination, words))
    }
}

impl Destination for SupervisedDestination {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (destination, words) = self.destination().await?;
            words.explain(destination.check().await).await
        })
    }

    fn open<'a>(
        &'a self,
        context: &'a OpenContext,
    ) -> BoxFuture<'a, rdlt_connector::Result<OpenedSession>> {
        Box::pin(async move {
            let (destination, words) = self.destination().await?;
            let opened = words.explain(destination.open(context).await).await?;
            Ok(OpenedSession {
                session: Box::new(SupervisedSession {
                    inner: opened.session,
                    words,
                }),
                ..opened
            })
        })
    }
}
