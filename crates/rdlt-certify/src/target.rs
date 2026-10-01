//! What a certification reaches: a connector served in this process, spawned from its binary, or
//! listening at an endpoint.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector::wire::TRANSPORT;
use rdlt_connector::{BoxFuture, ConnectorError, ConnectorErrorKind, Role};
use rdlt_host::remote::{CONNECTOR_LOST, Client, DEADLINE_EXCEEDED, client};
use rdlt_host::{Connection, ConnectorRef, Local, Options, Remote, Stream, Witness};
use rdlt_wire::Limits;
use rdlt_wire::tonic::{Response, Status};

/// How long a failed connection waits for a spawned connector's standard error to close.
const LAST_WORDS: Duration = Duration::from_secs(1);

/// A connector to certify, and how to reach it.
#[derive(Debug)]
pub struct Target {
    reach: Reach,
    options: Options,
    kill_seed: Option<u64>,
    kill_timeout: Option<Duration>,
}

enum Reach {
    /// Opened by a function, each connection a stream of its own.
    Connected(Arc<Connect>),
    /// Spawned from its binary for each connection.
    Spawned {
        local: Local,
        reference: ConnectorRef,
    },
    /// Listening at the reference's endpoint.
    Listening {
        remote: Remote,
        reference: ConnectorRef,
    },
}

/// What opens a fresh stream to a connector.
type Connect = dyn Fn() -> BoxFuture<'static, std::io::Result<Box<dyn Stream>>> + Send + Sync;

impl std::fmt::Debug for Reach {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connected(_) => formatter.write_str("Connected"),
            Self::Spawned { local, reference } => formatter
                .debug_struct("Spawned")
                .field("local", local)
                .field("reference", reference)
                .finish(),
            Self::Listening { remote, reference } => formatter
                .debug_struct("Listening")
                .field("remote", remote)
                .field("reference", reference)
                .finish(),
        }
    }
}

impl Target {
    /// The connector `served` serves, in this process, each connection over a socket of its own.
    pub fn served(served: Served) -> Self {
        let served = Arc::new(served);
        Self::connected(move || {
            let served = Arc::clone(&served);
            Box::pin(async move {
                let (host, connector) = tokio::net::UnixStream::pair()?;
                tokio::spawn(serve_connection(served, connector, Limits::default()));
                Ok(Box::new(host) as Box<dyn Stream>)
            })
        })
    }

    /// The connector `connect` reaches, by opening a fresh stream to it for each connection: over
    /// any transport, or to a connector that is no `rdlt-connector` binary.
    pub fn connected(
        connect: impl Fn() -> BoxFuture<'static, std::io::Result<Box<dyn Stream>>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self::reaching(Reach::Connected(Arc::new(connect)))
    }

    /// The connector `reference` names, spawned by `local` for each connection.
    pub fn spawned(local: Local, reference: ConnectorRef) -> Self {
        Self::reaching(Reach::Spawned { local, reference })
    }

    /// The connector listening at `reference`'s endpoint, dialed by `remote`.
    pub fn listening(remote: Remote, reference: ConnectorRef) -> Self {
        Self::reaching(Reach::Listening { remote, reference })
    }

    fn reaching(reach: Reach) -> Self {
        Self {
            reach,
            options: Options::default(),
            kill_seed: None,
            kill_timeout: None,
        }
    }

    /// Runs each connection with `options`: its deadlines, heartbeat and limits.
    #[must_use]
    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// Kills the connector at the points `seed` draws, as a failed kill clause reports it did, to
    /// reproduce the failure; each certification otherwise draws points of its own.
    #[must_use]
    pub fn kill_seed(mut self, seed: u64) -> Self {
        self.kill_seed = Some(seed);
        self
    }

    /// Gives each kill clause `bound` for all its loads, rather than 300 s: a connector slower
    /// than about two seconds a commit needs more.
    #[must_use]
    pub fn kill_timeout(mut self, bound: Duration) -> Self {
        self.kill_timeout = Some(bound);
        self
    }

    /// How long each kill clause may take, when that is chosen.
    #[cfg(feature = "kill")]
    pub(crate) fn chosen_timeout(&self) -> Option<Duration> {
        self.kill_timeout
    }

    /// The seed the kill clauses draw their points from, when one is chosen.
    #[cfg(feature = "kill")]
    pub(crate) fn chosen_seed(&self) -> Option<u64> {
        self.kill_seed
    }

    /// The limits this host enforces.
    pub(crate) fn limits(&self) -> Limits {
        self.options.limits
    }

    /// What the target is, for a report that could not learn the connector's id.
    pub fn describe(&self) -> String {
        match &self.reach {
            Reach::Connected(_) => "a connector reached by a function".to_owned(),
            Reach::Spawned { local, reference } => match local.resolve(reference) {
                Ok(path) => path.display().to_string(),
                Err(_) => reference.id.to_string(),
            },
            Reach::Listening { reference, .. } => reference
                .endpoint
                .clone()
                .unwrap_or_else(|| reference.id.to_string()),
        }
    }

    /// A fresh raw connection to the connector.
    pub(crate) async fn wire(&self) -> Result<Box<dyn Stream>, ConnectorError> {
        self.witnessed().await.map(|(wire, _)| wire)
    }

    /// A fresh raw connection to the connector, and a witness to how it ends when spawned.
    async fn witnessed(&self) -> Result<(Box<dyn Stream>, Option<Witness>), ConnectorError> {
        let unreachable = |error: &dyn std::fmt::Display| {
            ConnectorError::new(
                ConnectorErrorKind::Transient,
                format!("the connector could not be reached: {error}"),
            )
        };
        match &self.reach {
            Reach::Connected(connect) => {
                let deadline = self.options.deadlines.connect;
                let opened = tokio::time::timeout(deadline, connect()).await;
                let opened = opened.unwrap_or_else(|_| Err(std::io::ErrorKind::TimedOut.into()));
                opened
                    .map(|wire| (wire, None))
                    .map_err(|error| unreachable(&error))
            }
            Reach::Spawned { local, reference } => local
                .wire(reference)
                .map(|wire| {
                    let witness = wire.witness();
                    (Box::new(wire) as Box<dyn Stream>, witness)
                })
                .map_err(|error| unreachable(&error)),
            Reach::Listening { remote, reference } => remote
                .wire(reference)
                .await
                .map(|wire| (Box::new(wire) as Box<dyn Stream>, None))
                .map_err(|error| unreachable(&error)),
        }
    }

    /// A fresh connection to the connector, handshaken as `role` with `config`.
    pub(crate) async fn connect(
        &self,
        role: Role,
        config: &serde_json::Value,
    ) -> Result<Arc<Connection>, ConnectorError> {
        let (wire, witness) = self.witnessed().await?;
        let connected = Connection::connect(wire, role, config, self.options).await;
        match (connected, witness) {
            // A spawned connector whose transport failed most likely ended: say what it said.
            (Err(error), Some(witness))
                if matches!(error.code(), Some(CONNECTOR_LOST | TRANSPORT)) =>
            {
                Err(error.with_source(witness.last_words(LAST_WORDS).await))
            }
            (connected, _) => connected,
        }
    }

    /// A fresh client of the protocol, with no handshake yet.
    pub(crate) async fn client(&self) -> Result<Client, ConnectorError> {
        client(self.wire().await?, self.options).await
    }

    /// The answer to `call`, a handshake or a configuration a raw client makes, within the
    /// deadline a connection's own has: a raw client's calls have none, and `what` names the call.
    pub(crate) async fn opened<T>(
        &self,
        what: &str,
        call: impl Future<Output = Result<Response<T>, Status>>,
    ) -> Result<T, ConnectorError> {
        let deadline = self.options.deadlines.connect;
        match tokio::time::timeout(deadline, call).await {
            Ok(Ok(answer)) => Ok(answer.into_inner()),
            Ok(Err(status)) => Err(rdlt_connector::wire::error(&status)),
            Err(_) => {
                let message = format!("{what} took longer than its deadline of {deadline:?}");
                Err(ConnectorError::new(ConnectorErrorKind::Transient, message)
                    .with_code(DEADLINE_EXCEEDED))
            }
        }
    }
}

/// Bytes: the credit a source placed for a kill clause reads within.
#[cfg(feature = "kill")]
const KILL_WINDOW: u64 = 65_536;

#[cfg(feature = "kill")]
impl Target {
    /// A provider placing the connector as an engine's placement does, supervised, so a lost one
    /// is started or reached again, killed by `kills`; and the reference to it, as `id`, which
    /// its handshake answered.
    ///
    /// A spawned connector is killed outright; one reached otherwise has its connections cut. A
    /// source reads within [`KILL_WINDOW`] of credit, so it runs little ahead of the commits the
    /// kills follow.
    pub(crate) fn provider(
        &self,
        id: &rdlt_connector::ConnectorId,
        kills: &rdlt_host::Kills,
    ) -> (Box<dyn rdlt_host::Provider>, ConnectorRef) {
        let reference = |from: Option<&ConnectorRef>| ConnectorRef {
            id: id.clone(),
            version_req: None,
            path: from.and_then(|reference| reference.path.clone()),
            endpoint: from.and_then(|reference| reference.endpoint.clone()),
            digest: from.and_then(|reference| reference.digest),
        };
        let options = Options {
            read_window: KILL_WINDOW,
            ..self.options
        };
        match &self.reach {
            Reach::Connected(connect) => {
                let connect = Arc::clone(connect);
                let provider = rdlt_host::Connect::new(move || connect())
                    .options(options)
                    .kills(kills);
                (Box::new(provider), reference(None))
            }
            Reach::Spawned {
                local,
                reference: spawned,
            } => {
                let provider = local.clone().options(options).kills(kills);
                (Box::new(provider), reference(Some(spawned)))
            }
            Reach::Listening {
                remote,
                reference: listening,
            } => {
                let provider = remote.clone().options(options).kills(kills);
                (Box::new(provider), reference(Some(listening)))
            }
        }
    }
}
