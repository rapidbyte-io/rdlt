//! What a certification reaches: a connector served in this process, spawned from its binary, or
//! listening at an endpoint.

#[cfg(test)]
mod tests;

use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector::wire::{Invalid, TRANSPORT, v1};
use rdlt_connector::{
    BoxFuture, ConnectorError, ConnectorErrorKind, ConnectorId, ConnectorSpec, Role,
};
use rdlt_host::remote::{CONNECTOR_LOST, Client, DEADLINE_EXCEEDED, client};
use rdlt_host::{
    Connection, ConnectorRef, Digest, Local, Options, ReadWindow, Redactions, Remote, Stream,
    Witness,
};
use rdlt_wire::Limits;
use rdlt_wire::tonic::{Response, Status};

/// How long a failed connection waits for a spawned connector's standard error to close.
const LAST_WORDS: Duration = Duration::from_secs(1);

/// Bytes: the floor of the credit a source placed for a kill clause reads within.
const KILL_FLOOR: u64 = 65_536;

/// Bytes: the window of credit `S-BARRIER`'s reads are granted, which a fixed window
/// ([`ReadWindow::Fixed`]) keeps: each frame is granted back as it is taken and no more, so one
/// frame is in flight whatever its size, and a barrier the clause raises during a read reaches
/// the source within the events the serving end holds.
const BARRIER_FLOOR: u64 = 1;

/// A connector to certify, and how to reach it.
///
/// From its first connection as a role on, a target holds its connector to the version that role
/// answered and, spawned on Linux, to one binary ([`Held`]): a later connection of another, in
/// any certification made through it, fails the clause it is made in.
#[derive(Debug)]
pub struct Target {
    reach: Reach,
    options: Options,
    kill_seed: Option<u64>,
    kill_timeout: Option<Duration>,
    credit_watch: Option<Duration>,
    /// What the kill clauses run against, where not this target.
    killed: Option<Box<Target>>,
    /// The secrets the connector was sent: nothing it says is shown with one in it.
    pub(crate) redactions: Redactions,
    /// What the certification found the connector to be, which every later connection is held
    /// to.
    holding: Holding,
}

/// What a certification held its connector to as a role: the version its first connection as the
/// role answered, and the digest of the binary its first spawn ran, where it spawned one on a
/// platform that checks one, Linux; every connection after was refused as another connector unless
/// it was the same.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Held {
    /// The connector's version, as its spec declared it.
    pub version: Option<String>,
    /// The SHA-256 digest of the connector's binary.
    pub digest: Option<Digest>,
}

/// What a certification holds its connector to, once it has found it: a version for each role,
/// as a binary may serve a source and a destination of their own versions, and one binary.
#[derive(Debug, Default)]
struct Holding {
    source: OnceLock<String>,
    destination: OnceLock<String>,
    digest: OnceLock<Digest>,
}

impl Holding {
    /// The version `role` is held to, once it is.
    fn version(&self, role: Role) -> &OnceLock<String> {
        match role {
            Role::Source => &self.source,
            Role::Destination => &self.destination,
        }
    }
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

    /// The connector binary at `path`, answering to `id`, spawned for each connection with this
    /// process's own access: a test certifying its own build trusts it as itself.
    pub fn trusted_binary(id: ConnectorId, path: impl Into<PathBuf>) -> Self {
        Self::spawned(Local::trusting_binaries(), ConnectorRef::new(id).path(path))
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
            credit_watch: None,
            killed: None,
            redactions: Redactions::new(),
            holding: Holding::default(),
        }
    }

    /// Runs each connection with `options`: its deadlines, heartbeat and limits; the credit
    /// `S-BARRIER`'s reads and the kill clauses' sources are granted stays the certification's
    /// own.
    #[must_use]
    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// Scrubs what the connector says, its errors and its output, of `redactions`, the secrets
    /// its configuration was resolved with, before any of it is shown; a kill target keeps its
    /// own.
    #[must_use]
    pub fn redactions(mut self, redactions: Redactions) -> Self {
        self.redactions = redactions;
        self
    }

    /// Kills the connector at the points `seed` draws, as a failed kill clause reports it did, to
    /// reproduce the failure; each certification otherwise draws points of its own.
    #[must_use]
    pub fn kill_seed(mut self, seed: u64) -> Self {
        self.kill_seed = Some(seed);
        self
    }

    /// Runs the kill clauses against `target`, with its own kill seed and bound, rather than this
    /// target: a certification that checks its other clauses against a connector served in this
    /// process kills its installed binary, which a kill ends rather than cuts.
    #[must_use]
    pub fn kill_target(mut self, target: Target) -> Self {
        self.killed = Some(Box::new(target));
        self
    }

    /// What the kill clauses run against: the kill target, where one is chosen, else this one.
    pub(crate) fn killed(&self) -> &Target {
        self.killed.as_deref().unwrap_or(self)
    }

    /// Gives each kill clause `bound` for all its loads, rather than 300 s: a connector slower
    /// than about two seconds a commit needs more.
    #[must_use]
    pub fn kill_timeout(mut self, bound: Duration) -> Self {
        self.kill_timeout = Some(bound);
        self
    }

    /// Watches a read whose credit is spent for `watch` after each grant, rather than
    /// [`CREDIT_WATCH`](crate::CREDIT_WATCH): `P-CREDIT` then takes four times `watch`, tells a
    /// source that ignores credit only when it sends within that, and notes the watch beside
    /// its pass.
    #[must_use]
    pub fn credit_watch(mut self, watch: Duration) -> Self {
        self.credit_watch = Some(watch);
        self
    }

    /// How long a read whose credit is spent is watched after each grant, when that is chosen.
    pub(crate) fn chosen_watch(&self) -> Option<Duration> {
        self.credit_watch
    }

    /// How long each kill clause may take, when that is chosen.
    pub(crate) fn chosen_timeout(&self) -> Option<Duration> {
        self.kill_timeout
    }

    /// The seed the kill clauses draw their points from, when one is chosen.
    pub(crate) fn chosen_seed(&self) -> Option<u64> {
        self.kill_seed
    }

    /// The limits this host enforces.
    pub(crate) fn limits(&self) -> Limits {
        self.options.limits
    }

    /// How often this host pings the connector it reaches.
    pub(crate) fn heartbeat(&self) -> Duration {
        self.options.heartbeat
    }

    /// What the certification held the connector to so far as `role`: its version and, where
    /// spawned, its binary's digest.
    pub fn held(&self, role: Role) -> Held {
        Held {
            version: self.holding.version(role).get().cloned(),
            digest: self.holding.digest.get().copied(),
        }
    }

    /// Holds the connector, as `role`, to `version` from now on, where it holds it to none yet;
    /// an error coded `version_mismatch` where it holds it to another.
    pub(crate) fn hold_version(&self, role: Role, version: &str) -> Result<(), ConnectorError> {
        let held = self
            .holding
            .version(role)
            .get_or_init(|| version.to_owned());
        if held == version {
            return Ok(());
        }
        let message = format!(
            "the connector answered as version {version}, not {held}, the version its \
             certification began with"
        );
        Err(ConnectorError::config(message).with_code("version_mismatch"))
    }

    /// Holds every later spawn of the connector to the binary of `digest`, where it holds them to
    /// none yet.
    pub(crate) fn hold_digest(&self, digest: Option<Digest>) {
        if let Some(digest) = digest {
            self.holding.digest.get_or_init(|| digest);
        }
    }

    /// `reference`, holding a spawn to the digest the certification holds the connector's binary
    /// to, where the reference requires none of its own.
    fn held_to(&self, reference: &ConnectorRef) -> ConnectorRef {
        let mut reference = reference.clone();
        reference.digest = reference.digest.or(self.holding.digest.get().copied());
        reference
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
            Reach::Spawned { local, reference } => {
                let local = local.clone().redactions(self.redactions.clone());
                let wire = local
                    .wire(&self.held_to(reference))
                    .await
                    .map_err(|error| unreachable(&error))?;
                self.hold_digest(wire.digest());
                let witness = wire.witness();
                Ok((Box::new(wire) as Box<dyn Stream>, witness))
            }
            Reach::Listening { remote, reference } => remote
                .wire(reference)
                .await
                .map(|wire| (Box::new(wire) as Box<dyn Stream>, None))
                .map_err(|error| unreachable(&error)),
        }
    }

    /// How every connection runs, but the kill clauses' and `S-BARRIER`'s.
    pub(crate) fn connections(&self) -> Options {
        self.options
    }

    /// How `S-BARRIER`'s connections run: each read's window fixed at [`BARRIER_FLOOR`].
    pub(crate) fn barriers_read(&self) -> Options {
        Options {
            read_floor: BARRIER_FLOOR,
            read_window: ReadWindow::Fixed,
            ..self.options
        }
    }

    /// A fresh connection to the connector, handshaken as `role` with `config`, of the version
    /// the certification holds it to.
    pub(crate) async fn connect(
        &self,
        role: Role,
        config: &serde_json::Value,
    ) -> Result<Arc<Connection>, ConnectorError> {
        self.connect_with(role, config, &self.options).await
    }

    /// A fresh connection as [`connect`](Self::connect) makes, running with `options`.
    pub(crate) async fn connect_with(
        &self,
        role: Role,
        config: &serde_json::Value,
        options: &Options,
    ) -> Result<Arc<Connection>, ConnectorError> {
        let (wire, witness) = self.witnessed().await?;
        let redactions = self.redactions.clone();
        let connected = Connection::connect(wire, role, config, *options, redactions).await;
        let connection = match (connected, witness) {
            // A spawned connector whose transport failed most likely ended: say what it said.
            (Err(error), Some(witness))
                if matches!(error.code(), Some(CONNECTOR_LOST | TRANSPORT)) =>
            {
                Err(error.with_source(witness.last_words(LAST_WORDS).await))
            }
            (connected, _) => connected,
        }?;
        self.hold_version(role, &connection.connector_spec()?.version)?;
        Ok(connection)
    }

    /// The error `status` carries, its texts scrubbed of what the connector was sent before they
    /// are shown.
    pub(crate) fn error(&self, status: &Status) -> ConnectorError {
        rdlt_connector::wire::error(status, &|text| self.redactions.scrubbed(text))
    }

    /// The error the connector sent as `error`, scrubbed as [`error`](Self::error) scrubs.
    pub(crate) fn decoded(&self, error: v1::Error) -> Result<ConnectorError, Invalid> {
        ConnectorError::from_wire(error, &|text| self.redactions.scrubbed(text))
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
            Ok(Err(status)) => Err(self.error(&status)),
            Err(_) => {
                let message = format!("{what} took longer than its deadline of {deadline:?}");
                Err(ConnectorError::new(ConnectorErrorKind::Transient, message)
                    .with_code(DEADLINE_EXCEEDED))
            }
        }
    }

    /// The options a kill clause places the connector with: a source's credit opens at
    /// [`KILL_FLOOR`] and grows to two of its frames, and what it sends keeps within `admitted`,
    /// the limits the loading engine admits within.
    fn killing(&self, admitted: &Limits) -> Options {
        Options {
            read_floor: KILL_FLOOR,
            read_window: ReadWindow::Growing,
            limits: self.options.limits.lesser(admitted),
            ..self.options
        }
    }

    /// A provider placing the connector as an engine's placement does, supervised, so a lost one
    /// is started or reached again, killed by `kills`; and the reference to it: the connector
    /// `spec` names, of its version, which every start's handshake must answer, and of the
    /// binary the certification holds this target to.
    ///
    /// A spawned connector is killed outright; one reached otherwise has its connections cut. A
    /// source's credit opens at [`KILL_FLOOR`] and grows to two of its frames, so it runs little
    /// ahead of the commits the kills follow, and is told `admitted`, the limits the loading
    /// engine admits within.
    pub(crate) fn provider(
        &self,
        spec: &ConnectorSpec,
        kills: &rdlt_host::Kills,
        admitted: &Limits,
    ) -> (Box<dyn rdlt_host::Provider>, ConnectorRef) {
        let reference = |from: Option<&ConnectorRef>| ConnectorRef {
            id: spec.id.clone(),
            version_req: None,
            path: from.and_then(|reference| reference.path.clone()),
            endpoint: from.and_then(|reference| reference.endpoint.clone()),
            digest: from.and_then(|reference| reference.digest).or(self
                .holding
                .digest
                .get()
                .copied()),
            isolation: from.and_then(|reference| reference.isolation),
            access: from
                .map(|reference| reference.access.clone())
                .unwrap_or_default(),
        };
        // A version that is no semantic version is no requirement a reference can hold: such a
        // connector's starts are held to its id alone.
        let reference = |from: Option<&ConnectorRef>| {
            reference(from)
                .exactly(&spec.version)
                .unwrap_or_else(|_| reference(from))
        };
        let options = self.killing(admitted);
        match &self.reach {
            Reach::Connected(connect) => {
                let connect = Arc::clone(connect);
                let provider = rdlt_host::Connect::new(move || connect())
                    .options(options)
                    .kills(kills)
                    .redactions(self.redactions.clone());
                (Box::new(provider), reference(None))
            }
            Reach::Spawned {
                local,
                reference: spawned,
            } => {
                let provider = local.clone().options(options).kills(kills);
                let provider = provider.redactions(self.redactions.clone());
                (Box::new(provider), reference(Some(spawned)))
            }
            Reach::Listening {
                remote,
                reference: listening,
            } => {
                let provider = remote.clone().options(options).kills(kills);
                let provider = provider.redactions(self.redactions.clone());
                (Box::new(provider), reference(Some(listening)))
            }
        }
    }
}
