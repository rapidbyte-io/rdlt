//! What a certification reaches: a connector served in this process, spawned from its binary, or
//! listening at an endpoint.

use std::sync::Arc;

use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector::{BoxFuture, ConnectorError, ConnectorErrorKind, Role};
use rdlt_host::remote::{Client, client};
use rdlt_host::{Connection, ConnectorRef, Local, Options, Remote, Stream};
use rdlt_wire::Limits;

/// A connector to certify, and how to reach it.
#[derive(Debug)]
pub struct Target {
    reach: Reach,
    options: Options,
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
        }
    }

    /// Runs each connection with `options`: its deadlines, heartbeat and limits.
    #[must_use]
    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
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
        let unreachable = |error: &dyn std::fmt::Display| {
            ConnectorError::new(
                ConnectorErrorKind::Transient,
                format!("the connector could not be reached: {error}"),
            )
        };
        match &self.reach {
            Reach::Connected(connect) => connect().await.map_err(|error| unreachable(&error)),
            Reach::Spawned { local, reference } => local
                .wire(reference)
                .map(|wire| Box::new(wire) as Box<dyn Stream>)
                .map_err(|error| unreachable(&error)),
            Reach::Listening { remote, reference } => remote
                .wire(reference)
                .await
                .map(|wire| Box::new(wire) as Box<dyn Stream>)
                .map_err(|error| unreachable(&error)),
        }
    }

    /// A fresh connection to the connector, handshaken as `role` with `config`.
    pub(crate) async fn connect(
        &self,
        role: Role,
        config: &serde_json::Value,
    ) -> Result<Arc<Connection>, ConnectorError> {
        Connection::connect(self.wire().await?, role, config, self.options).await
    }

    /// A fresh client of the protocol, with no handshake yet.
    pub(crate) async fn client(&self) -> Result<Client, ConnectorError> {
        client(self.wire().await?, self.options).await
    }
}
