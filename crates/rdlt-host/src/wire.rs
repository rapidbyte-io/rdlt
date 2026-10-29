//! Raw connections to connectors, before any handshake, for clients that speak the protocol
//! themselves, as a certification suite does.

use std::sync::Arc;

use rdlt_connector::{BoxFuture, ConnectorError, Role};

use crate::local::process::{Process, Witness};
use crate::network::Stream;
use crate::opener::{Opener, Opening};
use crate::remote::{Client, Connection, Options, client, client_opened};

/// A raw connection to a connector, before its handshake: a stream whose other end serves the
/// protocol, its data calls `Data` calls of it, or an opener passing the connector a stream for
/// its control connection and for each data call; and the connector's process when this process
/// spawned it, which stops once what the wire becomes is dropped.
pub struct Wire {
    reach: Reach,
    witness: Option<Witness>,
}

/// How a wire reaches its connector.
enum Reach {
    Stream(Box<dyn Stream>),
    Opener(Arc<dyn Opener>),
}

impl std::fmt::Debug for Wire {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Wire")
            .field("spawned", &self.witness.is_some())
            .finish_non_exhaustive()
    }
}

/// An opener whose connector's process lives as long as it does.
struct Spawned {
    opener: Arc<dyn Opener>,
    _process: Process,
}

impl Opener for Spawned {
    fn open(&self, opening: Opening) -> BoxFuture<'_, std::io::Result<Box<dyn Stream>>> {
        self.opener.open(opening)
    }
}

impl Wire {
    /// The wire `stream` is, to a connector this process did not spawn.
    pub(crate) fn stream(stream: Box<dyn Stream>) -> Self {
        Self {
            reach: Reach::Stream(stream),
            witness: None,
        }
    }

    /// The wire whose streams `opener` passes to the connector `process` runs.
    pub(crate) fn spawned(opener: Arc<dyn Opener>, process: Process) -> Self {
        let witness = Some(process.witness());
        let spawned = Spawned {
            opener,
            _process: process,
        };
        Self {
            reach: Reach::Opener(Arc::new(spawned)),
            witness,
        }
    }

    /// The wire whose streams `opener` passes to its connector.
    pub fn opened(opener: Box<dyn Opener>) -> Self {
        Self {
            reach: Reach::Opener(Arc::from(opener)),
            witness: None,
        }
    }

    /// A witness to how the connector ends, when this process spawned it: what it last said, for
    /// the errors of a transport that failed.
    pub fn witness(&self) -> Option<Witness> {
        self.witness.clone()
    }

    /// A client of the protocol over the wire, with no handshake yet.
    ///
    /// # Errors
    ///
    /// As [`client`] and [`client_opened`] fail.
    pub async fn client(self, options: Options) -> Result<Client, ConnectorError> {
        match self.reach {
            Reach::Stream(stream) => client(stream, options).await,
            Reach::Opener(opener) => client_opened(opener, options).await,
        }
    }

    /// Connects over the wire to its connector, as `role`, with `config`.
    ///
    /// # Errors
    ///
    /// As [`Connection::connect`] and [`Connection::connect_opened`] fail.
    pub async fn connect(
        self,
        role: Role,
        config: &serde_json::Value,
        options: Options,
    ) -> Result<Arc<Connection>, ConnectorError> {
        match self.reach {
            Reach::Stream(stream) => Connection::connect(stream, role, config, options).await,
            Reach::Opener(opener) => {
                Connection::connect_opened(opener, role, config, options).await
            }
        }
    }
}
