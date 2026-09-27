//! Remote placement: connectors listening on the network, reached over mutual TLS at a `grpcs`
//! endpoint, and redialed when they are lost.

mod rewound;
#[cfg(test)]
mod tests;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{BoxFuture, Destination, Role, Source};
use rdlt_wire::tls::Identity;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

pub(crate) use rewound::Rewound;

use crate::provider::{ConnectorRef, Placed, Placement, Provider, ProviderError};
use crate::remote::Options;
use crate::supervise::{Spawned, Start, SupervisedDestination, SupervisedSource, Supervisor};

/// A byte stream to a connector, over whatever network reached it.
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Stream for T {}

/// How the host reaches a connector's endpoint: the operating system's TCP, or another network.
pub trait Network: fmt::Debug + Send + Sync + 'static {
    /// A stream to `port` on `host`, a host name or IP address.
    ///
    /// # Errors
    ///
    /// The I/O error connecting failed with.
    fn connect<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> BoxFuture<'a, std::io::Result<Box<dyn Stream>>>;
}

/// The operating system's TCP.
#[derive(Clone, Copy, Debug, Default)]
pub struct Tcp;

impl Network for Tcp {
    fn connect<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> BoxFuture<'a, std::io::Result<Box<dyn Stream>>> {
        Box::pin(async move {
            let stream = TcpStream::connect((host, port)).await?;
            // Frames are small and answered at once: batching them only adds latency.
            stream.set_nodelay(true)?;
            Ok(Box::new(stream) as Box<dyn Stream>)
        })
    }
}

/// Places connectors a reference gives an endpoint for, over mutual TLS: the host presents
/// `identity`, and verifies each connector's certificate against the CA bundle `ca` and the
/// endpoint's host name.
pub struct Remote {
    identity: Identity,
    ca: PathBuf,
    network: Arc<dyn Network>,
    options: Options,
    fallback: Option<Box<dyn Provider>>,
}

impl fmt::Debug for Remote {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Remote")
            .field("identity", &self.identity)
            .field("ca", &self.ca)
            .field("network", &self.network)
            .field("options", &self.options)
            .field("fallback", &self.fallback.is_some())
            .finish()
    }
}

impl Remote {
    /// Places connectors at their endpoints, presenting `identity` and trusting the CA bundle at
    /// `ca`.
    pub fn new(identity: Identity, ca: impl Into<PathBuf>) -> Self {
        Self {
            identity,
            ca: ca.into(),
            network: Arc::new(Tcp),
            options: Options::default(),
            fallback: None,
        }
    }

    /// Reaches endpoints over `network`, not the operating system's TCP.
    #[must_use]
    pub fn network(mut self, network: impl Network) -> Self {
        self.network = Arc::new(network);
        self
    }

    /// Runs each connection with `options`.
    #[must_use]
    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// Places connectors whose reference has no endpoint with `provider`.
    #[must_use]
    pub fn fallback(mut self, provider: impl Provider + 'static) -> Self {
        self.fallback = Some(Box::new(provider));
        self
    }

    /// Dials the connector at `endpoint` as `role`, and checks it is the connector `reference` names.
    async fn start(
        &self,
        reference: &ConnectorRef,
        endpoint: &str,
        role: Role,
        config: &serde_json::Value,
    ) -> Result<Supervisor, ProviderError> {
        let unreachable = |source| ProviderError::Unreachable {
            id: reference.id.clone(),
            endpoint: endpoint.to_owned(),
            source,
        };
        let tls = |source: Box<dyn std::error::Error + Send + Sync>| ProviderError::Tls {
            id: reference.id.clone(),
            endpoint: endpoint.to_owned(),
            source,
        };
        let address = Endpoint::parse(endpoint).map_err(unreachable)?;
        let config_tls = rdlt_wire::tls::client_config(&self.identity, &self.ca)
            .map_err(|error| tls(Box::new(error)))?;
        let dial = Dial {
            endpoint: address,
            network: Arc::clone(&self.network),
            tls: Arc::new(config_tls),
        };
        let supervisor = Supervisor::start(Start::Dial(dial), role, config.clone(), self.options)
            .await
            .map_err(|spawned| match spawned {
                Spawned::Io(source) | Spawned::Unreachable(source) => unreachable(source),
                Spawned::Tls(source) => tls(Box::new(source)),
                Spawned::Connect(source) => ProviderError::HandshakeFailed {
                    id: reference.id.clone(),
                    source: Box::new(source),
                },
            })?;
        Ok(supervisor)
    }
}

impl Provider for Remote {
    fn source<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Source>>, ProviderError>> {
        Box::pin(async move {
            let Some(endpoint) = &reference.endpoint else {
                return match &self.fallback {
                    Some(fallback) => fallback.source(reference, config).await,
                    None => Err(not_found(reference)),
                };
            };
            let supervisor = self
                .start(reference, endpoint, Role::Source, config)
                .await?;
            let spec = supervisor.checked_spec(reference, endpoint).await?;
            Ok(Placed {
                connector: Box::new(SupervisedSource(Arc::new(supervisor))) as Box<dyn Source>,
                spec,
                placement: Placement::Remote {
                    endpoint: endpoint.clone(),
                },
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
            let Some(endpoint) = &reference.endpoint else {
                return match &self.fallback {
                    Some(fallback) => fallback.destination(reference, config).await,
                    None => Err(not_found(reference)),
                };
            };
            let supervisor = self
                .start(reference, endpoint, Role::Destination, config)
                .await?;
            let spec = supervisor.checked_spec(reference, endpoint).await?;
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
                placement: Placement::Remote {
                    endpoint: endpoint.clone(),
                },
                digest: None,
            })
        })
    }
}

fn not_found(reference: &ConnectorRef) -> ProviderError {
    ProviderError::NotFound {
        id: reference.id.clone(),
        source: None,
    }
}

/// Where a connector listens: a host name or IP address, and a port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Endpoint {
    pub(crate) host: String,
    pub(crate) port: u16,
}

impl Endpoint {
    /// The endpoint `grpcs://host:port`; anything else, a path or credentials included, is
    /// refused.
    pub(crate) fn parse(endpoint: &str) -> std::io::Result<Self> {
        let invalid = |why: &str| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("`{endpoint}` {why}: an endpoint is `grpcs://host:port`"),
            )
        };
        let rest = endpoint
            .strip_prefix("grpcs://")
            .ok_or_else(|| invalid("is not grpcs"))?;
        let authority = rest.strip_suffix('/').unwrap_or(rest);
        if authority.contains(['/', '@', '?', '#']) {
            return Err(invalid("has more than a host and a port"));
        }
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| invalid("has no port"))?;
        let port = port.parse().map_err(|_| invalid("has no valid port"))?;
        // An IPv6 address is in brackets, and nothing else is.
        let host = match host.strip_prefix('[') {
            Some(inner) => {
                let inner = inner
                    .strip_suffix(']')
                    .ok_or_else(|| invalid("has an unclosed bracket"))?;
                inner
                    .parse::<std::net::Ipv6Addr>()
                    .map_err(|_| invalid("has no IPv6 address in brackets"))?;
                inner
            }
            None if host.contains([':', ']']) => {
                return Err(invalid("has an IPv6 address outside brackets"));
            }
            None => host,
        };
        if host.is_empty() {
            return Err(invalid("has no host"));
        }
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }
}

/// How to reach a connector: its endpoint, the network it is on, and the TLS to speak there.
pub(crate) struct Dial {
    pub(crate) endpoint: Endpoint,
    pub(crate) network: Arc<dyn Network>,
    pub(crate) tls: Arc<ClientConfig>,
}

/// Connects to `dial`'s endpoint, completes the TLS handshake and waits for the connector to
/// accept it, all within `deadline`.
pub(crate) async fn dial(dial: &Dial, deadline: Duration) -> Result<Dialed, Spawned> {
    let timed_out = || {
        let Endpoint { host, port } = &dial.endpoint;
        Spawned::Unreachable(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("{host}:{port} did not answer within {deadline:?}"),
        ))
    };
    let dialed = async { accepted(connect(dial).await?).await };
    tokio::time::timeout(deadline, dialed)
        .await
        .map_err(|_| timed_out())?
}

/// A connection to a connector that accepted the host's certificate.
pub(crate) type Dialed = Rewound<TlsStream<Box<dyn Stream>>>;

/// Waits until the connector has accepted the host's certificate.
///
/// In TLS 1.3 the host's handshake completes before the connector has checked the host's
/// certificate: a refusal arrives as an alert afterwards, and the host's first write would only
/// find the connection closed. An HTTP/2 server speaks first, so the connector's first bytes, or
/// its alert, are its verdict; the bytes are read again by the protocol.
async fn accepted(mut stream: TlsStream<Box<dyn Stream>>) -> Result<Dialed, Spawned> {
    let mut first = vec![0; FIRST_BYTES];
    match stream.read(&mut first).await {
        // Closed with no alert: no certificate was refused, and the connector may listen again.
        Ok(0) => Err(Spawned::Unreachable(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "the connector closed the connection after its handshake",
        ))),
        Ok(read) => {
            first.truncate(read);
            Ok(Rewound::new(first, stream))
        }
        Err(error)
            if error
                .get_ref()
                .is_some_and(<dyn std::error::Error + Send + Sync>::is::<rustls::Error>) =>
        {
            Err(Spawned::Tls(error))
        }
        Err(error) => Err(Spawned::Unreachable(error)),
    }
}

/// The most of the connector's first bytes read before the protocol takes the connection.
const FIRST_BYTES: usize = 16_384;

/// Connects to `dial`'s endpoint over its network and completes the TLS handshake.
async fn connect(dial: &Dial) -> Result<TlsStream<Box<dyn Stream>>, Spawned> {
    let Endpoint { host, port } = &dial.endpoint;
    let name = ServerName::try_from(host.clone()).map_err(|error| {
        Spawned::Unreachable(std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    })?;
    let stream = dial
        .network
        .connect(host, *port)
        .await
        .map_err(Spawned::Unreachable)?;
    TlsConnector::from(Arc::clone(&dial.tls))
        .connect(name, stream)
        .await
        .map_err(Spawned::Tls)
}
