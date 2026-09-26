//! Remote placement: connectors listening on the network, reached over mutual TLS at a `grpcs`
//! endpoint, and redialed when they are lost.

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
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::provider::{ConnectorRef, Placed, Placement, Provider, ProviderError};
use crate::remote::Options;
use crate::supervise::{Spawned, Start, SupervisedDestination, SupervisedSource, Supervisor};

/// Places connectors a reference gives an endpoint for, over mutual TLS: the host presents
/// `identity`, and verifies each connector's certificate against the CA bundle `ca` and the
/// endpoint's host name.
pub struct Remote {
    identity: Identity,
    ca: PathBuf,
    options: Options,
    fallback: Option<Box<dyn Provider>>,
}

impl fmt::Debug for Remote {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Remote")
            .field("identity", &self.identity)
            .field("ca", &self.ca)
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
            options: Options::default(),
            fallback: None,
        }
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
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        if host.is_empty() {
            return Err(invalid("has no host"));
        }
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }
}

/// How to reach a connector: its endpoint, and the TLS to speak there.
pub(crate) struct Dial {
    pub(crate) endpoint: Endpoint,
    pub(crate) tls: Arc<ClientConfig>,
}

/// Connects to `dial`'s endpoint, completes the TLS handshake and waits for the connector to
/// accept it, all within `deadline`.
pub(crate) async fn dial(dial: &Dial, deadline: Duration) -> Result<TlsStream<TcpStream>, Spawned> {
    let timed_out = || {
        let Endpoint { host, port } = &dial.endpoint;
        Spawned::Unreachable(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("{host}:{port} did not answer within {deadline:?}"),
        ))
    };
    let dialed = async {
        let mut stream = connect(dial).await?;
        accepted(&mut stream).await?;
        Ok(stream)
    };
    tokio::time::timeout(deadline, dialed)
        .await
        .map_err(|_| timed_out())?
}

/// Waits until the connector has accepted the host's certificate.
///
/// In TLS 1.3 the host's handshake completes before the connector has checked the host's
/// certificate: a refusal arrives as an alert afterwards, and the host's first write would only
/// find the connection closed. An HTTP/2 server speaks first, so the connector's first bytes, or
/// its alert, are its verdict; bytes read here stay buffered for the protocol.
async fn accepted(stream: &mut TlsStream<TcpStream>) -> Result<(), Spawned> {
    loop {
        let (tcp, tls) = stream.get_mut();
        let state = tls.process_new_packets().map_err(refused)?;
        if state.plaintext_bytes_to_read() > 0 {
            return Ok(());
        }
        tcp.readable().await.map_err(Spawned::Unreachable)?;
        match tls.read_tls(&mut Ready(tcp)) {
            // Closed with no alert: no certificate was refused, and the connector may listen
            // again.
            Ok(0) => {
                let closed = std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "the connector closed the connection after its handshake",
                );
                return Err(Spawned::Unreachable(closed));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(Spawned::Unreachable(error)),
        }
    }
}

/// The TLS error `error`, as a refusal to start.
fn refused(error: rustls::Error) -> Spawned {
    Spawned::Tls(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

/// A TCP stream read as far as it is ready, without waiting.
struct Ready<'a>(&'a TcpStream);

impl std::io::Read for Ready<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.0.try_read(buffer)
    }
}

/// Connects to `dial`'s endpoint and completes the TLS handshake.
async fn connect(dial: &Dial) -> Result<TlsStream<TcpStream>, Spawned> {
    let Endpoint { host, port } = &dial.endpoint;
    let name = ServerName::try_from(host.clone()).map_err(|error| {
        Spawned::Unreachable(std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    })?;
    let stream = TcpStream::connect((host.as_str(), *port))
        .await
        .map_err(Spawned::Unreachable)?;
    // Frames are small and answered at once: batching them for the network only adds latency.
    stream.set_nodelay(true).map_err(Spawned::Unreachable)?;
    TlsConnector::from(Arc::clone(&dial.tls))
        .connect(name, stream)
        .await
        .map_err(Spawned::Tls)
}
