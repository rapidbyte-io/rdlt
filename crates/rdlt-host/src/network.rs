//! Remote placement: connectors listening on the network, reached over mutual TLS at a `grpcs`
//! endpoint, and redialed when they are lost.

mod endpoint;
mod rewound;

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

pub use endpoint::{Endpoint, EndpointError};
pub(crate) use rewound::Rewound;

use crate::kills::{Kills, Severing};
use crate::provider::{
    ConnectorRef, Honours, Isolation, Placed, Placement, Provider, ProviderError,
};
use crate::remote::Options;
use crate::secrets::{Config, SecretResolver, Secrets};
use crate::supervise::{
    Configured, Gate, Spawned, Start, SupervisedDestination, SupervisedSource, Supervisor,
};
use crate::wire::Wire;

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
#[derive(Clone)]
pub struct Remote {
    identity: Identity,
    ca: PathBuf,
    network: Arc<dyn Network>,
    options: Options,
    fallback: Option<Arc<dyn Provider>>,
    secrets: Arc<dyn SecretResolver>,
}

/// What remote placement honours of a reference: its endpoint, whose host is the name the
/// connector's certificate must carry, and the isolation of another machine.
const HONOURS: Honours = Honours {
    placement: "remote",
    path: false,
    endpoint: true,
    digest: false,
    isolation: &[Isolation::Remote],
};

impl fmt::Debug for Remote {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Remote")
            .field("identity", &self.identity)
            .field("ca", &self.ca)
            .field("network", &self.network)
            .field("options", &self.options)
            .field("fallback", &self.fallback.is_some())
            .field("secrets", &self.secrets)
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
            secrets: Arc::new(Secrets::new()),
        }
    }

    /// Resolves the secret references of each configuration with `secrets`.
    #[must_use]
    pub fn secrets(mut self, secrets: impl SecretResolver + 'static) -> Self {
        self.secrets = Arc::new(secrets);
        self
    }

    /// Reaches endpoints over `network`, not the operating system's TCP.
    #[must_use]
    pub fn network(mut self, network: impl Network) -> Self {
        self.network = Arc::new(network);
        self
    }

    /// Reaches each connector so that `kills` cuts every connection to it.
    #[must_use]
    pub fn kills(mut self, kills: &Kills) -> Self {
        self.network = Arc::new(Severing {
            network: self.network,
            kills: kills.clone(),
        });
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
        self.fallback = Some(Arc::new(provider));
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
        let dialing = self.dialing(reference, endpoint)?;
        let at = dialing.endpoint.to_string();
        let gate = Gate {
            reference,
            found_at: &at,
        };
        let configured = Configured {
            config: Config::from(config),
            secrets: Arc::clone(&self.secrets),
        };
        Supervisor::start(Start::Dial(dialing), role, configured, self.options, &gate)
            .await
            .map_err(|spawned| refused(reference, &at, spawned))
    }

    /// How to reach `endpoint`, for the connector `reference` names.
    ///
    /// An endpoint that is refused is not repeated in the error: what is wrong with it may be a
    /// credential written into it.
    fn dialing(&self, reference: &ConnectorRef, endpoint: &str) -> Result<Dial, ProviderError> {
        HONOURS.admit(reference)?;
        let address = Endpoint::parse(endpoint).map_err(|source| ProviderError::Endpoint {
            id: reference.id.clone(),
            source,
        })?;
        let tls = rdlt_wire::tls::client_config(&self.identity, &self.ca).map_err(|error| {
            ProviderError::Tls {
                id: reference.id.clone(),
                endpoint: address.to_string(),
                source: Box::new(error),
            }
        })?;
        Ok(Dial {
            endpoint: address,
            network: Arc::clone(&self.network),
            tls: Arc::new(tls),
        })
    }

    /// A raw connection to the connector at `reference`'s endpoint, before its handshake: over
    /// mutual TLS, within the connect deadline, once the connector has accepted the host's
    /// certificate.
    ///
    /// # Errors
    ///
    /// [`ProviderError::NotFound`] when `reference` names no endpoint;
    /// [`ProviderError::Unreachable`] or [`ProviderError::Tls`] when the dial fails.
    pub async fn wire(&self, reference: &ConnectorRef) -> Result<Wire, ProviderError> {
        let Some(endpoint) = &reference.endpoint else {
            return Err(not_found(reference));
        };
        let dialing = self.dialing(reference, endpoint)?;
        let stream = dial(&dialing, self.options.deadlines.connect)
            .await
            .map_err(|spawned| refused(reference, &dialing.endpoint.to_string(), spawned))?;
        Ok(Wire::new(Box::new(stream), None))
    }
}

/// The provider's error for a dial of the endpoint at `endpoint`, its host and port, that failed
/// as `spawned` says.
fn refused(reference: &ConnectorRef, endpoint: &str, spawned: Spawned) -> ProviderError {
    match spawned {
        Spawned::Io(source) | Spawned::Unreachable(source) => ProviderError::Unreachable {
            id: reference.id.clone(),
            endpoint: endpoint.to_owned(),
            source,
        },
        Spawned::Tls(source) => ProviderError::Tls {
            id: reference.id.clone(),
            endpoint: endpoint.to_owned(),
            source: Box::new(source),
        },
        Spawned::Connect(source) => ProviderError::HandshakeFailed {
            id: reference.id.clone(),
            source: Box::new(source),
        },
        Spawned::Secret(source) => ProviderError::Secret {
            id: reference.id.clone(),
            source,
        },
        Spawned::Refused(refused) => refused,
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
            let spec = supervisor.spec();
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
            let spec = supervisor.spec();
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
        Spawned::Unreachable(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("{} did not answer within {deadline:?}", dial.endpoint),
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
    let (host, port) = (dial.endpoint.host(), dial.endpoint.port());
    let name = ServerName::try_from(host.to_owned()).map_err(|error| {
        Spawned::Unreachable(std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    })?;
    let stream = dial
        .network
        .connect(host, port)
        .await
        .map_err(Spawned::Unreachable)?;
    TlsConnector::from(Arc::clone(&dial.tls))
        .connect(name, stream)
        .await
        .map_err(Spawned::Tls)
}
