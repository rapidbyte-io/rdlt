//! Serving a connector over the wire protocol, for a host that runs it out of process.
//!
//! One connection is one session of the protocol: it opens with a handshake that names the role
//! and carries the configuration, and every later call works on the connector that handshake
//! connected. A binary serves every role it has a factory for.

mod args;
mod binary;
mod classes;
mod handshake;
mod listen;
mod noted;
mod probes;
#[cfg(feature = "certify")]
mod published;
mod read;
mod sending;
mod service;
mod sessions;
mod until;
mod write;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_wire::Limits;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::ServiceExt as _;

pub use binary::serve;
pub use listen::{Listener, Listening, Log, serve_listener};

use crate::destination::DestinationFactory;
use crate::source::SourceFactory;

/// The roles a connector binary serves, each by its factory.
#[derive(Default)]
pub struct Served {
    source: Option<Box<dyn SourceFactory>>,
    destination: Option<Box<dyn DestinationFactory>>,
    /// What each host was sent or read from, on whichever connection.
    sent: crate::source::Sent,
}

impl std::fmt::Debug for Served {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Served")
            .field(
                "source",
                &self.source.as_ref().map(|factory| &factory.spec().id),
            )
            .field(
                "destination",
                &self.destination.as_ref().map(|factory| &factory.spec().id),
            )
            .finish_non_exhaustive()
    }
}

impl Served {
    /// Nothing served yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Serves the source role with `factory`.
    #[must_use]
    pub fn with_source(mut self, factory: Box<dyn SourceFactory>) -> Self {
        self.source = Some(factory);
        self
    }

    /// Serves the destination role with `factory`.
    #[must_use]
    pub fn with_destination(mut self, factory: Box<dyn DestinationFactory>) -> Self {
        self.destination = Some(factory);
        self
    }
}

/// Whom a connection serves: the host, where a listening connector accepted it by name, how
/// many destination sessions it may hold open at once, how long its writes may make no
/// progress, and how long a stop drains it.
struct Hosted {
    name: Option<Arc<str>>,
    sessions: usize,
    send: Duration,
    drain: Duration,
}

impl Hosted {
    /// The host that spawned the connector, or serves it in its own process: it has no name.
    fn spawning() -> Self {
        Self::named(None, &crate::limits::ListenLimits::default())
    }

    /// The host `name`, served within `limits`.
    fn named(name: Option<Arc<str>>, limits: &crate::limits::ListenLimits) -> Self {
        Self {
            name,
            sessions: limits.connection_sessions(),
            send: limits.send,
            drain: limits.drain,
        }
    }
}

/// Serving a connection failed in its transport.
#[derive(Debug, thiserror::Error)]
#[error("serving the connection failed")]
pub struct ServeError(#[source] hyper::Error);

/// How often a served connection pings its host over HTTP/2, as the host's heartbeat pings it.
const KEEP_ALIVE: Duration = Duration::from_secs(5);

/// How long a ping may go unanswered before the host counts as gone.
const KEEP_ALIVE_PATIENCE: Duration = Duration::from_secs(30);

/// The calls one connection may hold open at once: a host's reads, writes, heartbeat and
/// control calls.
const MAX_CALLS: u32 = 200;

/// Serves the protocol on `io`, enforcing `limits` on what it receives, until the host closes the
/// connection, which ends it cleanly.
///
/// # Errors
///
/// A [`ServeError`] when the connection fails in its transport.
pub async fn serve_connection<IO>(
    served: Arc<Served>,
    io: IO,
    limits: Limits,
) -> Result<(), ServeError>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let host = Hosted::spawning();
    serve_until(served, io, limits, host, std::future::pending()).await
}

/// Serves as [`serve_connection`] does, the host named `host` where a listening connector
/// accepted it by name, and once `stop` ends, stops taking new calls and ends when the calls in
/// flight have.
async fn serve_until<IO>(
    served: Arc<Served>,
    io: IO,
    limits: Limits,
    host: Hosted,
    stop: impl Future<Output = ()>,
) -> Result<(), ServeError>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let stopping = tokio_util::sync::CancellationToken::new();
    let service = service::Service::new(served, limits, host.name, host.sessions, stopping.clone());
    let service = classes::Classed::new(service, &limits);
    let service = service.map_request(|request: http::Request<hyper::body::Incoming>| {
        request.map(rdlt_wire::tonic::body::Body::new)
    });
    let builder = {
        let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        builder
            .timer(TokioTimer::new())
            .initial_connection_window_size(rdlt_wire::limits::CONNECTION_WINDOW)
            .max_concurrent_streams(MAX_CALLS)
            // Pings notice a host the network dropped silently, which would hold its session.
            .keep_alive_interval(Some(KEEP_ALIVE))
            .keep_alive_timeout(KEEP_ALIVE_PATIENCE);
        builder
    };
    let io = sending::Sending::within(io, host.send);
    let connection = builder.serve_connection(TokioIo::new(io), TowerToHyperService::new(service));
    tokio::pin!(connection, stop);
    let served = tokio::select! {
        biased;
        served = connection.as_mut() => served,
        () = stop => {
            // Ends the host's heartbeat stream, which would otherwise hold the connection open.
            stopping.cancel();
            connection.as_mut().graceful_shutdown();
            // Calls still in flight once the drain is over end with the connection.
            tokio::time::timeout(host.drain, connection)
                .await
                .unwrap_or(Ok(()))
        }
    };
    served.or_else(|error| {
        if gone(&error) {
            Ok(())
        } else {
            Err(ServeError(error))
        }
    })
}

/// Whether `error` says the host had already closed its end, which is how a connection ends: some
/// platforms, macOS among them, fail the shutdown of a socket whose peer has closed.
fn gone(error: &hyper::Error) -> bool {
    use std::io::ErrorKind;
    std::error::Error::source(error)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .is_some_and(|io| {
            matches!(
                io.kind(),
                ErrorKind::NotConnected | ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
            )
        })
}
