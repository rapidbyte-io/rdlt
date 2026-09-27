//! A connector listening for hosts over the network: each connection is a TLS 1.3 handshake that
//! requires the host's certificate, then one session of the protocol.

use std::future::Future;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rdlt_wire::Limits;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_util::sync::CancellationToken;

use super::args::{Failure, Listen};
use super::{Served, serve_until};

/// How long a host has to complete its TLS handshake: a connection that has not is dropped.
const HANDSHAKE: Duration = Duration::from_secs(10);

/// Handshakes in flight at once: accepting waits until one ends.
///
/// Peers that never complete theirs hold these, and never a session.
const HANDSHAKES: usize = 1024;

/// Sessions served at once: a further host, once it has handshaken, waits until one ends.
const SESSIONS: usize = 256;

/// How long accepting pauses after it fails, as when the process is out of file descriptors.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Where a listening connector takes its hosts' connections from: a TCP listener, or any other
/// network's.
pub trait Listener: Send + 'static {
    /// A connection.
    type Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// The next connection, and its peer's address.
    fn accept(
        &mut self,
    ) -> impl Future<Output = std::io::Result<(Self::Stream, SocketAddr)>> + Send;
}

impl Listener for TcpListener {
    type Stream = TcpStream;

    async fn accept(&mut self) -> std::io::Result<(TcpStream, SocketAddr)> {
        let (stream, peer) = TcpListener::accept(self).await?;
        // Frames are small and answered at once: batching them for the network only adds latency.
        stream.set_nodelay(true).ok();
        Ok((stream, peer))
    }
}

/// Serves hosts connecting to `listen`'s address until `stop` ends, then stops taking new
/// connections and ends when those in flight have.
pub(super) async fn listen(
    served: Arc<Served>,
    listen: &Listen,
    limits: Limits,
    stop: impl Future<Output = ()>,
) -> Result<(), Failure> {
    let config = rdlt_wire::tls::server_config(&listen.identity, &listen.client_ca)
        .map_err(|error| failure("the TLS configuration", &error))?;
    let listener = TcpListener::bind(listen.address)
        .await
        .map_err(|error| failure(&format!("listening on {}", listen.address), &error))?;
    let address = listener
        .local_addr()
        .map_err(|error| failure("the listening address", &error))?;
    announce(address)?;
    serve_listener(served, listener, Arc::new(config), limits, stop).await;
    Ok(())
}

/// Serves each host connecting through `listener` one session of the protocol, over mutual TLS
/// with `tls`, until `stop` ends; then drops `listener`, stops taking connections, and ends when
/// those in flight have.
///
/// A host has 10 s to complete its TLS handshake, and at most 1024 handshakes run at once. At
/// most 256 sessions are served at once: a further host waits once it has handshaken. What
/// happens to each connection is reported on standard error.
pub async fn serve_listener<L: Listener>(
    served: Arc<Served>,
    mut listener: L,
    tls: Arc<ServerConfig>,
    limits: Limits,
    stop: impl Future<Output = ()>,
) {
    let listening = Arc::new(Listening {
        acceptor: TlsAcceptor::from(tls),
        served,
        limits,
        stopping: CancellationToken::new(),
        sessions: Arc::new(Semaphore::new(SESSIONS)),
    });
    let handshakes = Arc::new(Semaphore::new(HANDSHAKES));
    let mut connections = JoinSet::new();
    tokio::pin!(stop);
    loop {
        let handshake = tokio::select! {
            biased;
            () = &mut stop => break,
            // The semaphore is never closed.
            permit = Arc::clone(&handshakes).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let (stream, peer) = tokio::select! {
            biased;
            () = &mut stop => break,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    report(&format!("accepting a connection failed: {error}"));
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            },
        };
        connections.spawn(connection(stream, peer, handshake, Arc::clone(&listening)));
        while connections.try_join_next().is_some() {}
    }
    // Frees the address for whatever listens next, while the connections in flight drain.
    drop(listener);
    listening.stopping.cancel();
    while connections.join_next().await.is_some() {}
}

/// What every connection of a listening connector shares.
struct Listening {
    acceptor: TlsAcceptor,
    served: Arc<Served>,
    limits: Limits,
    /// Cancelled once the connector is stopping.
    stopping: CancellationToken,
    sessions: Arc<Semaphore>,
}

/// Serves one host's connection: its handshake, holding `handshake`, then its session, once one
/// is free, until it closes or the connector is stopping.
async fn connection<S>(
    stream: S,
    peer: SocketAddr,
    handshake: OwnedSemaphorePermit,
    listening: Arc<Listening>,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let accepted = tokio::time::timeout(HANDSHAKE, listening.acceptor.accept(stream)).await;
    drop(handshake);
    let tls = match accepted {
        Ok(Ok(tls)) => tls,
        Ok(Err(error)) => return report(&format!("refused a connection from {peer}: {error}")),
        Err(_) => return report(&format!("{peer} did not complete its handshake in time")),
    };
    let _session = tokio::select! {
        biased;
        () = listening.stopping.cancelled() => return,
        permit = Arc::clone(&listening.sessions).acquire_owned() => permit.expect("the semaphore is never closed"),
    };
    let (served, stopping) = (Arc::clone(&listening.served), listening.stopping.clone());
    if let Err(error) = serve_until(served, tls, listening.limits, stopping.cancelled_owned()).await
    {
        report(&format!("serving {peer} failed: {error}"));
    }
}

/// Says on standard output where the connector listens, so whatever started it, with port 0
/// perhaps, can connect.
fn announce(address: SocketAddr) -> Result<(), Failure> {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "listening on {address}")
        .and_then(|()| stdout.flush())
        .map_err(|error| failure("announcing the address", &error))
}

/// Reports what happened to a connection on standard error, which whoever runs the connector
/// keeps.
fn report(message: &str) {
    let mut stderr = std::io::stderr().lock();
    writeln!(stderr, "{message}").ok();
}

/// `what` failed with `error`, with every cause of `error`.
fn failure(what: &str, error: &dyn std::error::Error) -> Failure {
    use std::fmt::Write as _;
    let mut message = format!("{what} failed: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        write!(message, ": {cause}").ok();
        source = cause.source();
    }
    message.into()
}
