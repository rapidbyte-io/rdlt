//! A connector listening for hosts over the network: each connection is a TLS 1.3 handshake that
//! requires the host's certificate, then one session of the protocol.

use std::future::Future;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rdlt_wire::Limits;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use super::args::{Failure, Listen};
use super::{Served, serve_until};

/// How long a host has to complete its TLS handshake: a connection that has not is dropped.
const HANDSHAKE: Duration = Duration::from_secs(10);

/// Connections served at once: a further host waits until one ends.
const CONNECTIONS: usize = 256;

/// How long accepting pauses after it fails, as when the process is out of file descriptors.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

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
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind(listen.address)
        .await
        .map_err(|error| failure(&format!("listening on {}", listen.address), &error))?;
    let address = listener
        .local_addr()
        .map_err(|error| failure("the listening address", &error))?;
    announce(address)?;
    let stopping = CancellationToken::new();
    let room = Arc::new(Semaphore::new(CONNECTIONS));
    let mut connections = JoinSet::new();
    tokio::pin!(stop);
    loop {
        let permit = tokio::select! {
            biased;
            () = &mut stop => break,
            permit = Arc::clone(&room).acquire_owned() => permit.expect("the semaphore is never closed"),
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
        let served = Arc::clone(&served);
        let (acceptor, stopping) = (acceptor.clone(), stopping.clone());
        connections.spawn(connection(
            stream, peer, acceptor, served, limits, stopping, permit,
        ));
        while connections.try_join_next().is_some() {}
    }
    stopping.cancel();
    while connections.join_next().await.is_some() {}
    Ok(())
}

/// Serves one host's connection: its handshake, then its session until it closes or `stopping`
/// is cancelled.
async fn connection(
    stream: TcpStream,
    peer: SocketAddr,
    acceptor: TlsAcceptor,
    served: Arc<Served>,
    limits: Limits,
    stopping: CancellationToken,
    _permit: OwnedSemaphorePermit,
) {
    // Frames are small and answered at once: batching them for the network only adds latency.
    stream.set_nodelay(true).ok();
    let tls = match tokio::time::timeout(HANDSHAKE, acceptor.accept(stream)).await {
        Ok(Ok(tls)) => tls,
        Ok(Err(error)) => return report(&format!("refused a connection from {peer}: {error}")),
        Err(_) => return report(&format!("{peer} did not complete its handshake in time")),
    };
    if let Err(error) = serve_until(served, tls, limits, stopping.cancelled_owned()).await {
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
