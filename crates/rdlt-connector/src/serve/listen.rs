//! A connector listening for hosts over the network: each connection is a TLS 1.3 handshake that
//! requires the certificate of a host named to the connector, then one session of the protocol.
//!
//! Connections are accepted as they come and never wait for one another. Those that have not
//! authenticated are few, and the newest takes the place of another; those of accepted hosts are
//! bounded for each host, and wait for a session in a bounded queue.

mod admission;
mod descriptors;
mod refusals;
mod speaking;

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io::Write as _;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rdlt_wire::Limits;
use rdlt_wire::tls::Hosts;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::server::TlsStream;
use tokio_util::sync::CancellationToken;

pub use refusals::Log;

use super::args::{Failure, Listen};
use super::{Hosted, Served, serve_until};
use crate::limits::ListenLimits;
use admission::{Admitted, Origin, Unauthenticated};
use refusals::{Refusals, Refused};
use speaking::Speaking;

/// How long accepting pauses after it fails.
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

/// How a connector listens: the TLS it serves with, the hosts it accepts, how many connections
/// it holds, and where it reports.
#[derive(Clone, Debug)]
pub struct Listening {
    /// The configuration [`rdlt_wire::tls::server_config`] built.
    pub tls: Arc<ServerConfig>,
    /// The hosts that configuration accepts, which name each session's host.
    pub hosts: Hosts,
    /// How many connections it holds, and for how long.
    pub limits: ListenLimits,
    /// Where it reports the host of each session, and the connections it refused.
    pub log: Log,
}

impl Listening {
    /// Listens with `tls` for `hosts`, within the default limits, each host holding its share
    /// of the sessions, reporting on standard error.
    pub fn new(tls: Arc<ServerConfig>, hosts: Hosts) -> Self {
        let limits = ListenLimits::default();
        // The default sessions outnumber any hosts a command line names.
        let limits = limits.shared(hosts.count(), None, None).unwrap_or(limits);
        Self {
            tls,
            hosts,
            limits,
            log: Log::stderr(),
        }
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
    let config = rdlt_wire::tls::server_config(&listen.identity, &listen.accepted)
        .map_err(|error| failure("the TLS configuration", &error))?;
    let shares = ListenLimits::default()
        .shared(
            listen.accepted.hosts.count(),
            listen.sessions.map(NonZeroUsize::get),
            listen.host_sessions.map(NonZeroUsize::get),
        )
        .map_err(|error| failure("listening", &error))?;
    let listening = Listening {
        limits: shares,
        ..Listening::new(Arc::new(config), listen.accepted.hosts.clone())
    };
    descriptors::reserve(&listening.limits).map_err(|error| failure("listening", &error))?;
    let listener = TcpListener::bind(listen.address)
        .await
        .map_err(|error| failure(&format!("listening on {}", listen.address), &error))?;
    let address = listener
        .local_addr()
        .map_err(|error| failure("the listening address", &error))?;
    announce(address)?;
    serve_listener(served, listener, listening, limits, stop).await;
    Ok(())
}

/// Serves each host connecting through `listener` one session of the protocol, over mutual TLS
/// as `listening` says, until `stop` ends; then drops `listener`, stops taking connections, and
/// ends when those in flight have.
///
/// Every connection is accepted at once. One that has not completed its TLS handshake is among
/// [`ListenLimits::unauthenticated`] at most, and is closed for a newer one once they are that
/// many, or when [`ListenLimits::handshake`] passes. A host's connection beyond
/// [`ListenLimits::host_sessions`] is closed; beyond [`ListenLimits::sessions`] it waits, among
/// [`ListenLimits::waiting`] at most and for [`ListenLimits::wait`] at most. Each session's host
/// is reported once, and refused connections once every [`ListenLimits::report_every`].
pub async fn serve_listener<L: Listener>(
    served: Arc<Served>,
    mut listener: L,
    listening: Listening,
    limits: Limits,
    stop: impl Future<Output = ()>,
) {
    let mut doors = Doors::new(served, listening, limits);
    let every = doors.shared.limits.report_every;
    let mut reports = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    reports.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // After an accept failed, accepting waits out a pause, and nothing else does.
    let pause = tokio::time::sleep(Duration::ZERO);
    let mut paused = false;
    tokio::pin!(stop, pause);
    loop {
        tokio::select! {
            biased;
            () = &mut stop => break,
            // Connections that have ended come before new ones, so a flood of new connections
            // delays neither a host that has authenticated nor the count of those that left.
            Some(ended) = doors.sessions.join_next_with_id() => doors.left(ended),
            handshaken = doors.unauthenticated.next() => doors.authenticated(handshaken),
            _ = reports.tick() => doors.report(),
            () = &mut pause, if paused => paused = false,
            accepted = listener.accept(), if !paused => {
                if let Ok((stream, peer)) = accepted {
                    doors.accepted(stream, peer);
                } else {
                    doors.refusals.count(Refused::Accept);
                    pause.as_mut().reset(tokio::time::Instant::now() + ACCEPT_BACKOFF);
                    paused = true;
                }
            }
        }
    }
    // Frees the address for whatever listens next, while the sessions in flight drain.
    drop(listener);
    drop(std::mem::replace(
        &mut doors.unauthenticated,
        Unauthenticated::new(1, 0),
    ));
    doors.shared.stopping.cancel();
    while let Some(ended) = doors.sessions.join_next_with_id().await {
        doors.left(ended);
    }
    doors.report();
}

/// A connection's TLS handshake, within its deadline: the stream, and its peer's address.
type Handshake<S> = dyn Future<Output = Result<(TlsStream<S>, SocketAddr), Refused>> + Send;

/// A listening connector's connections, at each stage.
struct Doors<S> {
    shared: Arc<Shared>,
    unauthenticated: Unauthenticated<Handshake<S>>,
    /// The connections of accepted hosts, waiting or served; each ends with why it was refused,
    /// where it was.
    sessions: JoinSet<Option<Refused>>,
    /// The host of each of those connections, by its task: however a task ends, its host is
    /// known.
    serving: HashMap<tokio::task::Id, Arc<str>>,
    /// How many connections each host holds.
    hosts: BTreeMap<Arc<str>, usize>,
    refusals: Refusals,
}

/// What every connection of a listening connector shares.
struct Shared {
    acceptor: TlsAcceptor,
    hosts: Hosts,
    served: Arc<Served>,
    wire: Limits,
    limits: ListenLimits,
    log: Log,
    /// Cancelled once the connector is stopping.
    stopping: CancellationToken,
    sessions: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
}

impl<S> Doors<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    fn new(served: Arc<Served>, listening: Listening, wire: Limits) -> Self {
        let limits = listening.limits;
        Self {
            unauthenticated: Unauthenticated::new(limits.unauthenticated, admission::seed()),
            sessions: JoinSet::new(),
            serving: HashMap::new(),
            hosts: BTreeMap::new(),
            refusals: Refusals::default(),
            shared: Arc::new(Shared {
                acceptor: TlsAcceptor::from(listening.tls),
                hosts: listening.hosts,
                served,
                wire,
                limits,
                log: listening.log,
                stopping: CancellationToken::new(),
                sessions: Arc::new(Semaphore::new(limits.sessions)),
                waiting: Arc::new(Semaphore::new(limits.waiting)),
            }),
        }
    }

    /// Begins the TLS handshake of the connection just accepted, among the unauthenticated.
    fn accepted(&mut self, stream: S, peer: SocketAddr) {
        let shared = Arc::clone(&self.shared);
        let handshake: Pin<Box<Handshake<S>>> = Box::pin(async move {
            let accepting = shared.acceptor.accept(stream);
            match tokio::time::timeout(shared.limits.handshake, accepting).await {
                Ok(Ok(tls)) => Ok((tls, peer)),
                Ok(Err(error)) => Err(Refused::handshake(&error)),
                Err(_) => Err(Refused::Slow),
            }
        });
        let admitted = self.unauthenticated.admit(Origin::of(peer), handshake);
        if admitted == Admitted::InPlaceOfAnother {
            self.refusals.count(Refused::Displaced);
        }
    }

    /// Gives the connection whose handshake ended as `handshaken` a session, or a place among
    /// those waiting for one, where its host and the connector have one free.
    fn authenticated(&mut self, handshaken: Result<(TlsStream<S>, SocketAddr), Refused>) {
        let (tls, peer) = match handshaken {
            Ok(handshaken) => handshaken,
            Err(why) => return self.refusals.count(why),
        };
        // A handshake that asked for no protocol agreed on none: only HTTP/2 is spoken here.
        if tls.get_ref().1.alpn_protocol() != Some(rdlt_wire::tls::ALPN) {
            return self.refusals.count(Refused::Handshake);
        }
        let chain = tls.get_ref().1.peer_certificates().unwrap_or_default();
        let named = chain.first().and_then(|leaf| self.shared.hosts.named(leaf));
        let Some(host) = named.map(Arc::<str>::from) else {
            return self.refusals.count(Refused::Handshake);
        };
        let held = self.hosts.get(&host).copied().unwrap_or(0);
        if held >= self.shared.limits.host_sessions {
            return self.refusals.count(Refused::HostFull);
        }
        let slot = match Arc::clone(&self.shared.sessions).try_acquire_owned() {
            Ok(session) => Slot::Free(session),
            Err(_) => match Arc::clone(&self.shared.waiting).try_acquire_owned() {
                Ok(place) => Slot::Waiting(place),
                Err(_) => return self.refusals.count(Refused::QueueFull),
            },
        };
        self.hosts.insert(Arc::clone(&host), held + 1);
        let (shared, served) = (Arc::clone(&self.shared), Arc::clone(&host));
        let task = self
            .sessions
            .spawn(async move { session(tls, peer, &served, slot, &shared).await });
        self.serving.insert(task.id(), host);
    }

    /// Counts out the connection whose task ended as `ended` says: its host holds one fewer,
    /// whether the task returned, panicked or was aborted.
    fn left(&mut self, ended: Result<(tokio::task::Id, Option<Refused>), tokio::task::JoinError>) {
        let (task, refused) = match ended {
            Ok((task, refused)) => (task, refused),
            Err(failed) => (failed.id(), Some(Refused::Transport)),
        };
        // An entry stays once its host holds none: the hosts are those named to the connector.
        if let Some(host) = self.serving.remove(&task)
            && let Some(held) = self.hosts.get_mut(&host)
        {
            *held = held.saturating_sub(1);
        }
        if let Some(why) = refused {
            self.refusals.count(why);
        }
    }

    /// Reports the connections refused since the last report, in one line, where any was.
    fn report(&mut self) {
        if let Some(line) = self.refusals.report(self.shared.limits.report_every) {
            self.shared.log.line(&line);
        }
    }
}

/// What a host's connection holds as it is admitted.
enum Slot {
    /// A session.
    Free(OwnedSemaphorePermit),
    /// A place among the connections waiting for a session.
    Waiting(OwnedSemaphorePermit),
}

/// Serves `host`'s connection its session, once it has one, until it closes or the connector is
/// stopping; answers why it was refused, where it was.
async fn session<S>(
    tls: TlsStream<S>,
    peer: SocketAddr,
    host: &Arc<str>,
    slot: Slot,
    shared: &Shared,
) -> Option<Refused>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let _session = match slot {
        Slot::Free(session) => session,
        Slot::Waiting(_place) => {
            let freed = Arc::clone(&shared.sessions).acquire_owned();
            tokio::select! {
                biased;
                () = shared.stopping.cancelled() => return None,
                freed = tokio::time::timeout(shared.limits.wait, freed) => match freed {
                    // The semaphore is never closed.
                    Ok(Ok(session)) => session,
                    Ok(Err(_)) | Err(_) => return Some(Refused::Waited),
                },
            }
        }
    };
    shared
        .log
        .line(&format!("serving host `{host}` from {peer}"));
    // A host has as long to send HTTP/2's preface as it had to complete its TLS handshake.
    let tls = Speaking::within(tls, shared.limits.handshake);
    let (served, stopping) = (Arc::clone(&shared.served), shared.stopping.clone());
    let host = Hosted {
        name: Some(Arc::clone(host)),
        sessions: shared.limits.connection_sessions(),
    };
    serve_until(served, tls, shared.wire, host, stopping.cancelled_owned())
        .await
        .err()
        .map(|_| Refused::Transport)
}

/// Says on standard output where the connector listens, so whatever started it, with port 0
/// perhaps, can connect.
fn announce(address: SocketAddr) -> Result<(), Failure> {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "listening on {address}")
        .and_then(|()| stdout.flush())
        .map_err(|error| failure("announcing the address", &error))
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
