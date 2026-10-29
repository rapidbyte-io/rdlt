//! Serving a connector over the wire protocol, for a host that runs it out of process.
//!
//! One connection is one session of the protocol: it opens with a handshake that names the role
//! and carries the configuration, and every later call works on the connector that handshake
//! connected. Its control calls ride HTTP/2; each data call is a stream of its own, passed over
//! an opener beside the control connection, or one `Data` call of it. A binary serves every role
//! it has a factory for.

mod args;
mod binary;
mod classes;
mod handshake;
mod listen;
#[cfg(test)]
mod live;
mod noted;
mod probes;
#[cfg(feature = "certify")]
mod published;
mod read;
mod sending;
mod service;
mod sessions;
mod stream;
#[cfg(test)]
mod tests;
mod until;
mod write;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rdlt_wire::Limits;
use rdlt_wire::opener::Accepting;
use rdlt_wire::transport::Transport;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinSet;
use tower::ServiceExt as _;

pub use binary::serve;
pub use listen::{Listener, Listening, Log, serve_listener};
pub use rdlt_wire::opener::Opening;

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

/// Serving a connection failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServeError {
    /// Its control connection's transport failed.
    #[error("serving the connection failed")]
    Transport(#[source] hyper::Error),
    /// Its opener failed, or passed no control connection first.
    #[error("taking the connection's streams failed")]
    Opener(#[source] std::io::Error),
}

/// How often a served connection pings its host over HTTP/2, as the host's heartbeat pings it.
const KEEP_ALIVE: Duration = Duration::from_secs(5);

/// How long a ping may go unanswered before the host counts as gone.
const KEEP_ALIVE_PATIENCE: Duration = Duration::from_secs(30);

/// Serves the protocol on `io`, its data calls as `Data` calls of it, enforcing `limits` on what
/// it receives, until the host closes the connection, which ends it cleanly.
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

/// What passes a connection's streams to the connector serving it: the control connection
/// first, then a stream for each data call.
pub trait Opened: Send {
    /// A stream passed.
    type Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// The next stream passed, and what it is; `None` once nothing more is passed.
    fn next(
        &mut self,
    ) -> impl Future<Output = std::io::Result<Option<(Opening, Self::Stream)>>> + Send;
}

impl Opened for Accepting {
    type Stream = tokio::net::UnixStream;

    async fn next(&mut self) -> std::io::Result<Option<(Opening, Self::Stream)>> {
        let Some((opening, stream)) = Accepting::next(self).await? else {
            return Ok(None);
        };
        Ok(Some((opening, passed(stream)?)))
    }
}

impl<S: AsyncRead + AsyncWrite + Send + Unpin + 'static> Opened
    for tokio::sync::mpsc::Receiver<(Opening, S)>
{
    type Stream = S;

    async fn next(&mut self) -> std::io::Result<Option<(Opening, S)>> {
        Ok(self.recv().await)
    }
}

/// Serves the protocol on the streams its host passes over `opener`, the opener's end it holds:
/// the first its control connection, each later one a data call's stream; enforcing `limits` on
/// what it receives, until the host closes the control connection, which ends it cleanly.
///
/// # Errors
///
/// A [`ServeError`] when the opener fails, its first stream is not the control connection, or
/// the control connection fails in its transport.
pub async fn serve_opened(
    served: Arc<Served>,
    opener: std::os::fd::OwnedFd,
    limits: Limits,
) -> Result<(), ServeError> {
    let opener = Accepting::new(opener).map_err(ServeError::Opener)?;
    serve_streams(served, opener, limits, std::future::pending()).await
}

/// Serves the protocol on the streams `opened` passes, as [`serve_opened`] serves those an
/// opener passes, and once `stop` ends, stops taking new calls and ends when the calls in flight
/// have.
///
/// # Errors
///
/// As [`serve_opened`].
pub async fn serve_streams<O: Opened>(
    served: Arc<Served>,
    opened: O,
    limits: Limits,
    stop: impl Future<Output = ()>,
) -> Result<(), ServeError> {
    let host = Hosted::spawning();
    serve_opened_until(served, opened, limits, host, stop).await
}

/// Serves as [`serve_streams`] does, for the host `host` names.
async fn serve_opened_until<O: Opened>(
    served: Arc<Served>,
    mut opener: O,
    limits: Limits,
    host: Hosted,
    stop: impl Future<Output = ()>,
) -> Result<(), ServeError> {
    let control = match opener.next().await.map_err(ServeError::Opener)? {
        Some((Opening::Control, control)) => control,
        Some((Opening::Data, _)) | None => {
            let refused = "the opener passed no control connection first";
            return Err(ServeError::Opener(std::io::Error::other(refused)));
        }
    };
    let drain = host.drain;
    // A stop reaches the control connection once the data calls in flight have ended, within the
    // drain: they ride no call of it, so its own drain would not wait for them.
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let released = async {
        released.await.ok();
    };
    let (service, serving) = serving(served, control, limits, host, released);
    let mut calls = DataCalls {
        service: Arc::clone(&service),
        calls: JoinSet::new(),
    };
    let (mut release, mut open, mut stopping) = (Some(release), true, false);
    let drained = tokio::time::sleep(drain);
    tokio::pin!(serving, stop, drained);
    let served = loop {
        if stopping
            && calls.calls.is_empty()
            && let Some(release) = release.take()
        {
            release.send(()).ok();
        }
        tokio::select! {
            biased;
            served = &mut serving => break served,
            () = &mut stop, if !stopping => {
                stopping = true;
                drained.as_mut().reset(tokio::time::Instant::now() + drain);
            }
            () = &mut drained, if stopping && release.is_some() => {
                if let Some(release) = release.take() {
                    release.send(()).ok();
                }
            }
            passed = opener.next(), if open => match passed {
                Ok(Some((Opening::Data, stream))) => calls.serve(stream, stopping),
                // A second control connection is no stream of this one.
                Ok(Some((Opening::Control, _))) => {}
                Ok(None) => open = false,
                // A connection that can take no more data calls ends, rather than leave its host
                // waiting on each call it opens: a spawned connector then exits, and is started
                // again.
                Err(error) => break Err(ServeError::Opener(error)),
            },
            Some(_) = calls.calls.join_next() => {}
        }
    };
    // The data calls end with the control connection: a host gone takes them with it.
    service.ended.cancel();
    while calls.calls.join_next().await.is_some() {}
    served
}

/// A passed connection's data calls in flight.
///
/// A call's writes wait as long as its host takes nothing, an engine waiting for room among
/// them: a host gone ends its calls with its control connection, whose own writes have a send
/// wait.
struct DataCalls {
    service: Arc<service::Service>,
    calls: JoinSet<()>,
}

impl DataCalls {
    /// Serves the data call `stream` carries, unless the connection is `stopping` or carries as
    /// many as it may: such a stream ends unanswered.
    fn serve<S: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
        &mut self,
        stream: S,
        stopping: bool,
    ) {
        while self.calls.try_join_next().is_some() {}
        if !stopping && self.calls.len() < MAX_DATA_CALLS {
            self.calls
                .spawn(stream::serve(Arc::clone(&self.service), stream));
        }
    }
}

/// The data calls one passed connection serves at once: as many as its HTTP/2 calls.
const MAX_DATA_CALLS: usize = rdlt_wire::limits::MAX_CALLS as usize;

/// `stream`, a socket passed over an opener, as the runtime drives it.
fn passed(stream: std::os::fd::OwnedFd) -> std::io::Result<tokio::net::UnixStream> {
    let stream = std::os::unix::net::UnixStream::from(stream);
    stream.set_nonblocking(true)?;
    tokio::net::UnixStream::from_std(stream)
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
    let (service, serving) = serving(served, io, limits, host, stop);
    let served = serving.await;
    // Its `Data` calls end with it.
    service.ended.cancel();
    served
}

/// The service of the connection `io` carries to `host`, and its serving, as
/// [`serve_until`] serves it.
fn serving<IO>(
    served: Arc<Served>,
    io: IO,
    limits: Limits,
    host: Hosted,
    stop: impl Future<Output = ()>,
) -> (
    Arc<service::Service>,
    impl Future<Output = Result<(), ServeError>>,
)
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let stopping = tokio_util::sync::CancellationToken::new();
    let service = Arc::new(service::Service::new(
        served,
        limits,
        host.name,
        host.sessions,
        stopping.clone(),
    ));
    let serving = control(
        Arc::clone(&service),
        io,
        host.send,
        host.drain,
        stop,
        stopping,
    );
    (service, serving)
}

/// Serves the control connection `io` of `service`'s connection until it ends, or until `stop`
/// ends and its calls in flight have, within `drain`; its writes fail once they make no
/// progress for `send`.
async fn control<IO>(
    service: Arc<service::Service>,
    io: IO,
    send: Duration,
    drain: Duration,
    stop: impl Future<Output = ()>,
    stopping: tokio_util::sync::CancellationToken,
) -> Result<(), ServeError>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let service = classes::classed(service);
    let service = service.map_request(|request: http::Request<hyper::body::Incoming>| {
        request.map(rdlt_wire::tonic::body::Body::new)
    });
    let builder = {
        let transport = Transport::default();
        let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        builder
            .timer(TokioTimer::new())
            // Hosts never read the date, and its text changes each second of the wall clock,
            // which would make two runs of one simulated seed write header blocks of different
            // lengths.
            .auto_date_header(false)
            .initial_stream_window_size(transport.stream_window)
            .initial_connection_window_size(transport.connection_window)
            .max_frame_size(transport.max_frame)
            .max_concurrent_streams(rdlt_wire::limits::MAX_CALLS)
            // Pings notice a host the network dropped silently, which would hold its session.
            .keep_alive_interval(Some(KEEP_ALIVE))
            .keep_alive_timeout(KEEP_ALIVE_PATIENCE);
        builder
    };
    let io = sending::Sending::within(io, send);
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
            tokio::time::timeout(drain, connection)
                .await
                .unwrap_or(Ok(()))
        }
    };
    served.or_else(|error| {
        if gone(&error) {
            Ok(())
        } else {
            Err(ServeError::Transport(error))
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
