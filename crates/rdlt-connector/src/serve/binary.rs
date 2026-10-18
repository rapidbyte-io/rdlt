//! A connector binary's whole `main`: its arguments, the connection its host passed it, and its
//! shutdown when the host says so.

use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;

use rdlt_wire::Limits;
use tokio::io::AsyncReadExt as _;
use tokio_util::sync::CancellationToken;

use super::args::{Args, Failure, parse};
use super::{Hosted, Served, listen, serve_until};
use crate::factory::{RoleFactory, Serve};

/// Serves `C` as a whole binary's `main` does: `fn main() -> ExitCode { serve::<C>() }`.
///
/// Spawned by its host, with `--rdlt-fd`, it takes the socket the host passed before it builds
/// `C`'s factory, so no connector code runs first, and serves it until the host closes it. It
/// shuts down gracefully once its standard input ends or it receives `SIGTERM`. The host manages
/// it, so it ignores `SIGINT`: a terminal's Ctrl-C reaches the host, which stops its connectors
/// itself.
///
/// Run on its own, with `--listen <address> --tls-cert <path> --tls-key <path> --tls-client-ca
/// <path>` and a `--tls-allow-host <name>` for each host it accepts, it serves those hosts over
/// mutual TLS, and says where on standard output (`listening on <address>`). A host is named by a
/// DNS name or URI in its certificate; `--tls-client-crl <path>` refuses the certificates its
/// revocation lists name, and `--max-sessions <count>` serves fewer sessions at once than
/// [`ListenLimits`](crate::limits::ListenLimits) does, for a process that may open few files: it
/// refuses to listen where its limits need more file descriptors than it may open. Each named
/// host holds an equal share of the sessions, or `--max-host-sessions <count>`; it refuses to
/// listen where the hosts could leave one of them no session. The first `SIGTERM` or `SIGINT`
/// stops it gracefully; a second, at once.
///
/// Either way, `--max-state-bytes <bytes>` sets the bytes of state one request may carry: a
/// commit's changes, a plan's or a report's positions (by default
/// [`STATE_BYTES`](rdlt_wire::limits::STATE_BYTES)). A host spawning the connector passes its
/// own limit.
pub fn serve<C: Serve>() -> ExitCode {
    Served::serve(|| C::factory().into())
}

impl From<RoleFactory> for Served {
    fn from(factory: RoleFactory) -> Self {
        match factory {
            RoleFactory::Source(factory) => Self::new().with_source(factory),
            RoleFactory::Destination(factory) => Self::new().with_destination(factory),
        }
    }
}

impl Served {
    /// Serves the roles `build` makes as a whole binary's `main` does; see [`serve`].
    ///
    /// A spawned connector calls `build` once it has taken its host's socket, so nothing `build`
    /// runs comes before.
    pub fn serve(build: impl FnOnce() -> Self) -> ExitCode {
        match run(build) {
            Ok(()) => ExitCode::SUCCESS,
            Err(message) => {
                #[expect(
                    clippy::print_stderr,
                    reason = "a connector binary reports why it failed on its standard error, \
                              which its host keeps"
                )]
                {
                    eprintln!("{message}");
                }
                ExitCode::FAILURE
            }
        }
    }
}

fn run(build: impl FnOnce() -> Served) -> Result<(), Failure> {
    // The arguments name the descriptor to take, and reading them runs no connector code.
    let (args, limits) = parse(std::env::args().skip(1))?;
    match args {
        Args::Inherited { fd } => inherited_socket(build, fd, limits),
        Args::Listen(listening) => {
            let served = build();
            let runtime = runtime()?;
            let served = runtime.block_on(async move {
                let (stop, now) =
                    signalled().map_err(|error| format!("watching for stops failed: {error}"))?;
                tokio::select! {
                    biased;
                    () = now => Ok(()),
                    served = listen::listen(Arc::new(served), &listening, limits, stop) => served,
                }
            });
            runtime.shutdown_background();
            served
        }
    }
}

/// Serves the socket the host passed at `fd` with the roles `build` makes, enforcing `limits` on
/// what it receives.
fn inherited_socket(
    build: impl FnOnce() -> Served,
    fd: i32,
    limits: Limits,
) -> Result<(), Failure> {
    // Before any connector code runs, its factories' construction included: see
    // `rdlt_adopt::adopt`.
    let socket = rdlt_adopt::adopt(fd)
        .map_err(|error| format!("taking the host's socket failed: {error}"))?;
    #[cfg(target_os = "linux")]
    nix::sys::prctl::set_pdeathsig(nix::sys::signal::Signal::SIGTERM)
        .map_err(|error| format!("asking to end with the host failed: {error}"))?;
    let served = build();
    let runtime = runtime()?;
    let served = runtime.block_on(async move {
        socket
            .set_nonblocking(true)
            .map_err(|error| format!("the host's socket failed: {error}"))?;
        let io = tokio::net::UnixStream::from_std(socket)
            .map_err(|error| format!("the host's socket failed: {error}"))?;
        let stop = told_to_stop().map_err(|error| format!("watching for stops failed: {error}"))?;
        serve_until(Arc::new(served), io, limits, Hosted::spawning(), stop)
            .await
            .map_err(|error| match std::error::Error::source(&error) {
                Some(source) => format!("{error}: {source}"),
                None => error.to_string(),
            })
    });
    // Reading standard input blocks a thread the runtime would otherwise wait for.
    runtime.shutdown_background();
    Ok(served?)
}

fn runtime() -> Result<tokio::runtime::Runtime, Failure> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("starting the runtime failed: {error}").into())
}

/// A listening connector's stops: the first `SIGTERM` or `SIGINT` ends the first future, a
/// graceful stop; the second ends the second, a stop at once.
fn signalled() -> std::io::Result<(impl Future<Output = ()>, impl Future<Output = ()>)> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let (graceful, now) = (CancellationToken::new(), CancellationToken::new());
    let (first, second) = (graceful.clone(), now.clone());
    tokio::spawn(async move {
        for stop in [first, second] {
            tokio::select! {
                biased;
                _ = terminate.recv() => {}
                _ = interrupt.recv() => {}
            }
            stop.cancel();
        }
    });
    Ok((graceful.cancelled_owned(), now.cancelled_owned()))
}

/// Ends once the host says to stop: standard input ends, or `SIGTERM` arrives; `SIGINT` is
/// taken, and ignored.
fn told_to_stop() -> std::io::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    let interrupt = signal(SignalKind::interrupt())?;
    Ok(async move {
        // Registered, so a Ctrl-C at the host's terminal does not end the connector.
        let _interrupt = interrupt;
        let mut stdin = tokio::io::stdin();
        let mut buffer = [0; 64];
        let input = async move {
            // Anything written is ignored; the end, or a broken stream, is the stop.
            while matches!(stdin.read(&mut buffer).await, Ok(read) if read > 0) {}
        };
        tokio::select! {
            biased;
            _ = terminate.recv() => {}
            () = input => {}
        }
    })
}
