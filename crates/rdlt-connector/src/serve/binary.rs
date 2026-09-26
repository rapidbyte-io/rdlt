//! A connector binary's whole `main`: its arguments, the connection its host passed it, and its
//! shutdown when the host says so.

#[cfg(test)]
mod tests;

use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;

use rdlt_wire::Limits;
use tokio::io::AsyncReadExt as _;

use super::{Served, inherited, serve_until};
use crate::factory::{RoleFactory, Serve};

/// Why a connector binary could not serve.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub(super) struct Failure(String);

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self(message)
    }
}

impl From<&str> for Failure {
    fn from(message: &str) -> Self {
        Self(message.to_owned())
    }
}

/// What a connector binary was asked to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Args {
    /// The file descriptor of the socket the host passed.
    pub(super) fd: i32,
}

/// The binary's arguments: `--rdlt-fd N`, or `--rdlt-fd=N`.
pub(super) fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, Failure> {
    let mut fd = None;
    while let Some(arg) = args.next() {
        let value = match arg.strip_prefix("--rdlt-fd") {
            Some("") => args.next(),
            Some(rest) => rest.strip_prefix('=').map(str::to_owned),
            None => return Err(format!("unknown argument `{arg}`").into()),
        };
        let value = value.ok_or("`--rdlt-fd` needs a file descriptor")?;
        let number = value
            .parse()
            .map_err(|_| format!("`--rdlt-fd {value}` is not a file descriptor"))?;
        if fd.replace(number).is_some() {
            return Err("`--rdlt-fd` is given twice".into());
        }
    }
    let fd = fd.ok_or("a connector binary is started by its host, with `--rdlt-fd`")?;
    Ok(Args { fd })
}

/// Serves `C` as a whole binary's `main` does: `fn main() -> ExitCode { serve::<C>() }`.
///
/// It serves the socket its host passed with `--rdlt-fd` until the host closes it, and shuts down
/// gracefully once its standard input ends or it receives `SIGTERM`. The host manages it, so it
/// ignores `SIGINT`: a terminal's Ctrl-C reaches the host, which stops its connectors itself.
pub fn serve<C: Serve>() -> ExitCode {
    Served::from(C::factory()).serve()
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
    /// Serves these roles as a whole binary's `main` does; see [`serve`].
    pub fn serve(self) -> ExitCode {
        match run(self) {
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

fn run(served: Served) -> Result<(), Failure> {
    let args = parse(std::env::args().skip(1))?;
    // First, before anything in this process opens a file: see `inherited::adopt`.
    let socket = inherited::adopt(args.fd)
        .map_err(|error| format!("taking the host's socket failed: {error}"))?;
    #[cfg(target_os = "linux")]
    nix::sys::prctl::set_pdeathsig(nix::sys::signal::Signal::SIGTERM)
        .map_err(|error| format!("asking to end with the host failed: {error}"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("starting the runtime failed: {error}"))?;
    let served = runtime.block_on(async move {
        socket
            .set_nonblocking(true)
            .map_err(|error| format!("the host's socket failed: {error}"))?;
        let io = tokio::net::UnixStream::from_std(socket)
            .map_err(|error| format!("the host's socket failed: {error}"))?;
        let stop = told_to_stop().map_err(|error| format!("watching for stops failed: {error}"))?;
        serve_until(Arc::new(served), io, Limits::default(), stop)
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
