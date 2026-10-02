//! Sandboxes: what confines a connector's process to what it was granted.

use std::ffi::OsString;
use std::fmt;
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::process::Command;

/// What a sandboxed connector may reach beyond its own program: nothing unless granted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Grants {
    /// Paths it may read, each absolute.
    pub read: Vec<PathBuf>,
    /// Paths it may read and write, each absolute.
    pub write: Vec<PathBuf>,
    /// Whether it may reach the network.
    pub network: NetworkGrant,
}

/// Whether a sandboxed connector may reach the network.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NetworkGrant {
    /// It has no network but a loopback of its own.
    #[default]
    Denied,
    /// It shares the host's network.
    Granted,
}

/// A connector to run confined: its program, already open, and all it is to be given.
#[derive(Debug)]
pub struct Confined<'a> {
    /// The descriptor the launcher finds the connector's program at, open to read: the
    /// program is run from this descriptor, not from a path.
    pub program: RawFd,
    /// The connector's arguments.
    pub args: &'a [OsString],
    /// The connector's whole environment.
    pub env: &'a [(OsString, OsString)],
    /// The descriptor the launcher finds the connector's socket at, which the connector must
    /// find at the same number.
    pub socket: RawFd,
    /// What the connector may reach.
    pub grants: &'a Grants,
}

/// How a launcher's connector is asked to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stops {
    /// By `SIGTERM` to the launcher's process group and the end of its input: the launcher is
    /// the connector, or relays the signal to it.
    BySignal,
    /// By the end of its input alone: the launcher ends at `SIGTERM` and takes the connector
    /// with it, so the signal is kept for the kill.
    ByInputEnd,
}

/// The command that runs a connector confined, and how the connector is asked to stop.
#[derive(Debug)]
pub struct Launcher {
    /// The command, to which the host adds its descriptors and its standard streams, and which
    /// it spawns leading a process group: killing the process it starts must end the
    /// connector and everything the connector started.
    pub command: Command,
    /// How the connector is asked to stop.
    pub stops: Stops,
}

/// Why a connector cannot be sandboxed.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SandboxError {
    /// This platform has no sandbox: untrusted connectors run remotely.
    #[error("this platform has no sandbox for a local connector")]
    Unsupported,
    /// The sandbox's launcher is not where it is expected.
    #[error("the sandbox launcher is not at {}", path.display())]
    Missing {
        /// Where the launcher was looked for.
        path: PathBuf,
    },
    /// The launcher is there and cannot make a sandbox, as where unprivileged user namespaces
    /// are off, or it is too old to confine what it is asked to.
    #[error("the sandbox launcher at {} cannot make a sandbox: {said}", path.display())]
    Unavailable {
        /// The launcher.
        path: PathBuf,
        /// What the launcher said, shown.
        said: String,
    },
    /// A path granted is not absolute, or is not there.
    #[error("the granted path {} is not an absolute path to something that exists", path.display())]
    Grant {
        /// The path.
        path: PathBuf,
    },
}

impl SandboxError {
    /// The error's stable code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unsupported => "sandbox_unsupported",
            Self::Missing { .. } => "sandbox_missing",
            Self::Unavailable { .. } => "sandbox_unavailable",
            Self::Grant { .. } => "sandbox_grant",
        }
    }
}

/// Confines a connector's process: an operator may plug in another launcher than rdlt
/// ships.
///
/// The sandbox must give the connector no file, network, process or variable of the host's
/// but what [`Confined`] names, pass on no descriptor but its standard streams and the
/// socket, and end everything it started when the process the host spawned is killed.
pub trait Sandbox: fmt::Debug + Send + Sync {
    /// The command that runs `confined`.
    ///
    /// # Errors
    ///
    /// A [`SandboxError`] when the sandbox cannot be made: the connector is then not run.
    fn launcher(&self, confined: &Confined<'_>) -> Result<Launcher, SandboxError>;
}
