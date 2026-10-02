//! Sandboxes: what confines a connector's process to what it was granted.

use std::ffi::OsString;
use std::fmt;
use std::os::fd::{OwnedFd, RawFd};
use std::path::PathBuf;
use std::process::Command;

/// What a sandboxed connector may reach beyond its own program: nothing unless granted.
///
/// A pipeline asks for what its own connector needs, on the reference that places it
/// ([`ConnectorRef::grant_write`](crate::ConnectorRef::grant_write)), and is granted it only
/// within the roots the provider's operator names
/// ([`Local::grantable_write`](crate::Local::grantable_write)); a provider grants only paths
/// every connector it spawns may read ([`Local::grant_read`](crate::Local::grant_read)).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Grants {
    /// Paths it may read, each absolute.
    pub read: Vec<PathBuf>,
    /// Paths it may read and write, each absolute.
    pub write: Vec<PathBuf>,
    /// Whether it may reach the network.
    pub network: NetworkGrant,
    /// Whether its paths may overlap those another connector of this process was granted,
    /// that connector's grants being shared too: otherwise such a placement is refused.
    pub shared: bool,
}

impl Grants {
    /// Whether nothing is granted.
    pub fn is_empty(&self) -> bool {
        self.read.is_empty() && self.write.is_empty() && self.network == NetworkGrant::Denied
    }
}

/// Whether a sandboxed connector may reach the network.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NetworkGrant {
    /// It has no network but a loopback of its own.
    #[default]
    Denied,
    /// It shares the host's network: every interface and route of the host, the host's own
    /// loopback services, and the abstract Unix sockets of the host's network namespace.
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
    /// What the connector is granted of the host's files, read grants first.
    pub binds: &'a [Bind<'a>],
    /// Whether it may reach the network.
    pub network: NetworkGrant,
}

/// A path a connector is granted, open at a descriptor the launcher finds it at: what was
/// checked, which the launcher binds, and never a path it resolves again.
#[derive(Clone, Copy, Debug)]
pub struct Bind<'a> {
    /// The descriptor the launcher finds what is granted at, open as a location alone.
    pub fd: RawFd,
    /// Where the connector finds it: the path as it was granted.
    pub at: &'a std::path::Path,
    /// Whether the connector may write it.
    pub write: bool,
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
    /// Descriptors the command's process must find open at the numbers given, beside the
    /// program and the socket.
    pub given: Vec<(OwnedFd, RawFd)>,
    /// Descriptors this process must hold open until the command has spawned, as the file a
    /// command is executed from.
    pub held: Vec<OwnedFd>,
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
    /// The operating system refused a step of running the sandbox's launcher.
    #[error("the sandbox launcher at {} could not be run", path.display())]
    Failed {
        /// The launcher.
        path: PathBuf,
        /// What the operating system said.
        #[source]
        source: OsError,
    },
    /// A path granted is not absolute, or is not there.
    #[error("the granted path {} is not an absolute path to something that exists", path.display())]
    Grant {
        /// The path.
        path: PathBuf,
    },
    /// A path granted lies within no root its operator lets grants be made in: one to be
    /// written within no root that may be written.
    #[error("the granted path {} lies outside every path grants may be made in", path.display())]
    Outside {
        /// The path.
        path: PathBuf,
    },
    /// A root grants may write within holds, or lies within, what decides what a host runs or
    /// keeps: a connector directory or binary, the sandbox's launcher, the host's executable,
    /// its state, or a directory secrets are read from.
    #[error("grants may write within {}, which holds what the host runs or keeps", path.display())]
    Guarded {
        /// The root.
        path: PathBuf,
    },
    /// A path granted overlaps one another connector running now was granted, and the grants
    /// are not both shared; or a path granted to be written overlaps one every connector of
    /// the provider reads.
    #[error("the granted path {} overlaps a path another connector was granted", path.display())]
    Overlap {
        /// The path.
        path: PathBuf,
    },
    /// A path granted to be written holds a program another placement runs.
    #[error("the path {} granted to be written holds a program the host runs", path.display())]
    Covers {
        /// The path.
        path: PathBuf,
    },
    /// A program the placement runs lies within a path a connector running now may write.
    #[error("the program {} lies where a connector may write", path.display())]
    Exposed {
        /// The program.
        path: PathBuf,
    },
    /// This kernel cannot mark a child's descriptors close-on-exec in one call, as a sandboxed
    /// connector's spawn needs: Linux 5.11 and later can.
    #[error("this kernel cannot keep a sandboxed connector from inheriting descriptors")]
    Descriptors,
    /// The launcher, or a directory above it, belongs to another user or may be written by
    /// one.
    #[error("the sandbox launcher at {} may be changed by another user", path.display())]
    Shared {
        /// The launcher, or the directory.
        path: PathBuf,
    },
}

impl SandboxError {
    /// The launcher at `path` could not be run, as `error` says.
    pub(crate) fn failed(path: &std::path::Path, error: std::io::Error) -> Self {
        Self::Failed {
            path: path.to_owned(),
            source: OsError(std::sync::Arc::new(error)),
        }
    }

    /// The error's stable code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unsupported => "sandbox_unsupported",
            Self::Missing { .. } => "sandbox_missing",
            Self::Unavailable { .. } => "sandbox_unavailable",
            Self::Failed { .. } => "sandbox_failed",
            Self::Grant { .. } => "sandbox_grant",
            Self::Outside { .. } => "grant_outside",
            Self::Guarded { .. } => "grant_root_guarded",
            Self::Overlap { .. } => "grant_overlap",
            Self::Covers { .. } => "grant_covers",
            Self::Exposed { .. } => "program_exposed",
            Self::Descriptors => "sandbox_descriptors",
            Self::Shared { .. } => "sandbox_launcher_shared",
        }
    }
}

/// An error of the operating system's, kept whole as a cause and shared by every copy of the
/// error that holds it; two are equal where they are of one kind and one error number.
#[derive(Clone, Debug)]
pub struct OsError(std::sync::Arc<std::io::Error>);

impl OsError {
    /// The error as the operating system gave it.
    pub fn io(&self) -> &std::io::Error {
        &self.0
    }
}

impl PartialEq for OsError {
    fn eq(&self, other: &Self) -> bool {
        self.0.kind() == other.0.kind() && self.0.raw_os_error() == other.0.raw_os_error()
    }
}

impl Eq for OsError {}

impl fmt::Display for OsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for OsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// Confines a connector's process: an operator may plug in another launcher than rdlt
/// ships.
///
/// The sandbox must give the connector no file, network, process or variable of the host's
/// but what [`Confined`] names, pass on no descriptor but its standard streams and the
/// socket, put no value of the connector's environment where another user may read it, as a
/// command line, and end everything it started when the process the host spawned is killed.
pub trait Sandbox: fmt::Debug + Send + Sync {
    /// The command that runs `confined`.
    ///
    /// # Errors
    ///
    /// A [`SandboxError`] when the sandbox cannot be made: the connector is then not run.
    fn launcher(&self, confined: &Confined<'_>) -> Result<Launcher, SandboxError>;

    /// The files that decide how a connector is confined, as its launcher: no root grants may
    /// write within may hold one.
    fn programs(&self) -> Vec<PathBuf> {
        Vec::new()
    }
}
