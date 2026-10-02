//! The command that starts a connector: its program, executed from the file that was opened
//! and hashed, given its socket and nothing else of the host's, confined where it is
//! untrusted.

use std::ffi::OsString;
use std::os::fd::{OwnedFd, RawFd};
use std::os::unix::process::CommandExt as _;
use std::process::{Command, Stdio};
use std::sync::Arc;

use command_fds::{CommandFdExt as _, FdMapping};

use rdlt_adopt::Marking;

use super::super::binary::Binary;
use super::super::sandbox::{Bind, Confined, NetworkGrant, Sandbox, SandboxError, Stops};
use super::{GRANTS_FD, Launch, PROGRAM_FD, SOCKET_FD};
use crate::provider::Digest;

/// The sandbox a connector runs in, and whether it may reach the network there; what it is
/// granted of the host's files is its placement's lease's.
#[derive(Clone, Debug)]
pub(crate) struct Confinement {
    pub(crate) sandbox: Arc<dyn Sandbox>,
    pub(crate) network: NetworkGrant,
}

/// Why a connector's command could not be made.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Unspawned {
    /// Its sandbox cannot be used.
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    /// Its binary changed since it was placed.
    #[error("the binary's digest is {found}, not {expected}")]
    Changed {
        /// The digest it was placed with.
        expected: Digest,
        /// The digest it has.
        found: Digest,
    },
    /// Its binary was replaced or removed since it was placed, and a sandbox binds a program
    /// by a name, which the file placed no longer has.
    #[error("the binary placed has been replaced or removed")]
    Replaced,
    /// The operating system refused a step.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A connector's command, how the connector is asked to stop, and descriptors held open
/// until the command has spawned.
pub(crate) struct Commanded {
    pub(crate) command: Command,
    pub(crate) stops: Stops,
    pub(crate) held: Vec<OwnedFd>,
}

/// The arguments `launch`'s binary is given: the socket it serves, and the bytes of state one
/// request may carry where the host's limit is not the protocol's.
pub(crate) fn arguments(launch: &Launch) -> Vec<OsString> {
    let socket = OsString::from(format!("--rdlt-fd={SOCKET_FD}"));
    let state = launch
        .state_bytes
        .map(|bytes| OsString::from(format!("--max-state-bytes={bytes}")));
    std::iter::once(socket).chain(state).collect()
}

/// The command that starts `launch`'s binary serving `socket` at file descriptor 3, with only
/// the environment `launch` keeps, in a process group of its own, inheriting no other
/// descriptor of this process; and how the connector is asked to stop.
///
/// A sandboxed connector is spawned only where `marks_at_once` says the kernel marks every
/// other descriptor close-on-exec in one call.
pub(crate) fn command(
    launch: &Launch,
    socket: OwnedFd,
    marks_at_once: fn() -> bool,
) -> Result<Commanded, Unspawned> {
    if let Some(expected) = launch.digest {
        let found = launch.binary.digest()?;
        if found != expected {
            return Err(Unspawned::Changed { expected, found });
        }
    }
    let args = arguments(launch);
    let kept = |name: &String| Some((OsString::from(name), std::env::var_os(name)?));
    let env: Vec<(OsString, OsString)> = launch.env_passthrough.iter().filter_map(kept).collect();
    let mut given = vec![(socket, SOCKET_FD)];
    let (mut command, stops, held, marking) = if let Some(confinement) = &launch.confinement {
        if !marks_at_once() {
            return Err(SandboxError::Descriptors.into());
        }
        if !launch.binary.linked()? {
            return Err(Unspawned::Replaced);
        }
        given.push((launch.binary.file().try_clone()?.into(), PROGRAM_FD));
        let mut binds = Vec::new();
        for (bound, fd) in launch.lease.bound.iter().zip(GRANTS_FD..) {
            given.push((bound.file.try_clone()?.into(), fd));
            let (at, write) = (bound.at.as_path(), bound.write);
            binds.push(Bind { fd, at, write });
        }
        let confined = Confined {
            program: PROGRAM_FD,
            args: &args,
            env: &env,
            socket: SOCKET_FD,
            binds: &binds,
            network: confinement.network,
        };
        let launcher = confinement.sandbox.launcher(&confined)?;
        given.extend(launcher.given);
        let (command, stops) = (launcher.command, launcher.stops);
        (command, stops, launcher.held, Marking::AtOnce)
    } else {
        let (mut command, held) = trusted(&launch.binary, &mut given)?;
        command.args(&args).env_clear().envs(env);
        (command, Stops::BySignal, held, Marking::OrOneByOne)
    };
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // A group of its own, which this host owns: what the connector starts ends with it.
        .process_group(0);
    inheriting(&mut command, given, marking)?;
    Ok(Commanded {
        command,
        stops,
        held,
    })
}

/// Gives `command`'s process each of `given` at its number, and no other descriptor of this
/// process beside its standard streams: whatever another thread opens, the child marks every
/// other descriptor close-on-exec once those are in place, as `marking` says.
pub(crate) fn inheriting(
    command: &mut Command,
    given: Vec<(OwnedFd, RawFd)>,
    marking: Marking,
) -> std::io::Result<()> {
    let numbers: Vec<RawFd> = given.iter().map(|(_, at)| *at).collect();
    let mappings = given.into_iter().map(|(parent_fd, child_fd)| FdMapping {
        parent_fd,
        child_fd,
    });
    command
        .fd_mappings(mappings.collect())
        .map_err(|_| std::io::Error::other("a descriptor is given twice"))?;
    // Registered after the mappings' hook, so it runs after the descriptors are in place.
    rdlt_adopt::inheriting_only(command, &numbers, marking);
    Ok(())
}

/// The command that executes `binary`'s open file, not its path: whatever the path names by
/// now, what was opened, and hashed, is what runs; and the descriptor it is executed from,
/// held open until it has spawned.
///
/// A script's interpreter opens the script by the name it was executed by, so a script is
/// also `given` at [`PROGRAM_FD`], which its interpreter holds; a binary is given nowhere.
#[cfg(target_os = "linux")]
fn trusted(
    binary: &Binary,
    given: &mut Vec<(OwnedFd, RawFd)>,
) -> std::io::Result<(Command, Vec<OwnedFd>)> {
    if binary.is_script()? {
        given.push((binary.file().try_clone()?.into(), PROGRAM_FD));
        let command = Command::new(format!("/proc/self/fd/{PROGRAM_FD}"));
        return Ok((command, Vec::new()));
    }
    let (command, executed) = executed_from(binary.file(), PROGRAM_FD)?;
    Ok((command, vec![executed]))
}

/// The command that executes `binary` by its path: this platform executes no open file, so
/// its digest is neither checked nor reported.
#[cfg(not(target_os = "linux"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "it answers as the Linux spawn does, which can fail"
)]
fn trusted(
    binary: &Binary,
    _given: &mut Vec<(OwnedFd, RawFd)>,
) -> std::io::Result<(Command, Vec<OwnedFd>)> {
    Ok((Command::new(binary.path()), Vec::new()))
}

/// The lowest descriptor a command is executed from.
#[cfg(target_os = "linux")]
const EXECUTED_FD: RawFd = 8;

/// The command that executes the open `file` through `/proc/self/fd`, and the descriptor it
/// does so through: close-on-exec, held open until the command has spawned, and numbered above
/// `highest`, the highest the child is given, so that giving those replaces none.
#[cfg(target_os = "linux")]
pub(crate) fn executed_from(
    file: &std::fs::File,
    highest: RawFd,
) -> std::io::Result<(Command, OwnedFd)> {
    use std::os::fd::AsRawFd as _;
    let lowest = highest.saturating_add(1).max(EXECUTED_FD);
    let executed = rustix::io::fcntl_dupfd_cloexec(file, lowest)?;
    let command = Command::new(format!("/proc/self/fd/{}", executed.as_raw_fd()));
    Ok((command, executed))
}
