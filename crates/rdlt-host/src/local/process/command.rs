//! The command that starts a connector: its program, executed from the file that was opened
//! and hashed, given its socket and nothing else of the host's, confined where it is
//! untrusted.

use std::ffi::OsString;
use std::os::fd::{OwnedFd, RawFd};
use std::os::unix::process::CommandExt as _;
use std::process::{Command, Stdio};
use std::sync::Arc;

use command_fds::{CommandFdExt as _, FdMapping};

use super::super::binary::Binary;
use super::super::grants::Lease;
use super::super::sandbox::{Confined, Grants, Sandbox, SandboxError, Stops};
use super::{Launch, PROGRAM_FD, SOCKET_FD};
use crate::provider::Digest;

/// The sandbox a connector runs in, what it is granted there, and the hold on its grants.
#[derive(Clone, Debug)]
pub(crate) struct Confinement {
    pub(crate) sandbox: Arc<dyn Sandbox>,
    pub(crate) grants: Grants,
    pub(crate) lease: Arc<Lease>,
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

/// The command that starts `launch`'s binary serving `socket` at file descriptor 3, with only
/// the environment `launch` keeps, in a process group of its own, inheriting no other
/// descriptor of this process; and how the connector is asked to stop.
pub(crate) fn command(launch: &Launch, socket: OwnedFd) -> Result<Commanded, Unspawned> {
    if let Some(expected) = launch.digest {
        let found = launch.binary.digest()?;
        if found != expected {
            return Err(Unspawned::Changed { expected, found });
        }
    }
    let args = [OsString::from(format!("--rdlt-fd={SOCKET_FD}"))];
    let kept = |name: &String| Some((OsString::from(name), std::env::var_os(name)?));
    let env: Vec<(OsString, OsString)> = launch.env_passthrough.iter().filter_map(kept).collect();
    let mut given = vec![(socket, SOCKET_FD)];
    let (mut command, stops, held) = if let Some(confinement) = &launch.confinement {
        if !launch.binary.linked()? {
            return Err(Unspawned::Replaced);
        }
        given.push((launch.binary.file().try_clone()?.into(), PROGRAM_FD));
        let confined = Confined {
            program: PROGRAM_FD,
            args: &args,
            env: &env,
            socket: SOCKET_FD,
            grants: &confinement.grants,
        };
        let launcher = confinement.sandbox.launcher(&confined)?;
        given.extend(launcher.given);
        (launcher.command, launcher.stops, launcher.held)
    } else {
        let (mut command, held) = trusted(&launch.binary, &mut given)?;
        command.args(&args).env_clear().envs(env);
        (command, Stops::BySignal, held)
    };
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // A group of its own, which this host owns: what the connector starts ends with it.
        .process_group(0);
    inheriting(&mut command, given)?;
    Ok(Commanded {
        command,
        stops,
        held,
    })
}

/// Gives `command`'s process each of `given` at its number, and no other descriptor of this
/// process beside its standard streams: whatever another thread opens, the child marks every
/// descriptor above those it is given close-on-exec once they are in place.
pub(crate) fn inheriting(
    command: &mut Command,
    given: Vec<(OwnedFd, RawFd)>,
) -> std::io::Result<()> {
    let above = given.iter().map(|(_, at)| *at).max().unwrap_or(2);
    let mappings = given.into_iter().map(|(parent_fd, child_fd)| FdMapping {
        parent_fd,
        child_fd,
    });
    command
        .fd_mappings(mappings.collect())
        .map_err(|_| std::io::Error::other("a descriptor is given twice"))?;
    // Registered after the mappings' hook, so it runs after the descriptors are in place.
    rdlt_adopt::inheriting_below(command, above + 1);
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
    let (command, executed) = executed_from(binary.file())?;
    Ok((command, vec![executed]))
}

/// The command that executes `binary` by its path: this platform executes no open file, so
/// its digest is neither checked nor reported.
#[cfg(not(target_os = "linux"))]
fn trusted(
    binary: &Binary,
    _given: &mut Vec<(OwnedFd, RawFd)>,
) -> std::io::Result<(Command, Vec<OwnedFd>)> {
    Ok((Command::new(binary.path()), Vec::new()))
}

/// Descriptors below this one may be given to a child; one a command is executed from is put
/// at or above it, so that giving those replaces none.
const EXECUTED_FD: RawFd = 8;

/// The command that executes the open `file` through `/proc/self/fd`, and the descriptor it
/// does so through: close-on-exec, and held open until the command has spawned.
#[cfg(target_os = "linux")]
pub(crate) fn executed_from(file: &std::fs::File) -> std::io::Result<(Command, OwnedFd)> {
    use std::os::fd::AsRawFd as _;
    let executed = rustix::io::fcntl_dupfd_cloexec(file, EXECUTED_FD)?;
    let command = Command::new(format!("/proc/self/fd/{}", executed.as_raw_fd()));
    Ok((command, executed))
}
