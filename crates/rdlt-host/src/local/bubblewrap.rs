//! The sandbox rdlt ships for Linux: bubblewrap, which needs no privilege.

use std::ffi::OsString;
use std::os::fd::{OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, OnceLock};

use super::binary::{Binary, Unfit};
use super::process::{PROGRAM_FD, SOCKET_FD, inheriting};
use super::sandbox::{Confined, Grants, Launcher, NetworkGrant, Sandbox, SandboxError, Stops};

#[cfg(test)]
mod tests;

/// Where bubblewrap is installed, unless another path is given.
const WELL_KNOWN: &str = "/usr/bin/bwrap";

/// Where the connector's program is inside its sandbox.
const PROGRAM: &str = "/rdlt-connector";

/// The directories of the host a dynamically linked program needs to start, each given read
/// only where it exists, or as the link it is.
const SYSTEM: [&str; 6] = ["/usr", "/bin", "/sbin", "/lib", "/lib64", "/lib32"];

/// The files of the host a dynamically linked program needs to start, each given read only
/// where it exists.
const SYSTEM_FILES: [&str; 1] = ["/etc/ld.so.cache"];

/// Bytes: bounds what is kept of what the launcher said when it could not make a sandbox.
const SAID_BYTES: usize = 512;

/// The descriptor the launcher reads its arguments from: none is on its command line, which
/// every user of the machine may read.
const ARGS_FD: RawFd = 5;

/// Confines a connector with bubblewrap.
///
/// The connector sees the host's system directories read only, an empty `/tmp`, a `/proc`
/// and a `/dev` of its own, and what it was granted: nothing of any home directory. It has
/// no network unless granted one, its own user, process, IPC, host-name and control-group
/// namespaces and no further user namespace, only the environment the host states and the
/// `PWD` the launcher sets, and a session of its own, and it ends with its host. Its
/// arguments, the environment among them, reach the launcher through a descriptor, never its
/// command line.
///
/// The launcher is found at an absolute path, never through `PATH` or the working directory,
/// and must be no other user's to change, as a connector's binary must; it is opened once,
/// and executed from that open file. Whether it can make a sandbox here is tried once, the
/// first time one is asked for: it needs bubblewrap 0.8 or later and a kernel that gives
/// unprivileged user namespaces.
#[derive(Debug)]
pub struct Bubblewrap {
    launcher: PathBuf,
    usable: OnceLock<Result<Arc<Binary>, SandboxError>>,
}

impl Default for Bubblewrap {
    fn default() -> Self {
        Self::at(WELL_KNOWN)
    }
}

impl Bubblewrap {
    /// Bubblewrap at `/usr/bin/bwrap`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bubblewrap at `launcher`, an absolute path.
    pub fn at(launcher: impl Into<PathBuf>) -> Self {
        Self {
            launcher: launcher.into(),
            usable: OnceLock::new(),
        }
    }

    /// The arguments that confine a program found at descriptor `program`, with `grants`.
    fn confinement(confined: &Confined<'_>) -> Result<Vec<OsString>, SandboxError> {
        let mut args: Vec<OsString> = Vec::new();
        let mut flag = |flag: &str, values: &[&std::ffi::OsStr]| {
            args.push(flag.into());
            args.extend(values.iter().map(|value| OsString::from(*value)));
        };
        flag("--unshare-all", &[]);
        if confined.grants.network == NetworkGrant::Granted {
            flag("--share-net", &[]);
        }
        // Its own user namespace, in which it may make none: each is a surface of the kernel.
        flag("--unshare-user", &[]);
        flag("--disable-userns", &[]);
        for only in ["--die-with-parent", "--new-session", "--clearenv"] {
            flag(only, &[]);
        }
        flag("--hostname", &["rdlt-connector".as_ref()]);
        for system in SYSTEM.map(Path::new) {
            match std::fs::symlink_metadata(system) {
                Ok(found) if found.is_symlink() => {
                    if let Ok(target) = std::fs::read_link(system) {
                        flag("--symlink", &[target.as_ref(), system.as_ref()]);
                    }
                }
                Ok(_) => flag("--ro-bind", &[system.as_ref(), system.as_ref()]),
                Err(_) => {}
            }
        }
        for file in SYSTEM_FILES
            .map(Path::new)
            .iter()
            .filter(|file| file.exists())
        {
            flag("--ro-bind", &[file.as_ref(), file.as_ref()]);
        }
        flag("--proc", &["/proc".as_ref()]);
        flag("--dev", &["/dev".as_ref()]);
        flag("--tmpfs", &["/tmp".as_ref()]);
        let grants = &confined.grants;
        let granted = grants.read.iter().map(|path| ("--ro-bind", path));
        for (bind, path) in granted.chain(grants.write.iter().map(|path| ("--bind", path))) {
            if !path.is_absolute() || !path.exists() {
                return Err(SandboxError::Grant { path: path.clone() });
            }
            flag(bind, &[path.as_ref(), path.as_ref()]);
        }
        for (name, value) in confined.env {
            flag("--setenv", &[name, value]);
        }
        let program = confined.program.to_string();
        flag("--ro-bind-fd", &[program.as_ref(), PROGRAM.as_ref()]);
        flag("--chdir", &["/".as_ref()]);
        Ok(args)
    }

    /// Whether the launcher makes a sandbox here: it is run once, confining itself, and what
    /// it answers is kept.
    ///
    /// # Errors
    ///
    /// [`SandboxError::Unsupported`] where the platform has no sandbox,
    /// [`SandboxError::Missing`] where the launcher is not at its path,
    /// [`SandboxError::Shared`] where another user may change it, and
    /// [`SandboxError::Unavailable`] where it runs and makes no sandbox.
    pub fn usable(&self) -> Result<(), SandboxError> {
        self.opened().map(|_| ())
    }

    /// The launcher, opened and tried.
    fn opened(&self) -> Result<Arc<Binary>, SandboxError> {
        self.usable.get_or_init(|| self.tried()).clone()
    }

    fn tried(&self) -> Result<Arc<Binary>, SandboxError> {
        if cfg!(not(target_os = "linux")) {
            return Err(SandboxError::Unsupported);
        }
        let path = self.launcher.clone();
        if !path.is_absolute() {
            return Err(SandboxError::Missing { path });
        }
        let launcher = Binary::at(&path).map_err(|unfit| match unfit {
            Unfit::Absent(_) => SandboxError::Missing { path: path.clone() },
            Unfit::Shared { path, .. } => SandboxError::Shared { path },
        })?;
        // The launcher confines itself, found as a connector's program is, and says its version.
        let confined = Confined {
            program: PROGRAM_FD,
            args: &["--version".into()],
            env: &[],
            socket: SOCKET_FD,
            grants: &Grants::default(),
        };
        let Launcher {
            mut command,
            given,
            held,
            ..
        } = launched(&launcher, &confined)?;
        let unavailable = |said: &dyn std::fmt::Display| unavailable(&path, said);
        let program: OwnedFd = launcher
            .file()
            .try_clone()
            .map_err(|error| unavailable(&error))?
            .into();
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let given = std::iter::once((program, PROGRAM_FD))
            .chain(given)
            .collect();
        inheriting(&mut command, given).map_err(|error| unavailable(&error))?;
        let output = command.output().map_err(|error| unavailable(&error))?;
        drop(held);
        if output.status.success() {
            return Ok(Arc::new(launcher));
        }
        Err(unavailable(&String::from_utf8_lossy(&output.stderr)))
    }
}

/// The launcher at `path` cannot make a sandbox, as it said.
fn unavailable(path: &Path, said: &dyn std::fmt::Display) -> SandboxError {
    SandboxError::Unavailable {
        path: path.to_owned(),
        said: rdlt_connector::text::shown(said, SAID_BYTES),
    }
}

/// The command that runs `confined` through `launcher`, executed from its open file, its
/// arguments read from a descriptor.
#[cfg(target_os = "linux")]
fn launched(launcher: &Binary, confined: &Confined<'_>) -> Result<Launcher, SandboxError> {
    let unavailable = |error: std::io::Error| unavailable(launcher.path(), &error);
    let arguments = arguments(&Bubblewrap::confinement(confined)?).map_err(unavailable)?;
    let (mut command, executed) =
        super::process::executed_from(launcher.file()).map_err(unavailable)?;
    command
        .args(["--args", &ARGS_FD.to_string(), "--", PROGRAM])
        .args(confined.args)
        .env_clear();
    // Bubblewrap ends at `SIGTERM` and its sandbox with it: the connector is asked to stop by
    // the end of its input, and killed through the launcher.
    Ok(Launcher {
        command,
        stops: Stops::ByInputEnd,
        given: vec![(arguments, ARGS_FD)],
        held: vec![executed],
    })
}

#[cfg(not(target_os = "linux"))]
fn launched(_launcher: &Binary, _confined: &Confined<'_>) -> Result<Launcher, SandboxError> {
    Err(SandboxError::Unsupported)
}

/// A file only this process holds, of `arguments`, each ended by a NUL, read from its start.
#[cfg(target_os = "linux")]
fn arguments(arguments: &[OsString]) -> std::io::Result<OwnedFd> {
    use std::io::{Seek as _, Write as _};
    use std::os::unix::ffi::OsStrExt as _;
    let flags = rustix::fs::MemfdFlags::CLOEXEC;
    let mut file = std::fs::File::from(rustix::fs::memfd_create(c"rdlt-sandbox", flags)?);
    let mut bytes = Vec::new();
    for argument in arguments {
        let argument = argument.as_bytes();
        if argument.contains(&0) {
            let message = "an argument of the sandbox holds a NUL byte";
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                message,
            ));
        }
        bytes.extend_from_slice(argument);
        bytes.push(0);
    }
    file.write_all(&bytes)?;
    file.rewind()?;
    Ok(file.into())
}

impl Sandbox for Bubblewrap {
    fn launcher(&self, confined: &Confined<'_>) -> Result<Launcher, SandboxError> {
        let launcher = self.opened()?;
        // Whoever may write the launcher decides what confines every connector.
        let real =
            std::fs::canonicalize(launcher.path()).unwrap_or_else(|_| launcher.path().into());
        for written in &confined.grants.write {
            let resolved = std::fs::canonicalize(written).unwrap_or_else(|_| written.clone());
            if real.starts_with(&resolved) {
                return Err(SandboxError::Covers {
                    path: written.clone(),
                });
            }
        }
        launched(&launcher, confined)
    }
}
