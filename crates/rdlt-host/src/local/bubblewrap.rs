//! The sandbox rdlt ships for Linux: bubblewrap, which needs no privilege.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

use super::process::{PROGRAM_FD, SOCKET_FD, given};
use super::sandbox::{Confined, Launcher, NetworkGrant, Sandbox, SandboxError, Stops};

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

/// Confines a connector with bubblewrap.
///
/// The connector sees the host's system directories read only, an empty `/tmp`, a `/proc`
/// and a `/dev` of its own, and what it was granted: nothing of any home directory. It has
/// no network unless granted one, its own process, IPC and host-name namespaces, only the
/// environment the host states and the `PWD` the launcher sets, and a session of its own, and
/// it ends with its host.
///
/// The launcher is found at an absolute path, never through `PATH` or the working directory.
/// Whether it can make a sandbox here is tried once, the first time one is asked for.
#[derive(Debug)]
pub struct Bubblewrap {
    launcher: PathBuf,
    usable: OnceLock<Result<(), SandboxError>>,
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
        flag("--", &[PROGRAM.as_ref()]);
        args.extend(confined.args.iter().cloned());
        Ok(args)
    }

    /// Whether the launcher makes a sandbox here: it is run once, confining itself, and what
    /// it answers is kept.
    ///
    /// # Errors
    ///
    /// [`SandboxError::Unsupported`] where the platform has no sandbox,
    /// [`SandboxError::Missing`] where the launcher is not at its path, and
    /// [`SandboxError::Unavailable`] where it runs and makes no sandbox.
    pub fn usable(&self) -> Result<(), SandboxError> {
        self.usable.get_or_init(|| self.tried()).clone()
    }

    fn tried(&self) -> Result<(), SandboxError> {
        if cfg!(not(target_os = "linux")) {
            return Err(SandboxError::Unsupported);
        }
        let path = self.launcher.clone();
        if !path.is_absolute() {
            return Err(SandboxError::Missing { path });
        }
        let Ok(launcher) = std::fs::File::open(&path) else {
            return Err(SandboxError::Missing { path });
        };
        let unavailable = |said: &dyn std::fmt::Display| SandboxError::Unavailable {
            path: path.clone(),
            said: rdlt_connector::text::shown(said, SAID_BYTES),
        };
        // The launcher confines itself, found as a connector's program is, and says its version.
        let confined = Confined {
            program: PROGRAM_FD,
            args: &["--version".into()],
            env: &[],
            socket: SOCKET_FD,
            grants: &super::Grants::default(),
        };
        let mut command = Command::new(&path);
        command
            .args(Self::confinement(&confined)?)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        given(&mut command, &launcher, PROGRAM_FD).map_err(|error| unavailable(&error))?;
        let output = command.output().map_err(|error| unavailable(&error))?;
        if output.status.success() {
            return Ok(());
        }
        Err(unavailable(&String::from_utf8_lossy(&output.stderr)))
    }
}

impl Sandbox for Bubblewrap {
    fn launcher(&self, confined: &Confined<'_>) -> Result<Launcher, SandboxError> {
        self.usable()?;
        let mut command = Command::new(&self.launcher);
        command.args(Self::confinement(confined)?).env_clear();
        // Bubblewrap ends at `SIGTERM` and its sandbox with it: the connector is asked to
        // stop by the end of its input, and killed through the launcher.
        Ok(Launcher {
            command,
            stops: Stops::ByInputEnd,
        })
    }
}
