//! The sandbox rdlt ships for Linux: bubblewrap, which needs no privilege.

#[cfg(any(target_os = "linux", test))]
use std::ffi::OsString;
use std::os::fd::OwnedFd;
#[cfg(target_os = "linux")]
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, OnceLock};

use super::binary::{Binary, Unfit};
use super::process::{PROGRAM_FD, SOCKET_FD, inheriting};
#[cfg(target_os = "linux")]
use super::sandbox::Stops;
use super::sandbox::{Confined, Launcher, NetworkAccess, Sandbox, SandboxError};

#[cfg(test)]
mod tests;

/// Where bubblewrap is installed, unless another path is given.
const WELL_KNOWN: &str = "/usr/bin/bwrap";

/// Where the connector's program is inside its sandbox.
#[cfg(any(target_os = "linux", test))]
const PROGRAM: &str = "/rdlt-connector";

/// A connector's private scratch directory, in memory, which `TMPDIR` alone names: not a path a
/// person would write in a document, as what it holds goes with the process.
#[cfg(any(target_os = "linux", test))]
const SCRATCH: &str = "/.rdlt-scratch";

/// A connector's private shared memory, in memory as its scratch is: where named semaphores and
/// shared memory are made, as Python's `multiprocessing` makes them.
#[cfg(any(target_os = "linux", test))]
const SHARED_MEMORY: &str = "/dev/shm";

/// The files of the host a dynamically linked program needs to start, each given read only
/// where it exists.
#[cfg(any(target_os = "linux", test))]
const SYSTEM_FILES: [&str; 1] = ["/etc/ld.so.cache"];

/// The files and directories of the host a connector granted the network needs to use it, each
/// given read only where it exists when the sandbox is made.
///
/// Those that resolve names, services and protocols come first, then the certificate
/// authorities TLS clients trust, where each family of distributions keeps them. None is meant
/// to hold a secret, and each is read with the host user's own permissions.
#[cfg(any(target_os = "linux", test))]
const NETWORK_FILES: [&str; 16] = [
    "/etc/resolv.conf",
    "/etc/hosts",
    "/etc/nsswitch.conf",
    "/etc/host.conf",
    "/etc/gai.conf",
    "/etc/services",
    "/etc/protocols",
    "/etc/ssl/certs",
    "/etc/ssl/cert.pem",
    "/etc/ssl/ca-bundle.pem",
    "/etc/ca-certificates",
    "/etc/pki/tls/certs",
    "/etc/pki/tls/cert.pem",
    "/etc/pki/ca-trust",
    "/etc/pki/java",
    "/var/lib/ca-certificates",
];

/// Bytes: bounds what is kept of what the launcher said when it could not make a sandbox.
const SAID_BYTES: usize = 512;

/// The descriptor the launcher reads its arguments from: none is on its command line, which
/// every user of the machine may read.
#[cfg(target_os = "linux")]
const ARGS_FD: RawFd = 5;

/// Confines a connector with bubblewrap.
///
/// The connector sees the host's system directories read only, a `/proc` and a `/dev` of its
/// own, a private scratch directory that `TMPDIR` names, a private `/dev/shm`, and what it was
/// granted: nothing of any home directory. Its root, its working directory and `/tmp` are read
/// only and its `/dev` holds only device nodes and `/dev/shm`, so a write to anywhere but the
/// scratch directory, `/dev/shm` or a grant fails, rather than land in storage gone with the
/// process. It has no network unless granted one, and with one it sees the host's files that
/// resolve names and the certificate authorities TLS clients trust, as they were when it
/// started. It has its own user, process, IPC, host-name and control-group namespaces and no
/// further user namespace, only the environment the host states and the `PWD` the launcher
/// sets, and a session of its own, and it ends with its host. Its arguments, the environment
/// among them, reach the launcher through a descriptor, never its command line.
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
    /// The directories of the host a dynamically linked program needs to start, which a
    /// sandboxed connector sees, each given read only where it exists, or as the link it is.
    pub const SYSTEM: [&str; 6] = ["/usr", "/bin", "/sbin", "/lib", "/lib64", "/lib32"];

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

    /// What a sandboxed connector sees of the host's system, read only, and with a network grant
    /// what it needs to use the network: each a flag of bubblewrap's, with where it is bound from
    /// and to.
    #[cfg(any(target_os = "linux", test))]
    fn system(network: NetworkAccess) -> Vec<(&'static str, [OsString; 2])> {
        let mut binds = Vec::new();
        for system in Self::SYSTEM.map(Path::new) {
            match std::fs::symlink_metadata(system) {
                Ok(found) if found.is_symlink() => {
                    if let Ok(target) = std::fs::read_link(system) {
                        binds.push(("--symlink", [target.into(), system.into()]));
                    }
                }
                Ok(_) => binds.push(("--ro-bind", [system.into(), system.into()])),
                Err(_) => {}
            }
        }
        let files: &[&str] = match network {
            NetworkAccess::Granted => &NETWORK_FILES,
            NetworkAccess::Denied => &[],
        };
        // Bubblewrap skips one that is not there when it makes the sandbox.
        for &file in SYSTEM_FILES.iter().chain(files) {
            binds.push(("--ro-bind-try", [file.into(), file.into()]));
        }
        binds
    }

    /// The arguments that confine `confined`'s program, with what it is granted.
    #[cfg(any(target_os = "linux", test))]
    fn confinement(confined: &Confined<'_>) -> Vec<OsString> {
        let mut args: Vec<OsString> = Vec::new();
        let mut flag = |flag: &str, values: &[&std::ffi::OsStr]| {
            args.push(flag.into());
            args.extend(values.iter().map(|value| OsString::from(*value)));
        };
        flag("--unshare-all", &[]);
        if confined.network == NetworkAccess::Granted {
            flag("--share-net", &[]);
        }
        // Its own user namespace, in which it may make none: each is a surface of the kernel.
        flag("--unshare-user", &[]);
        flag("--disable-userns", &[]);
        for only in ["--die-with-parent", "--new-session", "--clearenv"] {
            flag(only, &[]);
        }
        flag("--hostname", &["rdlt-connector".as_ref()]);
        for (bind, [from, to]) in Self::system(confined.network) {
            flag(bind, &[from.as_os_str(), to.as_os_str()]);
        }
        flag("--proc", &["/proc".as_ref()]);
        flag("--dev", &["/dev".as_ref()]);
        flag("--tmpfs", &[SHARED_MEMORY.as_ref()]);
        // Empty and read only: a path beneath it is written only where a grant makes it so.
        flag("--dir", &["/tmp".as_ref()]);
        flag("--tmpfs", &[SCRATCH.as_ref()]);
        // Bound from what was opened and checked: a link swapped since changes nothing.
        for bind in confined.binds {
            let fd = bind.fd.to_string();
            let flag_of = if bind.write {
                "--bind-fd"
            } else {
                "--ro-bind-fd"
            };
            flag(flag_of, &[fd.as_ref(), bind.at.as_os_str()]);
        }
        for (name, value) in confined.env {
            flag("--setenv", &[name, value]);
        }
        // Last, so nothing the host passes on names another place.
        flag("--setenv", &["TMPDIR".as_ref(), SCRATCH.as_ref()]);
        let program = confined.program.to_string();
        flag("--ro-bind-fd", &[program.as_ref(), PROGRAM.as_ref()]);
        // After every bind has made its mount point in the sandbox's root, and each alone, so
        // the scratch and the shared memory within stay writable. Device nodes are written
        // through a read-only mount.
        flag("--remount-ro", &["/dev".as_ref()]);
        flag("--remount-ro", &["/".as_ref()]);
        flag("--chdir", &["/".as_ref()]);
        args
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
            binds: &[],
            network: NetworkAccess::Denied,
        };
        let Launcher {
            mut command,
            given,
            held,
            ..
        } = launched(&launcher, &confined)?;
        let failed = |error| SandboxError::failed(&path, error);
        let program: OwnedFd = launcher.file().try_clone().map_err(failed)?.into();
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let given = std::iter::once((program, PROGRAM_FD))
            .chain(given)
            .collect();
        inheriting(&mut command, given, rdlt_adopt::Marking::AtOnce).map_err(failed)?;
        let output = command.output().map_err(failed)?;
        drop(held);
        if output.status.success() {
            return Ok(Arc::new(launcher));
        }
        Err(unavailable(&path, &String::from_utf8_lossy(&output.stderr)))
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
    let failed = |error| SandboxError::failed(launcher.path(), error);
    // What runs is the file opened once: a name it was given since may lie where a grant held
    // now writes, whatever its path names now.
    if launcher.names().map_err(failed)? > 1 && !launcher.linked() {
        return Err(SandboxError::Exposed {
            path: launcher.path().to_owned(),
        });
    }
    let arguments = arguments(&Bubblewrap::confinement(confined)).map_err(failed)?;
    // Executed from a descriptor above every one the launcher is given.
    let given = confined.binds.iter().map(|bind| bind.fd);
    let highest = given
        .chain([confined.socket, confined.program])
        .fold(ARGS_FD, RawFd::max);
    let (mut command, executed) =
        super::process::executed_from(launcher.file(), highest).map_err(failed)?;
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
        launched(&*self.opened()?, confined)
    }

    /// The launcher: whoever may write it decides what confines every connector.
    fn programs(&self) -> Vec<PathBuf> {
        vec![self.launcher.clone()]
    }
}
