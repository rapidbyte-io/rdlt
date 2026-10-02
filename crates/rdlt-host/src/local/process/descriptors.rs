//! The descriptors a spawned connector is given: its standard streams and its socket, and no
//! other of its host's.
//!
//! Everything the standard library opens is closed on exec. A descriptor the host was
//! started with, or that other code opened to be inherited, is not, and nothing here may
//! close it in the child: each is covered in the child with one to the null device, so the
//! connector holds nothing of what the host's descriptor leads to.

use std::fs::File;
use std::os::fd::{OwnedFd, RawFd};
use std::sync::{Mutex, MutexGuard, PoisonError};

use command_fds::FdMapping;

#[cfg(test)]
mod tests;

/// Held from before a connector's socket is made until the connector is spawned: where a
/// descriptor is made and then marked to close on exec in two steps, as on macOS, no other
/// spawn of this host falls between them.
static SPAWNING: Mutex<()> = Mutex::new(());

/// Takes the lock a spawn holds.
pub(super) fn spawning() -> MutexGuard<'static, ()> {
    SPAWNING.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What a connector is given beside its standard streams: `given`, and the null device at
/// each descriptor of this process a child would inherit that `given` does not replace.
pub(super) fn mappings(given: Vec<FdMapping>) -> std::io::Result<Vec<FdMapping>> {
    let taken: Vec<RawFd> = given.iter().map(|mapping| mapping.child_fd).collect();
    let covered: Vec<RawFd> = inheritable()?
        .into_iter()
        .filter(|fd| *fd > 2 && !taken.contains(fd))
        .collect();
    if covered.is_empty() {
        return Ok(given);
    }
    let null = File::open("/dev/null")?;
    let mut mappings = given;
    for child_fd in covered {
        let parent_fd = OwnedFd::from(null.try_clone()?);
        mappings.push(FdMapping {
            parent_fd,
            child_fd,
        });
    }
    Ok(mappings)
}

/// The descriptors of this process that are not closed on exec, as `/proc` says.
///
/// # Errors
///
/// The error of listing them: a host that cannot tell what a connector would inherit
/// spawns none.
#[cfg(target_os = "linux")]
fn inheritable() -> std::io::Result<Vec<RawFd>> {
    let mut inheritable = Vec::new();
    for entry in std::fs::read_dir("/proc/self/fdinfo")? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(fd) = name.to_str().and_then(|name| name.parse().ok()) else {
            continue;
        };
        // One closed since it was listed, the listing's own among them, is none to inherit.
        let Ok(info) = std::fs::read_to_string(entry.path()) else {
            continue;
        };
        if !closed_on_exec(&info) {
            inheritable.push(fd);
        }
    }
    Ok(inheritable)
}

/// The descriptors of this process that are not closed on exec: unknown where no `/proc`
/// says, so none is covered there.
#[cfg(not(target_os = "linux"))]
fn inheritable() -> std::io::Result<Vec<RawFd>> {
    Ok(Vec::new())
}

/// Whether the descriptor `info` describes, as `/proc/self/fdinfo` does, is closed on exec;
/// one whose flags cannot be read is taken not to be.
#[cfg(target_os = "linux")]
fn closed_on_exec(info: &str) -> bool {
    let flags = info
        .lines()
        .find_map(|line| line.strip_prefix("flags:"))
        .and_then(|flags| u32::from_str_radix(flags.trim(), 8).ok());
    flags.is_some_and(|flags| flags & rustix::fs::OFlags::CLOEXEC.bits() != 0)
}
