//! Spawning a process that inherits no descriptor but those it is given.
//!
//! A host gives a connector its standard streams and a few descriptors at chosen numbers.
//! Everything the standard library opens is close-on-exec, but a descriptor the process
//! inherited, or that other code opened without the flag, is not, and another thread may open
//! one at any moment, between any look at the process's descriptors and the `fork` that copies
//! them. Only the child, after the fork, sees the set it will carry through `exec` and no
//! other: [`inheriting_below`] marks every descriptor from a number up close-on-exec there.
//!
//! Marking a descriptor in the child needs code that runs between `fork` and `exec`, which Rust
//! holds `unsafe`: in a multithreaded process only async-signal-safe calls may run there.

#[cfg(test)]
mod tests;

use std::os::fd::RawFd;
use std::os::unix::process::CommandExt as _;
use std::process::Command;

/// Has the process `command` starts inherit no descriptor numbered `first` or above.
///
/// Call it after every other hook that places a descriptor the child is given, as
/// `command-fds`'s mappings do: hooks run in the order they were registered. Every descriptor
/// from `first` up is then marked close-on-exec in the child, so `exec` closes it; none is
/// closed before, so a program executed through `/proc/self/fd` is still open when `exec` opens
/// it. The parent's descriptors are untouched.
///
/// On Linux 5.11 and later one call marks them all; on an older kernel, and elsewhere, each
/// descriptor up to the process's limit is marked in turn.
pub fn inheriting_below(command: &mut Command, first: RawFd) {
    let first = first.max(0);
    #[expect(
        unsafe_code,
        reason = "marking the child's descriptors runs between fork and exec"
    )]
    // SAFETY: between `fork` and `exec` the hook calls only the async-signal-safe `close_range`,
    // `getrlimit` and `fcntl`, allocates nothing, takes no lock, and reads only its own `first`.
    // It closes nothing, so no descriptor the standard library uses in the child is lost.
    unsafe {
        command.pre_exec(move || {
            mark_from(first);
            Ok(())
        });
    }
}

/// Marks every descriptor of this process from `first` up close-on-exec.
fn mark_from(first: RawFd) {
    #[cfg(target_os = "linux")]
    if marked_at_once(first) {
        return;
    }
    marked_one_by_one(first);
}

/// Marks every descriptor from `first` up close-on-exec in one call, where the kernel has it.
#[cfg(target_os = "linux")]
fn marked_at_once(first: RawFd) -> bool {
    let Ok(first) = libc::c_uint::try_from(first) else {
        return false;
    };
    #[expect(
        unsafe_code,
        reason = "close_range has no binding in the crates the workspace uses"
    )]
    // SAFETY: the system call takes three integers and touches no memory of the process; with
    // `CLOSE_RANGE_CLOEXEC` it marks descriptors and closes none.
    let marked = unsafe {
        libc::syscall(
            libc::SYS_close_range,
            first,
            libc::c_uint::MAX,
            libc::CLOSE_RANGE_CLOEXEC,
        )
    };
    marked == 0
}

/// Marks each descriptor from `first` up to the process's limit close-on-exec.
fn marked_one_by_one(first: RawFd) {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    #[expect(
        unsafe_code,
        reason = "reading the descriptor limit in the child has no allocation-free safe call"
    )]
    // SAFETY: `limit` is a valid `rlimit` this function owns, and `getrlimit` only writes it.
    let read = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) };
    // A limit that cannot be read is taken at its lowest common value.
    let last = if read == 0 { limit.rlim_cur } else { 1024 };
    let last = RawFd::try_from(last).unwrap_or(RawFd::MAX);
    for fd in first..last {
        #[expect(
            unsafe_code,
            reason = "each descriptor of the child is marked by number"
        )]
        // SAFETY: `fcntl` with `F_GETFD` and `F_SETFD` takes a number and touches no memory; a
        // number that names no descriptor fails, and is passed over.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags >= 0 && flags & libc::FD_CLOEXEC == 0 {
                libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
            }
        }
    }
}
