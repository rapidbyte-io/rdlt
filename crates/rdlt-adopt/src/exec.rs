//! Spawning a process that inherits no descriptor but those it is given.
//!
//! A host gives a connector its standard streams and a few descriptors at chosen numbers.
//! Everything the standard library opens is close-on-exec, but a descriptor the process
//! inherited, or that other code opened without the flag, is not, and another thread may open
//! one at any moment, between any look at the process's descriptors and the `fork` that copies
//! them. Only the child, after the fork, sees the set it will carry through `exec` and no
//! other: [`inheriting_only`] marks every descriptor but those it is given close-on-exec there.
//!
//! Marking a descriptor in the child needs code that runs between `fork` and `exec`, which Rust
//! holds `unsafe`: in a multithreaded process the child may make plain system calls there, and
//! may neither take a lock nor allocate.

#[cfg(test)]
mod tests;

use std::io;
use std::os::fd::RawFd;
use std::os::unix::process::CommandExt as _;
use std::process::Command;
use std::sync::OnceLock;

/// Descriptors the loop of [`Marking::OrOneByOne`] marks at most: it stops at the process's
/// soft limit, or at this number, whichever is lower.
pub const ONE_BY_ONE_CAP: RawFd = 65_536;

/// How a child's descriptors are marked where the kernel cannot mark a range in one call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Marking {
    /// Not at all, and the spawn fails: only `close_range` with `CLOSE_RANGE_CLOEXEC`, on Linux
    /// 5.11 and later, marks every descriptor whatever its number.
    AtOnce,
    /// Each descriptor in turn, up to the soft limit capped at [`ONE_BY_ONE_CAP`]: a measure of
    /// hygiene for a trusted child, which misses a descriptor numbered above the cap.
    OrOneByOne,
}

/// Has the process `command` starts inherit no descriptor but its standard streams and those
/// numbered `given`.
///
/// Call it after every other hook that places a descriptor the child is given, as
/// `command-fds`'s mappings do: hooks run in the order they were registered. Every other
/// descriptor from 3 up, below the highest given as well as above it, is then marked
/// close-on-exec in the child, so `exec` closes it; none is closed before, so a program executed
/// through `/proc/self/fd` is still open when `exec` opens it. The parent's descriptors are
/// untouched. Where the kernel cannot mark a range at once, `marking` says what is done.
pub fn inheriting_only(command: &mut Command, given: &[RawFd], marking: Marking) {
    let mut kept: Vec<RawFd> = given.iter().copied().filter(|fd| *fd > 2).collect();
    kept.sort_unstable();
    kept.dedup();
    let kept = kept.into_boxed_slice();
    #[expect(
        unsafe_code,
        reason = "marking the child's descriptors runs between fork and exec"
    )]
    // SAFETY: after the fork the hook makes only plain system calls, which take no lock and
    // allocate nothing, as is all that matters there; it reads `kept`, allocated before, and
    // closes nothing, so no descriptor the standard library uses in the child is lost.
    unsafe {
        command.pre_exec(move || mark_except(&kept, marking, AT_ONCE));
    }
}

/// Whether this kernel marks a range of descriptors close-on-exec in one call, as
/// [`Marking::AtOnce`] needs: asked once, of a descriptor this process holds.
pub fn marks_at_once() -> bool {
    static MARKS: OnceLock<bool> = OnceLock::new();
    *MARKS.get_or_init(|| {
        use std::os::fd::AsRawFd as _;
        // Opened close-on-exec, as the standard library opens a file: marking it changes nothing.
        std::fs::File::open("/dev/null").is_ok_and(|file| {
            let fd = file.as_raw_fd();
            AT_ONCE(fd, fd)
        })
    })
}

/// Marks every descriptor of this process from 3 up but `kept`, sorted, each above 2, as
/// `marking` says, through `at_once` where it can.
fn mark_except(
    kept: &[RawFd],
    marking: Marking,
    at_once: fn(RawFd, RawFd) -> bool,
) -> io::Result<()> {
    let mut first: RawFd = 3;
    for &fd in kept {
        if fd > first {
            mark_range(first, fd - 1, marking, at_once)?;
        }
        let Some(next) = fd.checked_add(1) else {
            return Ok(());
        };
        first = next;
    }
    mark_range(first, RawFd::MAX, marking, at_once)
}

/// Marks descriptors `first` to `last` close-on-exec, as `marking` says.
fn mark_range(
    first: RawFd,
    last: RawFd,
    marking: Marking,
    at_once: fn(RawFd, RawFd) -> bool,
) -> io::Result<()> {
    if at_once(first, last) {
        return Ok(());
    }
    match marking {
        Marking::AtOnce => Err(io::Error::last_os_error()),
        Marking::OrOneByOne => {
            marked_one_by_one(first, last);
            Ok(())
        }
    }
}

/// Marks descriptors `first` to `last` close-on-exec in one call, where the kernel has it.
#[cfg(target_os = "linux")]
fn marked_at_once(first: RawFd, last: RawFd) -> bool {
    // No descriptor is numbered above `RawFd::MAX`, so a range up to it is every one from
    // `first` up.
    let (Ok(first), Ok(last)) = (libc::c_uint::try_from(first), libc::c_uint::try_from(last))
    else {
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
            last,
            libc::CLOSE_RANGE_CLOEXEC,
        )
    };
    marked == 0
}

/// What marks a range of descriptors close-on-exec in one call, and whether it did.
#[cfg(target_os = "linux")]
const AT_ONCE: fn(RawFd, RawFd) -> bool = marked_at_once;

/// This platform marks no range at once.
#[cfg(not(target_os = "linux"))]
const AT_ONCE: fn(RawFd, RawFd) -> bool = |_, _| false;

/// Marks each descriptor from `first` to `last` close-on-exec, below the process's soft limit
/// and [`ONE_BY_ONE_CAP`].
fn marked_one_by_one(first: RawFd, last: RawFd) {
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
    let soft = if read == 0 { limit.rlim_cur } else { 1024 };
    let end = RawFd::try_from(soft).map_or(ONE_BY_ONE_CAP, |soft| soft.min(ONE_BY_ONE_CAP));
    for fd in first..end.min(last.saturating_add(1)) {
        #[expect(
            unsafe_code,
            reason = "each descriptor of the child is marked by number"
        )]
        // SAFETY: `fcntl` with `F_GETFD` and `F_SETFD` takes a number and touches no memory; a
        // number that names no descriptor fails, and is passed over.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags >= 0 {
                libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
            }
        }
    }
}
