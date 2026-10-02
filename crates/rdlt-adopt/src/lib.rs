//! The workspace's audited `unsafe` code: taking ownership of the socket a host passed a spawned
//! connector at a file descriptor, and spawning a connector that inherits no other descriptor
//! of its host's ([`inheriting_only`]).
//!
//! Rust cannot know that a file descriptor number names an open file nothing else owns, so
//! turning it into an owned socket is `unsafe`. [`adopt`] establishes both before it owns the
//! descriptor: it is an open socket; it is not close-on-exec, so this process did not open it, as
//! the standard library opens everything close-on-exec; and it is taken once per process.
//!
//! Every other crate of the workspace forbids `unsafe` code. This one holds nothing else, and
//! allows it only at the places that need it.
//!
//! [`adopt`] is a safe function, so that a crate forbidding `unsafe` code can call it, and what
//! it cannot check is left to its caller: that nothing else in the process owns the descriptor.
//! A process that made a socket, cleared its close-on-exec flag and kept its owner would have it
//! owned twice. `rdlt-connector` calls it first thing in a connector's `main`, on the descriptor
//! its host passed, and is the only crate the workspace's dependency rule lets use it;
//! `rdlt-host` alone uses [`inheriting_only`].
//!
//! ```no_run
//! let socket = rdlt_adopt::adopt(3)?;
//! # Ok::<(), std::io::Error>(())
//! ```

#![cfg(unix)]

mod exec;
#[cfg(test)]
mod tests;

use std::io;
use std::os::fd::{BorrowedFd, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};

pub use exec::{Marking, ONE_BY_ONE_CAP, inheriting_only, marks_at_once};

/// Whether this process has adopted its host's socket.
static ADOPTED: AtomicBool = AtomicBool::new(false);

/// Takes ownership of the socket the host passed this process at `fd`.
///
/// Call it first thing in `main`, before anything opens a file: a descriptor the host did not
/// pass is then closed, and no part of the process can own one it did. Nothing else in the
/// process may own `fd`: the checks below refuse what this process opened in the usual way, and
/// cannot see an owner of a descriptor whose close-on-exec flag the process cleared itself.
///
/// # Errors
///
/// When `fd` is a standard stream, is not open, is not a Unix socket, was opened by this process
/// (it is close-on-exec, as everything the standard library opens is), or a socket was adopted
/// already.
/// The socket returned is a close-on-exec duplicate: `fd` itself is closed.
pub fn adopt(fd: RawFd) -> io::Result<UnixStream> {
    if fd <= 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("file descriptor {fd} is a standard stream, not the host's socket"),
        ));
    }
    // Asked of the descriptor itself: `/dev/fd`, on macOS, at times answers that a descriptor
    // this process holds is not there.
    if !is_open(fd) {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("file descriptor {fd} is not open"),
        ));
    }
    let metadata = nix::sys::stat::fstat(borrowed(fd)).map_err(io::Error::from)?;
    let kind = nix::sys::stat::SFlag::from_bits_truncate(metadata.st_mode);
    if kind & nix::sys::stat::SFlag::S_IFMT != nix::sys::stat::SFlag::S_IFSOCK {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("file descriptor {fd} is not a socket"),
        ));
    }
    // The standard library opens every file close-on-exec, and `dup2`, through which the host
    // passes its socket, clears the flag: a descriptor that has it is this process's own.
    if close_on_exec(fd)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("file descriptor {fd} was opened by this process, not passed by its host"),
        ));
    }
    if ADOPTED.swap(true, Ordering::SeqCst) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "this process already adopted its host's socket",
        ));
    }
    let inherited = UnixStream::from(own(fd));
    // A socket of another family fails here, and is closed as `inherited` drops.
    inherited.local_addr()?;
    // The descriptor came through `dup2`, which clears close-on-exec, so a process the connector
    // starts would keep the host's socket open. The duplicate is close-on-exec, and `fd` closes.
    inherited.try_clone()
}

/// Whether descriptor `fd` is open in this process, as the kernel answers for the number: a
/// connector's own check of what it was started with asks this, as [`adopt`] does.
pub fn is_open(fd: RawFd) -> bool {
    #[expect(
        unsafe_code,
        reason = "asking whether a number names a descriptor takes the number"
    )]
    // SAFETY: `fcntl` with `F_GETFD` takes a number and touches no memory; a number that names
    // no descriptor fails, and nothing is opened, closed or kept.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    flags >= 0
}

/// Open descriptor `fd`, borrowed.
fn borrowed(fd: RawFd) -> BorrowedFd<'static> {
    #[expect(
        unsafe_code,
        reason = "reading an inherited descriptor's state borrows it"
    )]
    // SAFETY: `adopt` checked that `fd` is open, and nothing closes it while `adopt` reads its
    // type and flags; the borrow is used for those reads alone and neither closes nor keeps it.
    unsafe {
        BorrowedFd::borrow_raw(fd)
    }
}

/// Whether open descriptor `fd` is close-on-exec.
fn close_on_exec(fd: RawFd) -> io::Result<bool> {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    let flags = fcntl(borrowed(fd), FcntlArg::F_GETFD).map_err(io::Error::from)?;
    Ok(FdFlag::from_bits_truncate(flags).contains(FdFlag::FD_CLOEXEC))
}

/// Owns `fd`.
#[expect(
    unsafe_code,
    reason = "adopting an inherited descriptor needs `OwnedFd::from_raw_fd`"
)]
fn own(fd: RawFd) -> OwnedFd {
    // SAFETY: `adopt` checked that `fd` is open and not a standard stream, which the standard
    // library owns; it adopts once per process, and is called before the process opens anything,
    // so nothing else in the process owns `fd`.
    unsafe { OwnedFd::from_raw_fd(fd) }
}
