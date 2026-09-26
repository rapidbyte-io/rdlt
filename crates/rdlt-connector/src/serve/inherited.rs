//! The workspace's one audited `unsafe`: taking ownership of the socket a host passed a spawned
//! connector at a file descriptor.
//!
//! Rust cannot know that a file descriptor number names an open file nothing else owns, so
//! turning it into an owned socket is `unsafe`. [`adopt`] establishes both before [`own`] does:
//! the descriptor is an open socket; it is not close-on-exec, so this process did not open it, as
//! the standard library opens everything close-on-exec; and it is taken once per process.

#![expect(
    unsafe_code,
    reason = "adopting an inherited file descriptor needs `OwnedFd::from_raw_fd`; the one audited \
              module the workspace allows"
)]

#[cfg(test)]
mod tests;

use std::io;
use std::os::fd::{BorrowedFd, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::fs::FileTypeExt as _;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};

/// Whether this process has adopted its host's socket.
static ADOPTED: AtomicBool = AtomicBool::new(false);

/// Takes ownership of the socket the host passed this process at `fd`.
///
/// Call it first thing in `main`, before anything opens a file: a descriptor the host did not
/// pass is then closed, and no part of the process can own one it did.
///
/// # Errors
///
/// When `fd` is a standard stream, is not open, is not a Unix socket, was opened by this process
/// (it is close-on-exec, as everything the standard library opens is), or a socket was adopted
/// already.
/// The socket returned is a close-on-exec duplicate: `fd` itself is closed.
pub(crate) fn adopt(fd: RawFd) -> io::Result<UnixStream> {
    if fd <= 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("file descriptor {fd} is a standard stream, not the host's socket"),
        ));
    }
    // Resolves only while `fd` is open, on Linux and macOS alike.
    let metadata = std::fs::metadata(format!("/dev/fd/{fd}")).map_err(|error| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("file descriptor {fd} is not open: {error}"),
        )
    })?;
    if !metadata.file_type().is_socket() {
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

/// Whether open descriptor `fd` is close-on-exec.
fn close_on_exec(fd: RawFd) -> io::Result<bool> {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    // SAFETY: `adopt` checked that `fd` is open; it is borrowed for this call only, which reads
    // its flags and neither closes nor keeps it.
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let flags = fcntl(borrowed, FcntlArg::F_GETFD).map_err(io::Error::from)?;
    Ok(FdFlag::from_bits_truncate(flags).contains(FdFlag::FD_CLOEXEC))
}

/// Owns `fd`.
fn own(fd: RawFd) -> OwnedFd {
    // SAFETY: `adopt` checked that `fd` is open and not a standard stream, which the standard
    // library owns; it adopts once per process, and is called before the process opens anything,
    // so nothing else in the process owns `fd`.
    unsafe { OwnedFd::from_raw_fd(fd) }
}
