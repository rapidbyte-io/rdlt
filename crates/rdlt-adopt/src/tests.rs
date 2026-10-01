use std::io::{Read as _, Write as _};
use std::os::fd::{AsRawFd as _, IntoRawFd as _};
use std::os::unix::net::UnixStream;

use super::{adopt, own};

/// Makes `socket` look inherited: `dup2`, through which a host passes it, clears close-on-exec.
fn inherited(socket: &UnixStream) {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    fcntl(socket, FcntlArg::F_SETFD(FdFlag::empty())).expect("close-on-exec clears");
}

// The unsafe core, as Miri runs it: an owned descriptor reads and writes its socket, and closes
// it once when dropped.
#[test]
fn an_owned_descriptor_is_its_socket() {
    let (ours, theirs) = UnixStream::pair().expect("a socket pair");
    let mut owned = UnixStream::from(own(theirs.into_raw_fd()));
    owned.write_all(b"ping").expect("the owned end writes");
    let mut read = [0; 4];
    (&ours).read_exact(&mut read).expect("the other end reads");
    assert_eq!(&read, b"ping");
    drop(owned);
    assert_eq!(
        (&ours)
            .read(&mut read)
            .expect("a closed peer reads as the end"),
        0
    );
}

#[test]
fn a_standard_stream_or_a_closed_descriptor_is_not_adopted() {
    for (fd, kind) in [
        (0, std::io::ErrorKind::InvalidInput),
        (2, std::io::ErrorKind::InvalidInput),
        (i32::MAX, std::io::ErrorKind::NotFound),
    ] {
        assert_eq!(adopt(fd).expect_err("not adopted").kind(), kind, "{fd}");
    }
}

// A socket given up with `into_raw_fd` has no owner, as the host's socket in a connector has none.
#[test]
fn an_open_socket_is_adopted_once() {
    let (ours, theirs) = UnixStream::pair().expect("a socket pair");
    inherited(&theirs);
    let fd = theirs.into_raw_fd();
    let mut adopted = adopt(fd).expect("an open socket is adopted");
    // The adopted descriptor is closed: the socket is its close-on-exec duplicate.
    assert_eq!(
        adopt(fd).expect_err("closed").kind(),
        std::io::ErrorKind::NotFound
    );
    // Another open socket is refused: a process adopts one. Refused, it stays open, unowned.
    let (_, another) = UnixStream::pair().expect("a socket pair");
    inherited(&another);
    let kind = adopt(another.into_raw_fd())
        .expect_err("adopted once")
        .kind();
    assert_eq!(kind, std::io::ErrorKind::AlreadyExists);
    adopted
        .write_all(b"ping")
        .expect("the adopted socket writes");
    let mut read = [0; 4];
    (&ours).read_exact(&mut read).expect("the other end reads");
    assert_eq!(&read, b"ping");
}

// Anything this process opened itself is close-on-exec, as the standard library opens it; a
// descriptor that is not, or is not a socket, is refused, and left to its owner.
#[test]
fn a_descriptor_this_process_owns_is_refused_and_left_open() {
    let mut file = std::fs::File::open("/dev/null").expect("a file opens");
    let kind = adopt(file.as_raw_fd())
        .expect_err("a file is no socket")
        .kind();
    assert_eq!(kind, std::io::ErrorKind::InvalidInput);
    let (mut ours, theirs) = UnixStream::pair().expect("a socket pair");
    let kind = adopt(theirs.as_raw_fd())
        .expect_err("close-on-exec, so ours")
        .kind();
    assert_eq!(kind, std::io::ErrorKind::InvalidInput);
    // Both are still open, and still ours.
    assert_eq!(file.read(&mut [0; 1]).expect("the file still reads"), 0);
    ours.write_all(b"x").expect("the socket still writes");
    drop(theirs);
}
