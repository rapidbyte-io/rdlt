//! The opener: the socket a host passes a spawned connector each stream of its connection over,
//! one descriptor to a one-byte message saying what it is, the control connection first, then a
//! stream for each data call.
//!
//! A descriptor received is close-on-exec, so a process the connector starts keeps none of its
//! host's streams: on Linux the kernel marks it as it arrives; on macOS, which cannot, it is
//! marked at once after, and a process started in that instant could inherit it.

#[cfg(test)]
mod tests;

use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{BorrowedFd, OwnedFd};

use rustix::net::{
    RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SendAncillaryBuffer,
    SendAncillaryMessage, SendFlags, SocketType,
};
use tokio::io::Interest;
use tokio::net::UnixStream;

/// Open files: the most one process may open on macOS, which refuses an unlimited soft limit.
const OPEN_MAX: u64 = 10_240;

/// Raises this process's soft limit of open files to its hard limit, where the system lets it:
/// each data call to a spawned connector holds a descriptor of its own.
///
/// rdlt's own binaries call it as they start, a spawned connector as it takes its opener; the
/// host library leaves its embedder's limits alone.
pub fn raise_open_files() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    let limit = getrlimit(Resource::Nofile);
    let raised = limit.maximum.unwrap_or(OPEN_MAX);
    if limit.current.is_some_and(|current| current >= raised) {
        return;
    }
    let wanted = Rlimit {
        current: Some(raised),
        maximum: limit.maximum,
    };
    // A system that refuses keeps the limit it had: calls beyond it fail as transient.
    setrlimit(Resource::Nofile, wanted).ok();
}

/// What a stream passed over an opener is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Opening {
    /// The connection's control connection, which carries the protocol's other calls.
    Control,
    /// A data call's stream, whose first frame names its call.
    Data,
}

impl Opening {
    const fn byte(self) -> u8 {
        match self {
            Self::Control => b'c',
            Self::Data => b'd',
        }
    }

    const fn of(byte: u8) -> Option<Self> {
        match byte {
            b'c' => Some(Self::Control),
            b'd' => Some(Self::Data),
            _ => None,
        }
    }
}

/// The opener's space for one descriptor beside its message.
const SPACE: usize = rustix::cmsg_space!(ScmRights(1));

/// The host's end of an opener, which passes it streams.
#[derive(Debug)]
pub struct Passing {
    socket: UnixStream,
}

impl Passing {
    /// The host's end `socket` of an opener, a Unix stream socket.
    ///
    /// # Errors
    ///
    /// Where it cannot be set non-blocking or registered with the runtime.
    pub fn new(socket: OwnedFd) -> io::Result<Self> {
        Ok(Self {
            socket: registered(socket)?,
        })
    }

    /// Passes `stream` over the opener as `opening`; the host closes its copy once it returns.
    ///
    /// # Errors
    ///
    /// Where the opener's other end is gone, or passing fails.
    pub async fn pass(&self, opening: Opening, stream: BorrowedFd<'_>) -> io::Result<()> {
        let byte = [opening.byte()];
        loop {
            self.socket.writable().await?;
            let sent = self.socket.try_io(Interest::WRITABLE, || {
                let mut space = [MaybeUninit::uninit(); SPACE];
                let mut control = SendAncillaryBuffer::new(&mut space);
                let descriptors = [stream];
                if !control.push(SendAncillaryMessage::ScmRights(&descriptors)) {
                    return Err(io::Error::other(
                        "no room for a descriptor beside its message",
                    ));
                }
                let slices = [io::IoSlice::new(&byte)];
                Ok(rustix::net::sendmsg(
                    &self.socket,
                    &slices,
                    &mut control,
                    flags(),
                )?)
            });
            match sent {
                Ok(1) => return Ok(()),
                Ok(_) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(error) => return Err(error),
            }
        }
    }
}

/// The flags a stream is passed with: on Linux, a peer gone fails the send rather than raising
/// `SIGPIPE`.
fn flags() -> SendFlags {
    #[cfg(target_os = "linux")]
    {
        SendFlags::NOSIGNAL
    }
    #[cfg(not(target_os = "linux"))]
    {
        SendFlags::empty()
    }
}

/// The connector's end of an opener, which takes the streams its host passes.
#[derive(Debug)]
pub struct Accepting {
    socket: UnixStream,
}

impl Accepting {
    /// The connector's end `socket` of an opener, a Unix stream socket.
    ///
    /// # Errors
    ///
    /// Where it cannot be set non-blocking or registered with the runtime.
    pub fn new(socket: OwnedFd) -> io::Result<Self> {
        Ok(Self {
            socket: registered(socket)?,
        })
    }

    /// The next stream the host passed, and what it is; `None` once the host closed the opener.
    ///
    /// # Errors
    ///
    /// Where receiving fails, or what arrived is no stream socket with its message: a message
    /// without a descriptor, a descriptor cut short, or one of another kind.
    pub async fn next(&self) -> io::Result<Option<(Opening, OwnedFd)>> {
        loop {
            self.socket.readable().await?;
            match self
                .socket
                .try_io(Interest::READABLE, || received(&self.socket))
            {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                received => return received,
            }
        }
    }
}

/// `socket`, a Unix stream socket, set non-blocking and registered with the runtime.
fn registered(socket: OwnedFd) -> io::Result<UnixStream> {
    let socket = std::os::unix::net::UnixStream::from(socket);
    socket.set_nonblocking(true)?;
    UnixStream::from_std(socket)
}

/// The message and descriptor `socket` holds next, `None` at its end.
fn received(socket: &UnixStream) -> io::Result<Option<(Opening, OwnedFd)>> {
    let mut byte = [0_u8; 1];
    let mut space = [MaybeUninit::uninit(); SPACE];
    let mut control = RecvAncillaryBuffer::new(&mut space);
    let mut slices = [io::IoSliceMut::new(&mut byte)];
    let message = rustix::net::recvmsg(socket, &mut slices, &mut control, receiving())?;
    let mut descriptors: Vec<OwnedFd> = Vec::new();
    for ancillary in control.drain() {
        if let RecvAncillaryMessage::ScmRights(passed) = ancillary {
            descriptors.extend(passed);
        }
    }
    if message.bytes == 0 && descriptors.is_empty() {
        return Ok(None);
    }
    let refused = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_owned());
    if message.flags.contains(rustix::net::ReturnFlags::CTRUNC) {
        return Err(refused("a stream passed with its descriptors cut short"));
    }
    let Some(opening) = Opening::of(byte[0]).filter(|_| message.bytes == 1) else {
        return Err(refused("a message over the opener that names no stream"));
    };
    let mut passed = descriptors.into_iter();
    let (Some(stream), None) = (passed.next(), passed.next()) else {
        return Err(refused(
            "a message over the opener without exactly one descriptor",
        ));
    };
    #[cfg(not(target_os = "linux"))]
    rustix::io::fcntl_setfd(&stream, rustix::io::FdFlags::CLOEXEC)?;
    if rustix::net::sockopt::socket_type(&stream)? != SocketType::STREAM {
        return Err(refused(
            "a descriptor over the opener that is no stream socket",
        ));
    }
    Ok(Some((opening, stream)))
}

/// The flags a stream is received with: on Linux, close-on-exec as it arrives.
fn receiving() -> RecvFlags {
    #[cfg(target_os = "linux")]
    {
        RecvFlags::CMSG_CLOEXEC
    }
    #[cfg(not(target_os = "linux"))]
    {
        RecvFlags::empty()
    }
}
