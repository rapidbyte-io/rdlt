//! What opens the streams of a connection to a connector: its control connection first, then a
//! stream for each data call, each passed to the connector as it opens.

#[cfg(test)]
mod tests;

use std::io;
use std::os::fd::{AsFd as _, OwnedFd};
use std::sync::Arc;

use rdlt_connector::BoxFuture;
pub use rdlt_wire::opener::Opening;

use crate::network::Stream;

/// Opens the streams of one connection to a connector: the control connection first, then a
/// stream for each data call, each of which the connector serves as what it is opened as.
pub trait Opener: Send + Sync + 'static {
    /// Opens a stream to the connector, as `opening` says it is: the host's end of it.
    ///
    /// # Errors
    ///
    /// Where the stream cannot be made or passed: this process has no descriptor left, or the
    /// connector's end of the opener is gone.
    fn open(&self, opening: Opening) -> BoxFuture<'_, io::Result<Box<dyn Stream>>>;
}

/// An opener over a socket whose other end a connector takes streams from: each stream a new
/// socket pair, one end passed to the connector and closed here, the other the host's.
#[derive(Debug)]
pub struct Passing {
    opener: rdlt_wire::opener::Passing,
}

impl Passing {
    /// The opener whose host's end is `socket`, a Unix stream socket.
    ///
    /// # Errors
    ///
    /// Where it cannot be set non-blocking or registered with the runtime.
    pub fn new(socket: OwnedFd) -> io::Result<Self> {
        Ok(Self {
            opener: rdlt_wire::opener::Passing::new(socket)?,
        })
    }
}

impl Opener for Passing {
    fn open(&self, opening: Opening) -> BoxFuture<'_, io::Result<Box<dyn Stream>>> {
        Box::pin(async move {
            let (host, connector) = std::os::unix::net::UnixStream::pair()?;
            self.opener.pass(opening, connector.as_fd()).await?;
            // The host holds one descriptor a stream: the connector's end is the connector's.
            drop(connector);
            host.set_nonblocking(true)?;
            Ok(Box::new(tokio::net::UnixStream::from_std(host)?) as Box<dyn Stream>)
        })
    }
}

/// An opener whose control connection goes through `watched`, as a host watches or cuts the
/// connection to a connector a kill is to reach, and whose data streams go through `cut`, which a
/// kill ends with no count of its own.
pub(crate) struct Wrapping<O> {
    pub(crate) inner: O,
    pub(crate) watched: Wrap,
    pub(crate) cut: Wrap,
}

/// What a stream an opener opened goes through.
pub(crate) type Wrap = Arc<dyn Fn(Box<dyn Stream>) -> Box<dyn Stream> + Send + Sync>;

impl<O: Opener> Opener for Wrapping<O> {
    fn open(&self, opening: Opening) -> BoxFuture<'_, io::Result<Box<dyn Stream>>> {
        Box::pin(async move {
            let stream = self.inner.open(opening).await?;
            Ok(match opening {
                Opening::Control => (self.watched)(stream),
                Opening::Data => (self.cut)(stream),
            })
        })
    }
}

impl Opener for Box<dyn Opener> {
    fn open(&self, opening: Opening) -> BoxFuture<'_, io::Result<Box<dyn Stream>>> {
        (**self).open(opening)
    }
}

impl Opener for Arc<dyn Opener> {
    fn open(&self, opening: Opening) -> BoxFuture<'_, io::Result<Box<dyn Stream>>> {
        (**self).open(opening)
    }
}
