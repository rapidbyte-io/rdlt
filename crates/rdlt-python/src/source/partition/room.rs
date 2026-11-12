//! How an async read waits for room in its channel with its event loop free.
//!
//! A push that finds the channel full is sent by a task of the runtime; for a notice, which stays
//! its router's to be said again, the task waits for room alone. Either then writes to a socket
//! the read's event loop watches, so the loop wakes with no thread but its own taking the GIL.

use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{self, error::TryRecvError};
use tokio::task::AbortHandle;

use crate::source::partition::Pushed;
use crate::source::partition::pushes::Pushes;

/// A socket pair, the read's event loop watching one end and a task writing to the other once
/// what waited for room was sent.
///
/// Both ends close together, once nothing holds the pair, so no write meets a closed reader.
struct Wake {
    watched: UnixStream,
    rung: UnixStream,
}

impl Wake {
    fn new() -> io::Result<Self> {
        let (watched, rung) = UnixStream::pair()?;
        watched.set_nonblocking(true)?;
        rung.set_nonblocking(true)?;
        Ok(Self { watched, rung })
    }

    /// Wakes the loop: a socket too full to take the byte already holds a wake.
    fn ring(&self) {
        (&self.rung).write_all(&[0]).ok();
    }

    /// Takes what was written, so the loop sleeps until the next wake.
    fn drain(&self) {
        let mut buffer = [0_u8; 64];
        while matches!((&self.watched).read(&mut buffer), Ok(read) if read > 0) {}
    }
}

/// The task that sends a push that found no room, or waits for room for a notice, and what it
/// came to: true once the push was sent or room came, false once the read has gone.
///
/// Dropping it aborts the task, and a send it aborts holds no room.
struct Flight {
    task: AbortHandle,
    came: oneshot::Receiver<bool>,
}

impl Drop for Flight {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// What an async read has waiting for room, one thing at a time, and the socket it is woken by.
#[derive(Default)]
pub(super) struct Room {
    wake: Option<Arc<Wake>>,
    flight: Option<Flight>,
    /// What is left of the item whose push is in flight.
    rest: Option<Pushes>,
}

impl Room {
    /// The descriptor of the socket the read's event loop watches, made at the first ask.
    pub(super) fn watched(&mut self) -> io::Result<RawFd> {
        Ok(self.wake()?.watched.as_raw_fd())
    }

    /// Takes what made the watched socket readable.
    pub(super) fn woken(&self) {
        if let Some(wake) = &self.wake {
            wake.drain();
        }
    }

    /// Has a task of `runtime` send `push` through `sender`, or wait for room where there is no
    /// push, then wake the loop.
    ///
    /// A wait for room joins one still in flight.
    pub(super) fn wait(
        &mut self,
        runtime: &Runtime,
        sender: &mpsc::Sender<Pushed>,
        push: Option<Pushed>,
    ) -> io::Result<()> {
        let waiting =
            |flight: &mut Flight| matches!(flight.came.try_recv(), Err(TryRecvError::Empty));
        if push.is_none() && self.flight.as_mut().is_some_and(waiting) {
            return Ok(());
        }
        let wake = Arc::clone(self.wake()?);
        let (said, came) = oneshot::channel();
        let sender = sender.clone();
        let task = runtime.spawn(async move {
            let sent = match push {
                Some(push) => sender.send(push).await.is_ok(),
                None => sender.reserve().await.is_ok(),
            };
            drop(sender);
            said.send(sent).ok();
            wake.ring();
        });
        self.flight = Some(Flight {
            task: task.abort_handle(),
            came,
        });
        Ok(())
    }

    /// Keeps `rest`, what is left of the item whose push is in flight.
    pub(super) fn keep(&mut self, rest: Pushes) {
        self.rest = Some(rest);
    }

    /// What the flight came to, with the rest of its item; none while it still waits, as it may
    /// after a stray wake.
    pub(super) fn landed(&mut self) -> Option<(bool, Option<Pushes>)> {
        let came = match self.flight.as_mut().map(|flight| flight.came.try_recv()) {
            None | Some(Ok(true)) => true,
            Some(Err(TryRecvError::Empty)) => return None,
            Some(Ok(false) | Err(TryRecvError::Closed)) => false,
        };
        self.flight = None;
        Some((came, self.rest.take()))
    }

    fn wake(&mut self) -> io::Result<&Arc<Wake>> {
        let wake = match self.wake.take() {
            Some(wake) => wake,
            None => Arc::new(Wake::new()?),
        };
        Ok(self.wake.insert(wake))
    }
}
