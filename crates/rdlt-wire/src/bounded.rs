//! A call's body, its messages each held to their bounds before what reads the body sees them.
//!
//! A gRPC message is a five-byte prefix, its length among it, then the message. A decoder that
//! reads the prefix reserves the length it declares before the bytes arrive, and decodes the
//! message whole into what its fields become. The body passes a message on only once all of it
//! has arrived: within the wire bound of its call, and counted by its [scan](crate::scan) within
//! the bound of what it decodes to. A message beyond either, one the scan cannot walk, and one
//! the body ends within fail the call before anything decodes them.
//!
//! Where a connection shares a window, a message takes room in it for its whole length as its
//! prefix arrives, and gives it back once passed on. A body that finds no room reads no further,
//! so HTTP/2's flow control holds its sender, until a message that has room is passed on: each
//! message with room is read to its end, so room always comes back, and every sender is held
//! rather than refused.
//!
//! Where the body is [charged](charging), a message is passed on once what its scan counts is
//! charged, and the charge is held until it is decoded.

mod charge;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf as _, Bytes};
use http_body::{Body, Frame, SizeHint};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};
use tonic::Status;

pub use self::charge::{Charge, Charged, Charging, Held, charging, current};
use crate::limits::Class;
use crate::scan::{Form, decoded};

/// Bytes: a message's prefix, a flag and its length.
const PREFIX: usize = 5;

/// What a call's messages are held to.
#[derive(Clone, Copy, Debug)]
pub struct Bounds {
    /// What each message is.
    pub class: Class,
    /// The form of each message.
    pub form: Option<&'static Form>,
    /// The most bytes a message may take on the wire.
    pub wire: usize,
    /// The most bytes a message of `form` may hold decoded.
    pub decoded: usize,
}

impl Bounds {
    /// The bounds of a message of `class` and `form`, within `limits`.
    pub fn of(limits: &crate::Limits, class: Class, form: Option<&'static Form>) -> Self {
        Self {
            class,
            form,
            wire: limits.decoding(class),
            decoded: limits.decoded(class),
        }
    }
}

/// Bytes of messages still arriving that the bodies of one connection may hold together.
#[derive(Clone, Debug)]
pub struct Window {
    room: Arc<Semaphore>,
    bytes: u32,
}

impl Window {
    /// A window of `bytes`, at most `u32::MAX`.
    pub fn new(bytes: usize) -> Self {
        let bytes = u32::try_from(bytes).unwrap_or(u32::MAX);
        Self {
            room: Arc::new(Semaphore::new(bytes as usize)),
            bytes,
        }
    }

    /// The room taken in the window now.
    pub fn taken(&self) -> usize {
        (self.bytes as usize).saturating_sub(self.room.available_permits())
    }
}

/// Room a message is taking in a window, or has taken.
type Taking = Pin<Box<dyn Future<Output = Result<OwnedSemaphorePermit, AcquireError>> + Send>>;

/// The room the message arriving holds in the window.
enum Room {
    /// None, or no window.
    Free,
    /// Waiting for room.
    Taking(Taking),
    /// Room for the whole message, given back as it is dropped.
    Taken { _room: OwnedSemaphorePermit },
}

/// A body whose messages are held to their bounds before they are passed on.
pub struct Bounded {
    inner: tonic::body::Body,
    bounds: Bounds,
    window: Option<Window>,
    /// What has arrived of the message being received, and perhaps of those after it, in no
    /// more room than four times what arrived, and no more than the message's whole length.
    arriving: Vec<u8>,
    room: Room,
    /// Trailers that ended the body, passed on once every message before them has been.
    trailers: Option<tonic::codegen::http::HeaderMap>,
    done: bool,
    /// Whoever each message is charged to before it is passed on, where anyone is.
    charge: Option<Arc<dyn Charge>>,
    /// The charge of the message passed on last.
    charged: Charged,
    /// A message whole, waiting for its charge.
    charging: Option<(Charging, Bytes)>,
}

impl Bounded {
    /// `inner`, its messages held to `bounds`, and those still arriving to `window`, where one
    /// is given.
    pub fn new(inner: tonic::body::Body, bounds: Bounds, window: Option<Window>) -> Self {
        Self {
            inner,
            bounds,
            window,
            arriving: Vec::new(),
            room: Room::Free,
            trailers: None,
            done: false,
            charge: None,
            charged: Charged::default(),
            charging: None,
        }
    }

    /// The body, each message charged to `charge` before it is passed on, its charge held in
    /// `charged`, where a charge is given.
    #[must_use]
    pub fn charged(mut self, charge: Option<Arc<dyn Charge>>, charged: Charged) -> Self {
        (self.charge, self.charged) = (charge, charged);
        self
    }

    /// Releases the charge of the message passed on last: it has been decoded.
    pub fn release(&self) {
        self.charged.release();
    }

    /// The length the arriving message's prefix declares, where it has arrived, within the wire
    /// bound.
    fn declared(&self) -> Result<Option<usize>, Status> {
        if self.arriving.len() < PREFIX {
            return Ok(None);
        }
        let declared = (&self.arriving[1..PREFIX]).get_u32();
        let length = usize::try_from(declared).unwrap_or(usize::MAX);
        if length > self.bounds.wire {
            return Err(Status::out_of_range(format!(
                "a message of {length} bytes is too large, beyond the limit of {} bytes",
                self.bounds.wire
            )));
        }
        Ok(Some(length))
    }

    /// Whether the arriving message of `length` has room in the window, taking it if there is.
    fn roomed(&mut self, length: usize, context: &mut Context<'_>) -> Poll<Result<(), Status>> {
        let Some(window) = &self.window else {
            return Poll::Ready(Ok(()));
        };
        if let Room::Free = self.room {
            let wanted = u32::try_from(PREFIX.saturating_add(length)).unwrap_or(u32::MAX);
            let taking = Arc::clone(&window.room).acquire_many_owned(wanted.min(window.bytes));
            self.room = Room::Taking(Box::pin(taking));
        }
        if let Room::Taking(taking) = &mut self.room {
            match taking.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(taken)) => self.room = Room::Taken { _room: taken },
                Poll::Ready(Err(_)) => {
                    return Poll::Ready(Err(Status::internal("the connection's window closed")));
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    /// The arriving message of `length`, taken whole, checked against what it decodes to: the
    /// message, and what its scan counts it holds decoded, its length where it has no form.
    fn whole(&mut self, length: usize) -> Result<(Bytes, usize), Status> {
        let rest = self.arriving.split_off(PREFIX + length);
        let message = Bytes::from(std::mem::replace(&mut self.arriving, rest));
        // Its room is given back as it is passed on.
        self.room = Room::Free;
        let Some(form) = self.bounds.form else {
            return Ok((message, length));
        };
        let bound = self.bounds.decoded;
        let held = decoded(form, &message[PREFIX..], bound).map_err(|unscanned| {
            Status::invalid_argument(format!("a message of {length} bytes: {unscanned}"))
        })?;
        if held > bound {
            return Err(Status::out_of_range(format!(
                "a message of {length} bytes would hold over {bound} bytes decoded, too large"
            )));
        }
        Ok((message, held))
    }

    /// Passes on `message`, which holds `held` bytes decoded, once they are charged.
    fn charge(&mut self, message: Bytes, held: usize) -> Option<Bytes> {
        let Some(charge) = &self.charge else {
            return Some(message);
        };
        // The message before has been decoded: its charge goes before this one's is taken.
        self.charged.release();
        self.charging = Some((charge.charge(self.bounds.class, held), message));
        None
    }

    /// The message waiting for its charge, once charged.
    fn charged_message(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Status>>> {
        let Some((charging, _)) = &mut self.charging else {
            return Poll::Ready(None);
        };
        let held = std::task::ready!(charging.as_mut().poll(context));
        let message = self.charging.take().map(|(_, message)| message);
        Poll::Ready(match held {
            Ok(held) => {
                self.charged.hold(Some(held));
                message.map(Ok)
            }
            Err(status) => Some(Err(status)),
        })
    }

    /// Holds `data` as arriving: room doubles, and goes to the arriving message's end once
    /// that is no more than twice as far, so it never holds more than four times what arrived,
    /// nor the room it grew from beside more than half again the message.
    fn arrive(&mut self, data: &[u8]) -> Result<(), Status> {
        let needed = self.arriving.len() + data.len();
        if needed > self.arriving.capacity() {
            let end = self.declared()?.map_or(needed, |length| PREFIX + length);
            let doubled = self.arriving.capacity().saturating_mul(2).max(needed);
            let room = if doubled.saturating_mul(2) >= end {
                end.max(needed)
            } else {
                doubled
            };
            self.arriving.reserve_exact(room - self.arriving.len());
        }
        self.arriving.extend_from_slice(data);
        Ok(())
    }

    /// The next frame of the body, or what ends it.
    fn next(&mut self, context: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        loop {
            if let Some(charged) = std::task::ready!(self.charged_message(context)) {
                return Poll::Ready(Some(charged.map(Frame::data)));
            }
            if let Some(length) = self.declared()? {
                if self.roomed(length, context)?.is_pending() {
                    // No room: nothing more is read, and flow control holds the sender.
                    return Poll::Pending;
                }
                if self.arriving.len() >= PREFIX + length {
                    let (message, held) = self.whole(length)?;
                    if let Some(message) = self.charge(message, held) {
                        return Poll::Ready(Some(Ok(Frame::data(message))));
                    }
                    continue;
                }
            }
            if self.done {
                if !self.arriving.is_empty() {
                    return Poll::Ready(Some(Err(Status::internal(format!(
                        "the call ended within a message, {} bytes of it arrived",
                        self.arriving.len()
                    )))));
                }
                let trailers = self.trailers.take();
                return Poll::Ready(trailers.map(|trailers| Ok(Frame::trailers(trailers))));
            }
            match Pin::new(&mut self.inner).poll_frame(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => self.done = true,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => self.arrive(&data)?,
                    Err(frame) => {
                        self.trailers = frame.into_trailers().ok();
                        self.done = true;
                    }
                },
            }
        }
    }
}

impl std::fmt::Debug for Bounded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Bounded")
            .field("bounds", &self.bounds)
            .field("arriving", &self.arriving.len())
            .finish_non_exhaustive()
    }
}

impl Drop for Bounded {
    fn drop(&mut self) {
        // The answer ends: whatever it passed on last has been decoded.
        self.charged.release();
    }
}

impl Body for Bounded {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        self.next(context)
    }

    fn is_end_stream(&self) -> bool {
        self.done && self.arriving.is_empty() && self.trailers.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}
