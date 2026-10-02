//! A call's body, its messages each held to their bounds before what reads the body sees them.
//!
//! A gRPC message is a five-byte prefix, its length among it, then the message. A decoder that
//! reads the prefix reserves the length it declares before the bytes arrive, and decodes the
//! message whole into what its fields become. The body passes a message on only once all of it
//! has arrived: within the wire bound of its call, and, for a message of a form, counted by its
//! [scan](crate::scan) within the bound of what it decodes to. A message beyond either fails the
//! call before anything decodes it; one still arriving is held against a window the connection
//! shares, where one is given.

#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::{Buf as _, Bytes, BytesMut};
use http_body::{Body, Frame, SizeHint};
use tonic::Status;

use crate::scan::{Form, decoded};

/// Bytes: a message's prefix, a flag and its length.
const PREFIX: usize = 5;

/// What a call's messages are held to.
#[derive(Clone, Copy, Debug)]
pub struct Bounds {
    /// The form of each message, where what it decodes to is counted.
    pub form: Option<&'static Form>,
    /// The most bytes a message may take on the wire.
    pub wire: usize,
    /// The most bytes a message of `form` may hold decoded.
    pub decoded: usize,
}

/// Bytes of messages still arriving that the bodies of one connection may hold together.
#[derive(Clone, Debug)]
pub struct Window {
    held: Arc<AtomicUsize>,
    bytes: usize,
}

impl Window {
    /// A window of `bytes`.
    pub fn new(bytes: usize) -> Self {
        Self {
            held: Arc::new(AtomicUsize::new(0)),
            bytes,
        }
    }

    /// Takes `bytes` of the window, where it has them.
    fn take(&self, bytes: usize) -> bool {
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes).filter(|held| *held <= self.bytes)
            })
            .is_ok()
    }

    fn give(&self, bytes: usize) {
        self.held.fetch_sub(bytes, Ordering::SeqCst);
    }
}

/// A body whose messages are held to their bounds before they are passed on.
pub struct Bounded {
    inner: tonic::body::Body,
    bounds: Bounds,
    window: Option<Window>,
    /// What has arrived of the message being received.
    arriving: BytesMut,
    /// Trailers that came after a message still arriving, passed on after it.
    trailers: Option<tonic::codegen::http::HeaderMap>,
    done: bool,
}

impl Bounded {
    /// `inner`, its messages held to `bounds`, and those still arriving to `window`, where one
    /// is given.
    pub fn new(inner: tonic::body::Body, bounds: Bounds, window: Option<Window>) -> Self {
        Self {
            inner,
            bounds,
            window,
            arriving: BytesMut::new(),
            trailers: None,
            done: false,
        }
    }

    /// The messages `arriving` holds whole, taken from it, each checked against the bounds.
    fn whole(&mut self) -> Result<Option<Bytes>, Status> {
        let mut whole = BytesMut::new();
        while self.arriving.len() >= PREFIX {
            let declared = (&self.arriving[1..PREFIX]).get_u32();
            let length = usize::try_from(declared).unwrap_or(usize::MAX);
            if length > self.bounds.wire {
                return Err(Status::out_of_range(format!(
                    "a message of {length} bytes is too large, beyond the limit of {} bytes",
                    self.bounds.wire
                )));
            }
            let Some(end) = PREFIX
                .checked_add(length)
                .filter(|end| *end <= self.arriving.len())
            else {
                break;
            };
            let message = self.arriving.split_to(end);
            if let Some(form) = self.bounds.form {
                // An encoding that does not scan is left for the decoder to refuse.
                let bound = self.bounds.decoded;
                let held = decoded(form, &message[PREFIX..], bound).unwrap_or(0);
                if held > bound {
                    return Err(Status::out_of_range(format!(
                        "a message of {length} bytes would hold over {bound} bytes decoded, \
                         too large"
                    )));
                }
            }
            whole.unsplit(message);
        }
        Ok((!whole.is_empty()).then(|| whole.freeze()))
    }

    /// Holds `data` as arriving, against the window.
    fn arrive(&mut self, data: &[u8]) -> Result<(), Status> {
        if let Some(window) = &self.window
            && !window.take(data.len())
        {
            return Err(Status::resource_exhausted(format!(
                "the connection's messages still arriving pass its window of {} bytes",
                window.bytes
            )));
        }
        self.arriving.extend_from_slice(data);
        Ok(())
    }

    /// Gives the window back what `before` held and `arriving` no longer does.
    fn passed(&self, before: usize) {
        if let Some(window) = &self.window {
            window.give(before.saturating_sub(self.arriving.len()));
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

impl Bounds {
    /// The bounds of a message of `class` and `form`, within `limits`.
    pub fn of(
        limits: &crate::Limits,
        class: crate::limits::Class,
        form: Option<&'static Form>,
    ) -> Self {
        let decoded = limits.decoded(class);
        Self {
            form: form.filter(|_| decoded.is_some()),
            wire: limits.decoding(class),
            decoded: decoded.unwrap_or(usize::MAX),
        }
    }
}

impl Drop for Bounded {
    fn drop(&mut self) {
        if let Some(window) = &self.window {
            window.give(self.arriving.len());
        }
    }
}

impl Body for Bounded {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = &mut *self;
        loop {
            if this.done {
                // What arrived of a message the body ended within is passed on as it is, for
                // the decoder to refuse, then the trailers.
                if !this.arriving.is_empty() {
                    let before = this.arriving.len();
                    let rest = this.arriving.split().freeze();
                    this.passed(before);
                    return Poll::Ready(Some(Ok(Frame::data(rest))));
                }
                return Poll::Ready(
                    this.trailers
                        .take()
                        .map(|trailers| Ok(Frame::trailers(trailers))),
                );
            }
            let frame = match Pin::new(&mut this.inner).poll_frame(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.done = true;
                    continue;
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(frame))) => frame,
            };
            let data = match frame.into_data() {
                Ok(data) => data,
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        this.trailers = Some(trailers);
                    }
                    this.done = true;
                    continue;
                }
            };
            this.arrive(&data)?;
            let before = this.arriving.len();
            let whole = this.whole();
            this.passed(before);
            if let Some(whole) = whole? {
                return Poll::Ready(Some(Ok(Frame::data(whole))));
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done && self.arriving.is_empty() && self.trailers.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}
