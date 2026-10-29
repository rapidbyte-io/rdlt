//! A data call's stream: each data call, a read, a write or a read-back, is a byte stream of its
//! own, carrying frames of a [`Head`] and a body.
//!
//! A batch frame's body is one Arrow IPC encapsulated message, read into an allocation of its own
//! at the stream format's alignment, where it is decoded; a JSON frame's is a push's text; a
//! message frame's is one protocol buffers message of the call's, scanned before it is decoded.
//!
//! A host opens a call with a message frame naming it whose body is the call's start: a read's
//! `ReadStart`, a write's `WriteStart`, a read-back's `ReadPublishedRequest`. Its connector
//! answers with an empty message frame naming the call, or with the call's answer carrying the
//! error it refuses the call with.

mod head;
mod piped;
mod reader;
#[cfg(test)]
mod tests;
mod writer;

use crate::limits::{Class, Limit, Limits, MESSAGE_ROOM};
use crate::scan::{Form, Unscanned, forms};

pub use head::{Bound, Bounds, Call, HEAD_BYTES, Head, HeadError, Kind, Malformed, Rows};
pub use piped::{DATA, Pipe, Piped, Sent, pipe, piped};
pub use reader::{FrameReader, Received};
pub use writer::{Outgoing, TooLong};

/// The end of a call that sends a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    /// The host, which makes the call.
    Host,
    /// The connector, which serves it.
    Connector,
}

/// Reading a stream failed.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// A frame's head was refused.
    #[error(transparent)]
    Head(#[from] HeadError),
    /// The stream ended within a frame.
    #[error("the stream ended within a frame: {arrived} of its {expected} bytes arrived")]
    Truncated {
        /// Bytes of the head or body that arrived.
        arrived: u64,
        /// Bytes the head or body takes.
        expected: u64,
    },
    /// A message frame's body is not an encoding of its message.
    #[error("a message frame of {len} bytes: {source}")]
    Unscanned {
        /// Bytes: the body's length.
        len: u64,
        /// Why it is not.
        #[source]
        source: Unscanned,
    },
    /// A message frame's body would hold more decoded than it may.
    #[error("a message frame of {len} bytes would hold over {bound} bytes decoded")]
    Inflated {
        /// Bytes: the body's length.
        len: u64,
        /// Bytes: the most it may hold decoded.
        bound: u64,
    },
    /// A message frame's body does not decode as its message.
    #[error("a message frame does not decode")]
    Decode(#[source] prost::DecodeError),
    /// Reading the stream failed.
    #[error("reading the stream failed: {0}")]
    Io(#[source] std::io::Error),
}

impl Call {
    /// What `end` sends on the call: each kind of frame it sends, within `limits`, those of the
    /// end that receives them.
    pub fn bounds(self, end: End, limits: &Limits) -> Bounds {
        let bound = |bytes: u64, limit| Bound {
            bytes: u32::try_from(bytes).unwrap_or(u32::MAX),
            limit,
        };
        let message = |classes: &[Class]| {
            let bytes = classes
                .iter()
                .map(|class| limits.decoding(*class))
                .max()
                .unwrap_or(0);
            bound(crate::limits::count(bytes), Limit::ControlMessageBytes)
        };
        let batch = bound(
            limits.frame_bytes.saturating_add(MESSAGE_ROOM),
            Limit::FrameBytes,
        );
        let json = bound(limits.json_push_bytes, Limit::JsonPushBytes);
        match (self, end) {
            (Self::Read, End::Host) => Bounds {
                batch: None,
                json: None,
                message: message(&[Class::Cursor, Class::Control]),
            },
            (Self::Write, End::Host) | (Self::ReadPublished, End::Connector) => Bounds {
                batch: Some(batch),
                json: None,
                message: message(&[Class::Schema, Class::Control]),
            },
            (Self::ReadPublished, End::Host) | (Self::Write, End::Connector) => Bounds {
                batch: None,
                json: None,
                message: message(&[Class::Control]),
            },
            (Self::Read, End::Connector) => Bounds {
                batch: Some(batch),
                json: Some(json),
                message: message(&[Class::Schema, Class::Cursor, Class::Control]),
            },
        }
    }

    /// What a connector sends first on the call, within the host's `limits`: the opening's
    /// answer, empty, or the refusal of the call, a control message.
    pub fn answering(self, limits: &Limits) -> Bounds {
        Bounds {
            batch: None,
            json: None,
            message: Self::ReadPublished.bounds(End::Host, limits).message,
        }
    }

    /// The form of the message opening the call: its start.
    pub fn opening(self) -> &'static Form {
        match self {
            Self::Read => &forms::READ_START,
            Self::Write => &forms::WRITE_START,
            Self::ReadPublished => &forms::READ_PUBLISHED_REQUEST,
        }
    }

    /// The form of each message `end` sends on the call after its opening, where it sends any.
    pub fn form(self, end: End) -> Option<&'static Form> {
        match (self, end) {
            (Self::Read, End::Host) => Some(&forms::READ_CONTROL),
            (Self::Write, End::Host) => Some(&forms::WRITE_FRAME),
            (Self::ReadPublished, End::Host) => None,
            (Self::Read | Self::ReadPublished, End::Connector) => Some(&forms::READ_FRAME),
            (Self::Write, End::Connector) => Some(&forms::WRITE_ACK),
        }
    }
}

impl Received {
    /// The message its body holds, once the scan of its encoding against `form` counts no more
    /// than `bound` bytes decoded; its `bytes` fields are slices of the body, never copies.
    ///
    /// # Errors
    ///
    /// A [`StreamError`] where the body is no encoding of `form`, would hold more than `bound`
    /// decoded, or does not decode.
    pub fn message<M: prost::Message + Default>(
        &self,
        form: &'static Form,
        bound: usize,
    ) -> Result<M, StreamError> {
        let bytes = &self.body.bytes;
        let len = crate::limits::count(bytes.len());
        let held = crate::scan::decoded(form, bytes, bound)
            .map_err(|source| StreamError::Unscanned { len, source })?;
        if held > bound {
            let bound = crate::limits::count(bound);
            return Err(StreamError::Inflated { len, bound });
        }
        M::decode(bytes.clone()).map_err(StreamError::Decode)
    }
}
