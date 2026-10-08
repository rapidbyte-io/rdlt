//! A data-plane message as the chunks a body sends: its prefix and encoding, with a batch's
//! body after them as the bytes it already is.
//!
//! The chunks of a message are the bytes prost encodes it to behind its gRPC prefix. A batch's
//! body is its message's last field, so everything before it is encoded into a head chunk, and
//! the body follows as a chunk of its own, never copied.

use bytes::Bytes;
use prost::Message;

use crate::v1;

/// A message's bytes as a body sends them: `head`, then `body` where there is one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunks {
    /// The prefix, and every byte of the message before the body.
    pub head: Bytes,
    /// A batch's body, the message's last bytes.
    pub body: Option<Bytes>,
}

impl Chunks {
    /// Bytes: the message's length with its prefix.
    pub fn len(&self) -> usize {
        self.head.len() + self.body.as_ref().map_or(0, Bytes::len)
    }

    /// Whether the chunks hold no bytes, which no message's do: each has its prefix.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A message of the data plane, sent as [`Chunks`].
pub trait Chained: Message + Sized {
    /// The message's chunks, which hold its prefix and the bytes prost encodes it to.
    fn chunks(self) -> Chunks {
        todo!()
    }
}

impl Chained for v1::WriteAck {}

impl Chained for v1::WriteFrame {}
