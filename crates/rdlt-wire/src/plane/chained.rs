//! A data-plane message as the chunks a body sends: its prefix and encoding, with a batch's
//! body after them as the bytes it already is.
//!
//! The chunks of a message are the bytes prost encodes it to behind its gRPC prefix. A batch's
//! body is its message's last field, so everything before it is encoded into a head chunk, and
//! the body follows as a chunk of its own, never copied.

use bytes::{BufMut as _, Bytes, BytesMut};
use prost::Message;

use super::PREFIX;
use crate::v1;

/// The key of a length-delimited field numbered 3: a write frame's batch, and a batch's body.
const THIRD_FIELD: u8 = 0x1a;

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
    pub(crate) fn len(&self) -> usize {
        self.head.len() + self.body.as_ref().map_or(0, Bytes::len)
    }
}

/// A message of the data plane, sent as [`Chunks`].
pub trait Chained: Message + Sized {
    /// The message's chunks, which hold its prefix and the bytes prost encodes it to.
    fn chunks(self) -> Chunks {
        Chunks {
            head: whole(&self),
            body: None,
        }
    }
}

impl Chained for v1::WriteAck {}

impl Chained for v1::ReadControl {}

impl Chained for v1::ReadPublishedRequest {}

impl Chained for v1::ReadFrame {}

impl Chained for v1::WriteFrame {
    fn chunks(self) -> Chunks {
        let Some(v1::write_frame::Frame::Batch(mut batch)) = self.frame else {
            return Chunks {
                head: whole(&self),
                body: None,
            };
        };
        if batch.data_body.is_empty() {
            let frame = v1::WriteFrame {
                frame: Some(v1::write_frame::Frame::Batch(batch)),
            };
            return Chunks {
                head: whole(&frame),
                body: None,
            };
        }
        let batch_length = batch.encoded_len();
        let length = 1 + prost::length_delimiter_len(batch_length) + batch_length;
        let body = std::mem::take(&mut batch.data_body);
        // The head is all of the message but its body.
        let room = length - body.len();
        let mut head = prefixed(length, room);
        head.put_u8(THIRD_FIELD);
        delimit(batch_length, &mut head);
        encode(&batch, &mut head);
        head.put_u8(THIRD_FIELD);
        delimit(body.len(), &mut head);
        debug_assert_eq!(
            head.len(),
            PREFIX + room,
            "the head is the message less its body"
        );
        Chunks {
            head: head.freeze(),
            body: Some(body),
        }
    }
}

/// `message`, its prefix before it.
fn whole(message: &impl Message) -> Bytes {
    let length = message.encoded_len();
    let mut head = prefixed(length, length);
    encode(message, &mut head);
    head.freeze()
}

/// A buffer of room for `room` bytes of a message of `length` bytes, its prefix in it.
fn prefixed(length: usize, room: usize) -> BytesMut {
    let mut head = BytesMut::with_capacity(PREFIX + room);
    head.put_u8(0);
    // A message beyond what the prefix can say is refused by its length before it is sent.
    head.put_u32(u32::try_from(length).unwrap_or(u32::MAX));
    head
}

fn encode(message: &impl Message, buffer: &mut BytesMut) {
    message
        .encode(buffer)
        .expect("a buffer that grows has room for any message");
}

fn delimit(length: usize, buffer: &mut BytesMut) {
    prost::encode_length_delimiter(length, buffer)
        .expect("a buffer that grows has room for any length");
}
