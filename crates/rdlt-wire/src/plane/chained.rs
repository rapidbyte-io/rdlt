//! A data-plane message as the chunks a body sends: its prefix and encoding, with a batch's
//! body or a push's JSON after them as the bytes it already is.
//!
//! The chunks of a message are the bytes prost encodes it to behind its gRPC prefix. A batch's
//! body, and a push's JSON, is its message's last field, so everything before it is encoded into
//! a head chunk, and the body follows as a chunk of its own, never copied.

use bytes::{BufMut as _, Bytes, BytesMut};
use prost::Message;

use super::PREFIX;
use crate::v1;

/// The keys, field number and length-delimited wire type, of the fields that lead to a body.
mod key {
    /// `WriteFrame.batch`, field 3.
    pub(super) const WRITE_BATCH: u8 = 0x1a;
    /// `WriteBatch.data_body`, field 3.
    pub(super) const WRITE_BODY: u8 = 0x1a;
    /// `ReadFrame.batch`, field 2.
    pub(super) const READ_BATCH: u8 = 0x12;
    /// `BatchFrame.data_body`, field 4.
    pub(super) const READ_BODY: u8 = 0x22;
    /// `ReadFrame.json`, field 3.
    pub(super) const READ_JSON: u8 = 0x1a;
    /// `JsonFrame.data`, field 1.
    pub(super) const JSON_DATA: u8 = 0x0a;
}

/// A message's bytes as a body sends them: `head`, then `body` where there is one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunks {
    /// The prefix, and every byte of the message before the body.
    pub head: Bytes,
    /// A batch's body or a push's JSON, the message's last bytes.
    pub body: Option<Bytes>,
}

impl Chunks {
    /// Bytes: the message's length with its prefix.
    pub(crate) fn len(&self) -> usize {
        self.head.len() + self.body.as_ref().map_or(0, Bytes::len)
    }

    /// `message` whole, with no body.
    fn whole(message: &impl Message) -> Self {
        Self {
            head: whole(message),
            body: None,
        }
    }
}

/// A message of the data plane, sent as [`Chunks`].
pub trait Chained: Message + Sized {
    /// The message's chunks, which hold its prefix and the bytes prost encodes it to.
    fn chunks(self) -> Chunks {
        Chunks::whole(&self)
    }
}

impl Chained for v1::WriteAck {}

impl Chained for v1::ReadControl {}

impl Chained for v1::ReadPublishedRequest {}

impl Chained for v1::WriteFrame {
    fn chunks(self) -> Chunks {
        match self.frame {
            Some(v1::write_frame::Frame::Batch(mut batch)) if !batch.data_body.is_empty() => {
                let body = std::mem::take(&mut batch.data_body);
                ahead(key::WRITE_BATCH, &batch, key::WRITE_BODY, body)
            }
            frame => Chunks::whole(&v1::WriteFrame { frame }),
        }
    }
}

impl Chained for v1::ReadFrame {
    fn chunks(self) -> Chunks {
        use v1::read_frame::Frame;
        match self.frame {
            Some(Frame::Batch(mut batch)) if !batch.data_body.is_empty() => {
                let body = std::mem::take(&mut batch.data_body);
                ahead(key::READ_BATCH, &batch, key::READ_BODY, body)
            }
            Some(Frame::Json(json)) if !json.data.is_empty() => ahead(
                key::READ_JSON,
                &v1::JsonFrame::default(),
                key::JSON_DATA,
                json.data,
            ),
            frame => Chunks::whole(&v1::ReadFrame { frame }),
        }
    }
}

/// The chunks of a message whose only field set, keyed `outer`, holds `inner` and then `body`,
/// keyed `last`: a head of all but the body, and the body.
///
/// `inner` is its message less the body, whose field is the message's last, so prost encodes
/// all of `inner` before it.
fn ahead(outer: u8, inner: &impl Message, last: u8, body: Bytes) -> Chunks {
    let inner_length =
        inner.encoded_len() + 1 + prost::length_delimiter_len(body.len()) + body.len();
    let length = 1 + prost::length_delimiter_len(inner_length) + inner_length;
    // The head is all of the message but its body.
    let room = length - body.len();
    let mut head = prefixed(length, room);
    head.put_u8(outer);
    delimit(inner_length, &mut head);
    encode(inner, &mut head);
    head.put_u8(last);
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
