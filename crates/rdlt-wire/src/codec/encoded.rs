//! A frame as its sender sends it: one Arrow IPC encapsulated message, its prefix and metadata
//! built fresh and its body the batch's own buffers wherever Arrow keeps them.

use std::collections::VecDeque;

use arrow_buffer::Buffer;
use bytes::{Bytes, BytesMut};

use super::IpcFrame;
use super::measure::verified;
use super::pieces::{Body as _, Pieces};
use crate::aligned::{Aligned, Frozen};
use crate::error::{Frame, Problem, WireError};

/// Bytes: an encapsulated message's prefix, its continuation marker and its metadata's length.
pub(crate) const PREFIX: usize = 8;

/// The continuation marker an encapsulated message's prefix starts with.
const CONTINUATION: [u8; 4] = [0xff; 4];

/// Bytes: a piece shorter than this is copied into one allocation with the others like it in its
/// frame, so a wide frame is written in few slices.
const SCRATCH_BELOW: usize = 4096;

/// Tables deep a batch or dictionary message's metadata nests, its vectors of nodes and buffers
/// within its message.
const BATCH_TABLES: usize = 8;

/// One encapsulated IPC message, a dictionary or a record batch, as its sender sends it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Encoded {
    /// Its metadata, a flatbuffer padded to the stream format's alignment.
    header: Bytes,
    /// Its body: the batch's own buffers, and the padding between them.
    body: Pieces,
    /// Whether it is a dictionary batch.
    dictionary: bool,
}

impl Encoded {
    /// Bytes: the message's whole length, its prefix, metadata and body.
    pub fn len(&self) -> usize {
        PREFIX + self.header.len() + self.body.len()
    }

    /// Whether the message is empty, which none is: its prefix alone takes bytes.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Whether it is a dictionary, which batches after it use, rather than a record batch.
    pub fn is_dictionary(&self) -> bool {
        self.dictionary
    }

    /// Bytes: its metadata and body, as its receiver counts what a write stages.
    pub fn frame_bytes(&self) -> usize {
        self.header.len() + self.body.len()
    }

    /// Its metadata, as its receiver reads it.
    pub(crate) fn header(&self) -> &Bytes {
        &self.header
    }

    /// Its body, as the pieces it is sent from.
    pub(crate) fn body(&self) -> &Pieces {
        &self.body
    }

    /// The pieces it is sent as, in order: its prefix and metadata, then its body, the pieces
    /// shorter than a page or so copied together into one allocation.
    pub fn into_pieces(self) -> Vec<Bytes> {
        let prefix = prefix(self.header.len());
        let pieces = std::iter::once(self.header).chain(self.body.into_pieces());
        coalesced(&prefix, pieces)
    }

    /// The message gathered into one allocation from a first byte at a 64-byte address, as its
    /// receiver holds it.
    pub fn gathered(&self) -> IpcFrame {
        let mut aligned = Aligned::with_room(self.len());
        aligned.extend(&prefix(self.header.len()));
        aligned.extend(&self.header);
        for piece in self.body.pieces() {
            aligned.extend(piece);
        }
        let Frozen { bytes, allocation } = aligned.freeze();
        let end = PREFIX + self.header.len();
        IpcFrame {
            header: bytes.slice(PREFIX..end),
            body: bytes.slice(end..),
            allocation,
        }
    }
}

/// The prefix of a message whose metadata takes `header` bytes.
fn prefix(header: usize) -> [u8; PREFIX] {
    let mut prefix = [0; PREFIX];
    prefix[..4].copy_from_slice(&CONTINUATION);
    let len = i32::try_from(header).unwrap_or(i32::MAX);
    prefix[4..].copy_from_slice(&len.to_le_bytes());
    prefix
}

/// `prefix` and `pieces`, in order, those shorter than [`SCRATCH_BELOW`] copied into one
/// allocation, each run of them a slice of it.
fn coalesced(prefix: &[u8], pieces: impl Iterator<Item = Bytes> + Clone) -> Vec<Bytes> {
    let small = pieces
        .clone()
        .filter(|piece| piece.len() < SCRATCH_BELOW)
        .map(|piece| piece.len())
        .fold(prefix.len(), usize::saturating_add);
    let mut scratch = BytesMut::with_capacity(small);
    scratch.extend_from_slice(prefix);
    let mut sent = Vec::new();
    for piece in pieces {
        if piece.len() < SCRATCH_BELOW {
            scratch.extend_from_slice(&piece);
            continue;
        }
        if !scratch.is_empty() {
            sent.push(scratch.split().freeze());
        }
        sent.push(piece);
    }
    if !scratch.is_empty() {
        sent.push(scratch.split().freeze());
    }
    sent
}

/// An Arrow buffer as bytes a frame sends, sharing it.
struct Owned(Buffer);

impl AsRef<[u8]> for Owned {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

/// The bytes a stream encoder returned, read in order, each taken as the buffer it lies in or
/// copied.
struct Buffers(VecDeque<Buffer>);

impl Buffers {
    /// The next `len` bytes, sharing the buffers they lie in.
    fn shared(&mut self, mut len: usize) -> Result<Vec<Bytes>, WireError> {
        let mut pieces = Vec::new();
        while len > 0 {
            let Some(buffer) = self.0.pop_front() else {
                return Err(short());
            };
            if buffer.len() > len {
                let rest = buffer.slice_with_length(len, buffer.len() - len);
                self.0.push_front(rest);
                pieces.push(Bytes::from_owner(Owned(buffer.slice_with_length(0, len))));
                break;
            }
            len -= buffer.len();
            if !buffer.is_empty() {
                pieces.push(Bytes::from_owner(Owned(buffer)));
            }
        }
        Ok(pieces)
    }

    /// The next `len` bytes, copied into one allocation.
    fn copied(&mut self, len: usize) -> Result<Bytes, WireError> {
        let mut copied = BytesMut::with_capacity(len);
        for piece in self.shared(len)? {
            copied.extend_from_slice(&piece);
        }
        Ok(copied.freeze())
    }
}

/// An encoding that ends within a message, which no stream encoder makes.
fn short() -> WireError {
    WireError::malformed(Frame::Batch, Problem::Encapsulation)
}

/// The messages `buffers` holds, what a stream encoder returned for one batch, in order; its
/// first, the schema a stream encoder sends before its first batch, left out where `schema`.
pub(crate) fn split(buffers: Vec<Buffer>, schema: bool) -> Result<Vec<Encoded>, WireError> {
    let mut buffers = Buffers(buffers.into());
    let mut messages = Vec::new();
    let mut skipped = !schema;
    loop {
        // A body's last buffer may be empty, and an encoder hands it over all the same.
        while buffers.0.front().is_some_and(Buffer::is_empty) {
            buffers.0.pop_front();
        }
        if buffers.0.is_empty() {
            break;
        }
        let prefix = buffers.copied(PREFIX)?;
        if prefix[..4] != CONTINUATION {
            return Err(short());
        }
        let header = i32::from_le_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]);
        let header = usize::try_from(header).map_err(|_| short())?;
        let header = buffers.copied(header)?;
        if !skipped {
            // A schema message has no body.
            skipped = true;
            continue;
        }
        let message = verified(Frame::Batch, &header, BATCH_TABLES)?;
        let dictionary = message.header_as_dictionary_batch().is_some();
        let body = usize::try_from(message.bodyLength()).map_err(|_| short())?;
        let body = Pieces::new(buffers.shared(body)?);
        messages.push(Encoded {
            header,
            body,
            dictionary,
        });
    }
    Ok(messages)
}

impl IpcFrame {
    /// Bytes: the metadata and body an encapsulated message of `len` bytes holds, its prefix
    /// left out, as a write's receiver counts what it stages.
    pub const fn held_bytes(len: usize) -> usize {
        len.saturating_sub(PREFIX)
    }

    /// The frame whose encapsulated message is `received`, as a peer sent it in a frame of its
    /// own: its prefix, its metadata, then its body.
    ///
    /// # Errors
    ///
    /// [`WireError::Malformed`] where it is no encapsulated message, or takes more beside its
    /// metadata and body than a message may.
    pub fn framed(received: Frozen) -> Result<Self, WireError> {
        let Frozen { bytes, allocation } = received;
        let header = bytes
            .get(..PREFIX)
            .filter(|prefix| prefix[..4] == CONTINUATION)
            .map(|prefix| i32::from_le_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]))
            .and_then(|header| usize::try_from(header).ok())
            .filter(|header| PREFIX.saturating_add(*header) <= bytes.len());
        let Some(header) = header else {
            return Err(WireError::malformed(Frame::Batch, Problem::Encapsulation));
        };
        let end = PREFIX + header;
        Self::arrived(bytes.slice(PREFIX..end), bytes.slice(end..), allocation)
    }
}
