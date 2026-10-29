//! A frame on its way: its head, then its body's pieces, each the bytes it already is, written
//! with as few calls as the stream takes slices at once.

use std::io::IoSlice;

use bytes::{Buf, Bytes};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

use super::head::{HEAD_BYTES, Head, Kind};

/// A frame to send: its head, then the pieces of its body, in order.
///
/// Writing it advances it, so a write dropped part way and called again goes on where it was.
#[derive(Debug)]
pub struct Outgoing {
    head: [u8; HEAD_BYTES],
    /// Bytes of the head already written.
    written: usize,
    /// The body's pieces, each advanced past what of it was written.
    pieces: Vec<Bytes>,
    /// The piece being written.
    next: usize,
    /// Bytes of the head and body not yet written.
    left: usize,
}

/// A frame's body is longer than a head can say.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("a frame of {0} bytes is longer than a frame's head can say")]
pub struct TooLong(pub u64);

impl Outgoing {
    /// A frame of `kind` whose body is `pieces`, in order.
    ///
    /// # Errors
    ///
    /// [`TooLong`] where the pieces together are longer than a head's length can say.
    pub fn new(kind: Kind, pieces: Vec<Bytes>) -> Result<Self, TooLong> {
        let len = pieces
            .iter()
            .map(|piece| crate::limits::count(piece.len()))
            .fold(0, u64::saturating_add);
        let len = u32::try_from(len).map_err(|_| TooLong(len))?;
        let left = HEAD_BYTES.saturating_add(usize::try_from(len).unwrap_or(usize::MAX));
        Ok(Self {
            head: Head::new(kind, len).encode(),
            written: 0,
            pieces,
            next: 0,
            left,
        })
    }

    /// A message frame holding `message`, opening `opens` where it opens a call.
    ///
    /// # Errors
    ///
    /// [`TooLong`] for a message longer than a head can say.
    pub fn message(
        opens: Option<super::Call>,
        message: &impl prost::Message,
    ) -> Result<Self, TooLong> {
        Self::new(
            Kind::Message { opens },
            vec![Bytes::from(message.encode_to_vec())],
        )
    }

    /// Bytes: the body's length.
    pub fn len(&self) -> usize {
        let len = u32::from_le_bytes([self.head[0], self.head[1], self.head[2], self.head[3]]);
        usize::try_from(len).unwrap_or(usize::MAX)
    }

    /// Whether the body is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether any of the frame was written: a frame cut once it has leaves its stream torn.
    pub fn is_begun(&self) -> bool {
        self.left < HEAD_BYTES.saturating_add(self.len())
    }

    /// Writes what is left of the frame to `io`.
    ///
    /// # Errors
    ///
    /// The error writing failed with.
    pub async fn write<W: AsyncWrite + Unpin>(&mut self, io: &mut W) -> std::io::Result<()> {
        io.write_all_buf(self).await
    }
}

impl Buf for Outgoing {
    fn remaining(&self) -> usize {
        self.left
    }

    fn chunk(&self) -> &[u8] {
        if self.written < HEAD_BYTES {
            return &self.head[self.written..];
        }
        self.pieces.get(self.next).map_or(&[], |piece| &piece[..])
    }

    fn advance(&mut self, mut count: usize) {
        assert!(count <= self.left, "a frame advanced past its end");
        self.left -= count;
        let head = count.min(HEAD_BYTES - self.written);
        self.written += head;
        count -= head;
        while count > 0 {
            let piece = &mut self.pieces[self.next];
            let taken = count.min(piece.len());
            piece.advance(taken);
            count -= taken;
            if piece.is_empty() {
                self.next += 1;
            }
        }
        while self.pieces.get(self.next).is_some_and(Bytes::is_empty) {
            self.next += 1;
        }
    }

    fn chunks_vectored<'a>(&'a self, slices: &mut [IoSlice<'a>]) -> usize {
        let head = (self.written < HEAD_BYTES).then(|| &self.head[self.written..]);
        let pieces = self.pieces[self.next..]
            .iter()
            .filter(|piece| !piece.is_empty())
            .map(|piece| &piece[..]);
        let mut filled = 0;
        for (slot, chunk) in slices.iter_mut().zip(head.into_iter().chain(pieces)) {
            *slot = IoSlice::new(chunk);
            filled += 1;
        }
        filled
    }
}
