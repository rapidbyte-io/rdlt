//! A frame's body as its walk reads it: contiguous, as a receiver holds it, or as the pieces a
//! sender sends it from, each of the frame's buffers lying within one piece.

use std::ops::Range;

use bytes::Bytes;

/// A frame's body, whose buffers the walk reads by their ranges.
pub(crate) trait Body {
    /// Bytes: the body's length.
    fn len(&self) -> usize;

    /// The bytes of `range`, where it lies within the body and within one of its pieces.
    fn get(&self, range: Range<usize>) -> Option<&[u8]>;
}

impl Body for [u8] {
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn get(&self, range: Range<usize>) -> Option<&[u8]> {
        <[u8]>::get(self, range)
    }
}

/// A body as pieces, in order, and where each starts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Pieces {
    parts: Vec<Bytes>,
    /// Where each piece starts in the body.
    starts: Vec<usize>,
    len: usize,
}

impl Pieces {
    /// The body of `pieces`, in order.
    pub(crate) fn new(pieces: Vec<Bytes>) -> Self {
        let mut starts = Vec::with_capacity(pieces.len());
        let mut len = 0_usize;
        for piece in &pieces {
            starts.push(len);
            len = len.saturating_add(piece.len());
        }
        Self {
            parts: pieces,
            starts,
            len,
        }
    }

    /// The pieces, in order.
    pub(crate) fn pieces(&self) -> &[Bytes] {
        &self.parts
    }

    /// The pieces, in order, given up.
    pub(crate) fn into_pieces(self) -> Vec<Bytes> {
        self.parts
    }
}

impl Body for Pieces {
    fn len(&self) -> usize {
        self.len
    }

    fn get(&self, range: Range<usize>) -> Option<&[u8]> {
        if range.start > range.end || range.end > self.len {
            return None;
        }
        if range.is_empty() {
            return Some(&[]);
        }
        // The piece holding the range's first byte: the last that starts at or before it.
        let index = self
            .starts
            .partition_point(|start| *start <= range.start)
            .checked_sub(1)?;
        let start = self.starts[index];
        self.parts[index].get(range.start - start..range.end - start)
    }
}
