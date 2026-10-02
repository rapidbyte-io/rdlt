//! Lays a frame's buffers out in one allocation of the decoder's own, each at the alignment its
//! column's type needs, so Arrow reads them where they lie and never copies one to align it.

#[cfg(test)]
mod tests;

use arrow_buffer::{Buffer, MutableBuffer};
use arrow_ipc::RecordBatch;

use super::shape::{Placed, Walked};

/// A frame's buffers in an allocation of their own, and the message describing them there.
pub(super) struct Relocated {
    message: Vec<u8>,
    /// The buffers, each aligned for its column's type.
    pub(super) body: Buffer,
}

impl Relocated {
    /// The record batch message describing the buffers where they now lie.
    pub(super) fn batch(&self) -> Option<RecordBatch<'_>> {
        flatbuffers::root::<RecordBatch<'_>>(&self.message).ok()
    }
}

/// Where `buffer` lies once laid out after `end` bytes: its start and its end.
fn placed_at(end: usize, buffer: &Placed<'_>) -> (usize, usize) {
    let start = end.next_multiple_of(buffer.alignment);
    (start, start.saturating_add(buffer.bytes.len()))
}

/// Bytes: what the buffers `walked` describes take laid out in order, each at its alignment.
fn laid_out(walked: &Walked<'_>) -> usize {
    let placed = walked.placed.iter();
    placed.fold(0, |at, buffer| placed_at(at, buffer).1)
}

/// Bytes: the allocation a decoder holds a frame of `walked` in, as [`relocated`] makes it.
///
/// Whoever sends a frame measures with it what its receiver will hold, and the receiver what it
/// holds, so the two never disagree.
pub(crate) fn held_bytes(walked: &Walked<'_>) -> u64 {
    let bytes = arrow_buffer::bit_util::round_upto_multiple_of_64(laid_out(walked));
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// Copies `walked`'s buffers, those `batch` describes in order, into one allocation.
pub(super) fn relocated(batch: RecordBatch<'_>, walked: &Walked<'_>) -> Relocated {
    let placed = &walked.placed;
    let end = placed_at;
    let capacity = laid_out(walked);
    let mut body = MutableBuffer::with_capacity(capacity);
    let mut buffers = Vec::with_capacity(placed.len());
    for buffer in placed {
        let (start, _) = end(body.len(), buffer);
        body.extend_zeros(start - body.len());
        body.extend_from_slice(buffer.bytes);
        buffers.push(arrow_ipc::Buffer::new(int(start), int(buffer.bytes.len())));
    }
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let variadic: Vec<_> = batch
        .variadicBufferCounts()
        .unwrap_or_default()
        .iter()
        .collect();
    let nodes = fbb.create_vector(&walked.nodes);
    let buffers = fbb.create_vector(&buffers);
    let variadic = fbb.create_vector(&variadic);
    let mut rebuilt = arrow_ipc::RecordBatchBuilder::new(&mut fbb);
    rebuilt.add_length(batch.length());
    rebuilt.add_nodes(nodes);
    rebuilt.add_buffers(buffers);
    rebuilt.add_variadicBufferCounts(variadic);
    let rebuilt = rebuilt.finish();
    fbb.finish(rebuilt, None);
    Relocated {
        message: fbb.finished_data().to_vec(),
        body: body.into(),
    }
}

/// An offset or length as the IPC format holds it.
fn int(bytes: usize) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}
