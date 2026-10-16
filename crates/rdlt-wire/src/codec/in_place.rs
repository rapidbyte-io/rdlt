//! Hands Arrow a frame's body where it arrived, a slice of the allocation it lies in, once every
//! buffer is checked to lie at its type's alignment: nothing copies a buffer, Arrow neither.

#[cfg(test)]
mod tests;

use arrow_buffer::{Buffer, MutableBuffer};
use arrow_ipc::RecordBatch;

use super::IpcFrame;
use super::shape::{Span, Walked};
use crate::error::Problem;
use crate::limits::count;

/// Where `part` starts in `whole`, where it lies wholly within it.
fn within(part: &[u8], whole: &[u8]) -> Option<usize> {
    let start = part.as_ptr().addr().checked_sub(whole.as_ptr().addr())?;
    (start.checked_add(part.len())? <= whole.len()).then_some(start)
}

/// The body of `frame` as Arrow reads it: a slice of the allocation it lies in, so what is
/// decoded keeps that allocation alive and counts it whole; an empty body, an empty buffer.
pub(super) fn body(frame: &IpcFrame) -> Buffer {
    if frame.body.is_empty() {
        return Buffer::from(MutableBuffer::new(0));
    }
    match within(&frame.body, &frame.allocation) {
        Some(start) => {
            Buffer::from(frame.allocation.clone()).slice_with_length(start, frame.body.len())
        }
        None => Buffer::from(frame.body.clone()),
    }
}

/// Bytes: what is decoded from `frame` keeps alive of its own, the allocation its body lies in;
/// none for an empty body.
pub(super) fn held(frame: &IpcFrame) -> u64 {
    if frame.body.is_empty() {
        return 0;
    }
    match within(&frame.body, &frame.allocation) {
        Some(_) => count(frame.allocation.len()),
        None => count(frame.body.len()),
    }
}

/// The first of `spans`, buffers of `body`, that does not lie at a multiple of the alignment its
/// type needs, or of eight bytes, the format's padding.
pub(super) fn misaligned(body: &Buffer, spans: &[Span]) -> Option<Problem> {
    let start = body.as_ptr().addr();
    spans.iter().enumerate().find_map(|(index, span)| {
        let alignment = span.alignment.max(8);
        let at = start.wrapping_add(span.offset);
        (!at.is_multiple_of(alignment)).then(|| Problem::BufferUnaligned {
            index,
            offset: count(span.offset),
            alignment,
        })
    })
}

/// The record batch message Arrow reads `walked` by, where the walk emptied a run-end column of
/// no values: `batch`'s length and view counts, the nodes and buffers as the walk reads them.
pub(super) fn rewritten(batch: RecordBatch<'_>, walked: &Walked) -> Vec<u8> {
    let int = |bytes: usize| i64::try_from(bytes).unwrap_or(i64::MAX);
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let buffers: Vec<_> = walked
        .spans
        .iter()
        .map(|span| arrow_ipc::Buffer::new(int(span.offset), int(span.length)))
        .collect();
    let variadic: Vec<_> = batch
        .variadicBufferCounts()
        .unwrap_or_default()
        .iter()
        .collect();
    let nodes = fbb.create_vector(&walked.nodes);
    let buffers = fbb.create_vector(&buffers);
    let variadic = fbb.create_vector(&variadic);
    let mut read = arrow_ipc::RecordBatchBuilder::new(&mut fbb);
    read.add_length(batch.length());
    read.add_nodes(nodes);
    read.add_buffers(buffers);
    read.add_variadicBufferCounts(variadic);
    let read = read.finish();
    fbb.finish(read, None);
    fbb.finished_data().to_vec()
}
