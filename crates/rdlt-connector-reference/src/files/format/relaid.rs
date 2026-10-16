//! A block another writer laid out where the wire's decoder cannot read it in place, laid out
//! again with each buffer at a multiple of 64 bytes, for files padded to eight bytes, as Arrow C++
//! and pyarrow write them.

#[cfg(test)]
mod tests;

use arrow_ipc::MessageHeader;
use bytes::Bytes;
use rdlt_connector::Result;
use rdlt_wire::aligned::{ALIGNMENT, Aligned, Frozen};
use rdlt_wire::{IpcFrame, Limits, WireError};

use super::ipc::{malformed, refused, verifier};

/// `frame` with each of its buffers copied to a multiple of [`ALIGNMENT`] bytes of a body of its
/// own, and its message describing them there.
///
/// # Errors
///
/// A data error where its message is no batch or is compressed, a buffer lies outside its body,
/// before the buffer before it ends or off the format's eight bytes, or the layout passes the
/// frame limit.
pub(super) fn relaid(frame: &IpcFrame, limits: &Limits) -> Result<IpcFrame> {
    let message = arrow_ipc::root_as_message_with_opts(&verifier(limits), &frame.header)
        .map_err(|_| malformed("a block's message is no message"))?;
    let (batch, dictionary) = match message.header_type() {
        MessageHeader::RecordBatch => (message.header_as_record_batch(), None),
        MessageHeader::DictionaryBatch => {
            let wrapped = message.header_as_dictionary_batch();
            let id = wrapped.map(|wrapped| (wrapped.id(), wrapped.isDelta()));
            (wrapped.and_then(|wrapped| wrapped.data()), id)
        }
        _ => (None, None),
    };
    let batch = batch.ok_or_else(|| malformed("a block's message is no batch"))?;
    if batch.compression().is_some() {
        return Err(malformed("a block is compressed"));
    }
    let outside = || malformed("a block's buffer lies outside its body or off eight bytes");
    // Each buffer goes at the next multiple of the alignment after the buffer before it.
    let (mut spans, mut from, mut end) = (Vec::new(), 0_usize, 0_usize);
    for buffer in batch.buffers().unwrap_or_default() {
        let offset = usize::try_from(buffer.offset()).map_err(|_| outside())?;
        let length = usize::try_from(buffer.length()).map_err(|_| outside())?;
        let source_end = offset.checked_add(length).ok_or_else(outside)?;
        if offset < from || !offset.is_multiple_of(8) || source_end > frame.body.len() {
            return Err(outside());
        }
        from = source_end;
        let start = end.next_multiple_of(ALIGNMENT);
        end = start.checked_add(length).ok_or_else(outside)?;
        spans.push((offset, start, length));
    }
    limits
        .admit_frame(frame.header.len().saturating_add(end))
        .map_err(WireError::from)
        .map_err(refused)?;
    let mut body = Aligned::zeroed(end);
    for &(offset, start, length) in &spans {
        body.as_mut_slice()[start..start + length]
            .copy_from_slice(&frame.body[offset..offset + length]);
    }
    let header = rebuilt(&message, batch, dictionary, &spans, end);
    let Frozen { bytes, allocation } = body.freeze();
    Ok(IpcFrame {
        header,
        body: bytes,
        allocation,
    })
}

/// The message of `message`'s version describing `batch` with its buffers at `spans`, in a body
/// of `end` bytes, wrapped as the dictionary batch it was where it was one.
fn rebuilt(
    message: &arrow_ipc::Message<'_>,
    batch: arrow_ipc::RecordBatch<'_>,
    dictionary: Option<(i64, bool)>,
    spans: &[(usize, usize, usize)],
    end: usize,
) -> Bytes {
    let int = |bytes: usize| i64::try_from(bytes).unwrap_or(i64::MAX);
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let nodes: Vec<_> = batch.nodes().unwrap_or_default().iter().copied().collect();
    let buffers: Vec<_> = spans
        .iter()
        .map(|&(_, start, length)| arrow_ipc::Buffer::new(int(start), int(length)))
        .collect();
    let variadic: Vec<_> = batch
        .variadicBufferCounts()
        .unwrap_or_default()
        .iter()
        .collect();
    let nodes = fbb.create_vector(&nodes);
    let buffers = fbb.create_vector(&buffers);
    let variadic = fbb.create_vector(&variadic);
    let mut laid = arrow_ipc::RecordBatchBuilder::new(&mut fbb);
    laid.add_length(batch.length());
    laid.add_nodes(nodes);
    laid.add_buffers(buffers);
    laid.add_variadicBufferCounts(variadic);
    let laid = laid.finish();
    let (kind, header) = match dictionary {
        None => (MessageHeader::RecordBatch, laid.as_union_value()),
        Some((id, delta)) => {
            let mut wrapped = arrow_ipc::DictionaryBatchBuilder::new(&mut fbb);
            wrapped.add_id(id);
            wrapped.add_data(laid);
            wrapped.add_isDelta(delta);
            (
                MessageHeader::DictionaryBatch,
                wrapped.finish().as_union_value(),
            )
        }
    };
    let mut built = arrow_ipc::MessageBuilder::new(&mut fbb);
    built.add_version(message.version());
    built.add_header_type(kind);
    built.add_header(header);
    built.add_bodyLength(int(end));
    let built = built.finish();
    fbb.finish(built, None);
    Bytes::copy_from_slice(fbb.finished_data())
}
