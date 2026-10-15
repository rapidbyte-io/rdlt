//! Where a batch's message arrives: its body at a 64-byte address, in an allocation that holds
//! no more beside its frame than a message may.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use proptest::prelude::*;

use super::reads::read_answer;
use super::{batch_frame, chained, prefixed, requested, under_way};
use crate::aligned::is_aligned;
use crate::codec::{Decoder, Encoder, IpcFrame};
use crate::error::{Problem, WireError};
use crate::limits::{Limits, MESSAGE_ROOM, count};
use crate::plane::{Chained, Incoming, Received};
use crate::testing::chunks;
use crate::v1;

/// What `future` comes to, which needs nothing it has not been given.
fn now<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime")
        .block_on(future)
}

/// The write `bytes` carry, received from pieces of `cut` bytes.
fn write_received(bytes: &[u8], cut: usize) -> Received<v1::WriteFrame> {
    let pieces: Vec<&[u8]> = bytes.chunks(cut).collect();
    let mut incoming = Incoming::<v1::WriteFrame>::request(requested(chunks(&pieces)));
    now(incoming.arrived()).unwrap().expect("a message")
}

/// The read frame `bytes` carry, received whole.
fn read_received(bytes: &[u8]) -> Received<v1::ReadFrame> {
    let answer = under_way(read_answer(chunks(&[bytes])));
    let mut incoming = Incoming::<v1::ReadFrame>::answer(answer).unwrap();
    now(incoming.arrived()).unwrap().expect("a message")
}

/// The header, body and allocation of the batch `received` carries.
fn written(received: Received<v1::WriteFrame>) -> (Bytes, Bytes, Bytes) {
    let Some(v1::write_frame::Frame::Batch(batch)) = received.message.frame else {
        panic!("a batch");
    };
    (batch.data_header, batch.data_body, received.allocation)
}

/// Bytes the allocation a batch arrived in holds beside its header and body, which lies at a
/// 64-byte address where it has any.
fn beside((header, body, allocation): (Bytes, Bytes, Bytes)) -> u64 {
    assert!(
        body.is_empty() || is_aligned(&body),
        "the body lies aligned"
    );
    let frame = IpcFrame::arrived(header, body, allocation).expect("within its room");
    count(frame.allocation.len() - frame.header.len() - frame.body.len())
}

proptest! {
    /// A batch's body arrives at a multiple of the alignment, in an allocation that holds the
    /// message and at most MESSAGE_ROOM beside its header and body, however the chunks are cut.
    #[test]
    fn a_batch_arrives_aligned_within_its_room(
        segment in any::<u64>(),
        header in 0_usize..2_000,
        body in 0_usize..70_000,
        cut in 1_usize..9_000,
    ) {
        let frame = batch_frame(segment, &vec![1; header], &vec![2; body]);
        let extra = beside(written(write_received(&chained(frame.chunks()), cut)));
        prop_assert!(extra <= MESSAGE_ROOM, "{}", extra);
    }
}

#[test]
fn a_batch_at_the_extremes_stays_within_its_room() {
    // The widest fields around the frame, and lengths a byte short of taking a byte more.
    for (header, body) in [(16_383, 1), (120, 1 << 21), (0, 1), ((1 << 14) - 70, 127)] {
        let frame = batch_frame(u64::MAX, &vec![1; header], &vec![2; body]);
        let extra = beside(written(write_received(&chained(frame.chunks()), 1 << 20)));
        assert!(extra <= MESSAGE_ROOM, "{header} {body}: {extra}");
        // A kind no sender sends is still a peer's to send: it takes ten bytes.
        for kind in [v1::BatchKind::Change as i32, -1] {
            let frame = v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Batch(v1::BatchFrame {
                    schema_epoch: u64::MAX,
                    kind,
                    data_header: vec![1; header].into(),
                    padding: Bytes::new(),
                    data_body: vec![2; body].into(),
                })),
            };
            let received = read_received(&chained(frame.chunks()));
            let Some(v1::read_frame::Frame::Batch(batch)) = received.message.frame else {
                panic!("a batch");
            };
            let extra = beside((batch.data_header, batch.data_body, received.allocation));
            assert!(extra <= MESSAGE_ROOM, "{header} {body} {kind}: {extra}");
        }
    }
}

#[test]
fn a_message_beyond_its_room_is_refused_before_it_decodes() {
    // A peer stuffs 4 KiB of padding before the body: the message fits the wire bound, but the
    // batch it carries would keep alive 4 KiB its frame does not count.
    let mut frame = batch_frame(1, &[1; 8], &[2; 64]);
    if let Some(v1::write_frame::Frame::Batch(batch)) = &mut frame.frame {
        batch.padding = Bytes::from(vec![0; 4_096]);
    }
    let (header, body, allocation) = written(write_received(&prefixed(&frame), 1 << 20));
    let refused = IpcFrame::arrived(header, body, allocation).expect_err("beyond its room");
    assert!(
        matches!(
            refused,
            WireError::Malformed { problem: Problem::MessageRoom { extra }, .. } if extra > 4_096
        ),
        "{refused}"
    );
}

#[test]
fn a_zero_row_batch_crosses_the_plane_and_decodes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, true),
    ]));
    let empty = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![empty.clone(), empty]).unwrap();
    let mut encoder = Encoder::default();
    let schema_message = encoder.schema(&schema).unwrap();
    let [ipc] = &encoder.batch(&batch).unwrap()[..] else {
        panic!("one frame: no dictionaries");
    };
    assert!(ipc.body.is_empty(), "no rows, no body");
    let frame = v1::WriteFrame {
        frame: Some(v1::write_frame::Frame::Batch(v1::WriteBatch {
            segment: 1,
            data_header: ipc.header.clone(),
            padding: Bytes::new(),
            data_body: ipc.body.clone(),
        })),
    };
    let (header, body, allocation) = written(write_received(&chained(frame.chunks()), 1 << 20));
    let arrived = IpcFrame::arrived(header, body, allocation).expect("within its room");
    let mut decoder = Decoder::new(Limits::default());
    decoder.schema(&schema_message).unwrap();
    let read = decoder.frame(&arrived).unwrap().expect("a batch");
    assert_eq!(read, batch);
}
