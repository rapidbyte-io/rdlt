//! A batch, or a push of JSON, read off a data call's stream holds its body where it was read:
//! one allocation of the body's length, and no copy.

use std::task::{Context, Poll};

use bytes::{Buf as _, Bytes};
use rdlt_wire::limits::Limits;
use rdlt_wire::stream::{Call, End, FrameReader, Kind, Outgoing, Rows};

use super::HEAP;

/// Bytes: a batch's body.
const BODY: usize = 8 << 20;

/// Bytes: a push of JSON's.
const JSON: usize = 16 << 20;

/// What `future` comes to, which it does when first polled: everything it reads is in memory.
fn ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("everything the stream holds is ready"),
    }
}

/// The bytes of a frame of `kind` whose body is `body`.
fn framed(kind: Kind, body: Bytes) -> Vec<u8> {
    let mut frame = Outgoing::new(kind, vec![body]).expect("a body within a head");
    let mut bytes = Vec::with_capacity(frame.remaining());
    while frame.has_remaining() {
        bytes.extend_from_slice(frame.chunk());
        let taken = frame.chunk().len();
        frame.advance(taken);
    }
    bytes
}

#[test]
fn a_batch_or_push_read_off_a_stream_holds_one_allocation_of_its_length() {
    let limits = Limits {
        frame_bytes: 2 * JSON as u64,
        json_push_bytes: 2 * JSON as u64,
        ..Limits::default()
    };
    let batch = Kind::Batch {
        rows: Rows::Arrow,
        dictionary: false,
        tag: 1,
    };
    for (kind, len) in [(batch, BODY), (Kind::Json, JSON)] {
        let bytes = framed(kind, Bytes::from(vec![5; len]));
        HEAP.reset_peak_usage();
        let before = HEAP.current_usage();
        let mut reader = FrameReader::new(&bytes[..], Call::Read.bounds(End::Connector, &limits));
        let frame = ready(reader.next()).expect("a frame").expect("one");
        let peak = HEAP.peak_usage().saturating_sub(before);
        assert_eq!(frame.body.bytes.len(), len);
        assert!(
            peak < len + 4_096,
            "a body of {len} bytes held {peak} bytes on the heap at its peak"
        );
    }
}
