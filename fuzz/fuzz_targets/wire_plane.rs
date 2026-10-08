//! Whatever bytes a write's request carries, in whatever chunks, the data plane's reader passes
//! on only messages held to their bounds, without panicking, and every frame it decodes is sent
//! again as the bytes prost encodes it to.

#![forbid(unsafe_code)]
#![no_main]

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use libfuzzer_sys::fuzz_target;
use rdlt_wire::bounded::{Bounded, Bounds, Window};
use rdlt_wire::limits::{Class, Limits};
use rdlt_wire::plane::{Chained, Incoming};
use rdlt_wire::prost::Message;
use rdlt_wire::tonic::Status;
use rdlt_wire::v1;

/// A body of chunks, each ready at once, which then ends.
struct Chunked(VecDeque<Bytes>);

impl Body for Chunked {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        Poll::Ready(self.0.pop_front().map(|chunk| Ok(Frame::data(chunk))))
    }
}

/// `bytes` cut at each of `cuts`, taken as offsets into what is left.
fn chunked(bytes: &[u8], cuts: &[u16]) -> VecDeque<Bytes> {
    let bytes = Bytes::copy_from_slice(bytes);
    let mut chunks = VecDeque::new();
    let mut at = 0;
    for cut in cuts {
        let end = (at + usize::from(*cut)).min(bytes.len());
        chunks.push_back(bytes.slice(at..end));
        at = end;
    }
    chunks.push_back(bytes.slice(at..));
    chunks
}

/// Limits small enough that the fuzzer reaches every bound.
fn limits() -> Limits {
    Limits {
        frame_bytes: rdlt_wire::limits::MIN_FRAME_BYTES,
        ..Limits::default()
    }
}

fuzz_target!(|input: (Vec<u8>, Vec<u16>)| {
    let (bytes, cuts) = input;
    let limits = limits();
    let bounds = Bounds::of(
        &limits,
        Class::of_request("Write"),
        rdlt_wire::scan::request("Write"),
    );
    let window = Window::new(limits.largest() * 4);
    let body = rdlt_wire::tonic::body::Body::new(Chunked(chunked(&bytes, &cuts)));
    let mut frames = Incoming::<v1::WriteFrame>::request(Bounded::new(body, bounds, Some(window)));
    let mut context = Context::from_waker(std::task::Waker::noop());
    loop {
        let read = match std::pin::pin!(frames.message()).poll(&mut context) {
            Poll::Ready(read) => read,
            Poll::Pending => panic!("a body whose every chunk is ready waits"),
        };
        let Ok(Some(frame)) = read else {
            return;
        };
        let chunks = frame.clone().chunks();
        let mut sent = chunks.head.to_vec();
        sent.extend(chunks.body.unwrap_or_default());
        let mut encoded = vec![0];
        encoded.extend(u32::try_from(frame.encoded_len()).unwrap().to_be_bytes());
        frame.encode(&mut encoded).unwrap();
        assert_eq!(sent, encoded, "{frame:?} is sent as prost encodes it");
        assert_eq!(v1::WriteFrame::decode(&sent[5..]).unwrap(), frame);
    }
});
