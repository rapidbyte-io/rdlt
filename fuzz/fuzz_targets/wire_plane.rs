//! Whatever bytes a data-plane call's request or answer carries, in whatever chunks, the data
//! plane's reader passes on only messages held to their bounds, without panicking, and every
//! message it decodes is sent again as the bytes prost encodes it to.

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
use rdlt_wire::tonic::Status;
use rdlt_wire::tonic::codegen::http;
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

/// What `future` comes to, which it does when first polled: every chunk is ready.
fn ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("a body whose every chunk is ready waits"),
    }
}

/// Checks that `message` is sent as prost encodes it, and decodes from what is sent again.
fn sent_again<M: Chained + Default + Clone + std::fmt::Debug>(message: &M) {
    let chunks = message.clone().chunks();
    let mut sent = chunks.head.to_vec();
    sent.extend(chunks.body.unwrap_or_default());
    let mut encoded = vec![0];
    encoded.extend(u32::try_from(message.encoded_len()).unwrap().to_be_bytes());
    message.encode(&mut encoded).unwrap();
    assert_eq!(sent, encoded, "{message:?} is sent as prost encodes it");
    let again = M::decode(&sent[5..]).unwrap();
    assert_eq!(
        again.encode_to_vec(),
        &sent[5..],
        "{message:?} decodes as sent"
    );
}

/// Reads every message of `incoming` until it ends or fails, each sent again.
fn read<M: Chained + Default + Clone + std::fmt::Debug>(mut incoming: Incoming<M>) {
    while let Ok(Some(message)) = ready(incoming.message()) {
        sent_again(&message);
    }
}

fuzz_target!(|input: (u8, Vec<u8>, Vec<u16>)| {
    let (call, bytes, cuts) = input;
    let limits = limits();
    let body = || rdlt_wire::tonic::body::Body::new(Chunked(chunked(&bytes, &cuts)));
    // A request is held to the connection's window, as a served connector holds it.
    let request = |method| {
        let bounds = Bounds::of(
            &limits,
            Class::of_request(method),
            rdlt_wire::scan::request(method),
        );
        let window = Window::new(limits.largest() * 4);
        Bounded::new(body(), bounds, Some(window))
    };
    match call % 4 {
        0 => read(Incoming::<v1::WriteFrame>::request(request("Write"))),
        1 => read(Incoming::<v1::ReadControl>::request(request("Read"))),
        2 => {
            let unary = Incoming::<v1::ReadPublishedRequest>::request(request("ReadPublished"));
            if let Ok(read_back) = ready(unary.unary()) {
                sent_again(&read_back);
            }
        }
        _ => {
            let bounds = Bounds::of(
                &limits,
                Class::of_answer("Read"),
                rdlt_wire::scan::response("Read"),
            );
            let answer = http::Response::new(Bounded::new(body(), bounds, None));
            let frames = Incoming::<v1::ReadFrame>::answer(answer).expect("an answer under way");
            read(frames);
        }
    }
});
