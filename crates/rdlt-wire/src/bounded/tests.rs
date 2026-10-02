use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use tonic::{Code, Status};

use super::{Bounded, Bounds, Window};
use crate::scan::response;

/// A body of `frames`, in order, which ends after them, or waits for ever where it `stalls`.
struct Frames(VecDeque<Frame<Bytes>>, bool);

impl Body for Frames {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        match self.0.pop_front() {
            None if self.1 => Poll::Pending,
            frame => Poll::Ready(frame.map(Ok)),
        }
    }
}

/// A gRPC message of `payload`.
fn message(payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).unwrap();
    let mut message = vec![0];
    message.extend(length.to_be_bytes());
    message.extend(payload);
    message
}

fn chunks(chunks: &[&[u8]]) -> tonic::body::Body {
    let frames = chunks
        .iter()
        .map(|chunk| Frame::data(Bytes::copy_from_slice(chunk)))
        .collect();
    tonic::body::Body::new(Frames(frames, false))
}

/// A body of `chunk`, which then waits for ever.
fn stalled(chunk: &[u8]) -> tonic::body::Body {
    let frames = [Frame::data(Bytes::copy_from_slice(chunk))].into();
    tonic::body::Body::new(Frames(frames, true))
}

fn bounds(wire: usize, decoded: usize) -> Bounds {
    Bounds {
        form: response("Discover"),
        wire,
        decoded,
    }
}

/// Every frame `body` passes on: its data whole, or its error's code.
fn passed(mut body: Bounded) -> Vec<Result<Vec<u8>, Code>> {
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut passed = Vec::new();
    loop {
        match Pin::new(&mut body).poll_frame(&mut context) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => passed.push(Ok(data.to_vec())),
                Err(frame) => {
                    assert!(frame.is_trailers());
                    passed.push(Ok(b"trailers".to_vec()));
                }
            },
            Poll::Ready(Some(Err(status))) => {
                passed.push(Err(status.code()));
                return passed;
            }
            Poll::Ready(None) => return passed,
            Poll::Pending => panic!("a body of frames is always ready"),
        }
    }
}

#[test]
fn a_message_is_passed_on_once_it_has_arrived_whole() {
    let whole = message(&[0x0a, 0x00, 0x0a, 0x00]);
    let body = chunks(&[&whole[..3], &whole[3..7], &whole[7..]]);
    let passed = passed(Bounded::new(body, bounds(1024, 1 << 20), None));
    assert_eq!(passed, [Ok(whole)]);
}

#[test]
fn messages_arriving_together_are_passed_on_together_and_a_partial_one_waits() {
    let (first, second) = (message(&[0x0a, 0x00]), message(&[]));
    let mut joined = first.clone();
    joined.extend(&second);
    joined.extend(&second[..2]);
    let body = chunks(&[&joined, &second[2..]]);
    let passed = passed(Bounded::new(body, bounds(1024, 1 << 20), None));
    let mut together = first;
    together.extend(&second);
    assert_eq!(passed, [Ok(together), Ok(second)]);
}

#[test]
fn a_message_longer_than_its_wire_bound_is_refused_from_its_prefix() {
    let declared = message(&[0; 11]);
    let body = chunks(&[&declared[..5]]);
    assert_eq!(
        passed(Bounded::new(body, bounds(10, usize::MAX), None)),
        [Err(Code::OutOfRange)]
    );
    let at = message(&[0x0a, 0x00].repeat(5));
    let body = chunks(&[&at]);
    assert_eq!(
        passed(Bounded::new(body, bounds(10, usize::MAX), None)),
        [Ok(at)]
    );
}

#[test]
fn a_message_holding_more_decoded_than_its_bound_is_refused() {
    let payload = [0x0a, 0x00].repeat(4);
    let held = crate::scan::decoded(response("Discover").unwrap(), &payload, usize::MAX).unwrap();
    let whole = message(&payload);
    let refused = Bounded::new(chunks(&[&whole]), bounds(1024, held - 1), None);
    assert_eq!(passed(refused), [Err(Code::OutOfRange)]);
    let taken = Bounded::new(chunks(&[&whole]), bounds(1024, held), None);
    assert_eq!(passed(taken), [Ok(whole.clone())]);
    // Frames are not counted.
    let frames = Bounds {
        form: None,
        ..bounds(1024, 0)
    };
    assert_eq!(
        passed(Bounded::new(chunks(&[&whole]), frames, None)).len(),
        1
    );
}

#[test]
fn an_encoding_that_does_not_scan_is_left_for_the_decoder() {
    let whole = message(&[0x0b]);
    let body = chunks(&[&whole]);
    let least = size_of::<crate::v1::Catalog>();
    assert_eq!(
        passed(Bounded::new(body, bounds(1024, least), None)),
        [Ok(whole)]
    );
}

#[test]
fn what_arrived_of_a_message_a_body_ends_within_is_passed_on_then_its_trailers() {
    let whole = message(&[0x0a, 0x00]);
    let mut frames: VecDeque<_> = [Frame::data(Bytes::copy_from_slice(&whole[..4]))].into();
    frames.push_back(Frame::trailers(tonic::codegen::http::HeaderMap::new()));
    let body = tonic::body::Body::new(Frames(frames, false));
    let passed = passed(Bounded::new(body, bounds(1024, 1 << 20), None));
    assert_eq!(passed, [Ok(whole[..4].to_vec()), Ok(b"trailers".to_vec())]);
}

#[test]
fn messages_still_arriving_on_a_connection_share_its_window() {
    let window = Window::new(12);
    let whole = message(&[0x0a, 0x00, 0x0a, 0x00]);
    // Nine bytes arrive in two pieces, and all nine pass: the window is free again.
    let body = chunks(&[&whole[..6], &whole[6..]]);
    let first = passed(Bounded::new(
        body,
        bounds(1024, 1 << 20),
        Some(window.clone()),
    ));
    assert_eq!(first, [Ok(whole.clone())]);
    assert_eq!(window.held.load(std::sync::atomic::Ordering::SeqCst), 0);
    // Two bodies each holding six bytes of a message fill it, and a third is refused.
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut holding = Vec::new();
    for _ in 0..2 {
        let mut body = Bounded::new(
            stalled(&whole[..6]),
            bounds(1024, 1 << 20),
            Some(window.clone()),
        );
        assert!(Pin::new(&mut body).poll_frame(&mut context).is_pending());
        holding.push(body);
    }
    assert_eq!(window.held.load(std::sync::atomic::Ordering::SeqCst), 12);
    let refused = Bounded::new(
        chunks(&[&whole[..1]]),
        bounds(1024, 1 << 20),
        Some(window.clone()),
    );
    assert_eq!(passed(refused), [Err(Code::ResourceExhausted)]);
    drop(holding);
    assert_eq!(window.held.load(std::sync::atomic::Ordering::SeqCst), 0);
}
