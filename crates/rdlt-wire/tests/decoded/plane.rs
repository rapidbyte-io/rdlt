//! A batch, or a push of JSON, decoded through the data plane holds its body where the bounded
//! body passed it on: decoding it takes no copy, where tonic's `Streaming` takes one.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use rdlt_wire::bounded::{Bounded, Bounds};
use rdlt_wire::limits::{Class, Limits};
use rdlt_wire::plane::Incoming;
use rdlt_wire::prost::Message;
use rdlt_wire::v1;
use tonic::Status;
use tonic::codec::Streaming;
use tonic::codegen::http;

use super::HEAP;

/// Bytes: a batch's body.
const BODY: usize = 8 << 20;

/// Bytes: a push of JSON's.
const JSON: usize = 16 << 20;

/// A body of one chunk, which then ends.
struct Once(Option<Bytes>);

impl Body for Once {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        Poll::Ready(self.0.take().map(|chunk| Ok(Frame::data(chunk))))
    }
}

/// `frame` as a gRPC message.
fn prefixed(frame: &impl Message) -> Bytes {
    let mut message = vec![0];
    let length = u32::try_from(frame.encoded_len()).expect("a frame within a prefix's length");
    message.extend(length.to_be_bytes());
    frame
        .encode(&mut message)
        .expect("a vector has room for any message");
    Bytes::from(message)
}

/// A write frame of a batch of [`BODY`] bytes, as a gRPC message.
fn message() -> Bytes {
    prefixed(&v1::WriteFrame {
        frame: Some(v1::write_frame::Frame::Batch(v1::WriteBatch {
            segment: 7,
            data_header: Bytes::from_static(b"an IPC message's header"),
            data_body: Bytes::from(vec![3; BODY]),
        })),
    })
}

/// `message`, whole in one chunk, held to the bounds of `class` and `form`.
fn held(message: &Bytes, class: Class, form: Option<&'static rdlt_wire::scan::Form>) -> Bounded {
    let limits = Limits {
        frame_bytes: 2 * JSON as u64,
        ..Limits::default()
    };
    let body = tonic::body::Body::new(Once(Some(message.clone())));
    Bounded::new(body, Bounds::of(&limits, class, form), None)
}

/// `message`, whole in one chunk, held to the bounds of a write's frames.
fn bounded(message: &Bytes) -> Bounded {
    held(
        message,
        Class::of_request("Write"),
        rdlt_wire::scan::request("Write"),
    )
}

/// `message`, whole in one chunk, held to the bounds of a read's frames.
fn answered(message: &Bytes) -> Bounded {
    held(
        message,
        Class::of_answer("Read"),
        rdlt_wire::scan::response("Read"),
    )
}

/// What `future` comes to, which it does when first polled: everything it reads is ready.
fn ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("everything the body holds is ready"),
    }
}

/// What reading the first message with `read` held on the heap at its peak, beyond what was
/// held before the body was made.
fn peak_of<M>(read: impl FnOnce(Bounded) -> M, body: impl FnOnce() -> Bounded) -> usize {
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let decoded = read(body());
    let peak = HEAP.peak_usage().saturating_sub(before);
    drop(decoded);
    peak
}

#[test]
fn a_batch_decoded_through_the_plane_holds_no_copy_of_its_body() {
    let message = message();
    let plane = peak_of(
        |body| ready(Incoming::<v1::WriteFrame>::request(body).message()),
        || bounded(&message),
    );
    assert!(
        plane <= message.len() + (1 << 20),
        "the plane held {plane} bytes for a message of {}",
        message.len()
    );
    let tonic = peak_of(
        |body| {
            let decoder = tonic::codec::Codec::decoder(&mut tonic_prost::ProstCodec::<
                v1::WriteAck,
                v1::WriteFrame,
            >::default());
            let mut streaming = Streaming::new_request(decoder, body, None, None);
            ready(streaming.message())
        },
        || bounded(&message),
    );
    assert!(
        tonic > 2 * message.len(),
        "tonic copies the message it is passed: it held {tonic} bytes"
    );
}

/// A read's frame of a batch of [`BODY`] bytes, and one of a push of [`JSON`] bytes.
fn read_frames() -> [v1::ReadFrame; 2] {
    use v1::read_frame::Frame;
    let batch = Frame::Batch(v1::BatchFrame {
        schema_epoch: 1,
        kind: v1::BatchKind::Arrow as i32,
        data_header: Bytes::from_static(b"an IPC message's header"),
        data_body: Bytes::from(vec![5; BODY]),
    });
    let json = Frame::Json(v1::JsonFrame {
        data: Bytes::from(vec![b' '; JSON]),
    });
    [batch, json].map(|frame| v1::ReadFrame { frame: Some(frame) })
}

#[test]
fn a_read_s_batch_or_push_decoded_through_the_plane_holds_no_copy_of_its_body() {
    for frame in read_frames() {
        let message = prefixed(&frame);
        drop(frame);
        let plane = peak_of(
            |body| {
                let answer = http::Response::new(body);
                let mut frames = Incoming::<v1::ReadFrame>::answer(answer).expect("under way");
                ready(frames.message())
            },
            || answered(&message),
        );
        assert!(
            plane <= message.len() + (1 << 20),
            "the plane held {plane} bytes for a message of {}",
            message.len()
        );
        let tonic = peak_of(
            |body| {
                let decoder = tonic::codec::Codec::decoder(&mut tonic_prost::ProstCodec::<
                    v1::ReadControl,
                    v1::ReadFrame,
                >::default());
                let mut streaming =
                    Streaming::new_response(decoder, body, http::StatusCode::OK, None, None);
                ready(streaming.message())
            },
            || answered(&message),
        );
        assert!(
            tonic > 2 * message.len(),
            "tonic copies the message it is passed: it held {tonic} bytes"
        );
    }
}
