//! A read's messages through the plane: its controls and read-back request as the host sends
//! them, and its frames as the connector answers them, a batch's body and a push of JSON each a
//! chunk of its own.

use bytes::Bytes;
use http_body::Frame;
use proptest::prelude::*;
use tonic::{Code, Status};

use super::{bytes, holds_prost_s_bytes, joined, prefixed, sent, trailers, under_way};
use crate::bounded::{Bounded, Bounds};
use crate::limits::{Class, Limits};
use crate::plane::{Chained, Incoming, Outgoing};
use crate::testing::{chunks, fed};
use crate::v1;

/// A read's frame of a batch.
pub(super) fn read_batch(epoch: u64, header: &[u8], body: &[u8]) -> v1::ReadFrame {
    v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Batch(v1::BatchFrame {
            schema_epoch: epoch,
            kind: v1::BatchKind::Arrow as i32,
            data_header: Bytes::copy_from_slice(header),
            data_body: Bytes::copy_from_slice(body),
        })),
    }
}

/// A read's frame of a push of JSON.
pub(super) fn json(data: &[u8]) -> v1::ReadFrame {
    v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Json(v1::JsonFrame {
            data: Bytes::copy_from_slice(data),
        })),
    }
}

/// The frame that ends a read.
pub(super) fn done() -> v1::ReadFrame {
    v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Done(v1::Done {})),
    }
}

/// `body` held to the bounds of a read's frames.
pub(super) fn read_answer(body: tonic::body::Body) -> Bounded {
    let bounds = Bounds::of(
        &Limits::default(),
        Class::of_answer("Read"),
        crate::scan::response("Read"),
    );
    Bounded::new(body, bounds, None)
}

/// `body` held to the bounds of a request of `method`, a read's.
pub(super) fn read_request(body: tonic::body::Body, method: &str) -> Bounded {
    let bounds = Bounds::of(
        &Limits::default(),
        Class::of_request(method),
        crate::scan::request(method),
    );
    Bounded::new(body, bounds, None)
}

/// A request to read back the table `name`.
pub(super) fn read_back(name: &str) -> v1::ReadPublishedRequest {
    v1::ReadPublishedRequest {
        table: Some(v1::TableRef {
            name: name.to_owned(),
            ..v1::TableRef::default()
        }),
    }
}

#[test]
fn a_read_s_batch_body_and_json_are_slices_of_the_bytes_passed_on() {
    use v1::read_frame::Frame as F;
    for frame in [read_batch(1, b"h", &[3; 4_096]), json(&[b'{'; 4_096])] {
        let message = Bytes::from(prefixed(&frame));
        let decoded = super::super::incoming::decoded::<v1::ReadFrame>(&message).unwrap();
        let body = match decoded.frame {
            Some(F::Batch(batch)) => batch.data_body,
            Some(F::Json(json)) => json.data,
            other => panic!("a batch or JSON: {other:?}"),
        };
        let within = message.as_ptr_range();
        let body = body.as_ptr_range();
        assert!(
            within.start <= body.start && body.end <= within.end,
            "the body is no copy"
        );
    }
}

#[test]
fn a_chained_read_frame_at_its_edges_is_the_bytes_prost_encodes() {
    // prost leaves out an empty field, a zero epoch and an unspecified kind among them, and a
    // body of 2^21 bytes or more takes a length of four bytes.
    for (epoch, kind, header, body) in [
        (0, 0, 0, 0),
        (0, 0, 0, 1),
        (1, 1, 0, 0),
        (0, 2, 5, 0),
        (u64::MAX, 1, 127, 127),
        (7, 1, 128, 128),
        (7, 2, 16_383, 16_384),
        (7, 1, 3, (1 << 21) - 1),
        (7, 1, 3, 1 << 21),
    ] {
        let frame = v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Batch(v1::BatchFrame {
                schema_epoch: epoch,
                kind,
                data_header: vec![1; header].into(),
                data_body: vec![2; body].into(),
            })),
        };
        holds_prost_s_bytes(&frame).unwrap();
        let sent = frame.chunks().body.map_or(0, |body| body.len());
        assert_eq!(sent, body, "{epoch} {kind} {header} {body}");
    }
    for length in [0, 1, 127, 128, 16_384, (1 << 21) - 1, 1 << 21] {
        let frame = json(&vec![b' '; length]);
        holds_prost_s_bytes(&frame).unwrap();
        let sent = frame.chunks().body.map_or(0, |body| body.len());
        assert_eq!(sent, length, "JSON of {length} bytes");
    }
}

/// Every kind of read frame, and none.
fn read_frame() -> impl Strategy<Value = v1::ReadFrame> {
    use v1::read_frame::Frame as F;
    let cursor =
        (any::<u32>(), bytes(256)).prop_map(|(version, bytes)| v1::Cursor { version, bytes });
    let frame = prop_oneof![
        (any::<u64>(), bytes(512)).prop_map(|(schema_epoch, ipc_schema)| F::Schema(
            v1::SchemaFrame {
                schema_epoch,
                ipc_schema,
            }
        )),
        (any::<u64>(), 0..4_i32, bytes(512), bytes(65_536)).prop_map(
            |(schema_epoch, kind, data_header, data_body)| F::Batch(v1::BatchFrame {
                schema_epoch,
                kind,
                data_header,
                data_body,
            })
        ),
        bytes(65_536).prop_map(|data| F::Json(v1::JsonFrame { data })),
        (
            proptest::option::of(cursor),
            proptest::option::of(any::<u64>())
        )
            .prop_map(|(cursor, barrier)| F::Checkpoint(v1::CheckpointFrame { cursor, barrier })),
        (0..5_i32, ".{0,64}").prop_map(|(level, message)| F::Log(v1::LogFrame { level, message })),
        (".{0,40}", any::<f64>())
            .prop_map(|(name, value)| F::Metric(v1::MetricFrame { name, value })),
        Just(F::Done(v1::Done {})),
        Just(F::Replan(v1::ReplanFrame {})),
        any::<u64>().prop_map(|records| F::Behind(v1::BehindFrame { records })),
    ];
    proptest::option::of(frame).prop_map(|frame| v1::ReadFrame { frame })
}

/// Every kind of read control, and none.
fn read_control() -> impl Strategy<Value = v1::ReadControl> {
    use v1::read_control::Control as C;
    let start = (
        proptest::option::of(".{0,20}"),
        ".{0,20}",
        ".{0,20}",
        proptest::option::of((any::<u32>(), bytes(256))),
        any::<u64>(),
        any::<(bool, bool)>(),
    )
        .prop_map(
            |(namespace, name, partition, cursor, barrier, (unbounded, follow))| {
                C::Start(v1::ReadStart {
                    stream: Some(v1::StreamName { namespace, name }),
                    partition,
                    cursor: cursor.map(|(version, bytes)| v1::Cursor { version, bytes }),
                    barrier,
                    unbounded,
                    follow,
                })
            },
        );
    let control = prop_oneof![
        start,
        any::<u64>().prop_map(|barrier| C::Checkpoint(v1::CheckpointRequest { barrier })),
        any::<u64>().prop_map(|bytes| C::Credit(v1::Credit { bytes })),
        (0..3_i32).prop_map(|mode| C::Stop(v1::Stop { mode })),
    ];
    proptest::option::of(control).prop_map(|control| v1::ReadControl { control })
}

proptest! {
    #[test]
    fn a_chained_read_frame_is_the_bytes_prost_encodes(frame in read_frame()) {
        holds_prost_s_bytes(&frame)?;
    }

    #[test]
    fn a_chained_read_frame_s_body_is_its_own_chunk(
        epoch in any::<u64>(),
        header in bytes(512),
        body in bytes(65_536),
        is_json in any::<bool>(),
    ) {
        use v1::read_frame::Frame as F;
        let frame = if is_json {
            F::Json(v1::JsonFrame { data: body.clone() })
        } else {
            F::Batch(v1::BatchFrame {
                schema_epoch: epoch,
                kind: v1::BatchKind::Arrow as i32,
                data_header: header,
                data_body: body.clone(),
            })
        };
        let chunks = v1::ReadFrame { frame: Some(frame) }.chunks();
        prop_assert_eq!(chunks.body.is_some(), !body.is_empty());
        if let Some(sent) = chunks.body {
            prop_assert_eq!(sent.as_ptr(), body.as_ptr(), "the body is sent as it is");
        }
    }

    #[test]
    fn a_chained_read_control_is_the_bytes_prost_encodes(control in read_control()) {
        holds_prost_s_bytes(&control)?;
        prop_assert!(control.chunks().body.is_none());
    }

    #[test]
    fn a_chained_read_back_request_is_the_bytes_prost_encodes(name in ".{0,64}", empty: bool) {
        let request = if empty { v1::ReadPublishedRequest::default() } else { read_back(&name) };
        holds_prost_s_bytes(&request)?;
        prop_assert!(request.chunks().body.is_none());
    }
}

#[tokio::test]
async fn what_a_read_s_answer_sends_is_read_as_the_frames_it_sent() {
    let frames = vec![
        read_batch(1, b"head", &[1; 70_000]),
        json(br#"{"a":1}"#),
        json(b""),
        read_batch(1, b"", b""),
        done(),
    ];
    let answers = tonic::codegen::tokio_stream::iter(frames.clone().into_iter().map(Ok));
    let sent = sent(Outgoing::answer(Box::pin(answers), 1 << 20)).await;
    assert_eq!(
        sent.len(),
        2 + 2 + 1 + 1 + 1 + 1,
        "a batch's and a push's head and body each, the empty ones whole, then the trailers"
    );
    let expected: Vec<u8> = frames.iter().flat_map(prefixed).collect();
    assert_eq!(joined(&sent), expected);
    let (feed, body) = fed();
    for frame in sent {
        feed.send(frame.unwrap()).unwrap();
    }
    drop(feed);
    let mut incoming = Incoming::<v1::ReadFrame>::answer(under_way(read_answer(body))).unwrap();
    for frame in frames {
        assert_eq!(incoming.message().await.unwrap(), Some(frame));
    }
    assert_eq!(incoming.message().await.unwrap(), None, "ended well");
}

#[tokio::test]
async fn a_failed_read_ends_with_its_status_after_the_frames_before_it() {
    let (feed, body) = fed();
    let frame = read_batch(1, b"h", &[4; 300]);
    feed.send(Frame::data(Bytes::from(prefixed(&frame))))
        .unwrap();
    feed.send(Frame::trailers(trailers(&Status::unavailable(
        "the source went",
    ))))
    .unwrap();
    drop(feed);
    let mut incoming = Incoming::<v1::ReadFrame>::answer(under_way(read_answer(body))).unwrap();
    assert_eq!(incoming.message().await.unwrap(), Some(frame));
    let failed = incoming.message().await.unwrap_err();
    assert_eq!(failed.code(), Code::Unavailable);
    assert_eq!(incoming.message().await.unwrap(), None, "it failed");
}

#[tokio::test]
async fn a_unary_request_is_its_one_message_read_to_the_request_s_end() {
    let request = prefixed(&read_back("t"));
    let one = Incoming::<v1::ReadPublishedRequest>::request(read_request(
        chunks(&[&request[..3], &request[3..]]),
        "ReadPublished",
    ));
    assert_eq!(one.unary().await.unwrap(), read_back("t"));
    let none =
        Incoming::<v1::ReadPublishedRequest>::request(read_request(chunks(&[]), "ReadPublished"));
    assert_eq!(none.unary().await.unwrap_err().code(), Code::Internal);
    let more = prefixed(&read_back("u"));
    let two = Incoming::<v1::ReadPublishedRequest>::request(read_request(
        chunks(&[&request, &more]),
        "ReadPublished",
    ));
    assert_eq!(two.unary().await.unwrap(), read_back("t"), "the first");
    let (feed, body) = fed();
    feed.send(Frame::data(Bytes::from(request.clone())))
        .unwrap();
    feed.fail(Status::unavailable("refused")).unwrap();
    let failing =
        Incoming::<v1::ReadPublishedRequest>::request(read_request(body, "ReadPublished"));
    assert_eq!(
        failing.unary().await.unwrap_err().code(),
        Code::Unavailable,
        "a request that fails after its message fails"
    );
}

#[tokio::test]
async fn a_read_s_controls_decode_from_the_bytes_the_body_passed_on() {
    let controls = [
        v1::ReadControl {
            control: Some(v1::read_control::Control::Start(v1::ReadStart {
                partition: "p".to_owned(),
                cursor: Some(v1::Cursor {
                    version: 1,
                    bytes: Bytes::from_static(b"cursor"),
                }),
                ..v1::ReadStart::default()
            })),
        },
        v1::ReadControl {
            control: Some(v1::read_control::Control::Credit(v1::Credit { bytes: 9 })),
        },
    ];
    let whole: Vec<u8> = controls.iter().flat_map(prefixed).collect();
    let body = chunks(&[&whole[..7], &whole[7..]]);
    let mut incoming = Incoming::<v1::ReadControl>::request(read_request(body, "Read"));
    for control in controls {
        assert_eq!(incoming.message().await.unwrap(), Some(control));
    }
    assert_eq!(incoming.message().await.unwrap(), None);
}
