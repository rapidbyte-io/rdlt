mod oracle;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use http_body::{Body as _, Frame};
use proptest::prelude::*;
use prost::Message;
use tonic::codegen::http::{self, HeaderMap};
use tonic::{Code, Status};

use super::{Chained, Chunks, Incoming, Outgoing};
use crate::bounded::{Bounded, Bounds, Charge, Charged, Charging, Held};
use crate::limits::{Class, Limits};
use crate::testing::{chunks, fed};
use crate::v1;

/// A write frame of a batch.
fn batch_frame(segment: u64, header: &[u8], body: &[u8]) -> v1::WriteFrame {
    v1::WriteFrame {
        frame: Some(v1::write_frame::Frame::Batch(v1::WriteBatch {
            segment,
            data_header: Bytes::copy_from_slice(header),
            data_body: Bytes::copy_from_slice(body),
        })),
    }
}

/// A write's answer granting `bytes`.
fn credit(bytes: u64) -> v1::WriteAck {
    v1::WriteAck {
        ack: Some(v1::write_ack::Ack::Credit(v1::Credit { bytes })),
    }
}

/// `message` as prost encodes it, its gRPC prefix before it.
fn prefixed(message: &impl Message) -> Vec<u8> {
    let mut prefixed = vec![0];
    prefixed.extend(u32::try_from(message.encoded_len()).unwrap().to_be_bytes());
    message.encode(&mut prefixed).unwrap();
    prefixed
}

/// `body` held to the bounds of a write's frames.
fn requested(body: tonic::body::Body) -> Bounded {
    let bounds = Bounds::of(
        &Limits::default(),
        Class::of_request("Write"),
        crate::scan::request("Write"),
    );
    Bounded::new(body, bounds, None)
}

/// `body` held to the bounds of a write's answers.
fn answered(body: tonic::body::Body) -> Bounded {
    let bounds = Bounds::of(
        &Limits::default(),
        Class::of_answer("Write"),
        crate::scan::response("Write"),
    );
    Bounded::new(body, bounds, None)
}

/// An answer of `body` whose headers say it is under way.
fn under_way(body: Bounded) -> http::Response<Bounded> {
    http::Response::builder()
        .header("content-type", "application/grpc")
        .body(body)
        .unwrap()
}

/// The trailers ending a call with `status`.
fn trailers(status: &Status) -> HeaderMap {
    let mut trailers = HeaderMap::new();
    status.add_header(&mut trailers).unwrap();
    trailers
}

#[tokio::test]
async fn a_message_decodes_from_the_bytes_the_body_passed_on() {
    let frame = batch_frame(42, b"header", &[7; 3_000]);
    let whole = prefixed(&frame);
    let body = chunks(&[&whole[..9], &whole[9..]]);
    let mut incoming = Incoming::<v1::WriteFrame>::request(requested(body));
    assert_eq!(incoming.message().await.unwrap(), Some(frame));
    assert_eq!(incoming.message().await.unwrap(), None);
}

#[test]
fn a_batch_s_body_is_a_slice_of_the_bytes_passed_on() {
    let message = Bytes::from(prefixed(&batch_frame(1, b"h", &[3; 4_096])));
    let decoded = super::incoming::decoded::<v1::WriteFrame>(message.clone()).unwrap();
    let Some(v1::write_frame::Frame::Batch(batch)) = decoded.frame else {
        panic!("a batch");
    };
    let within = message.as_ptr_range();
    let body = batch.data_body.as_ptr_range();
    assert!(
        within.start <= body.start && body.end <= within.end,
        "the body is no copy"
    );
}

#[tokio::test]
async fn a_compressed_message_is_refused_before_it_decodes() {
    for flag in [1, 2] {
        let mut message = prefixed(&batch_frame(1, b"h", b"b"));
        message[0] = flag;
        let body = requested(chunks(&[&message]));
        let mut incoming = Incoming::<v1::WriteFrame>::request(body);
        assert_eq!(
            incoming.message().await.unwrap_err().code(),
            Code::Internal,
            "{flag}"
        );
        assert_eq!(incoming.message().await.unwrap(), None, "it failed");
    }
}

#[tokio::test]
async fn a_message_that_does_not_decode_fails_the_call() {
    // A start whose table's name is no UTF-8: the scan walks it, prost refuses it.
    let message = crate::testing::message(&[0x0a, 0x05, 0x12, 0x03, 0x12, 0x01, 0xff]);
    let mut incoming = Incoming::<v1::WriteFrame>::request(requested(chunks(&[&message])));
    assert_eq!(incoming.message().await.unwrap_err().code(), Code::Internal);
}

#[tokio::test]
async fn trailers_with_an_error_end_an_answer_with_it() {
    let (feed, body) = fed();
    feed.send(Frame::data(Bytes::from(prefixed(&credit(5)))))
        .unwrap();
    feed.send(Frame::trailers(trailers(&Status::resource_exhausted(
        "full",
    ))))
    .unwrap();
    drop(feed);
    let mut incoming = Incoming::<v1::WriteAck>::answer(under_way(answered(body))).unwrap();
    assert_eq!(incoming.message().await.unwrap(), Some(credit(5)));
    assert_eq!(
        incoming.message().await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert_eq!(incoming.message().await.unwrap(), None, "it failed");
}

#[tokio::test]
async fn trailers_of_success_end_an_answer() {
    let (feed, body) = fed();
    feed.send(Frame::data(Bytes::from(prefixed(&credit(5)))))
        .unwrap();
    feed.send(Frame::trailers(trailers(&Status::ok(""))))
        .unwrap();
    drop(feed);
    let mut incoming = Incoming::<v1::WriteAck>::answer(under_way(answered(body))).unwrap();
    assert_eq!(incoming.message().await.unwrap(), Some(credit(5)));
    assert_eq!(incoming.message().await.unwrap(), None);
}

#[test]
fn a_trailers_only_answer_fails_with_its_status_before_any_message() {
    let mut answer = under_way(answered(chunks(&[])));
    answer
        .headers_mut()
        .extend(trailers(&Status::unimplemented("no write")));
    let refused = Incoming::<v1::WriteAck>::answer(answer).unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
}

#[test]
fn an_answer_naming_a_compression_is_refused() {
    let mut answer = under_way(answered(chunks(&[])));
    answer
        .headers_mut()
        .insert("grpc-encoding", http::HeaderValue::from_static("gzip"));
    let refused = Incoming::<v1::WriteAck>::answer(answer).unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
}

#[tokio::test]
async fn a_body_ending_within_a_message_fails_as_bounded_says() {
    let message = prefixed(&batch_frame(1, b"h", &[0; 100]));
    let body = requested(chunks(&[&message[..50]]));
    let mut incoming = Incoming::<v1::WriteFrame>::request(body);
    assert_eq!(incoming.message().await.unwrap_err().code(), Code::Internal);
}

#[tokio::test]
async fn a_request_its_client_cancelled_ends_where_it_was_cut() {
    let (feed, body) = fed();
    let frame = batch_frame(1, b"h", b"b");
    feed.send(Frame::data(Bytes::from(prefixed(&frame))))
        .unwrap();
    feed.fail(Status::cancelled("reset")).unwrap();
    let mut incoming = Incoming::<v1::WriteFrame>::request(requested(body));
    assert_eq!(incoming.message().await.unwrap(), Some(frame));
    assert_eq!(incoming.message().await.unwrap(), None);
}

/// How many charges are held now, and the most held at once.
#[derive(Default)]
struct Counts {
    now: AtomicUsize,
    most: AtomicUsize,
}

impl Counts {
    fn now(&self) -> usize {
        self.now.load(Ordering::SeqCst)
    }

    fn most(&self) -> usize {
        self.most.load(Ordering::SeqCst)
    }
}

/// A charge held until it is dropped.
struct Hold(Arc<Counts>);

impl Drop for Hold {
    fn drop(&mut self) {
        self.0.now.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Charges every message at once, counting the charges held.
struct Counting(Arc<Counts>);

impl Charge for Counting {
    fn charge(&self, _: Class, _: usize) -> Charging {
        let counts = Arc::clone(&self.0);
        Box::pin(async move {
            let now = counts.now.fetch_add(1, Ordering::SeqCst) + 1;
            counts.most.fetch_max(now, Ordering::SeqCst);
            Ok(Box::new(Hold(counts)) as Held)
        })
    }
}

#[tokio::test]
async fn each_message_releases_the_charge_of_the_one_before() {
    let counts = Arc::new(Counts::default());
    let charge = Arc::new(Counting(Arc::clone(&counts))) as Arc<dyn Charge>;
    let body = chunks(&[
        &prefixed(&credit(1)),
        &prefixed(&credit(2)),
        &prefixed(&credit(3)),
    ]);
    let bounded = answered(body).charged(Some(charge), Charged::default());
    let mut incoming = Incoming::<v1::WriteAck>::answer(under_way(bounded)).unwrap();
    incoming.message().await.unwrap();
    assert_eq!(counts.now(), 1, "held while it is decoded");
    incoming.release();
    assert_eq!(counts.now(), 0, "released once decoded");
    incoming.message().await.unwrap();
    // Not released by its reader: the next message's charge releases it first.
    incoming.message().await.unwrap();
    assert_eq!((counts.now(), counts.most()), (1, 1));
    drop(incoming);
    assert_eq!(counts.now(), 0, "the call's end releases the last");
}

/// Every frame `body` sends, until it ends or fails.
async fn sent<M: Chained + Send + 'static>(
    mut body: Outgoing<M>,
) -> Vec<Result<Frame<Bytes>, Code>> {
    let mut sent = Vec::new();
    while let Some(frame) =
        std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await
    {
        let failed = frame.is_err();
        sent.push(frame.map_err(|status| status.code()));
        if failed {
            break;
        }
    }
    assert!(body.is_end_stream() || sent.last().is_some_and(Result::is_err));
    sent
}

/// The data of `frames`, joined.
fn joined(frames: &[Result<Frame<Bytes>, Code>]) -> Vec<u8> {
    frames
        .iter()
        .filter_map(|frame| frame.as_ref().ok()?.data_ref())
        .flat_map(|data| data.to_vec())
        .collect()
}

#[tokio::test]
async fn a_request_sends_each_message_s_chunks_and_ends_with_the_last() {
    let frames = [batch_frame(1, b"h", &[9; 1_000]), batch_frame(2, b"", b"")];
    let messages = tonic::codegen::tokio_stream::iter(frames.clone());
    let sent = sent(Outgoing::request(messages, 1 << 20)).await;
    assert_eq!(
        sent.len(),
        3,
        "a batch's head and body, then an empty batch whole"
    );
    let expected: Vec<u8> = frames.iter().flat_map(prefixed).collect();
    assert_eq!(joined(&sent), expected);
    assert!(
        sent.iter()
            .all(|frame| frame.as_ref().is_ok_and(Frame::is_data))
    );
}

#[tokio::test]
async fn an_answer_ends_with_trailers_of_success_or_of_its_failure() {
    let ended = tonic::codegen::tokio_stream::iter([Ok(credit(1))]);
    let succeeded = sent(Outgoing::answer(Box::pin(ended), 1 << 20)).await;
    let trailers = succeeded
        .last()
        .unwrap()
        .as_ref()
        .unwrap()
        .trailers_ref()
        .unwrap();
    assert_eq!(Status::from_header_map(trailers).unwrap().code(), Code::Ok);
    assert_eq!(joined(&succeeded), prefixed(&credit(1)));
    let failed = tonic::codegen::tokio_stream::iter([
        Ok(credit(1)),
        Err(Status::resource_exhausted("full")),
        Ok(credit(2)),
    ]);
    let failing = sent(Outgoing::answer(Box::pin(failed), 1 << 20)).await;
    assert_eq!(failing.len(), 2, "nothing after the failure");
    let trailers = failing[1].as_ref().unwrap().trailers_ref().unwrap();
    let status = Status::from_header_map(trailers).unwrap();
    assert_eq!(status.code(), Code::ResourceExhausted);
}

#[tokio::test]
async fn a_message_beyond_the_limit_fails_the_call_it_would_be_sent_on() {
    let large = batch_frame(1, b"h", &[0; 100]);
    let request = Outgoing::request(tonic::codegen::tokio_stream::iter([large.clone()]), 99);
    let refused = sent(request).await;
    assert!(
        matches!(refused[..], [Err(Code::OutOfRange)]),
        "{refused:?}"
    );
    let answer = Outgoing::answer(
        Box::pin(tonic::codegen::tokio_stream::iter([Ok(large)])),
        99,
    );
    let ended = sent(answer).await;
    let trailers = ended[0].as_ref().unwrap().trailers_ref().unwrap();
    let status = Status::from_header_map(trailers).unwrap();
    assert_eq!(status.code(), Code::OutOfRange);
}

#[tokio::test]
async fn what_a_request_sends_is_read_as_the_messages_it_sent() {
    let frames = vec![
        v1::WriteFrame {
            frame: Some(v1::write_frame::Frame::Flush(v1::Unit {})),
        },
        batch_frame(3, b"head", &[1; 70_000]),
        batch_frame(0, b"", &[2; 10]),
    ];
    let messages = tonic::codegen::tokio_stream::iter(frames.clone());
    let sent = sent(Outgoing::request(messages, 1 << 20)).await;
    let (feed, body) = fed();
    for frame in sent {
        feed.send(frame.unwrap()).unwrap();
    }
    drop(feed);
    let mut incoming = Incoming::<v1::WriteFrame>::request(requested(body));
    for frame in frames {
        assert_eq!(incoming.message().await.unwrap(), Some(frame));
    }
    assert_eq!(incoming.message().await.unwrap(), None);
}

/// The bytes `chunks` hold, head then body.
fn chained(chunks: Chunks) -> Vec<u8> {
    let mut chained = chunks.head.to_vec();
    chained.extend(chunks.body.unwrap_or_default());
    chained
}

/// Checks that `message`'s chunks are the bytes prost encodes it to, behind its prefix.
fn holds_prost_s_bytes<M: Chained + Clone>(message: &M) -> Result<(), TestCaseError> {
    let chunks = message.clone().chunks();
    prop_assert_eq!(chunks.len(), 5 + message.encoded_len());
    prop_assert_eq!(chained(chunks), prefixed(message));
    Ok(())
}

fn bytes(most: usize) -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(any::<u8>(), 0..most).prop_map(Bytes::from)
}

/// Every kind of write frame, and none.
fn write_frame() -> impl Strategy<Value = v1::WriteFrame> {
    use v1::write_frame::Frame as F;
    let frame = prop_oneof![
        (any::<u64>(), ".{0,40}").prop_map(|(session, name)| F::Start(v1::WriteStart {
            session,
            table: Some(v1::TableRef {
                name,
                ..v1::TableRef::default()
            }),
        })),
        (any::<u32>(), bytes(512)).prop_map(|(version, ipc_schema)| F::Schema(v1::WriteSchema {
            version,
            ipc_schema,
        })),
        (any::<u64>(), bytes(512), bytes(65_536)).prop_map(|(segment, data_header, data_body)| {
            F::Batch(v1::WriteBatch {
                segment,
                data_header,
                data_body,
            })
        }),
        Just(F::Flush(v1::Unit {})),
    ];
    proptest::option::of(frame).prop_map(|frame| v1::WriteFrame { frame })
}

/// Every kind of write answer, and none.
fn write_ack() -> impl Strategy<Value = v1::WriteAck> {
    use v1::write_ack::Ack;
    let ack = prop_oneof![
        any::<u64>().prop_map(|bytes| Ack::Credit(v1::Credit { bytes })),
        (any::<u64>(), any::<u64>())
            .prop_map(|(rows, bytes)| Ack::Flushed(v1::WriteStats { rows, bytes })),
        ".{0,64}".prop_map(|message| Ack::Error(v1::Error {
            message,
            ..v1::Error::default()
        })),
    ];
    proptest::option::of(ack).prop_map(|ack| v1::WriteAck { ack })
}

proptest! {
    #[test]
    fn a_chained_write_frame_is_the_bytes_prost_encodes(frame in write_frame()) {
        holds_prost_s_bytes(&frame)?;
    }

    #[test]
    fn a_chained_batch_s_body_is_its_own_chunk(
        segment in any::<u64>(),
        header in bytes(512),
        body in bytes(65_536),
    ) {
        let frame = v1::WriteFrame { frame: Some(v1::write_frame::Frame::Batch(v1::WriteBatch {
            segment, data_header: header, data_body: body.clone(),
        })) };
        let chunks = frame.chunks();
        prop_assert_eq!(chunks.body.is_some(), !body.is_empty());
        if let Some(sent) = chunks.body {
            prop_assert_eq!(sent.as_ptr(), body.as_ptr(), "the body is sent as it is");
        }
    }

    #[test]
    fn a_chained_write_answer_is_the_bytes_prost_encodes(ack in write_ack()) {
        holds_prost_s_bytes(&ack)?;
        prop_assert!(ack.chunks().body.is_none());
    }
}
