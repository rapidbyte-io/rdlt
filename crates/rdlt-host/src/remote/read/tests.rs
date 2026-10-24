use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, DictionaryArray, RecordBatch, StringArray};
use bytes::{BufMut, Bytes, BytesMut};
use proptest::prelude::*;
use rdlt_connector::cost::push_charge;
use rdlt_connector::wire::{MALFORMED_FRAME, v1};
use rdlt_connector::{Admission, BoxFuture, Permit, Push, SourceEvent, admitted_partition_channel};
use rdlt_wire::bounded::{Bounded, Bounds};
use rdlt_wire::limits::{Class, JSON_PUSH_BYTES, MESSAGE_ROOM, count};
use rdlt_wire::plane::{Incoming, Outgoing};
use rdlt_wire::prost::Message as _;
use rdlt_wire::prost::encoding::{WireType, encode_key, encode_varint};
use rdlt_wire::{Decoder, Encoder, Limits};

use super::{Kept, Read, Reader};

fn reader() -> Reader {
    let limits = Limits::default();
    Reader {
        decoder: Decoder::new(limits),
        limits,
        epoch: None,
        schema: 0,
        quiet: 0,
        free: 0,
    }
}

/// `frame` as a transport receives it: one buffer holding the message, padded with a megabyte
/// of a field this end does not know.
fn padded(frame: &v1::ReadFrame) -> Bytes {
    padded_by(frame, 1 << 20)
}

/// `frame` in one buffer holding the message, padded with `padding` bytes of a field this end
/// does not know.
fn padded_by(frame: &v1::ReadFrame, padding: usize) -> Bytes {
    let mut message = BytesMut::with_capacity(padding + frame.encoded_len() + 16);
    frame.encode(&mut message).expect("room for the frame");
    encode_key(1000, WireType::LengthDelimited, &mut message);
    encode_varint(u64::try_from(padding).expect("a length"), &mut message);
    message.put_bytes(0xaa, padding);
    message.freeze()
}

/// A frame of `text` as JSON.
fn json_frame(text: Bytes) -> v1::ReadFrame {
    v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Json(v1::JsonFrame { data: text })),
    }
}

/// The event `message` becomes; the frame decoded from it is dropped with the read's turn.
fn event(message: &Bytes) -> SourceEvent {
    let frame = v1::ReadFrame::decode(message.clone()).expect("a read frame");
    match reader()
        .event(frame, message.clone())
        .expect("an admitted frame")
    {
        Read::Event(event) => event,
        Read::Done | Read::Nothing => panic!("the frame is an event"),
    }
}

/// `frame` as the host receives it from a connector: sent as the connector's body sends it, held
/// as the host's transport holds it, and decoded; with the allocation its bytes lie in.
fn arrived(frame: v1::ReadFrame) -> (v1::ReadFrame, Bytes) {
    let limits = Limits::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async move {
        let body = tonic::body::Body::new(Outgoing::request(
            tokio_stream::iter([frame]),
            limits.largest(),
        ));
        let bounds = Bounds::of(
            &limits,
            Class::of_answer("Read"),
            rdlt_wire::scan::response("Read"),
        );
        let mut incoming = Incoming::<v1::ReadFrame>::request(Bounded::new(body, bounds, None));
        let received = incoming
            .arrived()
            .await
            .expect("the frame arrives")
            .expect("a frame");
        (received.message, received.allocation)
    })
}

/// Checks that a JSON push of `length` bytes, sent and received as a host receives it, is a
/// slice of the allocation it arrived in, whose every byte its charge counts.
fn charged_for_all_it_keeps_alive(length: usize) {
    let (frame, allocation) = arrived(json_frame(vec![b'x'; length].into()));
    let Ok(Read::Event(SourceEvent::Push(push))) = reader().event(frame, allocation.clone()) else {
        panic!("{length} bytes of JSON are a push");
    };
    let Push::Json(text) = &push else {
        panic!("the push is JSON");
    };
    assert_eq!(text.len(), length);
    let within = allocation.as_ptr_range();
    assert!(
        within.contains(&text.as_ptr()) && text.as_ptr_range().end <= within.end,
        "{length} bytes of JSON were copied out of their message"
    );
    assert!(
        push_charge(&push) >= count(allocation.len()),
        "{length} bytes of JSON keep {} alive, charged {}",
        allocation.len(),
        push_charge(&push)
    );
}

#[test]
fn a_json_push_is_a_slice_of_its_message_charged_for_all_it_keeps_alive() {
    // Every short text, each length where the lengths before it take another byte, the
    // engine's limit at the default budget, and the wire's.
    let lengths = (1..=300).chain([
        16_383,
        16_384,
        2_097_151,
        2_097_152,
        29_360_042,
        usize::try_from(JSON_PUSH_BYTES).expect("a length"),
    ]);
    for length in lengths {
        charged_for_all_it_keeps_alive(length);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(8)))]

    #[test]
    fn any_json_push_up_to_the_limit_is_charged_for_all_it_keeps_alive(
        length in 1..=usize::try_from(JSON_PUSH_BYTES).expect("a length"),
    ) {
        charged_for_all_it_keeps_alive(length);
    }
}

#[test]
fn a_json_frame_holding_more_than_its_room_beside_its_text_is_refused() {
    let text = Bytes::from_static(b"[{\"a\":1}]");
    let frame = json_frame(text.clone());
    let room = usize::try_from(MESSAGE_ROOM).expect("small");
    let message = |beside: usize| {
        (0..room)
            .map(|padding| padded_by(&frame, padding))
            .find(|message| message.len() == text.len() + beside)
            .expect("a padding gives the length")
    };
    let decoded = |message: &Bytes| {
        let frame = v1::ReadFrame::decode(message.clone()).expect("a read frame");
        reader().event(frame, message.clone())
    };
    let Ok(Read::Event(within)) = decoded(&message(room)) else {
        panic!("a frame within its room is an event");
    };
    assert_eq!(within, SourceEvent::Push(Push::Json(text.clone())));
    for message in [message(room + 1), padded(&frame)] {
        let Err(refused) = decoded(&message) else {
            panic!("a frame of {} bytes is refused", message.len());
        };
        assert_eq!(refused.code(), Some(MALFORMED_FRAME));
    }
}

#[test]
fn a_checkpoint_does_not_keep_its_message_alive() {
    let frame = v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Checkpoint(v1::CheckpointFrame {
            cursor: Some(v1::Cursor {
                version: 1,
                bytes: Bytes::from_static(b"7"),
            }),
            barrier: None,
        })),
    };
    let message = padded(&frame);
    let event = event(&message);
    let SourceEvent::Checkpoint { cursor, .. } = &event else {
        panic!("the frame is a checkpoint");
    };
    assert_eq!(cursor.bytes().as_ref(), b"7");
    assert!(message.is_unique(), "the cursor keeps its message alive");
}

/// Records what a read reserves beside its events.
#[derive(Default)]
struct Charges(Mutex<Vec<u64>>);

/// What a charge holds: where to record its release.
struct Charged(Arc<Charges>);

impl Drop for Charged {
    fn drop(&mut self) {
        self.0.0.lock().unwrap().push(0);
    }
}

/// Bytes: the most [`Charging`] lets a read keep.
const LIMIT: u64 = 100_000;

struct Charging(Arc<Charges>);

impl Admission for Charging {
    fn admit(&self, _event: &SourceEvent) -> BoxFuture<'_, rdlt_connector::Result<Option<Permit>>> {
        Box::pin(async { Ok(None) })
    }

    fn charge(&self, bytes: u64) -> rdlt_connector::Result<Permit> {
        if bytes > LIMIT {
            return Err(rdlt_connector::ConnectorError::exceeds(
                rdlt_connector::LimitExceeded {
                    name: "read kept bytes",
                    limit: LIMIT,
                    actual: bytes,
                },
            ));
        }
        self.0.0.lock().unwrap().push(bytes);
        Ok(Box::new(Charged(Arc::clone(&self.0))))
    }
}

/// A batch of one dictionary column of one row, its dictionary one value of `fill`.
fn keyed(fill: &str) -> RecordBatch {
    let value = fill.repeat(1_000);
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        vec![0].into(),
        Arc::new(StringArray::from(vec![value.as_str()])),
    )
    .expect("a valid dictionary");
    RecordBatch::try_from_iter([("tag", Arc::new(dictionary) as ArrayRef)]).expect("one column")
}

fn schema_frame(encoder: &mut Encoder, batch: &RecordBatch, epoch: u64) -> v1::ReadFrame {
    v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Schema(v1::SchemaFrame {
            schema_epoch: epoch,
            ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
        })),
    }
}

/// The frames of `batch`, each body held as a receiver holds it.
fn batch_frames(encoder: &mut Encoder, batch: &RecordBatch, epoch: u64) -> Vec<v1::ReadFrame> {
    let frames = encoder.batch(batch).expect("the batch encodes");
    frames
        .iter()
        .map(rdlt_wire::IpcFrame::aligned)
        .map(|frame| v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Batch(v1::BatchFrame {
                schema_epoch: epoch,
                kind: v1::BatchKind::Arrow as i32,
                data_header: frame.header,
                padding: Bytes::new(),
                data_body: frame.body,
            })),
        })
        .collect()
}

#[test]
fn a_read_s_schema_and_dictionaries_are_charged_while_its_decoder_holds_them() {
    let charges = Arc::new(Charges::default());
    let admission = Arc::new(Charging(Arc::clone(&charges)));
    let (sink, _feed) = admitted_partition_channel(NonZeroUsize::MIN, admission);
    let (mut reader, mut encoder) = (reader(), Encoder::default());
    let mut kept = Kept::default();
    let mut read = |frame: v1::ReadFrame| {
        let read = reader
            .event(frame, Bytes::new())
            .expect("an admitted frame");
        if matches!(read, Read::Nothing) {
            kept.charge(&sink, reader.kept()).expect("within the limit");
        }
        reader.kept()
    };
    let (first, second) = (keyed("x"), keyed("y"));
    // The schema is held from its frame on, with the message it came from.
    let frame = schema_frame(&mut encoder, &first, 1);
    let Some(v1::read_frame::Frame::Schema(message)) = &frame.frame else {
        panic!("the frame is a schema");
    };
    let message = u64::try_from(message.ipc_schema.len()).expect("a length");
    let schema = rdlt_connector::cost::schema_bytes(&first.schema()) + message;
    assert!(message > 0 && schema > message);
    assert_eq!(read(frame), schema);
    assert_eq!(*charges.0.lock().unwrap(), [schema]);
    let frames = batch_frames(&mut encoder, &first, 1);
    let held: Vec<u64> = frames.into_iter().map(&mut read).collect();
    assert!(held[0] >= schema + 1_000);
    // What was held is released before the schema and its dictionary are charged together, once;
    // the batch that uses the dictionary changes nothing.
    assert_eq!(*charges.0.lock().unwrap(), [schema, 0, held[0]]);
    // A dictionary that replaces it is charged in its place.
    let replaced: Vec<u64> = batch_frames(&mut encoder, &second, 1)
        .into_iter()
        .map(&mut read)
        .collect();
    assert_eq!(replaced, [held[0], held[0]]);
    assert_eq!(*charges.0.lock().unwrap(), [schema, 0, held[0]]);
    // A new schema forgets the dictionaries, and what held them is released.
    assert_eq!(read(schema_frame(&mut encoder, &second, 2)), schema);
    assert_eq!(*charges.0.lock().unwrap(), [schema, 0, held[0], 0, schema]);
}

#[test]
fn a_read_that_would_keep_more_than_it_may_is_refused_and_holds_nothing_more() {
    let charges = Arc::new(Charges::default());
    let admission = Arc::new(Charging(Arc::clone(&charges)));
    let (sink, _feed) = admitted_partition_channel(NonZeroUsize::MIN, admission);
    let mut kept = Kept::default();
    kept.charge(&sink, 60_000).expect("within the limit");
    let refused = kept.charge(&sink, LIMIT + 1).expect_err("beyond the limit");
    assert_eq!(refused.code(), Some("limit_exceeded"));
    let limit = refused.limit().expect("the limit passed");
    assert_eq!(
        (limit.name, limit.limit, limit.actual),
        ("read kept bytes", LIMIT, LIMIT + 1)
    );
    // What was kept before was released for the charge, and nothing is held in its place.
    assert_eq!(*charges.0.lock().unwrap(), [60_000, 0]);
    assert!(kept.held.is_none());
    assert_eq!(kept.bytes, 0);
    // A schema alone beyond the limit is refused at its frame.
    let columns = (0..4).map(|index| {
        let name = format!("{index}{}", "n".repeat(30_000));
        let ones: ArrayRef = Arc::new(arrow_array::Int8Array::from(vec![1_i8]));
        (name, ones)
    });
    let wide = RecordBatch::try_from_iter(columns).expect("a valid batch");
    let (mut reader, mut encoder) = (reader(), Encoder::default());
    let read = reader.event(schema_frame(&mut encoder, &wide, 1), Bytes::new());
    assert!(matches!(read, Ok(Read::Nothing)));
    assert!(reader.kept() > LIMIT);
    assert!(kept.charge(&sink, reader.kept()).is_err());
}

#[test]
fn batches_of_a_schema_sent_again_before_each_keep_one_schema_alive() {
    use rdlt_connector::cost::{Allocations, schema_bytes};
    // Sixteen columns named in sixty kilobytes each: about a megabyte of schema.
    let columns = (0..16).map(|index| {
        let name = format!("{index:02}{}", "n".repeat(59_998));
        let ones: ArrayRef = Arc::new(arrow_array::Int8Array::from(vec![1_i8]));
        (name, ones)
    });
    let batch = RecordBatch::try_from_iter(columns).expect("a valid batch");
    let schema = schema_bytes(&batch.schema());
    assert!(schema >= 960_000);
    let (mut reader, mut encoder) = (reader(), Encoder::default());
    let mut kept = Allocations::default();
    let (mut pushes, mut framed) = (Vec::new(), 0);
    for epoch in 1..=200 {
        let read = reader.event(schema_frame(&mut encoder, &batch, epoch), Bytes::new());
        assert!(matches!(read, Ok(Read::Nothing)));
        for frame in batch_frames(&mut encoder, &batch, epoch) {
            framed += frame.encoded_len() + usize::try_from(MESSAGE_ROOM).expect("small");
            let Ok(Read::Event(SourceEvent::Push(Push::Arrow(decoded)))) =
                reader.event(frame, Bytes::new())
            else {
                panic!("the frame is a batch");
            };
            kept.add(&decoded);
            pushes.push(decoded);
        }
    }
    assert_eq!(pushes.len(), 200);
    let first = pushes[0].schema();
    assert!(
        pushes
            .iter()
            .all(|push| Arc::ptr_eq(&push.schema(), &first))
    );
    // Two hundred batches keep one schema alive between them, and each the message it arrived
    // in.
    let framed = u64::try_from(framed).expect("a size");
    assert!(
        kept.bytes() < schema + framed,
        "{} bytes, {framed} of frames",
        kept.bytes()
    );
    // Each is still charged for the schema it holds, shared or not.
    assert!(Allocations::of(&pushes[199]).bytes() >= schema);
}
