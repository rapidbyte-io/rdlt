use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, DictionaryArray, RecordBatch, StringArray};
use bytes::{BufMut, Bytes, BytesMut};
use rdlt_connector::wire::v1;
use rdlt_connector::{Admission, BoxFuture, Permit, Push, SourceEvent, admitted_partition_channel};
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
    }
}

/// `frame` as a transport receives it: one buffer holding the message, padded with a megabyte
/// of a field this end does not know.
fn padded(frame: &v1::ReadFrame) -> Bytes {
    const PADDING: usize = 1 << 20;
    let mut message = BytesMut::with_capacity(PADDING + 1024);
    frame.encode(&mut message).expect("room for the frame");
    encode_key(1000, WireType::LengthDelimited, &mut message);
    encode_varint(PADDING as u64, &mut message);
    message.put_bytes(0xaa, PADDING);
    message.freeze()
}

/// The event `message` becomes; the frame decoded from it is dropped with the read's turn.
fn event(message: &Bytes) -> SourceEvent {
    let frame = v1::ReadFrame::decode(message.clone()).expect("a read frame");
    match reader().event(frame).expect("an admitted frame") {
        Read::Event(event) => event,
        Read::Done | Read::Nothing => panic!("the frame is an event"),
    }
}

#[test]
fn a_json_push_does_not_keep_its_message_alive() {
    let frame = v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Json(v1::JsonFrame {
            data: Bytes::from_static(b"[{\"a\":1}]"),
        })),
    };
    let message = padded(&frame);
    let event = event(&message);
    assert_eq!(
        event,
        SourceEvent::Push(Push::Json(Bytes::from_static(b"[{\"a\":1}]")))
    );
    assert!(message.is_unique(), "the push keeps its message alive");
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
    fn admit<'a>(
        &'a self,
        _event: &'a SourceEvent,
    ) -> BoxFuture<'a, rdlt_connector::Result<Option<Permit>>> {
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

fn batch_frames(encoder: &mut Encoder, batch: &RecordBatch, epoch: u64) -> Vec<v1::ReadFrame> {
    let frames = encoder.batch(batch).expect("the batch encodes");
    frames
        .into_iter()
        .map(|frame| v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Batch(v1::BatchFrame {
                schema_epoch: epoch,
                kind: v1::BatchKind::Arrow as i32,
                data_header: frame.header,
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
        let read = reader.event(frame).expect("an admitted frame");
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
    let read = reader.event(schema_frame(&mut encoder, &wide, 1));
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
    let mut pushes = Vec::new();
    for epoch in 1..=200 {
        let read = reader.event(schema_frame(&mut encoder, &batch, epoch));
        assert!(matches!(read, Ok(Read::Nothing)));
        for frame in batch_frames(&mut encoder, &batch, epoch) {
            let Ok(Read::Event(SourceEvent::Push(Push::Arrow(decoded)))) = reader.event(frame)
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
    // Two hundred batches keep one schema alive between them, and a few bytes each.
    assert!(
        kept.bytes() < schema + (200 << 10),
        "{} bytes",
        kept.bytes()
    );
    // Each is still charged for the schema it holds, shared or not.
    assert!(Allocations::of(&pushes[199]).bytes() >= schema);
}
