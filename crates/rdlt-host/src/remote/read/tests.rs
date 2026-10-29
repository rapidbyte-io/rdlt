use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, DictionaryArray, Int64Array, RecordBatch, StringArray};
use bytes::{BufMut, Bytes, BytesMut};
use proptest::prelude::*;
use rdlt_connector::cost::{Allocations, push_charge};
use rdlt_connector::wire::v1;
use rdlt_connector::{Admission, BoxFuture, Permit, Push, SourceEvent, admitted_partition_channel};
use rdlt_wire::limits::{JSON_PUSH_BYTES, count};
use rdlt_wire::prost::Message as _;
use rdlt_wire::prost::encoding::{WireType, encode_key, encode_varint};
use rdlt_wire::stream::{Call, End, FrameReader, Kind, Outgoing, Received, Rows};
use rdlt_wire::{Cut, Encoder, Limits};

use super::{Kept, Payload, Read, Reader, message, reservation};

fn reader() -> Reader {
    Reader::new(Limits::default())
}

/// `frames` as the host receives them from a connector: written to a stream as the connector
/// writes them, and read as the host reads them.
fn arrived(frames: Vec<Outgoing>) -> Vec<Received> {
    let limits = Limits::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async move {
        let (mut connector, host) = tokio::io::duplex(1 << 16);
        let count = frames.len();
        let writing = tokio::spawn(async move {
            for mut frame in frames {
                frame
                    .write(&mut connector)
                    .await
                    .expect("the frame is taken");
            }
        });
        let mut reader = FrameReader::new(host, Call::Read.bounds(End::Connector, &limits));
        let mut received = Vec::with_capacity(count);
        for _ in 0..count {
            let frame = reader.next().await.expect("the frame arrives whole");
            received.push(frame.expect("a frame"));
        }
        writing.await.expect("the writer does not panic");
        received
    })
}

/// The payload a frame of the connector's becomes, as the read's reader makes it.
fn payload(frame: Received) -> Payload {
    match frame.head.kind {
        Kind::Message { .. } => Payload::Message(message(&frame).expect("the message decodes")),
        Kind::Batch { .. } | Kind::Json => Payload::Data(frame),
    }
}

/// The frame of a message of the connector's.
fn message_frame(frame: &v1::ReadFrame) -> Outgoing {
    Outgoing::message(None, frame).expect("a message fits a head")
}

/// Checks that a JSON push of `length` bytes, sent and received as a host receives it, is a
/// slice of the allocation it arrived in, whose every byte its charge counts, within what its
/// frame was reserved.
fn charged_for_all_it_keeps_alive(length: usize) {
    let text = Bytes::from(vec![b'x'; length]);
    let json = Outgoing::new(Kind::Json, vec![text]).expect("a push fits a head");
    let frame = arrived(vec![json]).remove(0);
    let reserved = reservation(&frame.head, 0);
    let allocation = frame.body.allocation.clone();
    let Ok(Read::Event(SourceEvent::Push(push))) = reader().event(Payload::Data(frame)) else {
        panic!("{length} bytes of JSON are a push");
    };
    let Push::Json(text) = &push else {
        panic!("the push is JSON");
    };
    assert_eq!(text.len(), length);
    let within = allocation.as_ptr_range();
    assert!(
        within.contains(&text.as_ptr()) && text.as_ptr_range().end <= within.end,
        "{length} bytes of JSON were copied out of their frame"
    );
    let charge = push_charge(&push);
    assert!(
        charge >= count(allocation.len()),
        "{length} bytes of JSON keep {} alive, charged {charge}",
        allocation.len(),
    );
    assert!(
        charge <= reserved,
        "{length} bytes of JSON are charged {charge}, beyond the {reserved} their frame reserved"
    );
}

#[test]
fn a_json_push_is_a_slice_of_its_frame_charged_for_all_it_keeps_alive() {
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
fn a_checkpoint_does_not_keep_its_frame_alive() {
    let frame = v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Checkpoint(v1::CheckpointFrame {
            cursor: Some(v1::Cursor {
                version: 1,
                bytes: Bytes::from_static(b"7"),
            }),
            barrier: None,
        })),
    };
    // Padded with a megabyte of a field this end does not know.
    let mut padded = BytesMut::new();
    frame.encode(&mut padded).expect("room for the frame");
    encode_key(1000, WireType::LengthDelimited, &mut padded);
    encode_varint(1 << 20, &mut padded);
    padded.put_bytes(0xaa, 1 << 20);
    let sent = Outgoing::new(Kind::Message { opens: None }, vec![padded.freeze()])
        .expect("a message fits a head");
    let received = arrived(vec![sent]).remove(0);
    let allocation = received.body.allocation.clone();
    let Ok(Read::Event(event)) = reader().event(payload(received)) else {
        panic!("the frame is an event");
    };
    let SourceEvent::Checkpoint { cursor, .. } = &event else {
        panic!("the frame is a checkpoint");
    };
    assert_eq!(cursor.bytes().as_ref(), b"7");
    assert!(allocation.is_unique(), "the cursor keeps its frame alive");
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

/// The frames of `batch`, as a connector writes them, cut within `limits`.
fn batch_frames(
    encoder: &mut Encoder,
    batch: &RecordBatch,
    epoch: u64,
    limits: Limits,
) -> Vec<Outgoing> {
    let mut cut = Cut::new(batch.clone(), limits);
    let mut frames = Vec::new();
    while let Some(pieces) = encoder.piece(&mut cut).expect("the batch encodes") {
        for piece in pieces {
            let kind = Kind::Batch {
                rows: Rows::Arrow,
                dictionary: piece.is_dictionary(),
                tag: epoch,
            };
            frames.push(Outgoing::new(kind, piece.into_pieces()).expect("a piece fits a head"));
        }
    }
    frames
}

#[test]
fn a_read_s_schema_and_dictionaries_are_charged_while_its_decoder_holds_them() {
    let charges = Arc::new(Charges::default());
    let admission = Arc::new(Charging(Arc::clone(&charges)));
    let (sink, _feed) = admitted_partition_channel(NonZeroUsize::MIN, admission);
    let (mut reader, mut encoder) = (reader(), Encoder::default());
    let mut kept = Kept::default();
    let mut read = |frame: Received| {
        let read = reader.event(payload(frame)).expect("an admitted frame");
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
    assert_eq!(read(arrived(vec![message_frame(&frame)]).remove(0)), schema);
    assert_eq!(*charges.0.lock().unwrap(), [schema]);
    let frames = arrived(batch_frames(&mut encoder, &first, 1, Limits::default()));
    let held: Vec<u64> = frames.into_iter().map(&mut read).collect();
    assert!(held[0] >= schema + 1_000);
    // What was held is released before the schema and its dictionary are charged together, once;
    // the batch that uses the dictionary changes nothing.
    assert_eq!(*charges.0.lock().unwrap(), [schema, 0, held[0]]);
    // A dictionary that replaces it is charged in its place.
    let replaced: Vec<u64> = arrived(batch_frames(&mut encoder, &second, 1, Limits::default()))
        .into_iter()
        .map(&mut read)
        .collect();
    assert_eq!(replaced, [held[0], held[0]]);
    assert_eq!(*charges.0.lock().unwrap(), [schema, 0, held[0]]);
    // A new schema forgets the dictionaries, and what held them is released.
    let again = message_frame(&schema_frame(&mut encoder, &second, 2));
    assert_eq!(read(arrived(vec![again]).remove(0)), schema);
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
    let schema = schema_frame(&mut encoder, &wide, 1);
    let read = reader.event(Payload::Message(schema));
    assert!(matches!(read, Ok(Read::Nothing)));
    assert!(reader.kept() > LIMIT);
    assert!(kept.charge(&sink, reader.kept()).is_err());
}

#[test]
fn batches_of_a_schema_sent_again_before_each_keep_one_schema_alive() {
    use rdlt_connector::cost::schema_bytes;
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
        let read = reader.event(Payload::Message(schema_frame(&mut encoder, &batch, epoch)));
        assert!(matches!(read, Ok(Read::Nothing)));
        for frame in arrived(batch_frames(&mut encoder, &batch, epoch, Limits::default())) {
            framed += reservation(&frame.head, 0);
            let Ok(Read::Event(SourceEvent::Push(Push::Arrow(decoded)))) =
                reader.event(Payload::Data(frame))
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
    // Two hundred batches keep one schema alive between them, and each the frame it arrived in.
    assert!(
        kept.bytes() < schema + framed,
        "{} bytes, {framed} of frames",
        kept.bytes()
    );
    // Each is still charged for the schema it holds, shared or not.
    assert!(Allocations::of(&pushes[199]).bytes() >= schema);
}

/// Checks that each push of `batch`, cut within `limits` and received as a host receives it, is
/// charged no more than its frame was reserved, by what a push holds beside its frame once every
/// frame before it was decoded.
fn reserved_for_all_its_pushes_hold(batch: &RecordBatch, limits: Limits) {
    let (mut reader, mut encoder) = (reader(), Encoder::default());
    let schema = message_frame(&schema_frame(&mut encoder, batch, 1));
    let read = reader.event(payload(arrived(vec![schema]).remove(0)));
    assert!(matches!(read, Ok(Read::Nothing)));
    let mut pushes = 0;
    for frame in arrived(batch_frames(&mut encoder, batch, 1, limits)) {
        let reserved = reservation(&frame.head, reader.beside());
        let Ok(Read::Event(SourceEvent::Push(push))) = reader.event(Payload::Data(frame)) else {
            continue;
        };
        let charge = push_charge(&push);
        assert!(
            charge <= reserved,
            "a push charged {charge} was reserved {reserved}"
        );
        pushes += 1;
    }
    assert!(pushes > 0, "the batch was pushed");
}

#[test]
fn every_push_is_charged_within_what_its_frame_reserved_dictionaries_and_wide_schemas_too() {
    let small = Limits {
        frame_bytes: rdlt_wire::limits::MIN_FRAME_BYTES,
        ..Limits::default()
    };
    // A thousand columns of a few rows each.
    let columns = (0..1_000).map(|index| {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![index; 8]));
        (format!("c{index}"), values)
    });
    let wide = RecordBatch::try_from_iter(columns).expect("a valid batch");
    for limits in [Limits::default(), small] {
        reserved_for_all_its_pushes_hold(&keyed("x"), limits);
        reserved_for_all_its_pushes_hold(&wide, limits);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(16)))]

    #[test]
    fn any_batch_s_pushes_are_charged_within_what_their_frames_reserved(
        rows in 1_usize..20_000,
        width in 1_usize..64,
        text in 0_usize..256,
        keyed in any::<bool>(),
    ) {
        let columns = (0..width).map(|index| {
            let values: ArrayRef = if keyed && index % 2 == 0 {
                let keys: Vec<i8> = (0..rows)
                    .map(|row| i8::try_from(row % 7).unwrap_or(0))
                    .collect();
                let values: Vec<String> = (0..7).map(|value| "v".repeat(text + value)).collect();
                Arc::new(
                    DictionaryArray::<Int8Type>::try_new(
                        keys.into(),
                        Arc::new(StringArray::from(values)),
                    )
                    .expect("a valid dictionary"),
                )
            } else if index % 3 == 0 {
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|row| "s".repeat(text.min(row % 64))),
                ))
            } else {
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| i64::try_from(row).unwrap_or(0)),
                ))
            };
            (format!("c{index}"), values)
        });
        let batch = RecordBatch::try_from_iter(columns).expect("a valid batch");
        reserved_for_all_its_pushes_hold(&batch, Limits::default());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(64)))]

    #[test]
    fn any_message_a_source_sends_decodes_within_what_its_frame_reserved(
        length in 0_usize..(1 << 16),
        number in any::<u64>(),
        which in 0_u8..9,
    ) {
        use v1::read_frame::Frame;
        let text = "t".repeat(length);
        let frame = match which {
            0 => Frame::Schema(v1::SchemaFrame {
                schema_epoch: number,
                ipc_schema: Bytes::from(vec![1; length]),
            }),
            1 => Frame::Checkpoint(v1::CheckpointFrame {
                cursor: Some(v1::Cursor {
                    version: 1,
                    bytes: Bytes::from(vec![7; length]),
                }),
                barrier: Some(number),
            }),
            2 => Frame::Log(v1::LogFrame {
                level: v1::LogLevel::Info as i32,
                message: text,
            }),
            3 => Frame::Metric(v1::MetricFrame {
                name: text,
                value: 1.5,
            }),
            4 => Frame::Done(v1::Done {}),
            5 => Frame::Replan(v1::ReplanFrame {}),
            6 => Frame::Behind(v1::BehindFrame { records: number }),
            7 => Frame::Error(v1::Error {
                kind: v1::ErrorKind::Data as i32,
                message: text,
                code: Some("limit_exceeded".to_owned()),
                retry_after: Some(v1::Duration {
                    seconds: 1,
                    nanos: 2,
                }),
                limit: Some(v1::LimitExceeded {
                    name: "frame bytes".to_owned(),
                    limit: number,
                    actual: number,
                }),
            }),
            _ => Frame::Error(v1::Error::default()),
        };
        let sent = v1::ReadFrame { frame: Some(frame) };
        let received = arrived(vec![message_frame(&sent)]).remove(0);
        let decoded = message(&received);
        prop_assert!(decoded.is_ok(), "{:?}", decoded.err());
        prop_assert_eq!(decoded.ok(), Some(sent));
    }
}
