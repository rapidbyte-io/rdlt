use std::sync::Arc;

use arrow_array::Array as _;
use arrow_array::types::Int8Type;
use arrow_array::{
    ArrayRef, DictionaryArray, Int32Array, RecordBatch, RecordBatchOptions, StringArray,
};
use arrow_ipc::{MessageHeader, MetadataVersion};
use arrow_schema::{DataType, Field, Fields, Schema};
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_testkit::drawn::values;
use rdlt_testkit::drawn::{Drawn, Scalar, array, field};

use super::{Decoder, Encoder, IpcFrame};
use crate::error::{Frame, Problem, WireError};
use crate::limits::Limits;

/// The batch `drawn` describes.
fn batch((columns, rows): &Drawn) -> RecordBatch {
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .enumerate()
        .map(|(column, (_, shape))| {
            let values: Vec<&Scalar> = rows.iter().map(|row| &row[column]).collect();
            array(shape, &values)
        })
        .collect();
    let fields: Vec<_> = columns
        .iter()
        .zip(&arrays)
        .map(|((name, shape), array)| field(name, shape, array, true))
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options).unwrap()
}

/// `batch` sent through a fresh encoder and decoder.
fn crossed(batch: &RecordBatch) -> RecordBatch {
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    decoder.schema(&encoder.schema(&batch.schema())).unwrap();
    let mut last = None;
    for frame in encoder.batch(batch).unwrap() {
        last = decoder.frame(&frame).unwrap();
    }
    last.expect("the last frame is the batch")
}

fn strings(values: &[&str]) -> RecordBatch {
    let keys = values
        .iter()
        .map(|value| i8::from(value.len() > 1))
        .collect::<Vec<_>>();
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        keys.into(),
        Arc::new(StringArray::from(vec!["a", "bb"])),
    )
    .unwrap();
    let schema = Schema::new(vec![Field::new(
        "tag",
        dictionary.data_type().clone(),
        false,
    )]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dictionary)]).unwrap()
}

fn ints(count: i32) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("n", DataType::Int32, false)]);
    let values = Int32Array::from((0..count).collect::<Vec<_>>());
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values)]).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(512)))]

    #[test]
    fn every_drawn_batch_crosses_the_wire_unchanged(drawn in values::drawn()) {
        let batch = batch(&drawn);
        prop_assert_eq!(crossed(&batch), batch);
    }

    #[test]
    fn a_corrupted_frame_is_decoded_or_refused_but_never_unwinds(
        drawn in values::drawn(),
        flips in proptest::collection::vec((any::<usize>(), any::<u8>()), 1..8),
        header in any::<bool>(),
    ) {
        let batch = batch(&drawn);
        let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
        decoder.schema(&encoder.schema(&batch.schema())).unwrap();
        for mut frame in encoder.batch(&batch).unwrap() {
            let target = if header { &mut frame.header } else { &mut frame.body };
            let mut bytes = target.to_vec();
            if !bytes.is_empty() {
                for (at, value) in &flips {
                    let len = bytes.len();
                    bytes[at % len] ^= value;
                }
            }
            *target = Bytes::from(bytes);
            // Arrow panics on some corrupt frames; the decoder turns that into an error.
            let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                decoder.frame(&frame)
            }));
            prop_assert!(decoded.is_ok(), "decoding a corrupted frame unwound");
        }
    }
}

#[test]
fn a_dictionary_is_sent_once_per_schema_epoch_and_later_batches_use_it() {
    let first = strings(&["a", "bb", "a"]);
    let second = strings(&["bb", "bb"]);
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    decoder.schema(&encoder.schema(&first.schema())).unwrap();
    let frames = encoder.batch(&first).unwrap();
    assert_eq!(frames.len(), 2, "the dictionary, then the batch");
    assert_eq!(decoder.frame(&frames[0]).unwrap(), None);
    assert_eq!(decoder.frame(&frames[1]).unwrap(), Some(first.clone()));
    let frames = encoder.batch(&second).unwrap();
    assert_eq!(frames.len(), 1, "the dictionary continues");
    assert_eq!(decoder.frame(&frames[0]).unwrap(), Some(second.clone()));
    // A new schema epoch forgets the dictionary on both ends.
    decoder.schema(&encoder.schema(&second.schema())).unwrap();
    let frames = encoder.batch(&second).unwrap();
    assert_eq!(frames.len(), 2);
}

#[test]
fn a_batch_before_any_schema_is_malformed() {
    let batch = ints(2);
    let mut encoder = Encoder::default();
    encoder.schema(&batch.schema());
    let frames = encoder.batch(&batch).unwrap();
    let error = Decoder::new(Limits::default())
        .frame(&frames[0])
        .unwrap_err();
    assert!(
        matches!(
            error,
            WireError::Malformed {
                frame: Frame::Batch,
                problem: Problem::NoSchema
            }
        ),
        "{error}"
    );
}

#[test]
fn a_header_that_is_no_message_is_malformed() {
    let mut decoder = Decoder::new(Limits::default());
    let error = decoder
        .schema(&Bytes::from_static(b"not a flatbuffer"))
        .unwrap_err();
    assert!(
        matches!(
            error,
            WireError::Malformed {
                problem: Problem::NotAMessage,
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a_schema_where_a_batch_belongs_is_malformed() {
    let batch = ints(1);
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    let schema = encoder.schema(&batch.schema());
    decoder.schema(&schema).unwrap();
    let frame = IpcFrame {
        header: schema,
        body: Bytes::new(),
    };
    let error = decoder.frame(&frame).unwrap_err();
    assert!(
        matches!(
            error,
            WireError::Malformed {
                problem: Problem::Unexpected { found: "Schema" },
                ..
            }
        ),
        "{error}"
    );
}

#[test]
fn a_body_shorter_than_its_header_declares_is_malformed() {
    let batch = ints(4);
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    decoder.schema(&encoder.schema(&batch.schema())).unwrap();
    let mut frame = encoder.batch(&batch).unwrap().remove(0);
    frame.body = frame.body.slice(..frame.body.len() - 8);
    let error = decoder.frame(&frame).unwrap_err();
    assert!(
        matches!(
            error,
            WireError::Malformed {
                problem: Problem::BodyLength { .. },
                ..
            }
        ),
        "{error}"
    );
}

/// A record batch message of one row whose one buffer lies at `offset` for `length` bytes, in a
/// body of `body` bytes.
fn framed(offset: i64, length: i64, body: i64) -> Bytes {
    framed_node(offset, length, body, 1)
}

/// [`framed`], with one node of `values` values.
fn framed_node(offset: i64, length: i64, body: i64, values: i64) -> Bytes {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let buffers = fbb.create_vector(&[arrow_ipc::Buffer::new(offset, length)]);
    let nodes = fbb.create_vector(&[arrow_ipc::FieldNode::new(values, 0)]);
    let mut batch = arrow_ipc::RecordBatchBuilder::new(&mut fbb);
    batch.add_length(1);
    batch.add_nodes(nodes);
    batch.add_buffers(buffers);
    let batch = batch.finish();
    let mut message = arrow_ipc::MessageBuilder::new(&mut fbb);
    message.add_version(MetadataVersion::V5);
    message.add_header_type(MessageHeader::RecordBatch);
    message.add_bodyLength(body);
    message.add_header(batch.as_union_value());
    let message = message.finish();
    fbb.finish(message, None);
    Bytes::copy_from_slice(fbb.finished_data())
}

#[test]
fn a_buffer_outside_the_body_is_malformed_before_arrow_reads_it() {
    let batch = ints(1);
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    decoder.schema(&encoder.schema(&batch.schema())).unwrap();
    for (offset, length) in [(8, 16), (-8, 8), (0, -1), (i64::MAX, 1)] {
        let frame = IpcFrame {
            header: framed(offset, length, 8),
            body: Bytes::from(vec![0; 8]),
        };
        let error = decoder.frame(&frame).unwrap_err();
        assert!(
            matches!(
                error,
                WireError::Malformed {
                    problem: Problem::BufferOutOfBounds { index: 0, .. },
                    ..
                }
            ),
            "{offset} {length}: {error}"
        );
    }
}

#[test]
fn batches_beyond_the_row_limit_and_frames_beyond_the_byte_limit_are_refused() {
    let batch = ints(3);
    let limits = Limits {
        batch_rows: 2,
        ..Limits::default()
    };
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(limits));
    decoder.schema(&encoder.schema(&batch.schema())).unwrap();
    let frame = encoder.batch(&batch).unwrap().remove(0);
    let refused = |error: WireError| match error {
        WireError::Refused(refusal) => (refusal.field, refusal.limit, refusal.actual),
        other => panic!("{other}"),
    };
    assert_eq!(
        refused(decoder.frame(&frame).unwrap_err()),
        ("batch rows", 2, 3)
    );
    let small = Limits {
        frame_bytes: 16,
        ..Limits::default()
    };
    let mut decoder = Decoder::new(small);
    let error = decoder
        .schema(&encoder.schema(&batch.schema()))
        .unwrap_err();
    assert_eq!(refused(error).0, "frame bytes");
}

#[test]
fn schemas_beyond_the_column_and_depth_limits_are_refused() {
    let nested = (0..3).fold(DataType::Int32, |inner, _| {
        DataType::Struct(Fields::from(vec![Field::new("x", inner, true)]))
    });
    let schema = Schema::new(vec![
        Field::new("a", nested, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let mut encoder = Encoder::default();
    let message = encoder.schema(&schema);
    let refusal = |limits: Limits| match Decoder::new(limits).schema(&message).unwrap_err() {
        WireError::Refused(refusal) => (refusal.field, refusal.actual),
        other => panic!("{other}"),
    };
    // a, a.x, a.x.x, a.x.x.x and b: five columns, four levels deep.
    assert_eq!(
        refusal(Limits {
            schema_columns: 4,
            ..Limits::default()
        }),
        ("schema columns", 5)
    );
    assert_eq!(
        refusal(Limits {
            nesting_depth: 3,
            ..Limits::default()
        }),
        ("nesting depth", 4)
    );
    assert!(
        Decoder::new(Limits {
            schema_columns: 5,
            nesting_depth: 4,
            ..Limits::default()
        })
        .schema(&message)
        .is_ok()
    );
}

#[test]
fn every_kind_of_nested_type_counts_its_columns_and_levels() {
    use arrow_schema::{UnionFields, UnionMode};
    let item = || Arc::new(Field::new("item", DataType::Int32, true));
    let pair = Fields::from(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int32, true),
    ]);
    let cases: Vec<(DataType, u64, u64)> = vec![
        (DataType::Int32, 1, 1),
        (DataType::List(item()), 2, 2),
        (DataType::LargeList(item()), 2, 2),
        (DataType::ListView(item()), 2, 2),
        (DataType::LargeListView(item()), 2, 2),
        (DataType::FixedSizeList(item(), 2), 2, 2),
        (
            DataType::Map(
                Arc::new(Field::new("entries", DataType::Struct(pair.clone()), false)),
                false,
            ),
            4,
            3,
        ),
        (DataType::Struct(pair), 3, 2),
        (
            DataType::Union(
                UnionFields::try_new(
                    vec![0, 1],
                    vec![
                        Field::new("a", DataType::Int8, true),
                        Field::new("b", DataType::Utf8, true),
                    ],
                )
                .unwrap(),
                UnionMode::Dense,
            ),
            3,
            2,
        ),
        (
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                item(),
            ),
            3,
            2,
        ),
        (
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::List(item()))),
            2,
            2,
        ),
    ];
    for (data_type, columns, depth) in cases {
        let field = Arc::new(Field::new("c", data_type.clone(), true));
        assert_eq!(
            super::decode::measure([&field]),
            (columns, depth),
            "{data_type}"
        );
    }
}

/// `header`, a dictionary batch message, marked as a delta onto the dictionary sent before it.
fn as_delta(header: &Bytes) -> Bytes {
    let message = arrow_ipc::root_as_message(header).unwrap();
    let dictionary = message.header_as_dictionary_batch().unwrap();
    let data = dictionary.data().unwrap();
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let nodes: Vec<_> = data.nodes().unwrap().iter().copied().collect();
    let buffers: Vec<_> = data.buffers().unwrap().iter().copied().collect();
    let (nodes, buffers) = (fbb.create_vector(&nodes), fbb.create_vector(&buffers));
    let mut batch = arrow_ipc::RecordBatchBuilder::new(&mut fbb);
    batch.add_length(data.length());
    batch.add_nodes(nodes);
    batch.add_buffers(buffers);
    let batch = batch.finish();
    let mut delta = arrow_ipc::DictionaryBatchBuilder::new(&mut fbb);
    delta.add_id(dictionary.id());
    delta.add_data(batch);
    delta.add_isDelta(true);
    let delta = delta.finish();
    let mut rebuilt = arrow_ipc::MessageBuilder::new(&mut fbb);
    rebuilt.add_version(message.version());
    rebuilt.add_header_type(MessageHeader::DictionaryBatch);
    rebuilt.add_bodyLength(message.bodyLength());
    rebuilt.add_header(delta.as_union_value());
    let rebuilt = rebuilt.finish();
    fbb.finish(rebuilt, None);
    Bytes::copy_from_slice(fbb.finished_data())
}

#[test]
fn a_delta_dictionary_is_refused_so_no_peer_can_grow_one_without_bound() {
    let batch = strings(&["a", "bb"]);
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    decoder.schema(&encoder.schema(&batch.schema())).unwrap();
    let frames = encoder.batch(&batch).unwrap();
    assert_eq!(decoder.frame(&frames[0]).unwrap(), None);
    let delta = IpcFrame {
        header: as_delta(&frames[0].header),
        body: frames[0].body.clone(),
    };
    let error = decoder.frame(&delta).unwrap_err();
    assert!(
        matches!(
            error,
            WireError::Malformed {
                frame: Frame::Dictionary,
                problem: Problem::DeltaDictionary
            }
        ),
        "{error}"
    );
}

#[test]
fn a_node_of_more_values_than_its_body_could_hold_is_refused_before_arrow_reads_it() {
    let batch = ints(1);
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    decoder.schema(&encoder.schema(&batch.schema())).unwrap();
    let frame = IpcFrame {
        header: framed_node(0, 8, 8, 1 << 40),
        body: Bytes::from(vec![0; 8]),
    };
    match decoder.frame(&frame).unwrap_err() {
        WireError::Refused(refusal) => {
            assert_eq!(
                (refusal.field, refusal.actual),
                ("values per node", 1 << 40)
            );
        }
        other => panic!("{other}"),
    }
}

/// A schema of one column of lists nested `levels` deep, its innermost item an integer.
fn nested(levels: usize) -> Schema {
    let inner = (1..levels).fold(DataType::Int32, |item, _| {
        DataType::List(Arc::new(Field::new("item", item, true)))
    });
    Schema::new(vec![Field::new("deep", inner, true)])
}

#[test]
fn a_schema_nested_to_the_limit_decodes_and_one_level_deeper_is_refused_by_name() {
    let depth = usize::try_from(crate::limits::NESTING_DEPTH).unwrap();
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(Limits::default());
    assert_eq!(
        decoder
            .schema(&encoder.schema(&nested(depth)))
            .unwrap()
            .as_ref(),
        &nested(depth)
    );
    match decoder
        .schema(&encoder.schema(&nested(depth + 1)))
        .unwrap_err()
    {
        WireError::Refused(refusal) => assert_eq!(refusal.field, "nesting depth"),
        other => panic!("{other}"),
    }
}
