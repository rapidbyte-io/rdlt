pub(crate) mod frames;
pub(crate) mod nested;
pub(crate) mod odd;
pub(crate) mod samples;

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::cast::AsArray as _;
use arrow_array::types::Int8Type;
use arrow_array::{
    ArrayRef, DictionaryArray, Int32Array, RecordBatch, RecordBatchOptions, StringArray,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_testkit::drawn::values;
use rdlt_testkit::drawn::{Drawn, Scalar, array, field};

use super::{Decoder, Encoder, IpcFrame, Shape};
use crate::error::{Frame, Problem, WireError};
use crate::limits::Limits;

/// The batch `drawn` describes.
pub(crate) fn batch((columns, rows): &Drawn) -> RecordBatch {
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

/// `batch` sent through a fresh encoder and decoder, and the shape of its last frame.
fn crossed(batch: &RecordBatch) -> (RecordBatch, Shape) {
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
    let mut last = None;
    for frame in encoder.batch(batch).unwrap() {
        last = Some(decoder.shaped(&frame).unwrap());
    }
    let (batch, shape) = last.expect("a batch is at least one frame");
    (batch.expect("the last frame is the batch"), shape)
}

/// What `array` and the columns nested in it declare, counted from the array itself: its values,
/// and the bytes its views name in their data buffers.
fn declared(array: &dyn Array) -> (u64, u64) {
    let wide = |count: usize| u64::try_from(count).unwrap();
    let (mut values, mut view_bytes) = (wide(array.len()), 0);
    let lengths = |views: &[u128]| -> u64 {
        let lengths = views
            .iter()
            .map(|view| u64::try_from(*view & 0xFFFF_FFFF).unwrap());
        lengths.filter(|length| *length > 12).sum()
    };
    match array.data_type() {
        DataType::Utf8View => view_bytes += lengths(array.as_string_view().views()),
        DataType::BinaryView => view_bytes += lengths(array.as_binary_view().views()),
        DataType::ListView(_) => {
            let sizes = array.as_list_view::<i32>().sizes();
            values += sizes
                .iter()
                .map(|size| wide(usize::try_from(*size).unwrap()))
                .sum::<u64>();
        }
        DataType::LargeListView(_) => {
            let sizes = array.as_list_view::<i64>().sizes();
            values += sizes
                .iter()
                .map(|size| wide(usize::try_from(*size).unwrap()))
                .sum::<u64>();
        }
        _ => {}
    }
    // A dictionary's values travel in a frame of their own.
    if !matches!(array.data_type(), DataType::Dictionary(..)) {
        for child in array.to_data().child_data() {
            let (nested, named) = declared(&arrow_array::make_array(child.clone()));
            values += nested;
            view_bytes += named;
        }
    }
    (values, view_bytes)
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
        let (decoded, shape) = crossed(&batch);
        let mut expected = (0, 0);
        for column in decoded.columns() {
            let (values, view_bytes) = declared(column);
            expected = (expected.0 + values, expected.1 + view_bytes);
        }
        prop_assert_eq!((shape.values, shape.view_bytes), expected);
        prop_assert_eq!(decoded, batch);
    }

    #[test]
    fn a_corrupted_frame_is_decoded_or_refused_but_never_unwinds(
        drawn in values::drawn(),
        flips in proptest::collection::vec((any::<usize>(), any::<u8>()), 1..8),
        header in any::<bool>(),
    ) {
        let batch = batch(&drawn);
        let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
        decoder.schema(&encoder.schema(&batch.schema()).unwrap()).unwrap();
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
    decoder
        .schema(&encoder.schema(&first.schema()).unwrap())
        .unwrap();
    let frames = encoder.batch(&first).unwrap();
    assert_eq!(frames.len(), 2, "the dictionary, then the batch");
    assert_eq!(decoder.frame(&frames[0]).unwrap(), None);
    assert_eq!(decoder.frame(&frames[1]).unwrap(), Some(first.clone()));
    let frames = encoder.batch(&second).unwrap();
    assert_eq!(frames.len(), 1, "the dictionary continues");
    assert_eq!(decoder.frame(&frames[0]).unwrap(), Some(second.clone()));
    // A new schema epoch forgets the dictionary on both ends.
    decoder
        .schema(&encoder.schema(&second.schema()).unwrap())
        .unwrap();
    let frames = encoder.batch(&second).unwrap();
    assert_eq!(frames.len(), 2);
}

#[test]
fn a_batch_before_any_schema_is_malformed() {
    let batch = ints(2);
    let mut encoder = Encoder::default();
    encoder.schema(&batch.schema()).unwrap();
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
    let schema = encoder.schema(&batch.schema()).unwrap();
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
    decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
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

#[test]
fn batches_beyond_the_row_limit_and_frames_beyond_the_byte_limit_are_refused() {
    let batch = ints(3);
    let limits = Limits {
        batch_rows: 2,
        ..Limits::default()
    };
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(limits));
    decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
    let frame = encoder.batch(&batch).unwrap().remove(0);
    let refused = |error: WireError| match error {
        WireError::Refused(refusal) => (refusal.field, refusal.limit, refusal.actual),
        other => panic!("{other}"),
    };
    assert_eq!(
        refused(decoder.frame(&frame).unwrap_err()),
        ("batch rows", 2, 3)
    );
    let bytes = u64::try_from(frame.header.len() + frame.body.len()).unwrap();
    let mut decoder = frames::decoder(
        &batch.schema(),
        Limits {
            frame_bytes: bytes - 1,
            ..Limits::default()
        },
    );
    assert_eq!(
        refused(decoder.frame(&frame).unwrap_err()),
        ("frame bytes", bytes - 1, bytes)
    );
    let mut decoder = frames::decoder(
        &batch.schema(),
        Limits {
            frame_bytes: bytes,
            ..Limits::default()
        },
    );
    assert_eq!(decoder.frame(&frame).unwrap(), Some(batch));
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
    let message = encoder.schema(&schema).unwrap();
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
fn a_delta_dictionary_is_refused_so_no_peer_can_grow_one_without_bound() {
    let batch = strings(&["a", "bb"]);
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
    let frames = encoder.batch(&batch).unwrap();
    assert_eq!(decoder.frame(&frames[0]).unwrap(), None);
    let delta = frames::changed(&frames[..1], |parts| {
        parts.dictionary = parts.dictionary.map(|(id, _)| (id, true));
    });
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
            .schema(&encoder.schema(&nested(depth)).unwrap())
            .unwrap()
            .as_ref(),
        &nested(depth)
    );
    match decoder
        .schema(&encoder.schema(&nested(depth + 1)).unwrap())
        .unwrap_err()
    {
        WireError::Refused(refusal) => assert_eq!(refusal.field, "nesting depth"),
        other => panic!("{other}"),
    }
}

#[test]
fn a_batch_of_a_schema_nested_to_the_limit_is_sent_and_received() {
    let depth = usize::try_from(crate::limits::NESTING_DEPTH).unwrap();
    let schema = Arc::new(nested(depth));
    let column = arrow_array::new_null_array(schema.field(0).data_type(), 3);
    let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
    assert_eq!(crossed(&batch).0, batch);
}

/// A dictionary column nested in a column of every type that nests one.
fn nesting_a_dictionary() -> Vec<ArrayRef> {
    use arrow_array::{MapArray, RunArray, StructArray, UnionArray};
    let tags: ArrayRef = Arc::new(strings(&["a", "bb", "a"]).column(0).clone());
    let item = Arc::new(Field::new("item", tags.data_type().clone(), true));
    let lengths = arrow_buffer::OffsetBuffer::<i32>::from_lengths([2, 1]);
    let fields = Fields::from(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", tags.data_type().clone(), true),
    ]);
    let pairs = StructArray::new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])), Arc::clone(&tags)],
        None,
    );
    let union = arrow_schema::UnionFields::try_new(vec![0], vec![item.as_ref().clone()]).unwrap();
    let entries = Arc::new(Field::new("entries", DataType::Struct(fields), false));
    let keys = arrow_array::Int8Array::from(vec![0, 2, 1]);
    let mut nesting = lists_of(&item, &tags);
    let rest: [ArrayRef; 5] = [
        Arc::new(MapArray::new(entries, lengths, pairs.clone(), None, false)),
        Arc::new(pairs.clone()),
        Arc::new(
            UnionArray::try_new(union, vec![0; 3].into(), None, vec![Arc::clone(&tags)]).unwrap(),
        ),
        Arc::new(RunArray::try_new(&Int32Array::from(vec![1, 2, 3]), &tags).unwrap()),
        Arc::new(DictionaryArray::try_new(keys, Arc::new(pairs)).unwrap()),
    ];
    nesting.extend(rest);
    nesting
}

/// A list of every kind whose items are `tags`, three of them.
fn lists_of(item: &Arc<Field>, tags: &ArrayRef) -> Vec<ArrayRef> {
    use arrow_array::{
        FixedSizeListArray, LargeListArray, LargeListViewArray, ListArray, ListViewArray,
    };
    use arrow_buffer::OffsetBuffer;
    let lengths = || OffsetBuffer::<i32>::from_lengths([2, 1]);
    let long = || OffsetBuffer::<i64>::from_lengths([2, 1]);
    vec![
        Arc::new(ListArray::new(
            Arc::clone(item),
            lengths(),
            Arc::clone(tags),
            None,
        )),
        Arc::new(LargeListArray::new(
            Arc::clone(item),
            long(),
            Arc::clone(tags),
            None,
        )),
        Arc::new(ListViewArray::new(
            Arc::clone(item),
            vec![0, 2].into(),
            vec![2, 1].into(),
            Arc::clone(tags),
            None,
        )),
        Arc::new(LargeListViewArray::new(
            Arc::clone(item),
            vec![0, 2].into(),
            vec![2, 1].into(),
            Arc::clone(tags),
            None,
        )),
        Arc::new(FixedSizeListArray::new(
            Arc::clone(item),
            3,
            Arc::clone(tags),
            None,
        )),
    ]
}

#[test]
fn a_dictionary_nested_in_any_type_reaches_its_column() {
    for column in nesting_a_dictionary() {
        let batch = samples::batch_of(column);
        assert_eq!(crossed(&batch).0, batch, "{}", batch.schema());
    }
}

#[test]
fn a_dictionary_the_schema_does_not_name_is_refused() {
    let batch = strings(&["a", "bb"]);
    let (mut encoder, mut decoder) = (Encoder::default(), Decoder::new(Limits::default()));
    decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
    let frames = encoder.batch(&batch).unwrap();
    // A schema without dictionaries forgets the ids of the schema before it.
    decoder
        .schema(&encoder.schema(&ints(1).schema()).unwrap())
        .unwrap();
    let error = decoder.frame(&frames[0]).unwrap_err();
    assert!(
        matches!(
            error,
            WireError::Malformed {
                frame: Frame::Dictionary,
                problem: Problem::UnknownDictionary { .. }
            }
        ),
        "{error}"
    );
}

#[test]
fn a_dictionary_of_dictionaries_is_refused_by_its_sender() {
    // A field of the IPC format has one dictionary: a dictionary whose values are themselves a
    // dictionary has no schema message, so no receiver is ever sent one.
    let inner = DictionaryArray::<Int8Type>::try_new(
        vec![0, 1].into(),
        Arc::new(StringArray::from(vec!["a", "bb"])),
    );
    let outer =
        DictionaryArray::<Int8Type>::try_new(vec![1, 0, 1].into(), Arc::new(inner.unwrap()));
    let twice = samples::batch_of(Arc::new(outer.unwrap()));
    let item = Arc::new(Field::new(
        "item",
        twice.column(0).data_type().clone(),
        true,
    ));
    let listed = arrow_array::ListArray::new(
        item,
        arrow_buffer::OffsetBuffer::from_lengths([2, 1]),
        Arc::clone(twice.column(0)),
        None,
    );
    for batch in [twice, samples::batch_of(Arc::new(listed))] {
        let mut encoder = Encoder::default();
        let schema = encoder.schema(&batch.schema());
        assert_eq!(frames::problem(schema), Problem::DictionaryOfDictionaries);
        // No schema went, so no batch does.
        assert_eq!(frames::problem(encoder.batch(&batch)), Problem::NoSchema);
        let mut cut = super::Cut::new(batch.clone(), Limits::default());
        assert_eq!(frames::problem(encoder.piece(&mut cut)), Problem::NoSchema);
        // The schema epoch before it is over too.
        let plain = ints(1);
        encoder.schema(&plain.schema()).unwrap();
        assert!(encoder.schema(&batch.schema()).is_err());
        assert_eq!(frames::problem(encoder.batch(&plain)), Problem::NoSchema);
    }
}

#[test]
fn a_dictionary_of_dictionaries_is_found_wherever_it_nests() {
    use arrow_schema::{UnionFields, UnionMode};
    let keyed = |values: DataType| DataType::Dictionary(Box::new(DataType::Int8), Box::new(values));
    let field = |data_type: &DataType| Arc::new(Field::new("f", data_type.clone(), true));
    let ends = Arc::new(Field::new("run_ends", DataType::Int32, false));
    let nestings = |inner: &DataType| -> Vec<DataType> {
        let union = UnionFields::try_new(vec![0], vec![field(inner).as_ref().clone()]).unwrap();
        vec![
            inner.clone(),
            DataType::List(field(inner)),
            DataType::LargeList(field(inner)),
            DataType::ListView(field(inner)),
            DataType::LargeListView(field(inner)),
            DataType::FixedSizeList(field(inner), 2),
            DataType::Map(field(inner), false),
            DataType::Struct(Fields::from(vec![field(&DataType::Int8), field(inner)])),
            DataType::Union(union, UnionMode::Sparse),
            DataType::RunEndEncoded(Arc::clone(&ends), field(inner)),
            keyed(DataType::Struct(Fields::from(vec![field(inner)]))),
        ]
    };
    let once = keyed(DataType::Utf8);
    for data_type in nestings(&keyed(once.clone())) {
        assert!(super::twice_keyed(&data_type), "{data_type}");
    }
    for data_type in nestings(&once).into_iter().chain(nestings(&DataType::Utf8)) {
        assert!(!super::twice_keyed(&data_type), "{data_type}");
    }
}

#[test]
fn a_schema_that_is_refused_ends_the_schema_before_it() {
    let batch = samples::batch();
    let (mut decoder, frames) = frames::sent(&batch, Limits::default());
    let garbage = Bytes::from_static(b"not an IPC message");
    assert!(decoder.schema(&garbage).is_err());
    // No batch is read under the schema the refused one was to replace.
    for frame in &frames {
        let error = decoder.frame(frame).unwrap_err();
        assert!(
            matches!(
                error,
                WireError::Malformed {
                    problem: Problem::NoSchema,
                    ..
                }
            ),
            "{error}"
        );
    }
}

#[test]
fn a_refusal_names_the_part_a_message_lacks_or_does_not_need() {
    use crate::error::Part;
    let lacking = |part| Problem::Missing { part }.to_string();
    assert_eq!(
        lacking(Part::Node),
        "the message lacks a field node its schema needs"
    );
    assert_eq!(
        lacking(Part::Buffer),
        "the message lacks a buffer its schema needs"
    );
    let unused = Problem::Unused {
        part: Part::VariadicCount,
    };
    assert_eq!(
        unused.to_string(),
        "the message holds a count of data buffers its schema does not need"
    );
}

/// A batch of `columns` dictionary columns of one row, each dictionary one value of `bytes`
/// bytes holding `fill`.
fn keyed(columns: usize, bytes: usize, fill: &str) -> RecordBatch {
    let value = fill.repeat(bytes);
    let arrays = (0..columns).map(|column| {
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            vec![0].into(),
            Arc::new(StringArray::from(vec![value.as_str()])),
        )
        .unwrap();
        (format!("c{column}"), Arc::new(dictionary) as ArrayRef)
    });
    RecordBatch::try_from_iter(arrays).unwrap()
}

/// What the first dictionary of `frames`, sent after `schema`, holds once decoded.
fn dictionary_held(schema: &Bytes, frames: &[IpcFrame]) -> u64 {
    let mut measuring = Decoder::new(Limits::default());
    measuring.schema(schema).unwrap();
    assert_eq!(measuring.dictionary_bytes(), 0);
    let (_, shape) = measuring.shaped(&frames[0]).unwrap();
    assert_eq!(measuring.dictionary_bytes(), shape.held_bytes);
    shape.held_bytes
}

#[test]
fn the_dictionaries_a_decoder_holds_are_bounded_together() {
    let batch = keyed(3, 1_000, "x");
    let mut encoder = Encoder::default();
    let schema = encoder.schema(&batch.schema()).unwrap();
    let frames = encoder.batch(&batch).unwrap();
    assert_eq!(frames.len(), 4, "three dictionaries, then the batch");
    let each = dictionary_held(&schema, &frames);
    assert!(each >= 1_000);
    // A frame limit, and so a dictionary limit, of two such dictionaries and of a byte less
    // than three.
    for limit in [2 * each, 3 * each - 1] {
        let limits = Limits {
            frame_bytes: limit,
            ..Limits::default()
        };
        let mut decoder = Decoder::new(limits);
        decoder.schema(&schema).unwrap();
        assert_eq!(decoder.frame(&frames[0]).unwrap(), None);
        assert_eq!(decoder.frame(&frames[1]).unwrap(), None);
        assert_eq!(decoder.dictionary_bytes(), 2 * each);
        let WireError::Refused(refusal) = decoder.frame(&frames[2]).unwrap_err() else {
            panic!("a third dictionary beyond the limit was no refusal");
        };
        assert_eq!(
            (refusal.field, refusal.limit, refusal.actual),
            ("dictionary bytes", limit, 3 * each)
        );
        // The dictionary refused is not held.
        assert_eq!(decoder.dictionary_bytes(), 2 * each);
    }
    let limits = Limits {
        frame_bytes: 3 * each,
        ..Limits::default()
    };
    let mut decoder = Decoder::new(limits);
    decoder.schema(&schema).unwrap();
    for frame in &frames[..3] {
        assert_eq!(decoder.frame(frame).unwrap(), None);
    }
    assert_eq!(decoder.frame(&frames[3]).unwrap(), Some(batch));
}

#[test]
fn a_dictionary_replaces_the_one_of_its_id_and_a_schema_forgets_them_all() {
    let (first, second) = (keyed(1, 1_000, "x"), keyed(1, 1_000, "y"));
    let mut encoder = Encoder::default();
    let schema = encoder.schema(&first.schema()).unwrap();
    let frames = encoder.batch(&first).unwrap();
    let each = dictionary_held(&schema, &frames);
    // A limit two such dictionaries exceed holds each that replaces the last.
    let limits = Limits {
        frame_bytes: 2 * each - 1,
        ..Limits::default()
    };
    let mut decoder = Decoder::new(limits);
    decoder.schema(&schema).unwrap();
    assert_eq!(decoder.frame(&frames[0]).unwrap(), None);
    assert_eq!(decoder.frame(&frames[1]).unwrap(), Some(first));
    // The same column with other values sends its dictionary again, under the same id.
    let replaced = encoder.batch(&second).unwrap();
    assert_eq!(replaced.len(), 2, "the new dictionary, then the batch");
    assert_eq!(decoder.frame(&replaced[0]).unwrap(), None);
    assert_eq!(decoder.dictionary_bytes(), each);
    assert_eq!(decoder.frame(&replaced[1]).unwrap(), Some(second.clone()));
    decoder
        .schema(&encoder.schema(&second.schema()).unwrap())
        .unwrap();
    assert_eq!(decoder.dictionary_bytes(), 0);
}

#[test]
fn a_schema_sent_again_is_the_schema_the_decoder_holds() {
    let batch = ints(3);
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(Limits::default());
    let first = decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
    let before = decoder
        .frame(&encoder.batch(&batch).unwrap()[0])
        .unwrap()
        .unwrap();
    // The same schema, sent again for the next epoch: the batches after it share the first.
    let again = decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
    assert!(Arc::ptr_eq(&first, &again));
    let after = decoder
        .frame(&encoder.batch(&batch).unwrap()[0])
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&before.schema(), &after.schema()));
    assert_eq!(before, after);
    // A schema that differs is its own, and the frames after it are in it.
    let other = samples::batch_of(Arc::new(StringArray::from(vec!["a"])));
    let changed = decoder
        .schema(&encoder.schema(&other.schema()).unwrap())
        .unwrap();
    assert!(!Arc::ptr_eq(&first, &changed));
    let read = decoder.frame(&encoder.batch(&other).unwrap()[0]).unwrap();
    assert_eq!(read, Some(other));
}

#[test]
fn a_schema_sent_again_forgets_the_dictionaries_before_it() {
    let batch = keyed(1, 1_000, "x");
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(Limits::default());
    let schema = encoder.schema(&batch.schema()).unwrap();
    decoder.schema(&schema).unwrap();
    let frames = encoder.batch(&batch).unwrap();
    assert_eq!(decoder.frame(&frames[0]).unwrap(), None);
    assert!(decoder.dictionary_bytes() >= 1_000);
    decoder.schema(&schema).unwrap();
    assert_eq!(decoder.dictionary_bytes(), 0);
    // The batch's keys name a dictionary the decoder no longer holds.
    assert!(decoder.frame(&frames[1]).is_err());
}

/// A batch of dictionary columns, each of one row keyed into its one value.
fn tagged(columns: &[(&str, &str)]) -> RecordBatch {
    let keyed = |value: &str| -> ArrayRef {
        let values = Arc::new(StringArray::from(vec![value]));
        Arc::new(DictionaryArray::<Int8Type>::try_new(vec![0].into(), values).unwrap())
    };
    RecordBatch::try_from_iter(columns.iter().map(|(name, value)| (*name, keyed(value)))).unwrap()
}

/// The dictionary ids a schema message gives its fields, in order.
fn dictionary_ids(message: &[u8]) -> Option<Vec<Option<i64>>> {
    let message = arrow_ipc::root_as_message(message).ok()?;
    let schema = arrow_ipc::convert::fb_to_schema(message.header_as_schema()?);
    #[expect(deprecated, reason = "the ids a message names are read nowhere else")]
    let ids = schema
        .fields()
        .iter()
        .map(|field| field.dict_id())
        .collect();
    Some(ids)
}

/// Two schema messages of columns `a` and `b`, alike but for their dictionaries' ids: 1 and 2
/// in the first, exchanged in the second.
fn exchanged() -> (Bytes, Bytes) {
    use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
    let batch = tagged(&[("a", "banana"), ("b", "apple")]);
    let mut tracker = DictionaryTracker::new(false);
    tracker.next_dict_id();
    let first = IpcDataGenerator {}
        .schema_to_bytes_with_dictionary_tracker(
            &batch.schema(),
            &mut tracker,
            &IpcWriteOptions::default(),
        )
        .ipc_message;
    assert_eq!(dictionary_ids(&first), Some(vec![Some(1), Some(2)]));
    let (one, two) = (1_i64.to_le_bytes(), 2_i64.to_le_bytes());
    let places = |id: [u8; 8]| -> Vec<usize> {
        let windows = first.windows(8).enumerate();
        let found = windows.filter(|(_, window)| *window == id);
        found.map(|(place, _)| place).collect()
    };
    for (i, j) in places(one)
        .into_iter()
        .flat_map(|i| places(two).into_iter().map(move |j| (i, j)))
    {
        let mut patched = first.clone();
        patched[i..i + 8].copy_from_slice(&two);
        patched[j..j + 8].copy_from_slice(&one);
        if dictionary_ids(&patched) == Some(vec![Some(2), Some(1)]) {
            return (Bytes::from(first), Bytes::from(patched));
        }
    }
    panic!("the ids are in the message");
}

/// Frames of dictionaries 1 and 2, of "banana" and of "apple", and of a batch of two columns
/// of keys.
fn exchanged_frames() -> [IpcFrame; 3] {
    use arrow_ipc::writer::{
        DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
    };
    let (generator, options) = (IpcDataGenerator {}, IpcWriteOptions::default());
    let encoded = |batch: &RecordBatch| {
        let mut tracker = DictionaryTracker::new(false);
        generator.schema_to_bytes_with_dictionary_tracker(&batch.schema(), &mut tracker, &options);
        let mut context = IpcWriteContext::default();
        generator
            .encode(batch, &mut tracker, &options, &mut context)
            .unwrap()
    };
    let frame = |encoded: &arrow_ipc::writer::EncodedData| IpcFrame {
        header: Bytes::from(encoded.ipc_message.clone()),
        body: Bytes::from(encoded.arrow_data.clone()),
    };
    // Dictionaries are numbered from zero by place: of three columns, the last two are 1 and 2.
    let (dictionaries, _) = encoded(&tagged(&[("x", "x"), ("a", "banana"), ("b", "apple")]));
    let (_, keys) = encoded(&tagged(&[("a", "banana"), ("b", "apple")]));
    [
        frame(&dictionaries[1]),
        frame(&dictionaries[2]),
        frame(&keys),
    ]
}

#[test]
fn a_schema_naming_other_dictionary_ids_is_not_the_schema_held() {
    let (first, second) = exchanged();
    let [banana, apple, keys] = exchanged_frames();
    let read = |decoder: &mut Decoder| {
        assert_eq!(decoder.frame(&banana).unwrap(), None);
        assert_eq!(decoder.frame(&apple).unwrap(), None);
        let batch = decoder.frame(&keys).unwrap().unwrap();
        let text = |column: usize| {
            let keys = batch.column(column).as_dictionary::<Int8Type>();
            keys.values().as_string::<i32>().value(0).to_owned()
        };
        (text(0), text(1))
    };
    // A decoder that saw only the second schema: dictionary 2 is a's, 1 is b's.
    let mut alone = Decoder::new(Limits::default());
    alone.schema(&second).unwrap();
    let expected = read(&mut alone);
    assert_eq!(expected, ("apple".to_owned(), "banana".to_owned()));
    // A decoder that saw the first schema before it.
    let mut decoder = Decoder::new(Limits::default());
    let held = decoder.schema(&first).unwrap();
    let again = decoder.schema(&second).unwrap();
    // The schemas are equal but for the ids, which decide whose dictionary each is.
    assert_eq!(held, again);
    assert!(!Arc::ptr_eq(&held, &again));
    assert_eq!(read(&mut decoder), expected);
    // The same message again is the schema held.
    assert!(Arc::ptr_eq(&again, &decoder.schema(&second).unwrap()));
}

/// Whether a sender limited to `limit` bytes of dictionaries sends `batch`, after its schema,
/// as its first piece: its frames where it does.
fn sent_within(batch: &RecordBatch, limit: u64) -> Option<(Bytes, Vec<IpcFrame>)> {
    let limits = Limits {
        dictionary_bytes: limit,
        ..Limits::default()
    };
    let mut encoder = Encoder::default();
    let schema = encoder.schema(&batch.schema()).unwrap();
    let mut cut = super::Cut::new(batch.clone(), limits);
    let frames = encoder.piece(&mut cut).ok()?.unwrap();
    Some((schema, frames))
}

#[test]
fn what_a_sender_admits_of_dictionaries_its_receiver_holds_exactly() {
    // Sixty-five shapes: one to three hundred columns, of values empty to a hundred kilobytes.
    for columns in [1_usize, 2, 3, 10, 100, 300] {
        for bytes in [0_usize, 1, 7, 8, 9, 63, 64, 65, 1_000, 4_093, 100_000] {
            if columns * bytes > 10_000_000 {
                continue;
            }
            let batch = keyed(columns, bytes, "x");
            // What a receiver holds once it has decoded the batch's dictionaries.
            let (schema, frames) = sent_within(&batch, u64::MAX).unwrap();
            let mut measuring = Decoder::new(Limits::default());
            measuring.schema(&schema).unwrap();
            for frame in &frames {
                measuring.frame(frame).unwrap();
            }
            let held = measuring.dictionary_bytes();
            // The sender sends it within exactly that, and not a byte less.
            let (schema, frames) = sent_within(&batch, held)
                .unwrap_or_else(|| panic!("{columns} of {bytes} bytes refused at {held}"));
            assert!(
                held == 0 || sent_within(&batch, held - 1).is_none(),
                "{columns} of {bytes} bytes sent below {held}"
            );
            // A receiver limited to it takes every frame sent.
            let limits = Limits {
                dictionary_bytes: held,
                ..Limits::default()
            };
            let mut decoder = Decoder::new(limits);
            decoder.schema(&schema).unwrap();
            for frame in &frames {
                decoder.frame(frame).unwrap_or_else(|error| {
                    panic!("{columns} of {bytes} bytes, sent at {held}: {error}")
                });
            }
            assert_eq!(
                decoder.dictionary_bytes(),
                held,
                "{columns} of {bytes} bytes"
            );
        }
    }
}

/// A batch of two dictionary columns, `a` and `b`, each one value of the bytes given.
fn two_keyed(a: usize, b: usize) -> RecordBatch {
    let keyed = |bytes: usize| -> ArrayRef {
        let values = Arc::new(StringArray::from(vec!["x".repeat(bytes)]));
        Arc::new(DictionaryArray::<Int8Type>::try_new(vec![0].into(), values).unwrap())
    };
    RecordBatch::try_from_iter([("a", keyed(a)), ("b", keyed(b))]).unwrap()
}

#[test]
fn a_sender_counts_the_dictionaries_its_receiver_holds_from_the_batches_before() {
    let limits = Limits {
        dictionary_bytes: 2 << 20,
        ..Limits::default()
    };
    let big = 1_500_000;
    let mut encoder = Encoder::default();
    let schema = encoder.schema(&two_keyed(1, big).schema()).unwrap();
    let mut decoder = Decoder::new(limits);
    decoder.schema(&schema).unwrap();
    // Each batch's dictionaries are within the limit; the second's `a` beside the first's `b`,
    // which its receiver still holds, is not.
    let mut first = super::Cut::new(two_keyed(1, big), limits);
    for frame in encoder.piece(&mut first).unwrap().unwrap() {
        decoder.frame(&frame).unwrap();
    }
    let mut second = super::Cut::new(two_keyed(big, 1), limits);
    let refused = encoder.piece(&mut second).unwrap_err();
    let WireError::Refused(refusal) = refused else {
        panic!("{refused}");
    };
    assert_eq!(refusal.field, "dictionary bytes");
    assert_eq!(refusal.limit, 2 << 20);
    assert!(refusal.actual > 3_000_000, "{refusal:?}");
    // A schema sent again clears what its receiver holds, and the batch is sent then.
    let again = encoder.schema(&two_keyed(1, big).schema()).unwrap();
    decoder.schema(&again).unwrap();
    let mut second = super::Cut::new(two_keyed(big, 1), limits);
    for frame in encoder.piece(&mut second).unwrap().unwrap() {
        decoder.frame(&frame).unwrap();
    }
}

#[test]
fn a_schemas_message_is_measured_as_its_sender_encodes_it() {
    let nested = Field::new_list("items", Field::new("item", DataType::Utf8, true), true);
    let schemas = [
        samples::batch().schema(),
        keyed(3, 10, "x").schema(),
        Arc::new(Schema::new(vec![nested])),
    ];
    for schema in schemas {
        let sent = Encoder::default().schema(&schema).unwrap();
        assert_eq!(
            super::schema_message_bytes(&schema),
            sent.len(),
            "{schema:?}"
        );
    }
}

/// A schema of one column nested `levels` deep through `level`, which wraps the type below it
/// in one level more, its innermost type an integer.
fn nested_through(levels: usize, level: impl Fn(DataType) -> DataType) -> Schema {
    let inner = (1..levels).fold(DataType::Int32, |below, _| level(below));
    Schema::new(vec![Field::new("deep", inner, true)])
}

/// A union of one member, `a`, of `below`, in `mode`.
fn union_of(below: DataType, mode: arrow_schema::UnionMode) -> DataType {
    let fields = arrow_schema::UnionFields::from_fields(vec![Field::new("a", below, true)]);
    DataType::Union(fields, mode)
}

/// A dictionary keyed by `Int32` whose values are lists of `below`: the list's item is one
/// level below the dictionary's field.
fn keyed_list(below: DataType) -> DataType {
    let list = DataType::List(Arc::new(Field::new("item", below, true)));
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(list))
}

#[test]
fn a_schema_nested_to_the_limit_through_unions_or_dictionaries_is_sent_and_one_level_deeper_refused()
 {
    let depth = usize::try_from(crate::limits::NESTING_DEPTH).unwrap();
    let levels: [(&str, Box<dyn Fn(DataType) -> DataType>); 3] = [
        (
            "sparse union",
            Box::new(|below| union_of(below, arrow_schema::UnionMode::Sparse)),
        ),
        (
            "dense union",
            Box::new(|below| union_of(below, arrow_schema::UnionMode::Dense)),
        ),
        ("dictionary of lists", Box::new(keyed_list)),
    ];
    for (what, level) in levels {
        let mut encoder = Encoder::default();
        let mut decoder = Decoder::new(Limits::default());
        let deepest = nested_through(depth, &level);
        let sent = encoder
            .schema(&deepest)
            .unwrap_or_else(|error| panic!("{what}: {error}"));
        let received = decoder
            .schema(&sent)
            .unwrap_or_else(|error| panic!("{what}: {error}"));
        assert_eq!(
            received.field(0).data_type(),
            deepest.field(0).data_type(),
            "{what}"
        );
        let deeper = encoder.schema(&nested_through(depth + 1, &level)).unwrap();
        match decoder.schema(&deeper).unwrap_err() {
            WireError::Refused(refusal) => assert_eq!(refusal.field, "nesting depth", "{what}"),
            other => panic!("{what}: {other}"),
        }
    }
}
