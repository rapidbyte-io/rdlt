use std::sync::Arc;

use arrow_array::builder::{
    BinaryViewBuilder, Int64Builder, MapBuilder, StringBuilder, StringViewBuilder,
};
use arrow_array::types::{Int8Type, Int64Type};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int64Array, LargeBinaryArray, LargeListArray,
    LargeStringArray, RecordBatch, StringArray, TimestampSecondArray, UInt64Array,
};
use arrow_schema::DataType;

use super::root_ids;

/// `bytes` after their length in LEB128, as the canonical encoding writes them.
fn prefixed(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = bytes.len();
    while rest >= 0x80 {
        out.push(u8::try_from(rest & 0x7f).unwrap() | 0x80);
        rest >>= 7;
    }
    out.push(u8::try_from(rest).unwrap());
    out.extend_from_slice(bytes);
    out
}

/// The ids of rows keyed by `values`.
fn keyed_ids(values: ArrayRef) -> Vec<Vec<u8>> {
    let batch = RecordBatch::try_from_iter([("k", values)]).unwrap();
    root_ids(&batch, &[Arc::from("k")])
        .unwrap()
        .iter()
        .map(|id| id.unwrap().to_vec())
        .collect()
}

fn hashed(encodings: &[Vec<u8>]) -> Vec<Vec<u8>> {
    encodings
        .iter()
        .map(|encoding| xxhash_rust::xxh3::xxh3_128(encoding).to_be_bytes().to_vec())
        .collect()
}

fn tagged(tag: u8, bytes: &[u8]) -> Vec<u8> {
    [vec![tag], prefixed(bytes)].concat()
}

/// Checks that each array's rows, keyed by its values, have the ids of the encodings.
fn check(cases: Vec<(ArrayRef, Vec<Vec<u8>>)>) {
    for (values, encodings) in cases {
        let kind = values.data_type().to_string();
        assert_eq!(keyed_ids(values), hashed(&encodings), "{kind}");
    }
}

#[test]
fn numbers_have_one_rendering_whatever_their_type() {
    let half = arrow_cast::cast(
        &(Arc::new(Float32Array::from(vec![0.5])) as ArrayRef),
        &DataType::Float16,
    )
    .unwrap();
    let decimals = Decimal128Array::from(vec![12_300, 2_000])
        .with_precision_and_scale(5, 3)
        .unwrap();
    check(vec![
        (
            Arc::new(Int8Array::from(vec![Some(-5), Some(120), None])),
            vec![b"d-5;".to_vec(), b"d120;".to_vec(), b"n".to_vec()],
        ),
        (
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            vec![b"d18446744073709551615;".to_vec()],
        ),
        (
            Arc::new(Float32Array::from(vec![1.5, -0.0])),
            vec![b"d1.5;".to_vec(), b"d0;".to_vec()],
        ),
        (half, vec![b"d0.5;".to_vec()]),
        (
            Arc::new(Float64Array::from(vec![2.25, -0.0, 3.0])),
            vec![b"d2.25;".to_vec(), b"d0;".to_vec(), b"d3;".to_vec()],
        ),
        (
            Arc::new(decimals),
            vec![b"d12.3;".to_vec(), b"d2;".to_vec()],
        ),
        (
            Arc::new(BooleanArray::from(vec![true, false])),
            vec![b"t".to_vec(), b"f".to_vec()],
        ),
    ]);
}

#[test]
fn text_and_bytes_encode_alike_whatever_their_arrow_type() {
    let long = "x".repeat(200);
    let mut view = StringViewBuilder::new();
    view.append_value("ab");
    let mut bytes_view = BinaryViewBuilder::new();
    bytes_view.append_value(b"ab");
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0]),
        Arc::new(StringArray::from(vec!["ab"])),
    )
    .unwrap();
    check(vec![
        (
            Arc::new(StringArray::from(vec![long.as_str()])),
            vec![tagged(b's', long.as_bytes())],
        ),
        (
            Arc::new(LargeStringArray::from(vec!["ab"])),
            vec![tagged(b's', b"ab")],
        ),
        (Arc::new(view.finish()), vec![tagged(b's', b"ab")]),
        (Arc::new(dictionary), vec![tagged(b's', b"ab")]),
        (
            Arc::new(BinaryArray::from(vec![&b"ab"[..]])),
            vec![tagged(b'b', b"ab")],
        ),
        (
            Arc::new(LargeBinaryArray::from(vec![&b"ab"[..]])),
            vec![tagged(b'b', b"ab")],
        ),
        (Arc::new(bytes_view.finish()), vec![tagged(b'b', b"ab")]),
        (
            Arc::new(FixedSizeBinaryArray::try_from_iter([b"ab"].into_iter()).unwrap()),
            vec![tagged(b'b', b"ab")],
        ),
    ]);
}

#[test]
fn arrays_maps_and_other_types_encode_by_their_values() {
    let timestamps: ArrayRef = Arc::new(TimestampSecondArray::from(vec![0]));
    let kind = timestamps.data_type().to_string();
    let text = arrow_cast::display::array_value_to_string(&timestamps, 0).unwrap();
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    map.keys().append_value("k");
    map.values().append_value(1);
    map.append(true).unwrap();
    let entry = [
        b"[{k".to_vec(),
        prefixed(b"keys"),
        tagged(b's', b"k"),
        b"k".to_vec(),
        prefixed(b"values"),
        b"d1;}]".to_vec(),
    ]
    .concat();
    let list =
        LargeListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1), Some(2)])]);
    check(vec![
        (
            Arc::clone(&timestamps),
            vec![
                [
                    vec![b'x'],
                    prefixed(kind.as_bytes()),
                    prefixed(text.as_bytes()),
                ]
                .concat(),
            ],
        ),
        (Arc::new(list), vec![b"[d1;d2;]".to_vec()]),
        (Arc::new(map.finish()), vec![entry]),
    ]);
}

#[test]
fn a_key_the_batch_lacks_encodes_as_null() {
    let batch =
        RecordBatch::try_from_iter([("other", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let ids = root_ids(&batch, &[Arc::from("k")]).unwrap();
    assert_eq!(ids.value(0), hashed(&[b"n".to_vec()])[0].as_slice());
}
