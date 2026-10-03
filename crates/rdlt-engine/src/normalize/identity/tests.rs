mod recorded;
mod vectors;

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
    let timestamps: ArrayRef = Arc::new(TimestampSecondArray::from(vec![-2]));
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
            vec![[vec![b'i'], prefixed(b"-2000000000")].concat()],
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

#[test]
fn times_and_durations_encode_as_their_kind_and_nanoseconds() {
    let times: ArrayRef = Arc::new(arrow_array::Time32MillisecondArray::from(vec![1_500]));
    let durations: ArrayRef = Arc::new(arrow_array::DurationSecondArray::from(vec![-2]));
    check(vec![
        (times, vec![[vec![b'c'], prefixed(b"1500000000")].concat()]),
        (
            durations,
            vec![[vec![b'e'], prefixed(b"-2000000000")].concat()],
        ),
    ]);
}

/// A column of four values of `data_type`, one of them null.
fn narrow(data_type: &DataType) -> ArrayRef {
    let ints: ArrayRef = Arc::new(Int8Array::from(vec![Some(1), None, Some(3), Some(4)]));
    let through = |stored: &DataType| {
        let stored = arrow_cast::cast(&ints, stored).unwrap();
        arrow_cast::cast(&stored, data_type).unwrap()
    };
    match data_type {
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal256(..) => {
            through(&DataType::Decimal128(9, 0))
        }
        DataType::Date32 | DataType::Time32(_) => through(&DataType::Int32),
        DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(..)
        | DataType::Duration(_) => through(&DataType::Int64),
        other => arrow_cast::cast(&ints, other).unwrap(),
    }
}

#[test]
fn values_are_read_where_they_lie_not_from_a_wider_copy() {
    use arrow_schema::TimeUnit as U;
    let read_in_place = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(U::Second),
        DataType::Time32(U::Millisecond),
        DataType::Time64(U::Microsecond),
        DataType::Time64(U::Nanosecond),
        DataType::Timestamp(U::Second, None),
        DataType::Timestamp(U::Millisecond, None),
        DataType::Timestamp(U::Microsecond, None),
        DataType::Timestamp(U::Nanosecond, Some("UTC".into())),
        DataType::Duration(U::Second),
        DataType::Duration(U::Millisecond),
        DataType::Duration(U::Microsecond),
        DataType::Duration(U::Nanosecond),
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
    ];
    for data_type in read_in_place {
        let column = narrow(&data_type);
        let field = arrow_schema::Field::new("k", data_type.clone(), true);
        let (super::Encoder::Integer(held, values)
        | super::Encoder::Decimal(held, values, _)
        | super::Encoder::Temporal(_, held, values, _)) =
            super::Encoder::new(&field, &column).unwrap()
        else {
            panic!("{data_type} is encoded through a copy");
        };
        assert!(Arc::ptr_eq(&held, &column), "{data_type}");
        assert!(values.shares(column.as_ref()), "{data_type}");
        // The null stays one, and each value hashes as its 64-bit self does.
        let ids = keyed_ids(Arc::clone(&column));
        assert_eq!(ids[1], hashed(&[vec![super::NULL]])[0], "{data_type}");
        assert_ne!(ids[0], ids[2], "{data_type}");
    }
}

#[test]
fn an_encoded_column_hashes_by_the_values_its_rows_name() {
    // A dictionary holding a value no key names, and a run of one value.
    let words = StringArray::from(vec!["unnamed", "a", "b"]);
    let keyed: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(1), None, Some(2)]),
            Arc::new(words),
        )
        .unwrap(),
    );
    let plain: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]));
    assert_eq!(keyed_ids(keyed), keyed_ids(plain));
    let runs: ArrayRef = Arc::new(
        arrow_array::RunArray::<arrow_array::types::Int32Type>::try_new(
            &arrow_array::Int32Array::from(vec![2, 3]),
            &StringArray::from(vec!["a", "b"]),
        )
        .unwrap(),
    );
    let repeated: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b"]));
    assert_eq!(keyed_ids(runs), keyed_ids(repeated));
}

/// A batch of one column `k` of JSON text holding `values`.
fn json_column(values: Vec<Option<String>>) -> RecordBatch {
    let field = arrow_schema::Field::new("k", DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".to_owned(), "arrow.json".to_owned())].into());
    let schema = Arc::new(arrow_schema::Schema::new(vec![field]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(values)) as ArrayRef],
    )
    .unwrap()
}

/// The ids of rows keyed by the JSON text `values`, each its own batch's.
fn json_ids(values: &[&str]) -> Vec<Vec<u8>> {
    let batch = json_column(
        values
            .iter()
            .map(|value| Some((*value).to_owned()))
            .collect(),
    );
    root_ids(&batch, &[Arc::from("k")])
        .unwrap()
        .iter()
        .map(|id| id.unwrap().to_vec())
        .collect()
}

#[test]
fn json_text_nested_past_the_limit_is_refused_on_a_small_stack() {
    let limit = usize::try_from(rdlt_connector::limits::MAX_NESTING_DEPTH).unwrap();
    let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
    let hashing = std::thread::Builder::new()
        .stack_size(256 << 10)
        .spawn(move || {
            [limit, limit + 1, 100_000].map(|depth| {
                let batch = json_column(vec![Some(nested(depth))]);
                (
                    root_ids(&batch, &[]).is_ok(),
                    root_ids(&batch, &[Arc::from("k")]).is_ok(),
                )
            })
        })
        .unwrap();
    assert_eq!(
        hashing.join().unwrap(),
        [(true, true), (false, false), (false, false)]
    );
}

#[test]
fn json_numbers_hash_by_their_exact_value() {
    let ids = json_ids(&[
        "18446744073709551616",
        "18446744073709551617",
        "0.12345678901234567891",
        "0.12345678901234567892",
        r#"{"a":18446744073709551616}"#,
        r#"{"a":18446744073709551999}"#,
        "1e400",
        "1.0000000000000000000001e400",
    ]);
    for pair in ids.chunks(2) {
        assert_ne!(pair[0], pair[1], "values a float rounds alike hash apart");
    }
    // One value hashes alike whatever holds it: JSON text, a decimal, an integer or a float.
    let huge = Decimal128Array::from(vec![18_446_744_073_709_551_616_i128])
        .with_precision_and_scale(38, 0)
        .unwrap();
    assert_eq!(keyed_ids(Arc::new(huge))[0], ids[0]);
    let one = json_ids(&["1", "1.0", "10e-1", "1E0"]);
    assert!(one.iter().all(|id| *id == one[0]));
    assert_eq!(keyed_ids(Arc::new(Int64Array::from(vec![1])))[0], one[0]);
    assert_eq!(
        keyed_ids(Arc::new(Float64Array::from(vec![1.0])))[0],
        one[0]
    );
    let tenth = json_ids(&["0.1", "1e-1"]);
    assert_eq!(tenth[0], tenth[1]);
    assert_eq!(
        keyed_ids(Arc::new(Float64Array::from(vec![0.1])))[0],
        tenth[0]
    );
}

#[test]
fn json_text_that_is_not_json_is_refused_not_hashed_as_a_string() {
    for text in ["abc", "", "{\"a\":}", "[1,]", "\"abc"] {
        let batch = json_column(vec![Some(text.to_owned())]);
        assert!(root_ids(&batch, &[Arc::from("k")]).is_err(), "{text:?}");
        assert!(root_ids(&batch, &[]).is_err(), "{text:?}");
    }
    let string = json_ids(&["\"abc\""]);
    assert_eq!(
        keyed_ids(Arc::new(StringArray::from(vec!["abc"])))[0],
        string[0],
        "a JSON string hashes as the text it holds"
    );
    let nulls = json_column(vec![None, Some("null".to_owned())]);
    let ids = root_ids(&nulls, &[Arc::from("k")]).unwrap();
    assert_eq!(ids.value(0), ids.value(1), "a JSON null hashes as a null");
}

#[test]
fn json_identity_cannot_read_is_refused_typed_for_the_stream() {
    let stream = rdlt_connector::StreamName::new("events").unwrap();
    for (text, code) in [
        ("1e-1234567890123456789", "limit_exceeded"),
        ("{", "json_invalid"),
    ] {
        let field = arrow_schema::Field::new("k", DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".to_owned(), "arrow.json".to_owned())].into());
        let column: ArrayRef = Arc::new(StringArray::from(vec![text]));
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![field])),
            vec![column],
        )
        .unwrap();
        let error = root_ids(&batch, &[]).unwrap_err();
        let error = super::unread(&stream, "hashing", &error);
        assert_eq!(
            (error.kind(), error.code()),
            (crate::ErrorKind::Source, Some(code))
        );
    }
}
