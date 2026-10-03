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

/// The ids of roots whose encodings are `encodings`: their BLAKE3 hashes after a root's tag.
fn hashed(encodings: &[Vec<u8>]) -> Vec<Vec<u8>> {
    encodings
        .iter()
        .map(|encoding| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&[0x01]);
            hasher.update(encoding);
            hasher.finalize().as_bytes().to_vec()
        })
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
            vec![tagged(b'd', b"-5"), tagged(b'd', b"120"), b"n".to_vec()],
        ),
        (
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            vec![tagged(b'd', b"18446744073709551615")],
        ),
        (
            Arc::new(Float32Array::from(vec![1.5, -0.0])),
            vec![tagged(b'd', b"1.5"), tagged(b'd', b"0")],
        ),
        (half, vec![tagged(b'd', b"0.5")]),
        (
            Arc::new(Float64Array::from(vec![2.25, -0.0, 3.0])),
            vec![
                tagged(b'd', b"2.25"),
                tagged(b'd', b"0"),
                tagged(b'd', b"3"),
            ],
        ),
        (
            Arc::new(decimals),
            vec![tagged(b'd', b"12.3"), tagged(b'd', b"2")],
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
        tagged(b'd', b"1"),
        b"}]".to_vec(),
    ]
    .concat();
    let list =
        LargeListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1), Some(2)])]);
    check(vec![
        (
            Arc::clone(&timestamps),
            vec![[vec![b'i'], prefixed(b"-2000000000")].concat()],
        ),
        (
            Arc::new(list),
            vec![
                [
                    b"[".to_vec(),
                    tagged(b'd', b"1"),
                    tagged(b'd', b"2"),
                    b"]".to_vec(),
                ]
                .concat(),
            ],
        ),
        (Arc::new(map.finish()), vec![entry]),
    ]);
}

#[test]
fn a_key_the_batch_lacks_identifies_no_row() {
    let batch =
        RecordBatch::try_from_iter([("other", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(root_ids(&batch, &[Arc::from("k")]).is_err());
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
        // A `Date64` names the day it is within, so its values are whole days apart.
        DataType::Date64 => Arc::new(arrow_array::Date64Array::from(vec![
            Some(86_400_000),
            None,
            Some(3 * 86_400_000),
            Some(4 * 86_400_000),
        ])),
        DataType::Time64(_) | DataType::Timestamp(..) | DataType::Duration(_) => {
            through(&DataType::Int64)
        }
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

/// 400 bytes of `A` holding `window` at `at`: two windows of one XXH3 collision.
fn blob(window: [u8; 16], at: usize) -> Vec<u8> {
    let mut bytes = vec![0x41_u8; 400];
    bytes[at..at + 16].copy_from_slice(&window);
    bytes
}

const COLLIDING: [[u8; 16]; 2] = [
    [
        0x01, 0, 0, 0, 0xf6, 0x21, 0xad, 0x1c, 0, 0, 0, 0, 0x82, 0x90, 0x97, 0xdb,
    ],
    [
        0, 0, 0, 0, 0xf6, 0x21, 0xad, 0x1c, 0x01, 0, 0, 0, 0x82, 0x90, 0x97, 0xdb,
    ],
];

#[test]
fn values_crafted_to_collide_under_an_unkeyed_fast_hash_have_distinct_ids() {
    let whole = |at: usize| {
        let blobs: Vec<Vec<u8>> = COLLIDING.iter().map(|window| blob(*window, at)).collect();
        let column: ArrayRef = Arc::new(BinaryArray::from_iter_values(&blobs));
        let batch = RecordBatch::try_from_iter([("blob", column)]).unwrap();
        root_ids(&batch, &[]).unwrap()
    };
    let ids = whole(54);
    assert_ne!(ids.value(0), ids.value(1));
    assert_eq!(ids.value(0).len(), 32);
    let keyed = keyed_ids(Arc::new(BinaryArray::from_iter_values(
        COLLIDING.iter().map(|window| blob(*window, 61)),
    )));
    assert_ne!(keyed[0], keyed[1]);
    let parents = BinaryArray::from_iter_values(&keyed);
    let children = super::child_ids(&parents, &Int64Array::from(vec![0, 0]));
    assert_ne!(children.value(0), children.value(1));
    assert_ne!(children.value(0), keyed[0].as_slice());
}

#[test]
fn a_float32_hashes_as_the_float64_it_widens_to() {
    let singles = vec![
        0.1_f32,
        -0.1,
        1.0e-45,
        f32::MIN_POSITIVE,
        f32::MAX,
        f32::MIN,
        16_777_217.0,
        -0.0,
        0.0,
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        1.5,
    ];
    let narrow: ArrayRef = Arc::new(Float32Array::from(singles.clone()));
    let widened = arrow_cast::cast(&narrow, &DataType::Float64).unwrap();
    assert_eq!(keyed_ids(narrow), keyed_ids(widened));
    let half = arrow_cast::cast(
        &(Arc::new(Float32Array::from(vec![0.1_f32, 65_504.0])) as ArrayRef),
        &DataType::Float16,
    )
    .unwrap();
    let widened = arrow_cast::cast(&half, &DataType::Float64).unwrap();
    assert_eq!(keyed_ids(half), keyed_ids(widened));
}

#[test]
fn a_float32_and_a_float64_that_print_alike_differ() {
    for (single, double) in [(0.1_f32, 0.1_f64), (0.2, 0.2), (3.3, 3.3), (1e-8, 1e-8)] {
        let singles = keyed_ids(Arc::new(Float32Array::from(vec![single])));
        let doubles = keyed_ids(Arc::new(Float64Array::from(vec![double])));
        assert_eq!(single.to_string(), double.to_string());
        assert_ne!(singles, doubles, "{single}");
    }
}

#[test]
fn a_date64_identifies_as_the_day_it_is_within() {
    let day = 86_400_000_i64;
    let millis = vec![-1, -day, -day - 1, 0, day - 1, day, i64::MIN, i64::MAX];
    let days: Vec<i64> = millis.iter().map(|ms| ms.div_euclid(day)).collect();
    let date64 = keyed_ids(Arc::new(arrow_array::Date64Array::from(millis)));
    // The days the ends of a `Date64` are within begin before or after what it holds.
    let whole = keyed_ids(Arc::new(arrow_array::Date64Array::from(
        days[..6].iter().map(|days| days * day).collect::<Vec<_>>(),
    )));
    assert_eq!(date64[..6], whole[..]);
    let near = keyed_ids(Arc::new(arrow_array::Date32Array::from(
        days[..6]
            .iter()
            .map(|days| i32::try_from(*days).unwrap())
            .collect::<Vec<_>>(),
    )));
    assert_eq!(date64[..6], near[..]);
    assert_eq!(
        (date64[0] == date64[1], date64[1] == date64[2]),
        (true, false)
    );
    assert_ne!(date64[6], date64[7]);
}
