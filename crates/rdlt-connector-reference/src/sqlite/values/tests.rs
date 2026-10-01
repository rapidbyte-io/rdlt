use std::sync::Arc;

use arrow_array::builder::{
    BinaryDictionaryBuilder, PrimitiveDictionaryBuilder, StringDictionaryBuilder,
};
use arrow_array::types::{
    ArrowDictionaryKeyType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, NullArray, RecordBatch, RunArray, StringArray, StringViewArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{Field, Schema};
use rdlt_connector::sqlgen::Statement;
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rusqlite::Connection;
use rusqlite::types::Value;

use super::stage;

/// A table `t` of one column `x` of no declared type, so every value is stored as it was bound.
fn table() -> Connection {
    let connection = Connection::open_in_memory().expect("a database opens");
    connection
        .execute_batch("CREATE TABLE t (tag TEXT, x)")
        .expect("the table is created");
    connection
}

/// Stages `column` as `x`, each row tagged `staged`; returns what the table then holds of `x`.
fn staged(connection: &Connection, column: ArrayRef) -> Result<Vec<Value>, ConnectorError> {
    let schema = Schema::new(vec![Field::new("x", column.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![column]).expect("a valid batch");
    let statement = Statement {
        sql: "INSERT INTO t (tag, x) VALUES (?1, ?2)".to_owned(),
        params: vec![rdlt_connector::sqlgen::SqlValue::Text("staged".to_owned())],
    };
    let outcome = stage(connection, &statement, &batch);
    let mut rows = connection
        .prepare("SELECT x FROM t WHERE tag = 'staged' ORDER BY rowid")
        .expect("the listing prepares");
    let held = rows
        .query_map([], |row| row.get::<_, Value>(0))
        .expect("the listing runs")
        .collect::<Result<Vec<_>, _>>()
        .expect("the rows read");
    outcome.map(|()| held)
}

fn integers(values: &[Option<i64>]) -> Vec<Value> {
    values
        .iter()
        .map(|value| value.map_or(Value::Null, Value::Integer))
        .collect()
}

fn texts(values: &[Option<&str>]) -> Vec<Value> {
    values
        .iter()
        .map(|value| value.map_or(Value::Null, |text| Value::Text(text.to_owned())))
        .collect()
}

fn blobs(values: &[Option<&[u8]>]) -> Vec<Value> {
    values
        .iter()
        .map(|value| value.map_or(Value::Null, |blob| Value::Blob(blob.to_vec())))
        .collect()
}

/// A column of each integer and float type the destination stores, with what it stages.
fn numbers() -> Vec<(ArrayRef, Vec<Value>)> {
    let ints = integers(&[Some(-7), None, Some(9)]);
    let small = integers(&[Some(7), None, Some(9)]);
    vec![
        (
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            integers(&[Some(1), None, Some(0)]),
        ),
        (
            Arc::new(Int8Array::from(vec![Some(-7), None, Some(9)])),
            ints.clone(),
        ),
        (
            Arc::new(Int16Array::from(vec![Some(-7), None, Some(9)])),
            ints.clone(),
        ),
        (
            Arc::new(Int32Array::from(vec![Some(-7), None, Some(9)])),
            ints,
        ),
        (
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
            integers(&[Some(i64::MIN), None, Some(i64::MAX)]),
        ),
        (
            Arc::new(UInt8Array::from(vec![Some(7), None, Some(9)])),
            small.clone(),
        ),
        (
            Arc::new(UInt16Array::from(vec![Some(7), None, Some(9)])),
            small,
        ),
        (
            Arc::new(UInt32Array::from(vec![Some(u32::MAX), None, Some(9)])),
            integers(&[Some(i64::from(u32::MAX)), None, Some(9)]),
        ),
        (
            Arc::new(Float32Array::from(vec![
                Some(0.5),
                None,
                Some(f32::INFINITY),
            ])),
            vec![Value::Real(0.5), Value::Null, Value::Real(f64::INFINITY)],
        ),
        (
            Arc::new(Float64Array::from(vec![
                Some(f64::MIN),
                None,
                Some(f64::NEG_INFINITY),
            ])),
            vec![
                Value::Real(f64::MIN),
                Value::Null,
                Value::Real(f64::NEG_INFINITY),
            ],
        ),
    ]
}

/// A column of each text and bytes type the destination stores, and of nulls, with what it
/// stages.
fn texts_and_bytes() -> Vec<(ArrayRef, Vec<Value>)> {
    let words = [Some("a"), None, Some("")];
    let bytes = [Some(&b"\x00\x01"[..]), None, Some(&b""[..])];
    let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [Some([1_u8, 2]), None, Some([3, 4])].into_iter(),
        2,
    )
    .expect("fixed bytes");
    vec![
        (Arc::new(NullArray::new(2)), vec![Value::Null, Value::Null]),
        (Arc::new(StringArray::from(words.to_vec())), texts(&words)),
        (
            Arc::new(LargeStringArray::from(words.to_vec())),
            texts(&words),
        ),
        (Arc::new(BinaryArray::from(bytes.to_vec())), blobs(&bytes)),
        (
            Arc::new(LargeBinaryArray::from(bytes.to_vec())),
            blobs(&bytes),
        ),
        (
            Arc::new(fixed),
            blobs(&[Some(&[1, 2]), None, Some(&[3, 4])]),
        ),
    ]
}

#[test]
fn every_plain_type_is_staged_as_its_storage_class() {
    let cases: Vec<_> = numbers().into_iter().chain(texts_and_bytes()).collect();
    assert_eq!(cases.len(), 16);
    for (column, expected) in cases {
        let kind = column.data_type().clone();
        let connection = table();
        assert_eq!(
            staged(&connection, column).expect("the rows stage"),
            expected,
            "{kind}"
        );
    }
}

/// `values` dictionary-encoded with keys of `K`, a null key among them and a null value.
fn dictionaries<K: ArrowDictionaryKeyType>() -> Vec<(ArrayRef, Vec<Value>)> {
    let mut texts = StringDictionaryBuilder::<K>::new();
    let mut blobs = BinaryDictionaryBuilder::<K>::new();
    let mut ints = PrimitiveDictionaryBuilder::<K, Int32Type>::new();
    let mut reals = PrimitiveDictionaryBuilder::<K, Float64Type>::new();
    for value in [Some("a"), None, Some("b"), Some("a")] {
        texts.append_option(value);
        blobs.append_option(value.map(str::as_bytes));
    }
    for value in [Some(5), None, Some(-1), Some(5)] {
        ints.append_option(value);
        reals.append_option(value.map(f64::from));
    }
    let text = |value: &str| Value::Text(value.to_owned());
    let blob = |value: &str| Value::Blob(value.as_bytes().to_vec());
    vec![
        (
            Arc::new(texts.finish()),
            vec![text("a"), Value::Null, text("b"), text("a")],
        ),
        (
            Arc::new(blobs.finish()),
            vec![blob("a"), Value::Null, blob("b"), blob("a")],
        ),
        (
            Arc::new(ints.finish()),
            integers(&[Some(5), None, Some(-1), Some(5)]),
        ),
        (
            Arc::new(reals.finish()),
            vec![
                Value::Real(5.0),
                Value::Null,
                Value::Real(-1.0),
                Value::Real(5.0),
            ],
        ),
    ]
}

#[test]
fn a_dictionary_of_any_key_type_is_staged_as_the_values_it_encodes() {
    let mut cases = dictionaries::<Int8Type>();
    cases.extend(dictionaries::<Int16Type>());
    cases.extend(dictionaries::<Int32Type>());
    cases.extend(dictionaries::<Int64Type>());
    cases.extend(dictionaries::<UInt8Type>());
    cases.extend(dictionaries::<UInt16Type>());
    cases.extend(dictionaries::<UInt32Type>());
    cases.extend(dictionaries::<UInt64Type>());
    assert_eq!(cases.len(), 32);
    for (column, expected) in cases {
        let kind = column.data_type().clone();
        let connection = table();
        assert_eq!(
            staged(&connection, column).expect("the rows stage"),
            expected,
            "{kind}"
        );
    }
    // A key naming a null value is a null.
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
    let keys = Int8Array::from(vec![0, 1, 0]);
    let column = arrow_array::DictionaryArray::try_new(keys, values).expect("a dictionary");
    let connection = table();
    assert_eq!(
        staged(&connection, Arc::new(column)).expect("the rows stage"),
        [
            Value::Text("a".into()),
            Value::Null,
            Value::Text("a".into())
        ]
    );
}

#[test]
fn a_type_the_destination_does_not_store_is_refused_before_any_row() {
    let run_ends = Int32Array::from(vec![2, 3]);
    let run_values = StringArray::from(vec!["a", "b"]);
    let unstorable: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![1, 2])),
        Arc::new(Date32Array::from(vec![1, 2])),
        Arc::new(StringViewArray::from(vec!["a", "b"])),
        Arc::new(BinaryViewArray::from(vec![&b"a"[..], &b"b"[..]])),
        Arc::new(RunArray::try_new(&run_ends, &run_values).expect("a run array")),
        Arc::new(
            arrow_array::DictionaryArray::try_new(
                Int8Array::from(vec![0, 0]),
                Arc::new(Date32Array::from(vec![1])) as ArrayRef,
            )
            .expect("a dictionary"),
        ),
    ];
    for column in unstorable {
        let kind = column.data_type().clone();
        let connection = table();
        let error = staged(&connection, column).expect_err("the type is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{kind}");
        let rows: i64 = connection
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .expect("the count reads");
        assert_eq!(rows, 0, "{kind}");
    }
}

/// `values` as a `Float32` or `Float64` column, plain or dictionary-encoded.
fn floats(values: &[f64], narrow: bool, encoded: bool) -> ArrayRef {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the values are chosen to fit"
    )]
    let narrowed = || values.iter().map(|value| *value as f32);
    match (narrow, encoded) {
        (false, false) => Arc::new(Float64Array::from(values.to_vec())),
        (true, false) => Arc::new(Float32Array::from_iter_values(narrowed())),
        (false, true) => {
            let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Float64Type>::new();
            for value in values {
                builder.append_value(*value);
            }
            Arc::new(builder.finish())
        }
        (true, true) => {
            let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Float32Type>::new();
            for value in narrowed() {
                builder.append_value(value);
            }
            Arc::new(builder.finish())
        }
    }
}

#[test]
fn a_float_sqlite_would_change_is_refused_where_its_row_is_staged() {
    for unstorable in [f64::NAN, -0.0] {
        for (narrow, encoded) in [(false, false), (true, false), (false, true), (true, true)] {
            let connection = table();
            let column = floats(&[1.5, 2.5, unstorable, 3.5], narrow, encoded);
            let error = staged(&connection, column).expect_err("the value is refused");
            assert_eq!(
                (error.kind(), error.code()),
                (ConnectorErrorKind::Data, Some("float_unstorable")),
                "{unstorable} {narrow} {encoded}"
            );
            // Rows are bound one at a time: those before the refused one ran.
            let rows: i64 = connection
                .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
                .expect("the count reads");
            assert_eq!(rows, 2, "{unstorable} {narrow} {encoded}");
        }
    }
    // Zero, the infinities and the least values are stored as they are.
    let exact = [
        0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MIN_POSITIVE,
        5e-324,
    ];
    let connection = table();
    let held = staged(&connection, floats(&exact, false, false)).expect("the rows stage");
    let bits: Vec<u64> = held
        .iter()
        .map(|value| match value {
            Value::Real(real) => real.to_bits(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(bits, exact.map(f64::to_bits));
}
