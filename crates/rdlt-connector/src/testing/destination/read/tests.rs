use std::sync::Arc;

use arrow_array::builder::{
    BinaryViewBuilder, FixedSizeListBuilder, Int64Builder, LargeListViewBuilder, ListBuilder,
    ListViewBuilder, MapBuilder, StringBuilder, StringViewBuilder,
};
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    DurationSecondArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, NullArray, PrimitiveArray, RecordBatch, RunArray,
    StringArray, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampSecondArray, UnionArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, UnionFields};

use super::{Published, Read, flat, whole};
use crate::testing::limits::{PUBLISHED_BYTES, PUBLISHED_ROWS};

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable([("column", column, true)]).unwrap()
}

/// A dictionary, keyed by `K`, of `values`, each row the first value, the last a null.
fn dictionary<K: ArrowDictionaryKeyType>(values: ArrayRef, rows: usize) -> ArrayRef
where
    K::Native: From<bool>,
{
    let mut keys: Vec<Option<K::Native>> = vec![Some(K::Native::from(false)); rows];
    keys[rows - 1] = None;
    let keys = PrimitiveArray::<K>::from_iter(keys);
    Arc::new(DictionaryArray::<K>::try_new(keys, values).unwrap())
}

/// One run, with ends of `R`, of the first of `values`, `rows` long.
fn run<R: RunEndIndexType>(values: &dyn Array, rows: usize) -> ArrayRef
where
    R::Native: TryFrom<usize>,
{
    let end = R::Native::try_from(rows).ok().unwrap();
    let ends = PrimitiveArray::<R>::from_iter_values([end]);
    Arc::new(RunArray::<R>::try_new(&ends, &values.slice(0, 1)).unwrap())
}

fn texts() -> ArrayRef {
    Arc::new(StringArray::from(vec![Some("ann"), None, Some("longer")]))
}

fn zero_width() -> ArrayRef {
    let values = vec![Some(Vec::<u8>::new())];
    Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 0).unwrap())
}

/// Every kind of column certification writes, in every encoding.
fn admitted() -> Vec<ArrayRef> {
    let views = {
        let mut views = StringViewBuilder::new();
        views.append_value("short");
        views.append_null();
        views.append_value("a string longer than a view holds inline");
        Arc::new(views.finish()) as ArrayRef
    };
    let binary_views = {
        let mut views = BinaryViewBuilder::new();
        views.append_value(b"0123456789abcdef0123");
        Arc::new(views.finish()) as ArrayRef
    };
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let uuid = FixedSizeBinaryArray::try_from_iter([[7_u8; 16]].into_iter()).unwrap();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::clone(&ints),
        Arc::new(Int32Array::from(vec![1, 2])),
        Arc::new(Float64Array::from(vec![1.5])),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(NullArray::new(3)),
        Arc::new(TimestampMicrosecondArray::from(vec![1]).with_timezone("UTC")),
        Arc::new(Date32Array::from(vec![1])),
        Arc::new(Decimal128Array::from(vec![1])),
        Arc::new(uuid),
        texts(),
        Arc::new(LargeStringArray::from(vec!["ab", "abcd"])),
        Arc::new(BinaryArray::from_iter_values([b"abc"])),
        Arc::new(LargeBinaryArray::from_iter_values([b"abcde"])),
        Arc::clone(&views),
        Arc::clone(&binary_views),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        texts().slice(0, 2),
    ];
    for values in [texts(), Arc::clone(&ints), Arc::clone(&views)] {
        columns.extend([
            dictionary::<Int8Type>(Arc::clone(&values), 3),
            dictionary::<Int16Type>(Arc::clone(&values), 3),
            dictionary::<Int32Type>(Arc::clone(&values), 3),
            dictionary::<Int64Type>(Arc::clone(&values), 3),
            dictionary::<UInt8Type>(Arc::clone(&values), 3),
            dictionary::<UInt16Type>(Arc::clone(&values), 3),
            dictionary::<UInt32Type>(Arc::clone(&values), 3),
            dictionary::<UInt64Type>(Arc::clone(&values), 3),
            run::<Int16Type>(values.as_ref(), 3),
            run::<Int32Type>(values.as_ref(), 3),
            run::<Int64Type>(values.as_ref(), 3),
        ]);
    }
    columns
}

/// Columns no certification table holds: nested ones, in every list form, encodings of
/// encodings, and binary values of no width, encoded or not.
fn refused() -> Vec<ArrayRef> {
    let list = {
        let mut list = ListBuilder::new(Int64Builder::new());
        list.append_value([Some(1)]);
        Arc::new(list.finish()) as ArrayRef
    };
    let large_list = arrow_cast::cast(&list, &DataType::LargeList(item())).unwrap();
    let fixed_list = {
        let mut list = FixedSizeListBuilder::new(Int64Builder::new(), 1);
        list.values().append_value(1);
        list.append(true);
        Arc::new(list.finish()) as ArrayRef
    };
    let list_view = {
        let mut list = ListViewBuilder::new(Int64Builder::new());
        list.append_value([Some(1)]);
        Arc::new(list.finish()) as ArrayRef
    };
    let large_list_view = {
        let mut list = LargeListViewBuilder::new(Int64Builder::new());
        list.append_value([Some(1)]);
        Arc::new(list.finish()) as ArrayRef
    };
    let structure: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("a", DataType::Int64, true)),
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    )]));
    let map = {
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map.keys().append_value("k");
        map.values().append_value(1);
        map.append(true).unwrap();
        Arc::new(map.finish()) as ArrayRef
    };
    let union = {
        let fields = UnionFields::try_new([0], [Field::new("a", DataType::Int64, true)]).unwrap();
        let children = vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef];
        let ids = ScalarBuffer::from(vec![0_i8]);
        Arc::new(UnionArray::try_new(fields, ids, None, children).unwrap()) as ArrayRef
    };
    let encoded = dictionary::<Int8Type>(texts(), 2);
    vec![
        Arc::clone(&list),
        large_list,
        fixed_list,
        list_view,
        large_list_view,
        structure,
        map,
        union,
        zero_width(),
        dictionary::<Int32Type>(zero_width(), 2),
        run::<Int32Type>(zero_width().as_ref(), 2),
        dictionary::<Int32Type>(list, 2),
        dictionary::<Int32Type>(Arc::clone(&encoded), 2),
        dictionary::<Int32Type>(run::<Int32Type>(texts().as_ref(), 1), 2),
        run::<Int32Type>(encoded.as_ref(), 2),
        run::<Int32Type>(run::<Int16Type>(texts().as_ref(), 1).as_ref(), 2),
    ]
}

fn item() -> Arc<Field> {
    Arc::new(Field::new_list_field(DataType::Int64, true))
}

#[test]
fn every_kind_of_column_certification_writes_is_admitted_in_every_encoding() {
    for column in admitted() {
        let kind = column.data_type().clone();
        assert!(flat(column.as_ref()), "{kind}");
        let published = Published::admit(vec![batch(column)]);
        assert!(published.is_ok(), "{kind}: {published:?}");
    }
}

#[test]
fn a_nested_column_or_one_of_no_width_is_refused_whatever_encodes_it() {
    for column in refused() {
        let kind = column.data_type().clone();
        assert!(!flat(column.as_ref()), "{kind}");
        assert!(Published::admit(vec![batch(column)]).is_err(), "{kind}");
    }
}

#[test]
fn a_read_back_is_admitted_up_to_its_rows_and_no_further() {
    let rows = |rows: &[usize]| {
        let batches = rows
            .iter()
            .map(|rows| batch(Arc::new(NullArray::new(*rows))));
        Published::admit(batches.collect())
    };
    assert_eq!(rows(&[]).unwrap().rows(), 0);
    assert_eq!(rows(&[PUBLISHED_ROWS]).unwrap().rows(), PUBLISHED_ROWS);
    assert_eq!(
        rows(&[PUBLISHED_ROWS - 1, 1]).unwrap().rows(),
        PUBLISHED_ROWS
    );
    assert!(rows(&[PUBLISHED_ROWS + 1]).is_err());
    assert!(rows(&[PUBLISHED_ROWS, 1]).is_err());
    assert!(rows(&[1, PUBLISHED_ROWS]).is_err());
    assert!(rows(&[usize::MAX / 2, usize::MAX / 2, 2]).is_err());
}

#[test]
fn a_read_back_is_admitted_up_to_what_its_rows_expand_to_whatever_encodes_them() {
    // Each row holds, or shares, a value as long as there are rows at the limit.
    let side = PUBLISHED_BYTES.isqrt();
    assert_eq!(side * side, PUBLISHED_BYTES);
    let long: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(side)]));
    let view = arrow_cast::cast(&long, &DataType::Utf8View).unwrap();
    let encodings: Vec<Box<dyn Fn(usize) -> ArrayRef>> = vec![
        Box::new(|rows| dictionary::<Int8Type>(Arc::clone(&long), rows)),
        Box::new(|rows| dictionary::<Int16Type>(Arc::clone(&long), rows)),
        Box::new(|rows| dictionary::<Int32Type>(Arc::clone(&long), rows)),
        Box::new(|rows| dictionary::<Int64Type>(Arc::clone(&long), rows)),
        Box::new(|rows| dictionary::<UInt8Type>(Arc::clone(&long), rows)),
        Box::new(|rows| dictionary::<UInt16Type>(Arc::clone(&long), rows)),
        Box::new(|rows| dictionary::<UInt32Type>(Arc::clone(&long), rows)),
        Box::new(|rows| dictionary::<UInt64Type>(Arc::clone(&long), rows)),
        Box::new(|rows| run::<Int16Type>(long.as_ref(), rows)),
        Box::new(|rows| run::<Int32Type>(long.as_ref(), rows)),
        Box::new(|rows| run::<Int64Type>(long.as_ref(), rows)),
        Box::new(|rows| dictionary::<Int32Type>(Arc::clone(&view), rows)),
        Box::new(|rows| run::<Int32Type>(view.as_ref(), rows)),
        // Views that share one buffer, as a dictionary's keys share a value.
        Box::new(|rows| {
            let mut views = StringViewBuilder::new();
            let block = views.append_block(vec![b'x'; side].into());
            for _ in 0..rows {
                views
                    .try_append_view(block, 0, u32::try_from(side).unwrap())
                    .unwrap();
            }
            Arc::new(views.finish())
        }),
        Box::new(|rows| {
            Arc::new(StringArray::from(vec![
                long.as_string::<i32>().value(0);
                rows
            ]))
        }),
    ];
    for encoded in encodings {
        // Each row takes its value and, at most, an offset, a key and a run beside it.
        let (within, beyond) = (encoded(side - 64), encoded(side + 1));
        let kind = within.data_type().clone();
        assert!(Published::admit(vec![batch(within)]).is_ok(), "{kind}");
        assert!(Published::admit(vec![batch(beyond)]).is_err(), "{kind}");
    }
}

#[test]
fn a_read_back_is_admitted_up_to_what_all_its_batches_and_columns_expand_to() {
    let side = PUBLISHED_BYTES.isqrt();
    let long: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(side)]));
    let half = || batch(dictionary::<Int32Type>(Arc::clone(&long), side / 2 - 32));
    assert!(Published::admit(vec![half(), half()]).is_ok());
    let more = || batch(dictionary::<Int32Type>(Arc::clone(&long), 66));
    assert!(Published::admit(vec![half(), half(), more()]).is_err());
    let columns = [
        (
            "a",
            dictionary::<Int32Type>(Arc::clone(&long), side / 2),
            true,
        ),
        (
            "b",
            dictionary::<Int32Type>(Arc::clone(&long), side / 2),
            true,
        ),
        (
            "c",
            dictionary::<Int32Type>(Arc::clone(&long), side / 2),
            true,
        ),
    ];
    let wide = RecordBatch::try_from_iter_with_nullable(columns).unwrap();
    assert!(Published::admit(vec![wide]).is_err());
}

use arrow_array::cast::AsArray as _;

#[test]
fn a_cast_holding_a_value_for_each_row_is_whole_and_no_other_is() {
    let cast: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    assert!(whole("id", Arc::clone(&cast), 3).is_ok());
    for rows in [0, 2, 4] {
        assert!(whole("id", Arc::clone(&cast), rows).is_err(), "{rows}");
    }
}

#[test]
fn a_column_read_back_is_cast_to_what_was_written_a_value_for_each_row() {
    for column in admitted() {
        let kind = column.data_type().clone();
        let batch = batch(Arc::clone(&column));
        let read = Read(&batch);
        assert!(read.has("column") && !read.has("other"));
        assert_eq!(read.rows(), column.len());
        assert!(read.optional("other", &DataType::Utf8).unwrap().is_none());
        assert!(read.nullable("other", &DataType::Utf8).is_err());
        assert!(read.required("other", &DataType::Utf8).is_err());
        for to in [
            DataType::Int64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Boolean,
        ] {
            // Whatever a cast answers, it is an array as long as the batch, or a violation.
            if let Ok(Some(cast)) = read.optional("column", &to) {
                assert_eq!(cast.len(), column.len(), "{kind} to {to}");
                assert_eq!(cast.data_type(), &to, "{kind} to {to}");
            }
        }
    }
}

#[test]
fn a_column_with_a_null_is_read_where_nulls_may_be_and_refused_where_none_may() {
    let nulls = [
        Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
        // A null key, and a key of a null value.
        dictionary::<Int8Type>(Arc::new(Int64Array::from(vec![1])), 2),
        Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                vec![0_i8, 1].into(),
                Arc::new(Int64Array::from(vec![Some(1), None])),
            )
            .unwrap(),
        ),
        run::<Int32Type>(&Int64Array::from(vec![None::<i64>]), 2),
        Arc::new(NullArray::new(2)),
    ];
    for column in nulls {
        let kind = column.data_type().clone();
        let batch = batch(column);
        let read = Read(&batch);
        assert!(read.nullable("column", &DataType::Int64).is_ok(), "{kind}");
        assert!(read.required("column", &DataType::Int64).is_err(), "{kind}");
    }
    let whole = batch(Arc::new(Int64Array::from(vec![1, 2])));
    assert!(Read(&whole).required("column", &DataType::Int64).is_ok());
}

#[test]
fn a_temporal_column_is_never_rendered_as_text_whatever_encodes_it() {
    // An instant no calendar holds once its zone's offset is added.
    let edge = 8_210_266_876_799_i64;
    let zoned: ArrayRef =
        Arc::new(TimestampSecondArray::from(vec![edge; 2]).with_timezone("+14:00"));
    let temporal = [
        Arc::clone(&zoned),
        dictionary::<Int8Type>(Arc::clone(&zoned), 2),
        dictionary::<UInt64Type>(Arc::clone(&zoned), 2),
        run::<Int16Type>(zoned.as_ref(), 2),
        run::<Int64Type>(zoned.as_ref(), 2),
        Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX; 2])),
        Arc::new(Date32Array::from(vec![i32::MIN; 2])),
        Arc::new(Time64MicrosecondArray::from(vec![i64::MAX; 2])),
        Arc::new(DurationSecondArray::from(vec![i64::MIN; 2])),
    ];
    for column in temporal {
        let kind = column.data_type().clone();
        let batch = batch(column);
        let read = Read(&batch);
        for text in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            assert!(read.optional("column", &text).is_err(), "{kind} to {text}");
        }
    }
    // Read as the numbers they are, they are read.
    let batch = batch(zoned);
    let numbers = Read(&batch).required("column", &DataType::Int64).unwrap();
    assert_eq!(numbers.as_primitive::<Int64Type>().values(), &[edge; 2]);
}

/// A column of `T` at the ends of what it holds, and nothing.
fn ends<T: arrow_array::ArrowPrimitiveType>(low: T::Native, high: T::Native) -> ArrayRef
where
    T::Native: Default,
{
    Arc::new(PrimitiveArray::<T>::from_iter_values([
        low,
        high,
        T::Native::default(),
    ]))
}

/// Numbers, dates, times and spans, each at the ends of what it holds.
fn numbers() -> Vec<ArrayRef> {
    use arrow_array::types::{
        Date32Type, Date64Type, Decimal128Type, DurationNanosecondType, Float32Type, Float64Type,
        Time32SecondType, Time64NanosecondType,
    };
    vec![
        ends::<Int8Type>(i8::MIN, i8::MAX),
        ends::<Int16Type>(i16::MIN, i16::MAX),
        ends::<Int32Type>(i32::MIN, i32::MAX),
        ends::<Int64Type>(i64::MIN, i64::MAX),
        ends::<UInt8Type>(0, u8::MAX),
        ends::<UInt16Type>(0, u16::MAX),
        ends::<UInt32Type>(0, u32::MAX),
        ends::<UInt64Type>(0, u64::MAX),
        ends::<Float32Type>(f32::MIN, f32::MAX),
        ends::<Float64Type>(f64::NEG_INFINITY, f64::NAN),
        ends::<Decimal128Type>(i128::MIN, i128::MAX),
        ends::<Date32Type>(i32::MIN, i32::MAX),
        ends::<Date64Type>(i64::MIN, i64::MAX),
        ends::<Time32SecondType>(i32::MIN, i32::MAX),
        ends::<Time64NanosecondType>(i64::MIN, i64::MAX),
        ends::<DurationNanosecondType>(i64::MIN, i64::MAX),
    ]
}

/// Instants of every unit, zoned and not, each at the ends of what it holds.
fn instants() -> Vec<ArrayRef> {
    use arrow_array::types::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };
    let zoned = |array: ArrayRef, zone: &str| -> ArrayRef {
        let kind = match array.data_type() {
            DataType::Timestamp(unit, _) => DataType::Timestamp(*unit, Some(zone.into())),
            other => other.clone(),
        };
        let data = array.to_data().into_builder().data_type(kind);
        arrow_array::make_array(data.build().unwrap())
    };
    vec![
        ends::<TimestampSecondType>(i64::MIN, i64::MAX),
        ends::<TimestampMillisecondType>(i64::MIN, i64::MAX),
        ends::<TimestampMicrosecondType>(i64::MIN, i64::MAX),
        ends::<TimestampNanosecondType>(i64::MIN, i64::MAX),
        zoned(
            ends::<TimestampSecondType>(i64::MIN, 8_210_266_876_799),
            "+14:00",
        ),
        zoned(ends::<TimestampMicrosecondType>(i64::MIN, i64::MAX), "UTC"),
        zoned(
            ends::<TimestampNanosecondType>(i64::MIN, i64::MAX),
            "-12:00",
        ),
    ]
}

/// Every kind of scalar column a read-back admits, each at the ends of what it holds.
fn extremes() -> Vec<ArrayRef> {
    let bytes = || -> ArrayRef {
        Arc::new(BinaryArray::from_iter_values([
            &b""[..],
            &[255; 16],
            &[0; 17],
        ]))
    };
    let uuid = FixedSizeBinaryArray::try_from_iter([[0_u8; 16], [255; 16], [7; 16]].into_iter());
    let mut columns = numbers();
    columns.extend(instants());
    columns.extend([
        Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
        Arc::new(NullArray::new(3)),
        Arc::new(StringArray::from(vec!["", "9223372036854775808", "true"])),
        Arc::new(LargeStringArray::from(vec![
            "",
            "-1",
            "1970-01-01T00:00:00Z",
        ])),
        arrow_cast::cast(&texts(), &DataType::Utf8View).unwrap(),
        bytes(),
        arrow_cast::cast(&bytes(), &DataType::LargeBinary).unwrap(),
        arrow_cast::cast(&bytes(), &DataType::BinaryView).unwrap(),
        Arc::new(uuid.unwrap()),
    ]);
    columns
}

/// `column`, plain, and encoded by a dictionary of each key type and by run ends of each type.
fn encoded(column: &ArrayRef) -> Vec<ArrayRef> {
    fn keyed<K: ArrowDictionaryKeyType>(values: &ArrayRef) -> ArrayRef
    where
        K::Native: TryFrom<usize>,
    {
        let keys = (0..values.len()).map(|key| K::Native::try_from(key).ok().unwrap());
        let keys = PrimitiveArray::<K>::from_iter_values(keys);
        Arc::new(DictionaryArray::<K>::try_new(keys, Arc::clone(values)).unwrap())
    }
    fn runs<R: RunEndIndexType>(values: &ArrayRef) -> ArrayRef
    where
        R::Native: TryFrom<usize>,
    {
        let ends = (1..=values.len()).map(|end| R::Native::try_from(end).ok().unwrap());
        let ends = PrimitiveArray::<R>::from_iter_values(ends);
        Arc::new(RunArray::<R>::try_new(&ends, values.as_ref()).unwrap())
    }
    vec![
        Arc::clone(column),
        keyed::<Int8Type>(column),
        keyed::<Int16Type>(column),
        keyed::<Int32Type>(column),
        keyed::<Int64Type>(column),
        keyed::<UInt8Type>(column),
        keyed::<UInt16Type>(column),
        keyed::<UInt32Type>(column),
        keyed::<UInt64Type>(column),
        runs::<Int16Type>(column),
        runs::<Int32Type>(column),
        runs::<Int64Type>(column),
    ]
}

/// Every type a clause reads a column back as.
fn targets() -> [DataType; 6] {
    [
        DataType::Int64,
        DataType::Utf8,
        DataType::Binary,
        DataType::Boolean,
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        DataType::FixedSizeBinary(16),
    ]
}

/// `column`'s values as text, a null as none.
fn shown(column: &ArrayRef) -> Vec<Option<String>> {
    let options = arrow_cast::display::FormatOptions::default();
    let formatter = arrow_cast::display::ArrayFormatter::try_new(column.as_ref(), &options);
    let formatter = formatter.unwrap();
    (0..column.len())
        .map(|row| {
            column
                .is_valid(row)
                .then(|| formatter.value(row).to_string())
        })
        .collect()
}

#[test]
fn every_cast_a_clause_asks_of_a_read_back_is_exact_or_refused_and_never_panics() {
    let mut read = 0;
    for column in extremes() {
        // What each value is, read with no arithmetic: integers and instants as their numbers.
        let numbers = match column.data_type() {
            DataType::Timestamp(..) => {
                let data = column.to_data().into_builder().data_type(DataType::Int64);
                Some(shown(&arrow_array::make_array(data.build().unwrap())))
            }
            kind if kind.is_integer() => Some(shown(&column)),
            _ => None,
        };
        for encoded in encoded(&column) {
            let kind = encoded.data_type().clone();
            let batch = batch(encoded);
            for to in targets() {
                // A cast that computes on what a destination chose would panic here, or wrap.
                let Ok(Some(cast)) = Read(&batch).optional("column", &to) else {
                    continue;
                };
                read += 1;
                assert_eq!(cast.len(), column.len(), "{kind} to {to}");
                let (DataType::Int64, Some(numbers)) = (&to, &numbers) else {
                    continue;
                };
                // A number read back is the number held, or none where Int64 holds none such.
                for (row, number) in shown(&cast).into_iter().enumerate() {
                    assert!(
                        number.is_none() || number == numbers[row],
                        "{kind} to {to}: {number:?}, not {:?}",
                        numbers[row]
                    );
                }
            }
        }
    }
    assert!(read > 300, "only {read} casts were read");
}

#[test]
fn a_column_is_read_only_as_what_a_column_of_its_kind_is_written_as() {
    let micros = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
    let read = |column: ArrayRef, to: &DataType| {
        let batch = batch(column);
        Read(&batch).optional("column", to).is_ok()
    };
    let column = |index: usize| Arc::clone(&extremes()[index]);
    let kinds: Vec<DataType> = extremes()
        .iter()
        .map(|column| column.data_type().clone())
        .collect();
    for (index, kind) in kinds.iter().enumerate() {
        let integer = kind.is_integer();
        let instant = matches!(kind, DataType::Timestamp(..));
        let nothing = *kind == DataType::Null;
        let text = matches!(
            kind,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        let bytes = matches!(
            kind,
            DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
        );
        let flag = *kind == DataType::Boolean;
        for encoded in encoded(&column(index)) {
            let read = |to: &DataType| read(Arc::clone(&encoded), to);
            assert_eq!(
                read(&DataType::Int64),
                integer || instant || nothing,
                "{kind}"
            );
            assert_eq!(read(&DataType::Utf8), text || nothing, "{kind}");
            assert_eq!(read(&DataType::Boolean), flag || nothing, "{kind}");
            let whole = *kind == DataType::Int64;
            assert_eq!(read(&micros), instant || whole || nothing, "{kind}");
            assert_eq!(read(&DataType::Binary), bytes || nothing, "{kind}");
            // Bytes of another length than a column holds are refused by the cast itself.
            assert!(
                !read(&DataType::FixedSizeBinary(16)) || bytes || nothing,
                "{kind}"
            );
        }
    }
}

#[test]
fn the_integers_of_a_read_back_are_read_through_its_admission_whatever_their_columns_case() {
    use super::read_back_integers;
    let ids = |column: ArrayRef| RecordBatch::try_from_iter_with_nullable([("ID", column, true)]);
    let whole = ids(Arc::new(Int32Array::from(vec![3, 1]))).unwrap();
    let encoded = ids(dictionary::<Int8Type>(
        Arc::new(Int64Array::from(vec![7, 8])),
        2,
    ));
    let read = read_back_integers(vec![whole.clone(), whole], "id").unwrap();
    assert_eq!(read, [3, 1, 3, 1]);
    assert_eq!(
        read_back_integers(Vec::new(), "id").unwrap(),
        Vec::<i64>::new()
    );
    // A null, another column, another kind, a column no clause admits, and too many rows.
    assert!(read_back_integers(vec![encoded.unwrap()], "id").is_err());
    assert!(read_back_integers(vec![batch(Arc::new(Int64Array::from(vec![1])))], "id").is_err());
    let refused = refused().into_iter().chain([texts(), zero_width()]);
    for column in refused {
        let kind = column.data_type().clone();
        assert!(
            read_back_integers(vec![ids(column).unwrap()], "id").is_err(),
            "{kind}"
        );
    }
    let many = ids(Arc::new(Int64Array::from(vec![0; PUBLISHED_ROWS + 1]))).unwrap();
    assert!(read_back_integers(vec![many], "id").is_err());
}

#[test]
fn a_read_back_expanding_to_its_limit_to_the_byte_is_admitted_and_one_byte_more_is_not() {
    use crate::cost::Rendering;
    // One string, beside its two offsets.
    let text = |length: usize| batch(Arc::new(StringArray::from(vec!["x".repeat(length)])));
    let limit = u64::try_from(PUBLISHED_BYTES).unwrap();
    let full = text(PUBLISHED_BYTES - 16);
    assert_eq!(Rendering::native().expanded(&full, 0..1, u64::MAX), limit);
    assert!(Published::admit(vec![full]).is_ok());
    assert!(Published::admit(vec![text(PUBLISHED_BYTES - 15)]).is_err());
}
