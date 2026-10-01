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

use super::{Published, Read, whole, widest};
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

/// Every kind of column certification writes, in every encoding, and its widest value's bytes.
fn admitted() -> Vec<(ArrayRef, usize)> {
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
    let mut columns: Vec<(ArrayRef, usize)> = vec![
        (Arc::clone(&ints), 8),
        (Arc::new(Int32Array::from(vec![1, 2])), 4),
        (Arc::new(Float64Array::from(vec![1.5])), 8),
        (Arc::new(BooleanArray::from(vec![Some(true), None])), 1),
        (Arc::new(NullArray::new(3)), 1),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![1]).with_timezone("UTC")),
            8,
        ),
        (Arc::new(Date32Array::from(vec![1])), 4),
        (Arc::new(Decimal128Array::from(vec![1])), 16),
        (Arc::new(uuid), 16),
        (texts(), 6),
        (Arc::new(LargeStringArray::from(vec!["ab", "abcd"])), 4),
        (Arc::new(BinaryArray::from_iter_values([b"abc"])), 3),
        (Arc::new(LargeBinaryArray::from_iter_values([b"abcde"])), 5),
        (Arc::clone(&views), 40),
        (Arc::clone(&binary_views), 20),
        (Arc::new(StringArray::from(Vec::<&str>::new())), 0),
        // A slice is as wide as the values it holds, not those before it.
        (texts().slice(0, 2), 3),
    ];
    for values in [texts(), Arc::clone(&ints), Arc::clone(&views)] {
        let wide = widest(values.as_ref()).unwrap();
        columns.extend([
            (dictionary::<Int8Type>(Arc::clone(&values), 3), wide),
            (dictionary::<Int16Type>(Arc::clone(&values), 3), wide),
            (dictionary::<Int32Type>(Arc::clone(&values), 3), wide),
            (dictionary::<Int64Type>(Arc::clone(&values), 3), wide),
            (dictionary::<UInt8Type>(Arc::clone(&values), 3), wide),
            (dictionary::<UInt16Type>(Arc::clone(&values), 3), wide),
            (dictionary::<UInt32Type>(Arc::clone(&values), 3), wide),
            (dictionary::<UInt64Type>(Arc::clone(&values), 3), wide),
        ]);
        let first = widest(values.slice(0, 1).as_ref()).unwrap();
        columns.extend([
            (run::<Int16Type>(values.as_ref(), 3), first),
            (run::<Int32Type>(values.as_ref(), 3), first),
            (run::<Int64Type>(values.as_ref(), 3), first),
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
    for (column, wide) in admitted() {
        let kind = column.data_type().clone();
        assert_eq!(widest(column.as_ref()), Some(wide), "{kind}");
        let published = Published::admit(vec![batch(column)]);
        assert!(published.is_ok(), "{kind}: {published:?}");
    }
}

#[test]
fn a_nested_column_or_one_of_no_width_is_refused_whatever_encodes_it() {
    for column in refused() {
        let kind = column.data_type().clone();
        assert_eq!(widest(column.as_ref()), None, "{kind}");
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
        let (within, beyond) = (encoded(side), encoded(side + 1));
        let kind = within.data_type().clone();
        assert!(Published::admit(vec![batch(within)]).is_ok(), "{kind}");
        assert!(Published::admit(vec![batch(beyond)]).is_err(), "{kind}");
    }
    // The limit is of a read-back, all its batches and columns together.
    let half = || batch(dictionary::<Int32Type>(Arc::clone(&long), side / 2));
    assert!(Published::admit(vec![half(), half()]).is_ok());
    let more = batch(Arc::new(BooleanArray::from(vec![true])));
    assert!(Published::admit(vec![half(), half(), more]).is_err());
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
        ("c", Arc::new(NullArray::new(side / 2)), true),
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
    for (column, _) in admitted() {
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
