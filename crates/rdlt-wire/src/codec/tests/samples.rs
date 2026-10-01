//! Sample columns of every Arrow type and encoding, for checks that must hold for each.

use std::sync::Arc;

use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder};
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Int8Array, Int32Array, LargeListArray, LargeListViewArray,
    ListArray, ListViewArray, MapArray, NullArray, RecordBatch, RecordBatchOptions, RunArray,
    StringArray, StructArray, UnionArray, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields};

/// Rows in each sample column.
pub(crate) const ROWS: usize = 5;

const UNITS: [TimeUnit; 4] = [
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

/// Every type whose column is a validity buffer and one buffer of fixed-width values.
pub(crate) fn fixed_width() -> Vec<DataType> {
    let mut types = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, 2),
        DataType::Decimal256(76, 2),
    ];
    for unit in UNITS {
        types.push(DataType::Timestamp(unit, None));
        types.push(DataType::Timestamp(unit, Some("Europe/Berlin".into())));
        types.push(DataType::Duration(unit));
    }
    types
}

/// Every integer type a dictionary's keys may be of.
pub(crate) const KEYS: [DataType; 8] = [
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
];

fn item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, true))
}

/// Some rows null, in the pattern every sample with a validity buffer shares.
fn nulls() -> NullBuffer {
    NullBuffer::from(vec![true, false, true, true, false])
}

/// Text long enough that a view of it names a data buffer, in two data buffers.
pub(crate) fn text_views() -> ArrayRef {
    let mut builder = StringViewBuilder::new().with_fixed_block_size(32);
    builder.append_value("a string long enough to leave its view");
    builder.append_null();
    builder.append_value("short");
    builder.append_value("another string that a second block holds");
    builder.append_value("");
    Arc::new(builder.finish())
}

fn binary_views() -> ArrayRef {
    let mut builder = BinaryViewBuilder::new();
    for row in 0..ROWS {
        builder.append_option((row != 1).then_some([7_u8; 20]));
    }
    Arc::new(builder.finish())
}

fn ints() -> ArrayRef {
    Arc::new(Int32Array::from(vec![
        Some(1),
        None,
        Some(3),
        Some(4),
        None,
    ]))
}

fn lists() -> Vec<ArrayRef> {
    let lengths = [2, 0, 1, 0, 2];
    let (offsets, sizes) = (vec![0, 2, 2, 3, 3], vec![2, 0, 1, 0, 2]);
    let wide = |values: &[i32]| -> ScalarBuffer<i64> {
        values.iter().map(|value| i64::from(*value)).collect()
    };
    vec![
        Arc::new(ListArray::new(
            item(DataType::Int32),
            OffsetBuffer::from_lengths(lengths),
            ints(),
            Some(nulls()),
        )),
        Arc::new(LargeListArray::new(
            item(DataType::Utf8View),
            OffsetBuffer::from_lengths(lengths),
            text_views(),
            Some(nulls()),
        )),
        Arc::new(ListViewArray::new(
            item(DataType::Int32),
            offsets.clone().into(),
            sizes.clone().into(),
            ints(),
            Some(nulls()),
        )),
        Arc::new(LargeListViewArray::new(
            item(DataType::Int32),
            wide(&offsets),
            wide(&sizes),
            ints(),
            Some(nulls()),
        )),
        Arc::new(FixedSizeListArray::new(
            item(DataType::Null),
            3,
            Arc::new(NullArray::new(3 * ROWS)),
            Some(nulls()),
        )),
    ]
}

fn pairs() -> StructArray {
    let fields = Fields::from(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int32, true),
    ]);
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
    StructArray::new(fields, vec![keys, ints()], None)
}

fn unions() -> Vec<ArrayRef> {
    let fields = UnionFields::try_new(
        vec![0, 1],
        vec![
            Field::new("n", DataType::Int32, true),
            Field::new("s", DataType::Utf8View, true),
        ],
    )
    .unwrap();
    let ids: ScalarBuffer<i8> = vec![0, 1, 0, 1, 0].into();
    let sparse = UnionArray::try_new(
        fields.clone(),
        ids.clone(),
        None,
        vec![ints(), text_views()],
    );
    let offsets: ScalarBuffer<i32> = vec![0, 0, 1, 1, 2].into();
    let dense = UnionArray::try_new(fields, ids, Some(offsets), vec![ints(), text_views()]);
    vec![Arc::new(sparse.unwrap()), Arc::new(dense.unwrap())]
}

fn runs() -> Vec<ArrayRef> {
    let values = StringArray::from(vec![Some("x"), None, Some("z")]);
    vec![
        Arc::new(RunArray::<Int16Type>::try_new(&vec![2_i16, 3, 5].into(), &values).unwrap()),
        Arc::new(RunArray::<Int32Type>::try_new(&vec![2, 3, 5].into(), &values).unwrap()),
        Arc::new(RunArray::<Int64Type>::try_new(&vec![2_i64, 3, 5].into(), &values).unwrap()),
    ]
}

/// A column of every type and encoding, [`ROWS`] rows each.
pub(crate) fn columns() -> Vec<ArrayRef> {
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(NullArray::new(ROWS)),
        new_null_array(&DataType::Boolean, ROWS),
        new_null_array(&DataType::FixedSizeBinary(3), ROWS),
        new_null_array(&DataType::FixedSizeBinary(0), ROWS),
        Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("ccc"),
            Some(""),
            None,
        ])),
        new_null_array(&DataType::LargeUtf8, ROWS),
        new_null_array(&DataType::Binary, ROWS),
        new_null_array(&DataType::LargeBinary, ROWS),
        text_views(),
        binary_views(),
        Arc::new(pairs()),
        Arc::new(MapArray::new(
            Arc::new(Field::new("entries", pairs().data_type().clone(), false)),
            OffsetBuffer::from_lengths([1, 1, 1, 1, 1]),
            pairs(),
            Some(nulls()),
            false,
        )),
    ];
    columns.extend(
        fixed_width()
            .iter()
            .map(|data_type| new_null_array(data_type, ROWS)),
    );
    columns.extend(lists());
    columns.extend(unions());
    columns.extend(runs());
    for key in KEYS {
        let keyed = DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8));
        columns.push(new_null_array(&keyed, ROWS));
    }
    let keys = Int8Array::from(vec![Some(0), None, Some(1), Some(0), None]);
    let tagged = arrow_array::DictionaryArray::try_new(keys, lists().remove(0)).unwrap();
    columns.push(Arc::new(tagged));
    columns
}

/// A batch of one column, `column`, named `c`.
pub(crate) fn batch_of(column: ArrayRef) -> RecordBatch {
    let field = Field::new("c", column.data_type().clone(), true);
    let options = RecordBatchOptions::new().with_row_count(Some(column.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(vec![field])), vec![column], &options)
        .unwrap()
}

/// A batch of every sample column.
pub(crate) fn batch() -> RecordBatch {
    let columns = columns();
    let fields: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(at, column)| Field::new(format!("c{at}"), column.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// Columns holding a run-end column of no values, as slicing and empty lists leave one.
pub(crate) fn without_runs() -> Vec<ArrayRef> {
    let values = StringArray::from(vec![Some("x"), None, Some("z")]);
    let runs = RunArray::<Int32Type>::try_new(&vec![2, 3, 5].into(), &values).unwrap();
    let runs: ArrayRef = Arc::new(runs);
    let item = Arc::new(Field::new("item", runs.data_type().clone(), true));
    let lists = |offsets: Vec<i32>| -> ArrayRef {
        let offsets = OffsetBuffer::new(offsets.into());
        Arc::new(ListArray::new(
            Arc::clone(&item),
            offsets,
            Arc::clone(&runs),
            None,
        ))
    };
    let field = Field::new("r", runs.data_type().clone(), true);
    let parent: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(field),
        Arc::clone(&runs),
    )]));
    vec![
        runs.slice(0, 0),
        runs.slice(5, 0),
        lists(vec![0, 0, 0]),
        lists(vec![5, 5, 5]),
        lists(vec![0, 2, 5]).slice(1, 0),
        parent.slice(2, 0),
    ]
}
