//! Row ids held to the ids recorded before values were read where they lie: a row's id decides
//! which row a merge replaces, so it must not change between versions of the engine.

use std::sync::Arc;

use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Decimal256Array, DictionaryArray, Int16Array, Int32Array,
    Int64Array, ListArray, RecordBatch, RunArray, StructArray,
};
use arrow_buffer::{OffsetBuffer, i256};
use arrow_schema::{DataType, Field, Fields, TimeUnit as U};

use super::super::root_ids;
use super::vectors::RECORDED;

/// Whole numbers at the edges of every width, a null among them.
fn integers() -> ArrayRef {
    Arc::new(Int64Array::from(vec![
        Some(i64::MIN),
        Some(-1),
        None,
        Some(0),
        Some(1),
        Some(127),
        Some(-128),
        Some(32_767),
        Some(i64::from(i32::MAX)),
        Some(i64::from(i32::MIN)),
        Some(i64::MAX),
        Some(255),
        Some(65_535),
        Some(i64::from(u32::MAX)),
        Some(86_399),
        Some(-86_400),
    ]))
}

/// Every type the integers are cast to, the values a type cannot hold becoming nulls.
fn types() -> Vec<DataType> {
    let zoned = |unit, zone: &str| DataType::Timestamp(unit, Some(zone.into()));
    vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(U::Second),
        DataType::Time32(U::Millisecond),
        DataType::Time64(U::Microsecond),
        DataType::Time64(U::Nanosecond),
        DataType::Timestamp(U::Second, None),
        DataType::Timestamp(U::Nanosecond, None),
        zoned(U::Millisecond, "UTC"),
        zoned(U::Microsecond, "+02:00"),
        DataType::Duration(U::Second),
        DataType::Duration(U::Millisecond),
        DataType::Duration(U::Microsecond),
        DataType::Duration(U::Nanosecond),
        DataType::Decimal32(9, 0),
        DataType::Decimal32(9, 4),
        DataType::Decimal64(18, 0),
        DataType::Decimal64(18, 7),
        DataType::Decimal128(38, 0),
        DataType::Decimal128(38, 10),
        DataType::Decimal128(38, -3),
        DataType::Decimal256(76, 0),
        DataType::Decimal256(76, 30),
        DataType::Decimal256(76, -5),
    ]
}

/// The integers as `data_type`, through its storage where Arrow casts no integer to it.
fn column(data_type: &DataType) -> ArrayRef {
    let safe = arrow_cast::CastOptions {
        safe: true,
        ..Default::default()
    };
    let cast = |array: &ArrayRef, to: &DataType| {
        arrow_cast::cast_with_options(array, to, &safe).expect("a cast Arrow has")
    };
    match data_type {
        DataType::Date32 | DataType::Time32(_) => {
            cast(&cast(&integers(), &DataType::Int32), data_type)
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            cast(&cast(&integers(), &DataType::Utf8), data_type)
        }
        _ => cast(&integers(), data_type),
    }
}

/// 256-bit decimals beyond 128 bits, at `scale`.
fn wide(scale: i8) -> ArrayRef {
    let thousand = i256::from_i128(1_000);
    let values = Decimal256Array::from(vec![
        Some(i256::MAX.wrapping_div(thousand)),
        None,
        Some(i256::MIN.wrapping_div(thousand)),
        Some(i256::from_i128(i128::MIN)),
        Some(i256::from_i128(-1_200)),
    ]);
    Arc::new(
        values
            .with_precision_and_scale(76, scale)
            .expect("a decimal"),
    )
}

/// `column` behind keys of every key type, in reverse, every third key null.
fn keyed(column: &ArrayRef) -> Vec<ArrayRef> {
    let rows = column.len();
    macro_rules! keyed {
        ($($key:ty),*) => {
            vec![$(Arc::new(
                DictionaryArray::<$key>::try_new(
                    (0..rows)
                        .map(|row| (row % 3 != 2).then(|| (rows - 1 - row).try_into().expect("a few rows")))
                        .collect(),
                    Arc::clone(column),
                )
                .expect("keys within the values"),
            ) as ArrayRef),*]
        };
    }
    keyed!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type
    )
}

/// `column` as runs of two rows each, with run ends of every type.
fn runs(column: &ArrayRef) -> Vec<ArrayRef> {
    let ends = || (1..=column.len()).map(|run| 2 * run);
    let short =
        Int16Array::from_iter_values(ends().map(|end| end.try_into().expect("a short end")));
    let int = Int32Array::from_iter_values(ends().map(|end| end.try_into().expect("an end")));
    let long = Int64Array::from_iter_values(ends().map(|end| end.try_into().expect("an end")));
    vec![
        Arc::new(RunArray::<Int16Type>::try_new(&short, column).expect("runs")),
        Arc::new(RunArray::<Int32Type>::try_new(&int, column).expect("runs")),
        Arc::new(RunArray::<Int64Type>::try_new(&long, column).expect("runs")),
    ]
}

/// `column` as the items of three lists and as a field of structs.
fn nested(column: &ArrayRef) -> Vec<ArrayRef> {
    let item = Arc::new(Field::new("item", column.data_type().clone(), true));
    let lengths = [3, 0, column.len() - 3];
    let lists = ListArray::new(
        item,
        OffsetBuffer::from_lengths(lengths),
        Arc::clone(column),
        None,
    );
    let fields = Fields::from(vec![Field::new("field", column.data_type().clone(), true)]);
    let structs = StructArray::new(fields, vec![Arc::clone(column)], None);
    vec![Arc::new(lists), Arc::new(structs)]
}

/// Every shape `column` is given an id in, by what the shape is called: whole and sliced,
/// behind keys, as runs and nested.
fn shapes(column: &ArrayRef) -> Vec<(&'static str, Vec<ArrayRef>)> {
    let sliced = |arrays: &[ArrayRef]| -> Vec<ArrayRef> {
        let slices = arrays.iter().map(|array| array.slice(1, array.len() - 2));
        arrays.iter().cloned().chain(slices).collect()
    };
    vec![
        ("plain", sliced(std::slice::from_ref(column))),
        ("keyed", sliced(&keyed(column))),
        ("runs", sliced(&runs(column))),
        ("nested", sliced(&nested(column))),
    ]
}

/// A digest of the ids of the rows of each of `columns`, each a batch of one column; nothing
/// where a column has no ids.
pub(super) fn digest(columns: &[ArrayRef]) -> Option<u64> {
    // FNV-1a over each id's length and bytes, a null id as a length no id has.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut fold = |bytes: &[u8]| {
        for byte in bytes {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for column in columns {
        let batch = RecordBatch::try_from_iter([("c", Arc::clone(column))]).expect("a batch");
        let ids: BinaryArray = root_ids(&batch, &[]).ok()?;
        for row in 0..ids.len() {
            if ids.is_null(row) {
                fold(&u64::MAX.to_le_bytes());
            } else {
                let id = ids.value(row);
                fold(&u64::try_from(id.len()).expect("a length").to_le_bytes());
                fold(id);
            }
        }
    }
    Some(hash)
}

/// Every column an id is recorded for, by its name, with its shapes.
pub(super) fn recorded() -> Vec<(String, ArrayRef)> {
    let mut columns: Vec<(String, ArrayRef)> = types()
        .iter()
        .map(|data_type| (data_type.to_string(), column(data_type)))
        .collect();
    for scale in [0_i8, 5, 76, -4] {
        columns.push((format!("wide Decimal256(76, {scale})"), wide(scale)));
    }
    columns
}

#[test]
fn every_type_and_encoding_has_the_id_it_was_recorded_with() {
    let mut recorded = RECORDED.iter();
    for (name, column) in self::recorded() {
        for (shape, columns) in shapes(&column) {
            let (label, digest) = recorded.next().expect("an id was recorded for each shape");
            // The labels are the types' names as they were when recorded; the order decides.
            assert!(label.ends_with(shape), "{label} is not {name} {shape}");
            assert_eq!(self::digest(&columns), Some(*digest), "{name} {shape}");
        }
    }
    assert!(recorded.next().is_none(), "every recorded id is compared");
}
