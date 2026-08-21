//! Fuzz: arrow type mapping (feature 003 R22 target 4, clause E7).
//!
//! Decodes fuzz bytes into a bounded arbitrary `DataType` tree (including the
//! nested/dictionary/large variants pyarrow emits) and maps it: typed error or
//! success, never a panic.

#![no_main]

use arrow::datatypes::{DataType, Field, TimeUnit};
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

/// Bounded byte-driven DataType constructor: consumes one byte per node.
fn decode(bytes: &mut &[u8], depth: u8) -> DataType {
    let Some((&b, rest)) = bytes.split_first() else {
        return DataType::Null;
    };
    *bytes = rest;
    if depth == 0 {
        return leaf(b);
    }
    match b % 24 {
        0..=15 => leaf(b),
        16 => DataType::List(Arc::new(Field::new("item", decode(bytes, depth - 1), true))),
        17 => DataType::LargeList(Arc::new(Field::new("item", decode(bytes, depth - 1), true))),
        18 | 19 => {
            let n = (b as usize % 3) + 1;
            let fields = (0..n)
                .map(|i| Field::new(format!("f{i}"), decode(bytes, depth - 1), true))
                .collect::<Vec<_>>();
            DataType::Struct(fields.into())
        }
        20 => DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(decode(bytes, depth - 1)),
        ),
        21 => DataType::Map(
            Arc::new(Field::new("entries", decode(bytes, depth - 1), true)),
            false,
        ),
        22 => DataType::FixedSizeList(
            Arc::new(Field::new("item", decode(bytes, depth - 1), true)),
            (b as i32 % 4) + 1,
        ),
        _ => DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", decode(bytes, depth - 1), true)),
        ),
    }
}

fn leaf(b: u8) -> DataType {
    match b % 16 {
        0 => DataType::Boolean,
        1 => DataType::Int8,
        2 => DataType::Int64,
        3 => DataType::UInt64,
        4 => DataType::Float16,
        5 => DataType::Float64,
        6 => DataType::Utf8,
        7 => DataType::LargeUtf8,
        8 => DataType::Binary,
        9 => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        10 => DataType::Timestamp(TimeUnit::Second, None),
        11 => DataType::Date32,
        12 => DataType::Time64(TimeUnit::Nanosecond),
        13 => DataType::Decimal128((b as u8 % 38) + 1, (b % 10) as i8),
        14 => DataType::Decimal128(5, -2), // negative scale: must be a typed error
        _ => DataType::Null,
    }
}

fuzz_target!(|data: &[u8]| {
    let mut bytes = data;
    let dt = decode(&mut bytes, 4);
    rdlt_engine::fuzzing::map_arrow_type(&dt);
});
