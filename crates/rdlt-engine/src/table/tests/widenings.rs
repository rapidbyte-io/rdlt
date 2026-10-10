//! Every widening the type lattice makes keeps a value as it was: the same instant, time of day,
//! duration or number, the same row id and the same history hash.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int64Array, RecordBatch,
};
use arrow_buffer::i256;
use arrow_schema::DataType;
use rdlt_connector::testing::denoted;
use rdlt_connector::{DecimalType, LogicalType, TimeUnit};

use crate::normalize::identity::{root_ids, version_hashes};
use crate::table::convert::convert;

const UNITS: [TimeUnit; 4] = [
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

/// Zones a timestamp may show its instants in: none, UTC, named ones with and without daylight
/// saving and with offsets of seconds in their history, and fixed ones.
const ZONES: [Option<&str>; 6] = [
    None,
    Some("UTC"),
    Some("America/New_York"),
    Some("Asia/Kolkata"),
    Some("+05:30"),
    Some("-11:00"),
];

/// Every type a column of numbers or of temporal values may take.
fn types() -> Vec<LogicalType> {
    let decimal =
        |precision, scale| LogicalType::Decimal(DecimalType::new(precision, scale).unwrap());
    let mut types = vec![
        LogicalType::Int8,
        LogicalType::Int16,
        LogicalType::Int32,
        LogicalType::Int64,
        LogicalType::Float32,
        LogicalType::Float64,
        decimal(3, 0),
        decimal(5, 2),
        decimal(10, 0),
        decimal(19, 4),
        decimal(38, 10),
        decimal(39, 2),
        decimal(76, 38),
        LogicalType::Date,
    ];
    for unit in UNITS {
        types.extend([LogicalType::Time(unit), LogicalType::Duration(unit)]);
        types.extend(ZONES.map(|zone| LogicalType::Timestamp(unit, zone.map(Arc::from))));
    }
    types
}

/// Values of `logical`, at its edges and between, one a row.
fn values(logical: &LogicalType) -> ArrayRef {
    let data_type = logical.to_arrow();
    match &data_type {
        DataType::Float32 | DataType::Float64 => {
            let floats: ArrayRef = Arc::new(Float64Array::from(vec![
                0.0,
                -0.0,
                0.1,
                -1.5,
                3.0,
                f64::from(f32::MAX),
                f64::from(f32::MIN_POSITIVE),
                16_777_217.0,
            ]));
            arrow_cast::cast(&floats, &data_type).unwrap()
        }
        DataType::Decimal128(precision, scale) => {
            let most = 10_i128.pow(u32::from(*precision)) - 1;
            let values = Decimal128Array::from(vec![0, 1, -1, 1_234, most, -most]);
            Arc::new(values.with_precision_and_scale(*precision, *scale).unwrap())
        }
        DataType::Decimal256(precision, scale) => {
            let most = i256::from_string(&"9".repeat(usize::from(*precision))).unwrap();
            let values = Decimal256Array::from(vec![i256::ZERO, i256::ONE, most, -most]);
            Arc::new(values.with_precision_and_scale(*precision, *scale).unwrap())
        }
        DataType::Date32 | DataType::Time32(_) => {
            let values: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![
                i32::MIN,
                -1,
                0,
                1,
                18_262,
                86_399,
                i32::MAX,
            ]));
            arrow_cast::cast(&values, &data_type).unwrap()
        }
        _ => {
            let values: ArrayRef = Arc::new(Int64Array::from(vec![
                i64::MIN,
                -86_400_001,
                -1,
                0,
                1,
                1_577_854_800,
                86_399_999_999,
                i64::MAX,
            ]));
            arrow_cast::cast(&values, &data_type).unwrap()
        }
    }
}

/// The row id and history hash of each value of `array`, a column `v` keyed by itself.
fn identities(array: &ArrayRef) -> Vec<(Vec<u8>, Vec<u8>)> {
    let batch = RecordBatch::try_from_iter([("v", Arc::clone(array))]).unwrap();
    let ids = root_ids(&batch, &[Arc::from("v")]).unwrap();
    let hashes = version_hashes(&batch).unwrap();
    ids.iter()
        .zip(hashes.iter())
        .map(|(id, hash)| (id.unwrap().to_vec(), hash.unwrap().to_vec()))
        .collect()
}

#[test]
fn every_widening_keeps_each_value_its_id_and_its_history_hash() {
    let types = types();
    let mut widenings = 0;
    for from in &types {
        let values = values(from);
        let identified = identities(&values);
        for other in &types {
            let to = from.join(other);
            if to == *from || to == LogicalType::Json {
                continue;
            }
            widenings += 1;
            for (row, identity) in identified.iter().enumerate() {
                let value = values.slice(row, 1);
                // A value the wider type cannot hold is refused, never moved.
                let Ok(widened) = convert(&value, from, &to) else {
                    continue;
                };
                assert_eq!(widened.data_type(), &to.to_arrow(), "{from:?} to {to:?}");
                let at = format!("{from:?} to {to:?}, row {row}");
                assert_eq!(denoted(&widened, 0), denoted(&value, 0), "{at}");
                assert_eq!(&identities(&widened)[0], identity, "{at}");
            }
        }
    }
    assert!(widenings > 500, "{widenings} widenings");
}

#[test]
fn a_float_widened_keeps_the_sign_of_its_zero() {
    let zero: ArrayRef = Arc::new(arrow_array::Float32Array::from(vec![-0.0_f32]));
    let widened = convert(&zero, &LogicalType::Float32, &LogicalType::Float64).unwrap();
    assert!(
        widened
            .as_primitive::<Float64Type>()
            .value(0)
            .is_sign_negative()
    );
}
