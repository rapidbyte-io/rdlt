use std::sync::Arc;

use arrow_array::{
    ArrayRef, Date32Array, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
    Int64Array, StringArray, TimestampMillisecondArray, UInt64Array,
};
use arrow_buffer::i256;

use super::denoted;

#[test]
fn a_value_denotes_its_exact_number_instant_or_text() {
    let decimal = |value: i128, scale: i8| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(vec![value])
                .with_precision_and_scale(10, scale)
                .unwrap(),
        )
    };
    let cases: Vec<(ArrayRef, &str)> = vec![
        (Arc::new(Int8Array::from(vec![-5])), "-5"),
        (
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            "18446744073709551615",
        ),
        (decimal(1_500, 3), "1.5"),
        (decimal(-7, 3), "-0.007"),
        (decimal(42, 0), "42"),
        (decimal(42, -2), "4200"),
        (decimal(-42, -2), "-4200"),
        (
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(-12)])
                    .with_precision_and_scale(40, 1)
                    .unwrap(),
            ),
            "-1.2",
        ),
        (Arc::new(Float32Array::from(vec![0.5])), "0.5"),
        (
            Arc::new(Float32Array::from(vec![0.1])),
            "0.10000000149011612",
        ),
        (Arc::new(Float32Array::from(vec![-0.0])), "-0"),
        (Arc::new(Float64Array::from(vec![-0.0])), "-0"),
        (Arc::new(Float64Array::from(vec![3.0])), "3"),
        (
            Arc::new(TimestampMillisecondArray::from(vec![1])),
            "1000000 ns",
        ),
        (Arc::new(Date32Array::from(vec![1])), "86400000000000 ns"),
        (Arc::new(StringArray::from(vec!["x"])), "x"),
        (Arc::new(Int64Array::from(vec![None])), "null"),
    ];
    for (array, expected) in cases {
        assert_eq!(
            denoted(array.as_ref(), 0),
            expected,
            "{:?}",
            array.data_type()
        );
    }
}
