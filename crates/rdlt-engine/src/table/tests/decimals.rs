//! Decimals beyond the precision their type declares: refused by every conversion, never
//! widened, rescaled or rendered as a plausible shorter number.

use std::sync::Arc;

use arrow_array::types::{Int8Type, Int32Type};
use arrow_array::{
    Array, ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
    DictionaryArray, Int8Array, Int32Array, ListArray, RunArray, StructArray,
};
use arrow_buffer::{OffsetBuffer, i256};
use arrow_schema::{DataType, Field as ArrowField, Fields};
use rdlt_connector::{DecimalType, Field, LogicalType};

use super::super::convert::{convert, text};

/// One value beyond precision 5 and one within it, in each decimal width, at scale 2.
fn beyond() -> Vec<ArrayRef> {
    vec![
        Arc::new(
            Decimal32Array::from(vec![1_000_000, 150])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal64Array::from(vec![10_i64.pow(17), 150])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![10_i128.pow(37), 150])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![i256::from_i128(10_i128.pow(37)), i256::from_i128(150)])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ),
    ]
}

/// `values` as they may arrive: plain, a dictionary, run-end encoded, in a struct and in a list.
fn arrivals(values: &ArrayRef) -> Vec<(ArrayRef, LogicalType)> {
    let decimal = LogicalType::Decimal(DecimalType::new(5, 2).unwrap());
    let keys = Int8Array::from(vec![0, 1]);
    let dictionary: ArrayRef =
        Arc::new(DictionaryArray::<Int8Type>::try_new(keys, Arc::clone(values)).unwrap());
    let runs: ArrayRef =
        Arc::new(RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1, 2]), values).unwrap());
    let field = ArrowField::new("d", values.data_type().clone(), true);
    let fields = Fields::from(vec![field.clone()]);
    let nested: ArrayRef =
        Arc::new(StructArray::try_new(fields, vec![Arc::clone(values)], None).unwrap());
    let object = LogicalType::Struct(
        rdlt_connector::Fields::new(vec![Field::new("d", decimal.clone(), true)]).unwrap(),
    );
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(field),
            OffsetBuffer::from_lengths([2]),
            Arc::clone(values),
            None,
        )
        .unwrap(),
    );
    let items = LogicalType::List(Box::new(Field::new("d", decimal.clone(), true)));
    vec![
        (Arc::clone(values), decimal.clone()),
        (dictionary, decimal.clone()),
        (runs, decimal),
        (nested, object),
        (list, items),
    ]
}

#[test]
fn a_decimal_beyond_its_declared_precision_is_refused_by_every_conversion() {
    let wider = |logical: &LogicalType| match logical {
        LogicalType::Decimal(_) => LogicalType::Decimal(DecimalType::new(12, 4).unwrap()),
        other => other.clone(),
    };
    for values in beyond() {
        for (arrived, logical) in arrivals(&values) {
            let kind = arrived.data_type().to_string();
            for to in [logical.clone(), wider(&logical), LogicalType::Json] {
                assert!(convert(&arrived, &logical, &to).is_err(), "{kind} to {to}");
            }
            assert!(text(&arrived, &logical).is_err(), "{kind} as text");
            // The value within it alone converts, from a plain array or a dictionary's keys.
            let rowed = matches!(
                arrived.data_type(),
                DataType::Decimal32(..)
                    | DataType::Decimal64(..)
                    | DataType::Decimal128(..)
                    | DataType::Decimal256(..)
                    | DataType::Dictionary(..)
            );
            if rowed {
                let within = arrived.slice(1, 1);
                assert!(convert(&within, &logical, &logical).is_ok(), "{kind}");
            }
        }
    }
}

/// `values` in the other nestings a decimal may arrive in: a large list's items, a fixed-size
/// list's and a map's values.
fn other_nestings(values: &ArrayRef) -> Vec<ArrayRef> {
    let item = Arc::new(ArrowField::new("item", values.data_type().clone(), true));
    let large: ArrayRef = Arc::new(
        arrow_array::LargeListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::from_lengths([2]),
            Arc::clone(values),
            None,
        )
        .unwrap(),
    );
    let fixed: ArrayRef = Arc::new(
        arrow_array::FixedSizeListArray::try_new(item, 2, Arc::clone(values), None).unwrap(),
    );
    let entries = Fields::from(vec![
        ArrowField::new("key", DataType::Utf8, false),
        ArrowField::new("value", values.data_type().clone(), true),
    ]);
    let keys: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["a", "b"]));
    let entries = StructArray::try_new(entries, vec![keys, Arc::clone(values)], None).unwrap();
    let field = Arc::new(ArrowField::new(
        "entries",
        entries.data_type().clone(),
        false,
    ));
    let map: ArrayRef = Arc::new(
        arrow_array::MapArray::try_new(
            field,
            OffsetBuffer::from_lengths([2]),
            entries,
            None,
            false,
        )
        .unwrap(),
    );
    vec![large, fixed, map]
}

#[test]
fn a_decimal_beyond_its_precision_is_refused_in_every_nesting() {
    for values in beyond() {
        for arrived in other_nestings(&values) {
            let kind = arrived.data_type().to_string();
            let field = ArrowField::new("c", arrived.data_type().clone(), true);
            let field = Field::from_arrow(&field).expect("a logical type");
            let logical = field.logical_type();
            assert!(convert(&arrived, logical, logical).is_err(), "{kind}");
        }
    }
}
