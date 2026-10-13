//! A destination's merge holds a value of a column that widened to `Json` as the JSON text the
//! engine writes of a value of its type into a `Json` column, byte for byte: a value it stored as
//! its type, and one the engine rendered as text, its field naming its type, where the
//! destination did not store the type.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};
use proptest::prelude::*;
use rdlt_connector::{Field, LogicalType};
use rdlt_testkit::drawn::{self, Scalar, Shape};

use super::super::convert::{json, normalize, text};

/// A shape nested up to two levels, and up to six values of it.
fn column() -> impl Strategy<Value = (Shape, Vec<Scalar>)> {
    drawn::values::shape(2).prop_flat_map(|shape| {
        let values = proptest::collection::vec(drawn::values::value(&shape, true), 0..6);
        (Just(shape), values)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn a_stored_value_widened_to_json_is_the_text_the_engine_writes_of_it(
        (shape, values) in column(),
    ) {
        // A column of no type holds nulls alone, which the engine writes as nulls, never as text.
        if shape.logical == LogicalType::Null {
            return Ok(());
        }
        let values: Vec<&Scalar> = values.iter().collect();
        let sent = drawn::arrays::array(&shape, &values);
        // A destination holds a column in the plain type the engine lowered it to.
        let Ok(stored) = normalize(&sent, &shape.logical) else {
            return Ok(());
        };
        let Ok(written) = json(&stored, &shape.logical) else {
            return Ok(());
        };
        let field = arrow_schema::Field::new("v", stored.data_type().clone(), true);
        let field = Field::carrying_extension(field, &shape.logical);
        let merged = widened(field, Arc::clone(&stored));
        prop_assert_eq!(texts(merged.as_ref()), texts(Some(&written)), "{:?}", shape.logical);
        if shape.logical != LogicalType::Utf8 {
            let lowered = text(&stored, &shape.logical).expect("every value has text");
            let named = serde_json::to_string(&shape.logical).expect("a type serializes");
            let field = arrow_schema::Field::new("v", DataType::Utf8, true)
                .with_metadata([(rdlt_connector::LOGICAL_TYPE_KEY.to_owned(), named)].into());
            let merged = widened(field, lowered);
            prop_assert_eq!(texts(merged.as_ref()), texts(Some(&written)), "{:?} as text", shape.logical);
        }
    }
}

/// The column `values` of `field` as a merge reads it back once it widened to `Json`.
fn widened(field: arrow_schema::Field, values: ArrayRef) -> Option<ArrayRef> {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![values])
        .expect("a batch of the column");
    let widened = Schema::new(vec![Field::new("v", LogicalType::Json, true).to_arrow()]);
    let merged = rdlt_connector::merge::read_back(&Arc::new(widened), &[batch])
        .expect("every stored value has JSON text");
    merged.first().map(|batch| Arc::clone(batch.column(0)))
}

/// Each value of `column`, text, as it is; none for a null.
fn texts(column: Option<&ArrayRef>) -> Vec<Option<String>> {
    let Some(column) = column else {
        return Vec::new();
    };
    let column = column.as_string::<i32>();
    (0..column.len())
        .map(|row| column.is_valid(row).then(|| column.value(row).to_owned()))
        .collect()
}
