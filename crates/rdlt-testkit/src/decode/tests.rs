use std::sync::Arc;

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, DictionaryArray, Int8Array, StringArray};
use rdlt_connector::LogicalType;

use super::{cell, json_as};
use crate::canon::Canon;

#[test]
fn a_dictionary_cell_reads_as_its_value() {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let keys = Int8Array::from(vec![Some(1), None, Some(0)]);
    let array = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();
    let text = LogicalType::Utf8;
    let read = |row| cell(&array, row, &text, &text, &text);
    assert_eq!(read(0), Canon::Text("b".to_owned()));
    assert_eq!(read(1), Canon::Null);
    assert_eq!(read(2), Canon::Text("a".to_owned()));
}

#[test]
fn pushed_json_reads_as_its_columns_type() {
    // One double lies halfway between these decimals, which name it alike.
    let pushed = "1053724231278760.2";
    let as_float = json_as(pushed, &LogicalType::Float64);
    assert_eq!(
        as_float,
        json_as("1053724231278760.3", &LogicalType::Float64)
    );
    assert_eq!(
        json_as(pushed, &LogicalType::Json),
        Canon::Number(pushed.to_owned()),
        "in a JSON column a number keeps its decimal"
    );
    assert_eq!(json_as("null", &LogicalType::Json), Canon::Null);
}
