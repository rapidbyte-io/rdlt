use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, DictionaryArray, Int8Array, StringArray, StructArray};
use arrow_schema::{DataType, Field as ArrowField};
use rdlt_connector::{Field, Fields, LogicalType};

use super::{cell, json_as};
use crate::canon::{Canon, DAY};

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
fn a_native_struct_reads_each_field_by_its_name() {
    let fields = vec![
        ArrowField::new("y", DataType::Utf8, true),
        ArrowField::new("x", DataType::Utf8, true),
    ];
    let children: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["for y"])),
        Arc::new(StringArray::from(vec!["for x"])),
    ];
    let array = StructArray::new(fields.into(), children, None);
    let text = |name| Field::new(name, LogicalType::Utf8, true);
    let logical = LogicalType::Struct(Fields::new(vec![text("x"), text("y")]).unwrap());
    let expected = BTreeMap::from([
        ("x".to_owned(), Canon::Text("for x".to_owned())),
        ("y".to_owned(), Canon::Text("for y".to_owned())),
    ]);
    assert_eq!(
        cell(&array, 0, &logical, &logical, &logical),
        Canon::Object(expected)
    );
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

#[test]
fn a_date_reads_from_the_json_of_the_timestamp_it_widened_to() {
    let midnight = Canon::Instant(18_203 * DAY);
    assert_eq!(json_as("\"2019-11-03\"", &LogicalType::Date), midnight);
    assert_eq!(
        json_as("\"2019-11-03T00:00:00Z\"", &LogicalType::Date),
        midnight
    );
    assert_eq!(
        json_as("\"2019-11-03T01:00:00+01:00\"", &LogicalType::Date),
        midnight
    );
}
