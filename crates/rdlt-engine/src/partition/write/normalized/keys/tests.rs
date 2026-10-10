use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema};

use super::taken_apart;
use crate::normalize::Shape;

fn object() -> DataType {
    DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, true)]))
}

fn shape(max_depth: u8, whole: &[&str]) -> Shape {
    Shape {
        max_depth,
        whole: whole.iter().map(|name| Arc::from(*name)).collect(),
        key: vec![Arc::from("k")],
    }
}

#[test]
fn a_key_normalizing_takes_apart_is_found_and_one_it_keeps_whole_is_not() {
    let list = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let runs = DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int32, false)),
        Arc::new(Field::new("values", object(), true)),
    );
    let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(list.clone()));
    let cases = [
        (object(), shape(1, &[]), true),
        (list, shape(1, &[]), true),
        (DataType::Int64, shape(1, &[]), false),
        (object(), shape(0, &[]), false),
        (object(), shape(1, &["k"]), false),
        // Normalizing keeps an object or array held in runs or a dictionary whole, as a column.
        (runs, shape(1, &[]), false),
        (dictionary, shape(1, &[]), false),
    ];
    for (data_type, shape, taken) in cases {
        let schema = Schema::new(vec![Field::new("k", data_type.clone(), true)]);
        assert_eq!(taken_apart(&schema, &shape).is_some(), taken, "{data_type}");
    }
}
