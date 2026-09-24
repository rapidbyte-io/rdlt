use serde::de::Expected;

use super::{Context, Field, Render, Row, Skip, Value};
use crate::shred::build::{Column, Record};

fn expected(visitor: &dyn Expected) -> String {
    visitor.to_string()
}

#[test]
fn every_visitor_says_what_it_expects() {
    let context = Context::default();
    let mut record = Record::empty(0);
    let mut column = Column::Null(0);
    assert_eq!(
        expected(&Field {
            record: &mut record,
            hint: 0,
        }),
        "an object key"
    );
    assert_eq!(
        expected(&Row {
            record: &mut record,
            context: &context,
        }),
        "a record"
    );
    assert_eq!(
        expected(&Value {
            column: &mut column,
            context: &context,
            depth: 1,
            capacity: 0,
        }),
        "a JSON value"
    );
    assert_eq!(
        expected(&Skip {
            context: &context,
            depth: 1,
        }),
        "a JSON value"
    );
    let mut text = String::new();
    assert_eq!(
        expected(&Render {
            text: &mut text,
            context: &context,
        }),
        "a JSON value"
    );
}
