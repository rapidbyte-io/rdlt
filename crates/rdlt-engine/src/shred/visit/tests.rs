use serde::de::Expected;

use super::{Context, Field, Render, Row, Skip, Value};
use crate::shred::ShredError;
use crate::shred::build::{Column, Record};
use crate::shred::meter::{Columns, Meter};
use crate::shred::render::Rendering;

fn expected(visitor: &dyn Expected) -> String {
    visitor.to_string()
}

#[test]
fn every_visitor_says_what_it_expects() {
    let context = Context::new(Meter::new(0), Columns::new(0));
    let mut record = Record::empty(0);
    let mut column = Column::Null(0);
    assert_eq!(
        expected(&Field {
            record: &mut record,
            hint: 0,
            context: &context,
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
    let mut rendering = Rendering::default();
    assert_eq!(
        expected(&Render {
            rendering: &mut rendering,
            context: &context,
        }),
        "a JSON value"
    );
}

#[test]
fn a_noted_number_is_handed_out_once() {
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    context.note_number("1.25");
    let mut seen = Vec::new();
    context.noted_number(|text| seen.push(text.map(str::to_owned)));
    context.noted_number(|text| seen.push(text.map(str::to_owned)));
    assert_eq!(seen, [Some("1.25".to_owned()), None]);
}

#[test]
fn a_float_renders_only_the_note_made_for_it() {
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let render = |text: &str| {
        let mut rendering = Rendering::default();
        let mut deserializer = sonic_rs::Deserializer::from_str(text);
        let render = Render {
            rendering: &mut rendering,
            context: &context,
        };
        serde::de::DeserializeSeed::deserialize(render, &mut deserializer)
            .map(|()| rendering.text)
            .map_err(|_| context.fault())
    };
    context.note_number("1.50");
    assert_eq!(render("1.5"), Ok("1.50".to_owned()));
    assert!(matches!(render("2.5"), Err(Some(ShredError::Internal(_)))));
}
