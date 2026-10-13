use serde::de::value::{BytesDeserializer, Error as ValueError};
use serde::de::{DeserializeSeed, Expected, IntoDeserializer};

use super::{Context, Field, RecordSeed, Render, Row, Skip, Value};
use crate::shred::ShredError;
use crate::shred::build::{Column, Record};
use crate::shred::meter::{Columns, Meter};
use crate::shred::observe::Shape;
use crate::shred::observing;
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
        expected(&RecordSeed(Row {
            record: &mut record,
            context: &context,
        })),
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
            depth: 1,
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
fn a_float_renders_only_the_note_made_for_it_and_without_one_is_left_unwritten() {
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let render = |text: &str| {
        let mut rendering = Rendering::default();
        let mut deserializer = sonic_rs::Deserializer::from_str(text);
        let render = Render {
            rendering: &mut rendering,
            context: &context,
            depth: 1,
        };
        DeserializeSeed::deserialize(render, &mut deserializer).unwrap();
        (rendering.text, rendering.unwritten)
    };
    context.note_number("1.50");
    assert_eq!(render("1.5"), ("1.50".to_owned(), false));
    assert_eq!(render("[2.5]"), ("[]".to_owned(), true));
}

/// Feeds `seed` the record of kind `kind`, one of eight that are not objects: a null, a boolean,
/// a negative and a large integer, a float, a string, a vast integer's digits and an array.
fn not_object<S: DeserializeSeed<'static, Value = ()>>(
    seed: S,
    kind: usize,
) -> Result<(), ValueError> {
    match kind {
        0 => seed.deserialize(().into_deserializer()),
        1 => seed.deserialize(true.into_deserializer()),
        2 => seed.deserialize((-1_i64).into_deserializer()),
        3 => seed.deserialize(u64::MAX.into_deserializer()),
        4 => seed.deserialize(0.5_f64.into_deserializer()),
        5 => seed.deserialize("x".into_deserializer()),
        6 => seed.deserialize(BytesDeserializer::new(b"12")),
        _ => seed.deserialize(vec![1_i64].into_deserializer()),
    }
}

#[test]
fn every_visitor_tree_refuses_a_record_that_is_not_an_object() {
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let refused = |feed: &mut dyn FnMut(usize) -> Result<(), ValueError>| -> Vec<_> {
        (0..8)
            .map(|kind| {
                assert!(feed(kind).is_err(), "kind {kind}");
                context.fault()
            })
            .collect()
    };
    let not_objects = vec![Some(ShredError::NotObject); 8];
    let mut record = Record::empty(0);
    let rows = refused(&mut |kind| {
        let row = Row {
            record: &mut record,
            context: &context,
        };
        not_object(RecordSeed(row), kind)
    });
    assert_eq!(rows, not_objects);
    let mut shape = Shape::default();
    let observed = refused(&mut |kind| {
        let record = observing::Record {
            shape: &mut shape,
            context: &context,
        };
        not_object(RecordSeed(record), kind)
    });
    assert_eq!(observed, not_objects);
}
