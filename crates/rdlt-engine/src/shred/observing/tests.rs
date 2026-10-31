use serde::de::Expected;

use super::{Key, Look, Part, Record};
use crate::shred::meter::{Columns, Meter};
use crate::shred::observe::{Observed, Shape};
use crate::shred::visit::{Context, RecordSeed, Skip};

fn expected(visitor: &dyn Expected) -> String {
    visitor.to_string()
}

#[test]
fn every_observing_visitor_says_what_it_expects() {
    let context = Context::new(Meter::new(0), Columns::new(0));
    let mut shape = Shape::default();
    let mut node = Observed::Null;
    assert_eq!(
        expected(&RecordSeed(Record {
            shape: &mut shape,
            context: &context,
            excluded: None,
        })),
        "a record"
    );
    assert_eq!(
        expected(&Key {
            shape: &mut shape,
            context: &context,
            object: 1,
            fields: None,
            skipped: (),
        }),
        "an object key"
    );
    assert_eq!(
        expected(&Look {
            node: &mut node,
            context: &context,
            depth: 1,
            target: None,
        }),
        "a JSON value"
    );
    assert_eq!(
        expected(&Part {
            node: &mut node,
            skip: Skip {
                context: &context,
                depth: 1,
            },
        }),
        "a JSON value"
    );
}
