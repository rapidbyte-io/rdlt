use serde::de::Expected;

use super::{Key, Look, Record};
use crate::shred::meter::{Columns, Meter};
use crate::shred::observe::{Observed, Shape};
use crate::shred::visit::Context;

fn expected(visitor: &dyn Expected) -> String {
    visitor.to_string()
}

#[test]
fn every_observing_visitor_says_what_it_expects() {
    let context = Context::new(Meter::new(0), Columns::new(0));
    let mut shape = Shape::default();
    let mut node = Observed::Null;
    assert_eq!(
        expected(&Record {
            shape: &mut shape,
            context: &context,
        }),
        "a record"
    );
    assert_eq!(
        expected(&Key {
            shape: &mut shape,
            context: &context,
            object: 1,
        }),
        "an object key"
    );
    assert_eq!(
        expected(&Look {
            node: &mut node,
            context: &context,
            depth: 1,
        }),
        "a JSON value"
    );
}
