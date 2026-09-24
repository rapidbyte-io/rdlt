use std::sync::Arc;

use super::fits;
use crate::shred::observe::{Observed, Shape};

fn object(fields: &[(&str, Observed)]) -> Observed {
    let mut shape = Shape::default();
    for (name, observed) in fields {
        shape.push(Arc::from(*name), observed.clone());
    }
    Observed::Object(shape)
}

fn list(item: Observed) -> Observed {
    Observed::Array(Box::new(item))
}

#[test]
fn columns_fit_the_joined_shape_when_only_nulls_order_or_integer_widening_differ() {
    let exact = Observed::Int { exact: true };
    let inexact = Observed::Int { exact: false };
    for (local, joined) in [
        (Observed::Null, Observed::Text),
        (Observed::Null, Observed::Json),
        (Observed::Bool, Observed::Bool),
        (exact.clone(), inexact.clone()),
        (exact.clone(), Observed::Float),
        (inexact.clone(), Observed::Wide),
        (Observed::Json, Observed::Json),
        (list(Observed::Null), list(Observed::Text)),
        (list(exact.clone()), list(Observed::Float)),
        (
            object(&[("a", exact.clone())]),
            object(&[("b", Observed::Text), ("a", exact.clone())]),
        ),
        (
            object(&[("a", Observed::Null)]),
            object(&[("a", object(&[("x", Observed::Bool)]))]),
        ),
    ] {
        assert!(fits(&local, &joined), "{local:?} in {joined:?}");
    }
}

#[test]
fn columns_whose_values_joined_to_another_kind_do_not_fit() {
    let exact = Observed::Int { exact: true };
    for (local, joined) in [
        (exact.clone(), Observed::Json),
        (Observed::Text, Observed::Json),
        (Observed::Float, Observed::Json),
        (Observed::Wide, Observed::Json),
        (Observed::Bool, Observed::Json),
        (list(exact.clone()), Observed::Json),
        (list(Observed::Text), list(Observed::Json)),
        (
            object(&[("a", Observed::Text)]),
            object(&[("a", Observed::Json)]),
        ),
        (
            object(&[("a", Observed::Text)]),
            object(&[("b", Observed::Text)]),
        ),
        (object(&[("a", Observed::Text)]), Observed::Json),
    ] {
        assert!(!fits(&local, &joined), "{local:?} in {joined:?}");
    }
}
