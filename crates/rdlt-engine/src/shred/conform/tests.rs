use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, Int64Array, ListArray, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};

use super::{fit, fits};
use crate::shred::observe::{Observed, Shape};

fn object(fields: &[(&str, Observed)]) -> Observed {
    let mut shape = Shape::default();
    for (name, observed) in fields {
        shape.push(Arc::from(*name), observed.clone());
    }
    Observed::Object(shape)
}

fn list(item: Observed) -> Observed {
    Observed::Array(Box::new(item), 0)
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

#[test]
fn objects_and_lists_are_fitted_field_by_field_and_item_by_item() {
    let exact = Observed::Int { exact: true };
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    // Objects of `x`, fitted to objects of `y` and `x`: `y` all null, `x` kept.
    let fields = Fields::from(vec![Field::new("x", DataType::Int64, true)]);
    let objects: ArrayRef = Arc::new(StructArray::new(fields, vec![Arc::clone(&ints)], None));
    let joined = object(&[("y", Observed::Text), ("x", exact.clone())]);
    let fitted = fit(&objects, &object(&[("x", exact.clone())]), &joined).unwrap();
    let fitted = fitted.as_struct();
    let names: Vec<&str> = fitted
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, ["y", "x"]);
    assert_eq!(fitted.column(0).null_count(), 2);
    assert_eq!(fitted.column(1).as_ref(), ints.as_ref());
    // Lists of integers, fitted to lists of floats: each item cast.
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2]));
    let lists: ArrayRef = Arc::new(ListArray::new(item, offsets, ints, None));
    let fitted = fit(&lists, &list(exact), &list(Observed::Float)).unwrap();
    let items = fitted
        .as_list::<i32>()
        .values()
        .as_primitive::<Float64Type>()
        .clone();
    assert_eq!(items.values().as_ref(), [1.0, 2.0]);
}
