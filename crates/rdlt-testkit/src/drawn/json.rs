//! Values as a source pushes them in JSON: of the types JSON holds, so the engine infers each
//! value's type as the type it was drawn as.

use proptest::prelude::*;
use rdlt_connector::{Field, Fields, LogicalType};
use serde_json::{Map, Value, json};

use super::Scalar;
use super::arrays::{Encoding, Shape};

fn plain(logical: LogicalType, children: Vec<Shape>) -> Shape {
    Shape {
        logical,
        encoding: Encoding::Plain,
        children,
    }
}

/// A type JSON holds, nested up to `depth` levels: booleans, 64-bit integers and floats, text,
/// and objects and arrays of them.
pub fn shape(depth: u32) -> BoxedStrategy<Shape> {
    use LogicalType as T;
    let leaf = proptest::sample::select(vec![T::Bool, T::Int64, T::Float64, T::Utf8])
        .prop_map(|logical| plain(logical, Vec::new()))
        .boxed();
    if depth == 0 {
        return leaf;
    }
    let inner = move || shape(depth - 1);
    let object = proptest::sample::subsequence(vec!["x", "y", "z"], 1..=3)
        .prop_flat_map(move |names| {
            let count = names.len();
            (Just(names), proptest::collection::vec(inner(), count))
        })
        .prop_map(|(names, children)| {
            let fields = names
                .iter()
                .zip(&children)
                .map(|(name, child)| Field::new(*name, child.logical.clone(), true))
                .collect();
            plain(
                T::Struct(Fields::new(fields).expect("distinct names")),
                children,
            )
        });
    let array = inner().prop_map(|item| {
        let field = Field::new("item", item.logical.clone(), true);
        plain(T::List(Box::new(field)), vec![item])
    });
    prop_oneof![3 => leaf, 1 => object, 1 => array].boxed()
}

/// `value` as JSON: a float JSON cannot hold by its name, as the engine writes one.
pub fn rendered(value: &Scalar) -> Value {
    match value {
        Scalar::Bool(value) => json!(value),
        Scalar::Int(value) => json!(value),
        Scalar::Float64(value) if value.is_finite() => json!(value),
        Scalar::Float64(value) => json!(float_name(*value)),
        Scalar::Utf8(text) => json!(text),
        Scalar::Struct(fields) => Value::Object(
            fields
                .iter()
                .map(|(name, inner)| (name.clone(), rendered(inner)))
                .collect::<Map<_, _>>(),
        ),
        Scalar::List(items) => Value::Array(items.iter().map(rendered).collect()),
        _ => Value::Null,
    }
}

fn float_name(value: f64) -> &'static str {
    if value.is_nan() {
        "NaN"
    } else if value > 0.0 {
        "Infinity"
    } else {
        "-Infinity"
    }
}
