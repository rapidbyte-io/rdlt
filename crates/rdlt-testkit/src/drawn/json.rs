//! Values as a source pushes them in JSON: of the types JSON holds, so the engine infers each
//! value's type as the type it was drawn as.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use proptest::prelude::*;
use rdlt_connector::{DecimalType, Field, Fields, LogicalType};
use serde_json::{Map, Value, json};

use super::arrays::{Encoding, Shape, array, field};
use super::values::value;
use super::{Drawn, Scalar};

fn plain(logical: LogicalType, children: Vec<Shape>) -> Shape {
    Shape {
        logical,
        encoding: Encoding::Plain,
        children,
    }
}

/// A shape of whole numbers of any width: within 76 digits a decimal holds them, and past that
/// only JSON text does, so values of it are pushed as JSON and never built into Arrow.
pub fn integers() -> Shape {
    Shape {
        logical: LogicalType::Decimal(DecimalType::new(76, 0).expect("76 digits fit a decimal")),
        encoding: Encoding::Large,
        children: Vec::new(),
    }
}

/// A type JSON holds, nested up to `depth` levels: booleans, 64-bit integers and floats, text,
/// whole numbers of any width, and objects and arrays of them.
pub fn shape(depth: u32) -> BoxedStrategy<Shape> {
    shaped(depth, Width::Any)
}

/// A type JSON holds that an Arrow batch holds too, nested up to `depth` levels: as [`shape`]
/// draws, but no whole number wider than 64 bits.
pub fn arrow_shape(depth: u32) -> BoxedStrategy<Shape> {
    shaped(depth, Width::Bits64)
}

/// How wide a drawn whole number may be.
#[derive(Clone, Copy)]
enum Width {
    Any,
    Bits64,
}

/// A type JSON holds, nested up to `depth` levels, its whole numbers as wide as `width` says.
fn shaped(depth: u32, width: Width) -> BoxedStrategy<Shape> {
    use LogicalType as T;
    let scalar = proptest::sample::select(vec![T::Bool, T::Int64, T::Float64, T::Utf8])
        .prop_map(|logical| plain(logical, Vec::new()));
    let leaf = match width {
        Width::Any => prop_oneof![4 => scalar, 1 => Just(integers())].boxed(),
        Width::Bits64 => scalar.boxed(),
    };
    if depth == 0 {
        return leaf;
    }
    let inner = move || shaped(depth - 1, width);
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

/// A batch of one to three of the columns `a`, `b` and `c`, each of a type [`arrow_shape`]
/// draws, and up to six rows.
pub fn drawn() -> impl Strategy<Value = Drawn> {
    proptest::sample::subsequence(vec!["a", "b", "c"], 1..=3)
        .prop_flat_map(|names| {
            let count = names.len();
            (
                Just(names),
                proptest::collection::vec(arrow_shape(2), count),
            )
        })
        .prop_flat_map(|(names, shapes)| {
            let row: Vec<_> = shapes.iter().map(|shape| value(shape, true)).collect();
            let columns: Vec<(String, Shape)> = names
                .iter()
                .map(|name| (*name).to_owned())
                .zip(shapes)
                .collect();
            (Just(columns), proptest::collection::vec(row, 0..6))
        })
}

/// One to four batches as [`drawn`] draws each: a column an array in one batch may be an object
/// or a scalar in another, and arrays may sit beside each other.
pub fn batches() -> impl Strategy<Value = Vec<Drawn>> {
    proptest::collection::vec(drawn(), 1..5)
}

/// `drawn` as an Arrow batch, each column as its shape builds it.
///
/// # Panics
///
/// Where a value does not fit its column's shape, which drawn values always do.
pub fn batch((columns, rows): &Drawn) -> RecordBatch {
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .enumerate()
        .map(|(column, (_, shape))| {
            let values: Vec<&Scalar> = rows.iter().map(|row| &row[column]).collect();
            array(shape, &values)
        })
        .collect();
    let fields: Vec<_> = columns
        .iter()
        .zip(&arrays)
        .map(|((name, shape), array)| field(name, shape, array, true))
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
        .expect("a drawn batch is valid")
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

/// `value`, of `logical`, as JSON text: as [`rendered`] renders it, but with each decimal as the
/// number it is, which JSON holds whatever its width and a JSON value in memory may not.
pub fn text(value: &Scalar, logical: &LogicalType) -> String {
    match (value, logical) {
        (Scalar::Decimal(digits), LogicalType::Decimal(decimal)) => number(digits, decimal.scale()),
        (Scalar::Struct(fields), LogicalType::Struct(types)) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|(name, inner)| {
                    let field = types.iter().find(|field| field.name() == name);
                    let logical = field.map_or(&LogicalType::Null, |field| field.logical_type());
                    format!("{}:{}", Value::from(name.as_str()), text(inner, logical))
                })
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        (Scalar::List(items), LogicalType::List(item)) => {
            let items: Vec<String> = items
                .iter()
                .map(|inner| text(inner, item.logical_type()))
                .collect();
            format!("[{}]", items.join(","))
        }
        (other, _) => rendered(other).to_string(),
    }
}

/// The decimal whose unscaled value is the signed `digits`, `scale` of them after the point, as
/// JSON writes a number: no leading zeros.
fn number(digits: &str, scale: u8) -> String {
    let (sign, magnitude) = digits
        .strip_prefix('-')
        .map_or(("", digits), |magnitude| ("-", magnitude));
    let scale = usize::from(scale);
    let padded = format!("{magnitude:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let zero = whole == "0" && fraction.bytes().all(|digit| digit == b'0');
    let sign = if zero { "" } else { sign };
    if fraction.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    }
}
