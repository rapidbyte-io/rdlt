//! Drawn types and values: every logical type, in every encoding a source may send it in, and
//! values across each type's whole range.

use std::sync::Arc;

use proptest::prelude::*;
use rdlt_connector::{DecimalType, Field, Fields, LogicalType, TimeUnit};
use serde_json::{Value, json};

use super::Drawn;
use super::Scalar;
use super::arrays::{Encoding, Shape};

/// The source columns batches draw from.
const NAMES: [&str; 3] = ["a", "b", "c"];

/// Seconds since the epoch of the first and last days whose instants every destination renders:
/// 0001-01-02 and 9999-12-30.
const FIRST_SECOND: i64 = -62_135_596_800 + 86_400;
const LAST_SECOND: i64 = 253_402_300_799 - 86_400;

const UNITS: [TimeUnit; 4] = [
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

fn leaf(logical: LogicalType, encodings: &[Encoding]) -> BoxedStrategy<Shape> {
    // Any leaf may arrive dictionary or run-end encoded too.
    let mut encodings = encodings.to_vec();
    encodings.extend([Encoding::Dictionary, Encoding::RunEnd]);
    proptest::sample::select(encodings)
        .prop_map(move |encoding| Shape {
            logical: logical.clone(),
            encoding,
            children: Vec::new(),
        })
        .boxed()
}

/// A decimal type of any precision and scale, in any encoding that holds it.
fn decimal() -> BoxedStrategy<Shape> {
    (1_u8..=76)
        .prop_flat_map(|precision| (Just(precision), 0..=precision))
        .prop_flat_map(|(precision, scale)| {
            let mut encodings = vec![Encoding::Plain];
            if precision <= 9 {
                encodings.push(Encoding::Decimal32);
            }
            if precision <= 18 {
                encodings.push(Encoding::Decimal64);
            }
            if precision <= 38 {
                encodings.push(Encoding::Decimal256);
            }
            let logical = LogicalType::Decimal(DecimalType::new(precision, scale).expect("valid"));
            leaf(logical, &encodings)
        })
        .boxed()
}

/// Every type without children.
fn scalar_shape() -> BoxedStrategy<Shape> {
    use Encoding as E;
    use LogicalType as T;
    let unit = || proptest::sample::select(UNITS.to_vec());
    let zone = proptest::sample::select(vec![
        None,
        Some("UTC"),
        Some("+05:00"),
        Some("-03:30"),
        Some("America/Sao_Paulo"),
        Some("Asia/Kolkata"),
    ]);
    prop_oneof![
        leaf(T::Null, &[E::Plain]),
        leaf(T::Bool, &[E::Plain]),
        leaf(T::Int8, &[E::Plain]),
        leaf(T::Int16, &[E::Plain, E::Unsigned]),
        leaf(T::Int32, &[E::Plain, E::Unsigned]),
        leaf(T::Int64, &[E::Plain, E::Unsigned]),
        leaf(T::Float32, &[E::Plain, E::Half]),
        leaf(T::Float64, &[E::Plain]),
        decimal(),
        leaf(
            T::Decimal(DecimalType::new(20, 0).expect("valid")),
            &[E::Unsigned]
        ),
        leaf(T::Utf8, &[E::Plain, E::Large, E::View]),
        leaf(T::Json, &[E::Plain, E::Large, E::View]),
        (1_i32..4).prop_flat_map(|size| leaf(
            T::Binary,
            &[E::Plain, E::Large, E::View, E::FixedSize(size)]
        )),
        leaf(T::Date, &[E::Plain, E::Date64]),
        unit().prop_flat_map(|unit| leaf(T::Time(unit), &[E::Plain])),
        (unit(), zone).prop_flat_map(|(unit, zone)| leaf(
            T::Timestamp(unit, zone.map(Arc::from)),
            &[E::Plain]
        )),
        unit().prop_flat_map(|unit| leaf(T::Duration(unit), &[E::Plain])),
        leaf(T::Uuid, &[E::Plain]),
    ]
    .boxed()
}

/// Any type, nested up to `depth` levels.
pub(crate) fn shape(depth: u32) -> BoxedStrategy<Shape> {
    if depth == 0 {
        return scalar_shape();
    }
    let inner = move || shape(depth - 1);
    let structure = proptest::sample::subsequence(vec!["x", "y", "z"], 1..=3)
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
            Shape {
                logical: LogicalType::Struct(Fields::new(fields).expect("distinct names")),
                encoding: Encoding::Plain,
                children,
            }
        });
    let list_encodings = proptest::sample::select(vec![
        Encoding::Plain,
        Encoding::Large,
        Encoding::View,
        Encoding::LargeView,
        Encoding::FixedSize(1),
        Encoding::FixedSize(2),
    ]);
    let list = (inner(), list_encodings).prop_map(|(item, encoding)| Shape {
        logical: LogicalType::List(Box::new(Field::new("item", item.logical.clone(), true))),
        encoding,
        children: vec![item],
    });
    let map = inner().prop_map(|value| {
        let key = Shape {
            logical: LogicalType::Utf8,
            encoding: Encoding::Plain,
            children: Vec::new(),
        };
        let fields = vec![
            Field::new("key", LogicalType::Utf8, false),
            Field::new("value", value.logical.clone(), true),
        ];
        let entries = Shape {
            logical: LogicalType::Struct(Fields::new(fields).expect("distinct names")),
            encoding: Encoding::Plain,
            children: vec![key, value],
        };
        Shape {
            logical: LogicalType::List(Box::new(Field::new(
                "item",
                entries.logical.clone(),
                false,
            ))),
            encoding: Encoding::Map,
            children: vec![entries],
        }
    });
    prop_oneof![3 => scalar_shape(), 1 => structure, 1 => list, 1 => map].boxed()
}

/// A value of `shape`, null one time in five where its field is nullable.
pub(crate) fn value(shape: &Shape, nullable: bool) -> BoxedStrategy<Scalar> {
    let present = present(shape);
    if nullable && shape.logical != LogicalType::Null {
        prop_oneof![1 => Just(Scalar::Null), 4 => present].boxed()
    } else {
        present
    }
}

/// A present value of `shape`, within the range its encoding holds.
fn present(shape: &Shape) -> BoxedStrategy<Scalar> {
    use Encoding as E;
    use LogicalType as T;
    let unsigned = shape.encoding == E::Unsigned;
    match &shape.logical {
        T::Null => Just(Scalar::Null).boxed(),
        T::Bool => any::<bool>().prop_map(Scalar::Bool).boxed(),
        T::Int8 | T::Int16 | T::Int32 | T::Int64 => integer(&shape.logical, unsigned),
        T::Float32 if shape.encoding == E::Half => any::<u16>()
            .prop_map(|bits| Scalar::Float32(half(bits)))
            .boxed(),
        T::Float32 => any::<f32>().prop_map(Scalar::Float32).boxed(),
        T::Float64 => any::<f64>().prop_map(Scalar::Float64).boxed(),
        T::Decimal(_) if unsigned => any::<u64>()
            .prop_map(|value| Scalar::Decimal(value.to_string()))
            .boxed(),
        T::Decimal(decimal) => {
            let digits = usize::from(decimal.precision());
            (
                any::<bool>(),
                proptest::collection::vec(0_u8..10, 1..=digits),
            )
                .prop_map(|(negative, digits)| {
                    let digits: String = digits
                        .iter()
                        .map(|digit| char::from(b'0' + digit))
                        .collect();
                    Scalar::Decimal(if negative {
                        format!("-{digits}")
                    } else {
                        digits
                    })
                })
                .boxed()
        }
        T::Utf8 => proptest::collection::vec(any::<char>(), 0..6)
            .prop_map(|chars| Scalar::Utf8(chars.into_iter().collect()))
            .boxed(),
        T::Binary => {
            let sizes = match shape.encoding {
                E::FixedSize(size) => {
                    let size = usize::try_from(size).expect("a small size");
                    size..=size
                }
                _ => 0..=5,
            };
            proptest::collection::vec(any::<u8>(), sizes)
                .prop_map(Scalar::Binary)
                .boxed()
        }
        T::Date | T::Time(_) | T::Timestamp(..) | T::Duration(_) => temporal(&shape.logical),
        T::Uuid => any::<[u8; 16]>().prop_map(Scalar::Uuid).boxed(),
        T::Json => json_value(2).prop_map(Scalar::Json).boxed(),
        T::Struct(_) | T::List(_) => nested(shape),
    }
}

/// A present integer of `logical`, within the unsigned type one size down where `unsigned`.
fn integer(logical: &LogicalType, unsigned: bool) -> BoxedStrategy<Scalar> {
    use LogicalType as T;
    match logical {
        T::Int8 => any::<i8>()
            .prop_map(|value| Scalar::Int(value.into()))
            .boxed(),
        T::Int16 if unsigned => any::<u8>()
            .prop_map(|value| Scalar::Int(value.into()))
            .boxed(),
        T::Int16 => any::<i16>()
            .prop_map(|value| Scalar::Int(value.into()))
            .boxed(),
        T::Int32 if unsigned => any::<u16>()
            .prop_map(|value| Scalar::Int(value.into()))
            .boxed(),
        T::Int32 => any::<i32>()
            .prop_map(|value| Scalar::Int(value.into()))
            .boxed(),
        T::Int64 if unsigned => any::<u32>()
            .prop_map(|value| Scalar::Int(value.into()))
            .boxed(),
        T::Int64 => any::<i64>().prop_map(Scalar::Int).boxed(),
        other => unreachable!("{other} is not an integer type"),
    }
}

/// A present date, time, timestamp or duration of `logical`, across its whole range and often
/// within the years every destination renders.
fn temporal(logical: &LogicalType) -> BoxedStrategy<Scalar> {
    use LogicalType as T;
    match logical {
        T::Date => prop_oneof![
            3 => ((FIRST_SECOND / 86_400)..=(LAST_SECOND / 86_400))
                .prop_map(|days| i32::try_from(days).expect("a date in range")),
            1 => any::<i32>(),
        ]
        .prop_map(Scalar::Date)
        .boxed(),
        T::Time(unit) => (0..86_400 * per_second(*unit))
            .prop_map(Scalar::Temporal)
            .boxed(),
        T::Timestamp(unit, _) => {
            let scale = per_second(*unit);
            let first = FIRST_SECOND.saturating_mul(scale);
            let last = LAST_SECOND.saturating_mul(scale);
            prop_oneof![3 => first..=last, 1 => any::<i64>()]
                .prop_map(Scalar::Temporal)
                .boxed()
        }
        T::Duration(_) => any::<i64>().prop_map(Scalar::Temporal).boxed(),
        other => unreachable!("{other} is not temporal"),
    }
}

/// A present struct or list of `shape`.
fn nested(shape: &Shape) -> BoxedStrategy<Scalar> {
    use Encoding as E;
    use LogicalType as T;
    match &shape.logical {
        T::Struct(fields) => fields
            .iter()
            .zip(&shape.children)
            .map(|(field, child)| {
                let name = field.name().to_owned();
                value(child, field.is_nullable()).prop_map(move |inner| (name.clone(), inner))
            })
            .collect::<Vec<_>>()
            .prop_map(Scalar::Struct)
            .boxed(),
        T::List(item) => {
            let sizes = match shape.encoding {
                E::FixedSize(size) => {
                    let size = usize::try_from(size).expect("a small size");
                    size..=size
                }
                _ => 0..=3,
            };
            proptest::collection::vec(value(&shape.children[0], item.is_nullable()), sizes)
                .prop_map(Scalar::List)
                .boxed()
        }
        other => unreachable!("{other} is not nested"),
    }
}

/// The half-precision float of `bits`, which a single-precision one holds exactly.
fn half(bits: u16) -> f32 {
    let sign = if bits >> 15 == 1 { -1.0 } else { 1.0 };
    let exponent = i32::from((bits >> 10) & 0x1f);
    let fraction = f32::from(bits & 0x3ff);
    match exponent {
        0 => sign * fraction * 2_f32.powi(-24),
        0x1f if fraction == 0.0 => sign * f32::INFINITY,
        0x1f => f32::NAN,
        _ => sign * (1.0 + fraction / 1024.0) * 2_f32.powi(exponent - 15),
    }
}

/// Units of `unit` in a second.
fn per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// Any JSON value, nested up to `depth` levels.
fn json_value(depth: u32) -> BoxedStrategy<Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|value| json!(value)),
        any::<f64>()
            .prop_filter("JSON holds finite numbers", |value| value.is_finite())
            .prop_map(|value| json!(value)),
        proptest::collection::vec(any::<char>(), 0..4)
            .prop_map(|chars| Value::String(chars.into_iter().collect())),
    ];
    if depth == 0 {
        return leaf.boxed();
    }
    let inner = move || json_value(depth - 1);
    prop_oneof![
        2 => leaf,
        1 => proptest::collection::vec(inner(), 0..3).prop_map(Value::Array),
        1 => proptest::collection::btree_map("[a-z]{1,2}", inner(), 0..3)
            .prop_map(|members| Value::Object(members.into_iter().collect())),
    ]
    .boxed()
}

/// A batch of one to three of [`NAMES`], each of any shape, and up to six rows.
pub(crate) fn drawn() -> impl Strategy<Value = Drawn> {
    proptest::sample::subsequence(NAMES.to_vec(), 1..=NAMES.len())
        .prop_flat_map(|names| {
            let count = names.len();
            (Just(names), proptest::collection::vec(shape(2), count))
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
