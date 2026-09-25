//! Drawn Arrow data for property tests: every logical type, in every encoding a source may send
//! it in, with values across each type's whole range.

pub(crate) mod arrays;
pub(crate) mod neighbors;
pub(crate) mod values;

use serde_json::Value;

pub(crate) use arrays::{Encoding, Shape, array, field};

/// A value of any logical type, as a source sends it; its type says what it means.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Scalar {
    Null,
    Bool(bool),
    /// Any integer type's value.
    Int(i64),
    Float32(f32),
    Float64(f64),
    /// A decimal's unscaled value, as signed digits; its type gives the scale.
    Decimal(String),
    Utf8(String),
    Binary(Vec<u8>),
    /// Days since the epoch.
    Date(i32),
    /// A time of day, a timestamp or a duration, in its type's unit.
    Temporal(i64),
    Uuid([u8; 16]),
    Json(Value),
    /// An object's fields, by name.
    Struct(Vec<(String, Scalar)>),
    List(Vec<Scalar>),
}

/// Cases a property test runs: `PROPTEST_CASES` where set, for long local runs, else `default`.
pub(crate) fn cases(default: u32) -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(default)
}

/// A batch: its columns' names and shapes, and its rows' values.
pub(crate) type Drawn = (Vec<(String, Shape)>, Vec<Vec<Scalar>>);
