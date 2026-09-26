//! Drawn Arrow data: every logical type, in every encoding a source may send
//! it in, with values across each type's whole range.

pub mod arrays;
mod floats;
pub mod json;
pub mod neighbors;
#[cfg(test)]
mod tests;
pub mod values;

use rdlt_connector::TypeKind;
use serde_json::Value;

pub use arrays::{Encoding, Shape, array, field};

/// A value of any logical type, as a source sends it; its type says what it means.
#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    /// No value.
    Null,
    /// A boolean.
    Bool(bool),
    /// Any integer type's value.
    Int(i64),
    /// A single-precision float.
    Float32(f32),
    /// A double-precision float.
    Float64(f64),
    /// A decimal's unscaled value, as signed digits; its type gives the scale.
    Decimal(String),
    /// Text.
    Utf8(String),
    /// Bytes.
    Binary(Vec<u8>),
    /// Days since the epoch.
    Date(i64),
    /// A time of day, a timestamp or a duration, in its type's unit.
    Temporal(i64),
    /// A UUID's bytes.
    Uuid([u8; 16]),
    /// A JSON value.
    Json(Value),
    /// An object's fields, by name.
    Struct(Vec<(String, Scalar)>),
    /// A list's items.
    List(Vec<Scalar>),
}

/// A batch: its columns' names and shapes, and its rows' values.
pub type Drawn = (Vec<(String, Shape)>, Vec<Vec<Scalar>>);

/// Every kind of value a destination may store natively.
pub const KINDS: [TypeKind; 19] = [
    TypeKind::Null,
    TypeKind::Bool,
    TypeKind::Int8,
    TypeKind::Int16,
    TypeKind::Int32,
    TypeKind::Int64,
    TypeKind::Float32,
    TypeKind::Float64,
    TypeKind::Decimal,
    TypeKind::Utf8,
    TypeKind::Binary,
    TypeKind::Date,
    TypeKind::Time,
    TypeKind::Timestamp,
    TypeKind::Duration,
    TypeKind::Uuid,
    TypeKind::Json,
    TypeKind::Struct,
    TypeKind::List,
];
