//! Which values of a column of JSON a column of another type holds: a JSON value typed as the
//! shredder types one, and the value of that type it is, for a model to place each value of a
//! column of JSON whose own column is of another type.
//!
//! A `serde_json` value holds integers up to the unsigned 64-bit range; the shredder reads wider
//! ones as decimals, which such a value cannot carry, so a model draws none.

use rdlt_connector::{Field, Fields, LogicalType};
use serde_json::Value;

use crate::drawn::Scalar;

/// The largest magnitude of the integers a 64-bit float holds exactly.
const EXACT_IN_FLOAT: u64 = 1 << 53;

/// What a value is, as the shredder joins values.
#[derive(Clone, Debug, PartialEq)]
enum Kind {
    Null,
    Bool,
    Int {
        exact: bool,
    },
    /// An integer beyond the signed 64-bit range, within the unsigned one.
    Wide,
    Float,
    Text,
    List(Box<Kind>),
    Object(Vec<(String, Kind)>),
    Json,
}

fn kind(value: &Value) -> Kind {
    match value {
        Value::Null => Kind::Null,
        Value::Bool(_) => Kind::Bool,
        Value::Number(number) => match (number.as_i64(), number.is_u64()) {
            (Some(int), _) => Kind::Int {
                exact: int.unsigned_abs() <= EXACT_IN_FLOAT,
            },
            (None, true) => Kind::Wide,
            (None, false) => Kind::Float,
        },
        Value::String(_) => Kind::Text,
        Value::Array(items) => Kind::List(Box::new(items.iter().map(kind).fold(Kind::Null, join))),
        Value::Object(members) => Kind::Object(
            members
                .iter()
                .map(|(name, member)| (name.clone(), kind(member)))
                .collect(),
        ),
    }
}

fn join(left: Kind, right: Kind) -> Kind {
    match (left, right) {
        (Kind::Null, other) | (other, Kind::Null) => other,
        (Kind::Int { exact: a }, Kind::Int { exact: b }) => Kind::Int { exact: a && b },
        (Kind::Int { .. } | Kind::Wide, Kind::Int { .. } | Kind::Wide) => Kind::Wide,
        (Kind::Int { exact: true } | Kind::Float, Kind::Int { exact: true } | Kind::Float) => {
            Kind::Float
        }
        (Kind::List(a), Kind::List(b)) => Kind::List(Box::new(join(*a, *b))),
        (Kind::Object(mut a), Kind::Object(b)) => {
            for (name, member) in b {
                match a.iter_mut().find(|(field, _)| *field == name) {
                    Some((_, joined)) => *joined = join(joined.clone(), member),
                    None => a.push((name, member)),
                }
            }
            Kind::Object(a)
        }
        (a, b) if a == b => a,
        _ => Kind::Json,
    }
}

fn logical(kind: &Kind) -> LogicalType {
    match kind {
        Kind::Null => LogicalType::Null,
        Kind::Bool => LogicalType::Bool,
        Kind::Int { .. } => LogicalType::Int64,
        Kind::Wide => LogicalType::Decimal(
            rdlt_connector::DecimalType::new(20, 0).expect("20 digits of scale 0 are a decimal"),
        ),
        Kind::Float => LogicalType::Float64,
        Kind::Text => LogicalType::Utf8,
        Kind::List(item) => LogicalType::List(Box::new(Field::new("item", logical(item), true))),
        Kind::Object(fields) => LogicalType::Struct(
            Fields::new(
                fields
                    .iter()
                    .map(|(name, member)| Field::new(name.as_str(), logical(member), true))
                    .collect(),
            )
            .expect("distinct names"),
        ),
        Kind::Json => LogicalType::Json,
    }
}

/// `value` as a value of `kind`.
fn scalar(value: &Value, kind: &Kind) -> Scalar {
    match (value, kind) {
        (Value::Null, _) => Scalar::Null,
        (_, Kind::Json) => Scalar::Json(value.clone()),
        (Value::Bool(value), _) => Scalar::Bool(*value),
        (Value::Number(number), Kind::Int { .. }) => Scalar::Int(number.as_i64().unwrap_or(0)),
        (Value::Number(number), Kind::Wide) => Scalar::Decimal(number.to_string()),
        (Value::Number(number), _) => Scalar::Float64(number.as_f64().unwrap_or(0.0)),
        (Value::String(text), _) => Scalar::Utf8(text.clone()),
        (Value::Array(items), Kind::List(item)) => {
            Scalar::List(items.iter().map(|value| scalar(value, item)).collect())
        }
        (Value::Object(members), Kind::Object(fields)) => Scalar::Struct(
            members
                .iter()
                .map(|(name, member)| {
                    let kind = fields
                        .iter()
                        .find(|(field, _)| field == name)
                        .map(|(_, kind)| kind);
                    (name.clone(), scalar(member, kind.unwrap_or(&Kind::Json)))
                })
                .collect(),
        ),
        _ => Scalar::Json(value.clone()),
    }
}

/// Whether a column of `to`, nullable where `nullable` says, holds `value`, as the shredder
/// fits one value: a null where it is nullable, an object whose fields its struct holds by name,
/// those it lacks nullable, an array whose items its list holds, an integer of at most 2⁵³ in
/// magnitude in a column of 64-bit floats, and else a value whose type joins into the column's.
fn fits(to: &LogicalType, value: &Value, nullable: bool) -> bool {
    match (to, value) {
        (_, Value::Null) => nullable,
        (LogicalType::Struct(fields), Value::Object(members)) => {
            members.iter().all(|(name, member)| {
                fields
                    .get(name)
                    .is_some_and(|field| fits(field.logical_type(), member, field.is_nullable()))
            }) && fields
                .iter()
                .all(|field| field.is_nullable() || members.contains_key(field.name()))
        }
        (LogicalType::List(item), Value::Array(items)) => {
            items.is_empty()
                || (item.is_nullable()
                    && items
                        .iter()
                        .all(|value| fits(item.logical_type(), value, true)))
        }
        (LogicalType::Struct(_) | LogicalType::List(_), _) => false,
        (LogicalType::Float64, _) if kind(value) == (Kind::Int { exact: true }) => true,
        (to, value) => to.join(&logical(&kind(value))) == *to,
    }
}

/// `value` as a value of its own kind and that kind's type, where a column of `to`, which holds
/// nulls, holds it.
pub fn held(value: &Value, to: &LogicalType) -> Option<(Scalar, LogicalType)> {
    let kind = kind(value);
    fits(to, value, true).then(|| (scalar(value, &kind), logical(&kind)))
}
