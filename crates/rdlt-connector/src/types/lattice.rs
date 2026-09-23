//! The join of two logical types: the narrowest type that holds every value of both.

use std::collections::BTreeMap;

use super::{DecimalType, Field, Fields, LogicalType};

/// The name every list item field carries, so list types compare by item type alone.
pub(crate) const LIST_ITEM: &str = "item";

impl LogicalType {
    /// The narrowest type that holds every value of `self` and of `other`.
    ///
    /// The join is associative, commutative and idempotent, `Null` is its identity and `Json`
    /// absorbs every type. Floats and decimals never mix: they join to `Json` rather than round. An
    /// integer joined with a float becomes `Float64`, which holds integers exactly only up to 2^53.
    #[must_use]
    pub fn join(&self, other: &Self) -> Self {
        use LogicalType as T;
        if self == other {
            return self.clone();
        }
        match (self, other) {
            (T::Null, x) | (x, T::Null) => x.clone(),
            (a, b) if integer_digits(a).is_some() && integer_digits(b).is_some() => {
                if integer_digits(a) >= integer_digits(b) {
                    a.clone()
                } else {
                    b.clone()
                }
            }
            (a, b) if is_numeric_float_mix(a, b) => T::Float64,
            (T::Decimal(d), i) | (i, T::Decimal(d)) if integer_digits(i).is_some() => {
                let digits = integer_digits(i).unwrap_or(0);
                join_decimals(*d, digits, 0)
            }
            (T::Decimal(a), T::Decimal(b)) => {
                join_decimals(*a, b.precision() - b.scale(), b.scale())
            }
            (T::Timestamp(a, zone_a), T::Timestamp(b, zone_b)) => {
                let zone = if zone_a == zone_b {
                    zone_a.clone()
                } else {
                    Some("UTC".into())
                };
                T::Timestamp((*a).max(*b), zone)
            }
            (T::Date, T::Timestamp(unit, zone)) | (T::Timestamp(unit, zone), T::Date) => {
                T::Timestamp(*unit, zone.clone())
            }
            (T::Time(a), T::Time(b)) => T::Time((*a).max(*b)),
            (T::Duration(a), T::Duration(b)) => T::Duration((*a).max(*b)),
            (T::Struct(a), T::Struct(b)) => T::Struct(join_structs(a, b)),
            (T::List(a), T::List(b)) => T::List(Box::new(join_fields(LIST_ITEM, Some(a), Some(b)))),
            _ => T::Json,
        }
    }
}

/// Decimal digits needed for every value of an integer type.
fn integer_digits(logical_type: &LogicalType) -> Option<u8> {
    match logical_type {
        LogicalType::Int8 => Some(3),
        LogicalType::Int16 => Some(5),
        LogicalType::Int32 => Some(10),
        LogicalType::Int64 => Some(19),
        _ => None,
    }
}

fn is_numeric_float_mix(a: &LogicalType, b: &LogicalType) -> bool {
    let is_float = |t: &LogicalType| matches!(t, LogicalType::Float32 | LogicalType::Float64);
    let is_number = |t: &LogicalType| is_float(t) || integer_digits(t).is_some();
    (is_float(a) && is_number(b)) || (is_float(b) && is_number(a))
}

/// A decimal holding `decimal` and a value with `integer_digits` before and `scale` after the point.
fn join_decimals(decimal: DecimalType, integer_digits: u8, scale: u8) -> LogicalType {
    let integer_digits = (decimal.precision() - decimal.scale()).max(integer_digits);
    let scale = decimal.scale().max(scale);
    let precision = u8::try_from(u16::from(integer_digits) + u16::from(scale)).ok();
    precision
        .and_then(|precision| DecimalType::new(precision, scale).ok())
        .map_or(LogicalType::Json, LogicalType::Decimal)
}

/// Field-wise join; a field missing on one side becomes nullable.
///
/// Fields come out sorted by name, so the result does not depend on which side listed a field first.
fn join_structs(a: &Fields, b: &Fields) -> Fields {
    let mut names: BTreeMap<&str, (Option<&Field>, Option<&Field>)> = BTreeMap::new();
    for field in a.iter() {
        names.entry(field.name()).or_default().0 = Some(field);
    }
    for field in b.iter() {
        names.entry(field.name()).or_default().1 = Some(field);
    }
    let joined = names
        .into_iter()
        .map(|(name, (left, right))| join_fields(name, left, right))
        .collect();
    Fields::new(joined).expect("names come from a map, so they are distinct")
}

fn join_fields(name: &str, left: Option<&Field>, right: Option<&Field>) -> Field {
    match (left, right) {
        (Some(l), Some(r)) => Field::new(
            name,
            l.logical_type().join(r.logical_type()),
            l.is_nullable() || r.is_nullable(),
        ),
        (Some(only), None) | (None, Some(only)) => {
            Field::new(name, only.logical_type().clone(), true)
        }
        (None, None) => Field::new(name, LogicalType::Null, true),
    }
}
