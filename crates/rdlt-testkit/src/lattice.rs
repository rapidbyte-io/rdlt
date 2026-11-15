//! The order of logical types as a model: which types a column holds, decided field by field by
//! name and by a table of the scalars, never by the lattice's join, and the form a table holds a
//! type in.

#[cfg(test)]
mod tests;

use rdlt_connector::{Field, Fields, LogicalType};

/// The name a table's model gives every list's item.
const ITEM: &str = "item";

/// Whether a column of `column` holds every value of `arriving` as it is.
///
/// Every type holds `Null` and `Json` holds every type. A struct holds a struct each of whose
/// fields it has, by name in any order, holding that field's values and nulls, and the fields
/// that struct lacks may be null; a list holds a list whose items its item holds, whatever each
/// item is named.
pub fn holds(column: &LogicalType, arriving: &LogicalType) -> bool {
    use LogicalType as T;
    match (column, arriving) {
        (_, T::Null) | (T::Json, _) => true,
        (T::Struct(own), T::Struct(theirs)) => {
            theirs.iter().all(|field| {
                own.get(field.name())
                    .is_some_and(|mine| field_holds(mine, field))
            }) && own
                .iter()
                .all(|mine| mine.is_nullable() || theirs.get(mine.name()).is_some())
        }
        (T::List(own), T::List(theirs)) => field_holds(own, theirs),
        (T::Struct(_) | T::List(_), _) | (_, T::Struct(_) | T::List(_)) => false,
        (column, arriving) => scalar_holds(column, arriving),
    }
}

/// `logical` as a table's model holds it: each struct's fields in name order and each list's
/// item named `item`, at any depth.
pub fn as_modeled(logical: &LogicalType) -> LogicalType {
    use LogicalType as T;
    match logical {
        T::Struct(fields) => {
            let mut fields: Vec<Field> = fields
                .iter()
                .map(|field| {
                    let modeled = as_modeled(field.logical_type());
                    Field::new(field.name(), modeled, field.is_nullable())
                })
                .collect();
            fields.sort_by(|left, right| left.name().cmp(right.name()));
            T::Struct(Fields::new(fields).expect("a struct's names are distinct"))
        }
        T::List(item) => T::List(Box::new(Field::new(
            ITEM,
            as_modeled(item.logical_type()),
            item.is_nullable(),
        ))),
        other => other.clone(),
    }
}

/// Whether a field `own` holds the values of a field `theirs`: its type holds theirs, and it may
/// be null where theirs may.
fn field_holds(own: &Field, theirs: &Field) -> bool {
    holds(own.logical_type(), theirs.logical_type()) && (own.is_nullable() || !theirs.is_nullable())
}

/// Whether a column of the scalar `column` holds every value of the scalar `arriving`: a wider
/// integer, a decimal with as many digits before and after the point, a 64-bit float holding the
/// 32-bit floats and the integers of 32 bits or fewer, a finer time unit, and a timestamp in UTC
/// holding a timestamp shown in any zone, and any timestamp holding dates.
fn scalar_holds(column: &LogicalType, arriving: &LogicalType) -> bool {
    use LogicalType as T;
    if column == arriving {
        return true;
    }
    match (column, arriving) {
        (T::Float64, T::Float32 | T::Int8 | T::Int16 | T::Int32) | (T::Timestamp(..), T::Date) => {
            true
        }
        (T::Decimal(own), T::Decimal(theirs)) => {
            own.scale() >= theirs.scale()
                && own.precision() - own.scale() >= theirs.precision() - theirs.scale()
        }
        (T::Decimal(own), arriving) => {
            digits(arriving).is_some_and(|digits| own.precision() - own.scale() >= digits)
        }
        (T::Timestamp(unit, zone), T::Timestamp(theirs, their_zone)) => {
            unit >= theirs && (zone == their_zone || zone.as_deref() == Some("UTC"))
        }
        (T::Time(unit), T::Time(theirs)) | (T::Duration(unit), T::Duration(theirs)) => {
            unit >= theirs
        }
        (column, arriving) => {
            matches!((digits(column), digits(arriving)), (Some(own), Some(theirs)) if own >= theirs)
        }
    }
}

/// The decimal digits every value of an integer type takes.
fn digits(logical: &LogicalType) -> Option<u8> {
    match logical {
        LogicalType::Int8 => Some(3),
        LogicalType::Int16 => Some(5),
        LogicalType::Int32 => Some(10),
        LogicalType::Int64 => Some(19),
        _ => None,
    }
}
