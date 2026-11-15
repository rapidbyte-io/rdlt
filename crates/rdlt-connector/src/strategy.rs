//! Property-test strategies that draw logical types, for the type lattice's own tests and the
//! property tests of the crates built on the contract.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use proptest::prelude::*;

use crate::{DecimalType, Field, Fields, LogicalType, TimeUnit};

/// A time unit, each as likely.
pub fn time_unit() -> impl Strategy<Value = TimeUnit> {
    prop_oneof![
        Just(TimeUnit::Second),
        Just(TimeUnit::Millisecond),
        Just(TimeUnit::Microsecond),
        Just(TimeUnit::Nanosecond),
    ]
}

/// An integer type of any width or a float type of either, each as likely.
pub fn number_type() -> impl Strategy<Value = LogicalType> {
    use LogicalType as T;
    prop_oneof![
        Just(T::Int8),
        Just(T::Int16),
        Just(T::Int32),
        Just(T::Int64),
        Just(T::Float32),
        Just(T::Float64),
    ]
}

/// A type with no fields, `Null` and `Json` among them, each kind as likely.
pub fn scalar_type() -> impl Strategy<Value = LogicalType> {
    use LogicalType as T;
    let zone = prop_oneof![
        Just(None),
        Just(Some(Arc::<str>::from("UTC"))),
        Just(Some(Arc::<str>::from("Asia/Tokyo")))
    ];
    let decimal = (1_u8..=76)
        .prop_flat_map(|precision| (Just(precision), 0..=precision))
        .prop_filter_map(
            "a scale within a precision of at most 76",
            |(precision, scale)| DecimalType::new(precision, scale).ok().map(T::Decimal),
        );
    prop_oneof![
        1 => Just(T::Null),
        1 => Just(T::Bool),
        6 => number_type(),
        1 => decimal,
        1 => Just(T::Utf8),
        1 => Just(T::Binary),
        1 => Just(T::Date),
        1 => time_unit().prop_map(T::Time),
        1 => (time_unit(), zone).prop_map(|(unit, zone)| T::Timestamp(unit, zone)),
        1 => time_unit().prop_map(T::Duration),
        1 => Just(T::Uuid),
        1 => Just(T::Json),
    ]
}

/// Any type: a scalar, or lists and structs of them nested up to three deep, each struct's
/// fields in any order.
pub fn logical_type() -> impl Strategy<Value = LogicalType> {
    scalar_type().prop_recursive(3, 16, 3, |inner| {
        prop_oneof![
            (inner.clone(), any::<bool>()).prop_map(|(item, nullable)| {
                LogicalType::List(Box::new(Field::new("item", item, nullable)))
            }),
            proptest::collection::btree_map(
                prop_oneof![Just("a"), Just("b"), Just("c")],
                (inner, any::<bool>()),
                0..3
            )
            .prop_map(|fields| {
                fields
                    .into_iter()
                    .map(|(name, (logical, nullable))| Field::new(name, logical, nullable))
                    .collect::<Vec<_>>()
            })
            .prop_shuffle()
            .prop_map(structure),
        ]
    })
}

/// `logical` as a source may name it: each struct's fields in a drawn order and each list's item
/// under a drawn name, at any depth.
pub fn respelled(logical: &LogicalType) -> BoxedStrategy<LogicalType> {
    reshaped(logical, false)
}

/// A type `logical` holds every value of: `logical` respelled as [`respelled`] does, with any
/// struct field that may be null perhaps left out or holding only nulls, and any scalar perhaps
/// a narrower one.
pub fn cut(logical: &LogicalType) -> BoxedStrategy<LogicalType> {
    reshaped(logical, true)
}

/// `logical` respelled, and its fields that may be null perhaps cut where `cutting` says.
fn reshaped(logical: &LogicalType, cutting: bool) -> BoxedStrategy<LogicalType> {
    match logical {
        LogicalType::Struct(fields) => {
            let fields: Vec<BoxedStrategy<Option<Field>>> = fields
                .iter()
                .map(|field| reshaped_field(field, cutting))
                .collect();
            fields
                .prop_shuffle()
                .prop_map(|fields| structure(fields.into_iter().flatten().collect()))
                .boxed()
        }
        LogicalType::List(item) => {
            let nullable = item.is_nullable();
            let names = prop_oneof![Just("item"), Just("element")];
            (reshaped(item.logical_type(), cutting), names)
                .prop_map(move |(logical, name)| {
                    LogicalType::List(Box::new(Field::new(name, logical, nullable)))
                })
                .boxed()
        }
        other if cutting => proptest::sample::select(narrower(other)).boxed(),
        other => Just(other.clone()).boxed(),
    }
}

/// The scalar `logical` and scalars it holds every value of, as the join holds them: narrower
/// integers, floats and decimals, coarser time units, dates in a timestamp, and `Null`.
fn narrower(logical: &LogicalType) -> Vec<LogicalType> {
    use LogicalType as T;
    let coarser = |unit: TimeUnit| {
        [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ]
        .into_iter()
        .filter(move |coarser| *coarser <= unit)
    };
    let mut narrower = match logical {
        T::Int16 => vec![T::Int8],
        T::Int32 => vec![T::Int8, T::Int16],
        T::Int64 => vec![T::Int8, T::Int16, T::Int32],
        T::Float64 => vec![T::Float32, T::Int8, T::Int16, T::Int32],
        T::Decimal(decimal) => {
            let whole = decimal.precision() - decimal.scale();
            let fewer = DecimalType::new(decimal.precision() - 1, decimal.scale()).ok();
            let integers = [(T::Int8, 3), (T::Int16, 5), (T::Int32, 10), (T::Int64, 19)];
            let integers = integers.into_iter().filter(|(_, digits)| *digits <= whole);
            integers
                .map(|(integer, _)| integer)
                .chain(fewer.map(T::Decimal))
                .collect()
        }
        T::Timestamp(unit, zone) => coarser(*unit)
            .map(|unit| T::Timestamp(unit, zone.clone()))
            .chain([T::Date])
            .collect(),
        T::Time(unit) => coarser(*unit).map(T::Time).collect(),
        T::Duration(unit) => coarser(*unit).map(T::Duration).collect(),
        T::Json => vec![T::Bool, T::Int64, T::Utf8],
        _ => Vec::new(),
    };
    narrower.extend([logical.clone(), T::Null]);
    narrower
}

/// `field` reshaped as [`reshaped`] does, or, where `cutting` and it may be null, perhaps left
/// out or of `Null`.
fn reshaped_field(field: &Field, cutting: bool) -> BoxedStrategy<Option<Field>> {
    let (name, nullable) = (field.name().to_owned(), field.is_nullable());
    let named = move |logical| Some(Field::new(name.as_str(), logical, nullable));
    let kept = reshaped(field.logical_type(), cutting).prop_map(named.clone());
    if !(cutting && nullable) {
        return kept.boxed();
    }
    prop_oneof![
        2 => kept,
        1 => Just(None::<Field>),
        1 => Just(LogicalType::Null).prop_map(named),
    ]
    .boxed()
}

/// A struct of `fields`, which have distinct names.
fn structure(fields: Vec<Field>) -> LogicalType {
    LogicalType::Struct(Fields::new(fields).expect("a struct's drawn names are distinct"))
}
