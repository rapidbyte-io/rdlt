//! Property-test strategies that draw logical types, for the type lattice's own tests and the
//! property tests of the crates built on the contract.

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

/// Any type: a scalar, or lists and structs of them nested up to three deep.
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
            .prop_filter_map("fields named by a map's keys", |fields| {
                let fields = fields
                    .into_iter()
                    .map(|(name, (logical, nullable))| Field::new(name, logical, nullable))
                    .collect();
                Fields::new(fields).ok().map(LogicalType::Struct)
            }),
        ]
    })
}
