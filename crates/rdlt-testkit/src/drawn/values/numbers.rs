//! Present values of the numeric types: integers of every width, and decimals.

use proptest::prelude::*;
use rdlt_connector::{DecimalType, LogicalType};

use super::super::Scalar;
use super::super::arrays::Encoding;

/// Integers whose magnitude is at most this, 2⁵³, are exact as a 64-bit float.
const EXACT_IN_FLOAT: i64 = 1 << 53;

/// A present integer of `logical`, within the unsigned type one size down where `unsigned`.
pub(super) fn integer(logical: &LogicalType, unsigned: bool) -> BoxedStrategy<Scalar> {
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
        // Integers a 64-bit float holds exactly, and those it would round, and the edges between.
        T::Int64 => prop_oneof![
            2 => any::<i64>(),
            2 => -EXACT_IN_FLOAT..=EXACT_IN_FLOAT,
            1 => proptest::sample::select(vec![
                EXACT_IN_FLOAT,
                -EXACT_IN_FLOAT,
                EXACT_IN_FLOAT + 1,
                -EXACT_IN_FLOAT - 1,
                i64::MIN,
                i64::MAX,
            ]),
        ]
        .prop_map(Scalar::Int)
        .boxed(),
        other => unreachable!("{other} is not an integer type"),
    }
}

/// A present decimal of `decimal`, as `encoding` holds it: an unsigned one within 64 bits, and a
/// whole number of any width where the shape is pushed only as JSON.
pub(super) fn decimals(decimal: DecimalType, encoding: Encoding) -> BoxedStrategy<Scalar> {
    use Encoding as E;
    match encoding {
        E::Unsigned => any::<u64>()
            .prop_map(|value| Scalar::Decimal(value.to_string()))
            .boxed(),
        // Whole numbers of any width, pushed only as JSON: up to 100 digits, and none leading.
        E::Large => (
            any::<bool>(),
            1_u8..10,
            proptest::collection::vec(0_u8..10, 0..100),
        )
            .prop_map(|(negative, first, rest)| {
                let digits: String = std::iter::once(first)
                    .chain(rest)
                    .map(|digit| char::from(b'0' + digit))
                    .collect();
                Scalar::Decimal(if negative {
                    format!("-{digits}")
                } else {
                    digits
                })
            })
            .boxed(),
        _ => {
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
    }
}
