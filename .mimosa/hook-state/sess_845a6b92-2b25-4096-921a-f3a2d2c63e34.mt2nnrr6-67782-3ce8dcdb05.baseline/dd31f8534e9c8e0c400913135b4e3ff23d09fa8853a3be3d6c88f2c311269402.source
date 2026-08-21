//! Widening-lattice laws as executable properties.
//!
//! Order-insensitive inference requires `widen` to be a semilattice join:
//! commutative + associative + idempotent, with monotone results. A counterexample
//! here is a release blocker.

use proptest::prelude::*;
use rdlt_core::types::{DECIMAL_MAX_PRECISION, LogicalType, widen};

fn any_logical_type() -> impl Strategy<Value = LogicalType> {
    prop_oneof![
        Just(LogicalType::Bool),
        Just(LogicalType::Int64),
        Just(LogicalType::Float64),
        // scale <= precision <= 38, generated over the full valid space
        (0..=DECIMAL_MAX_PRECISION).prop_flat_map(|precision| {
            (Just(precision), 0..=precision)
                .prop_map(|(precision, scale)| LogicalType::Decimal { precision, scale })
        }),
        Just(LogicalType::Utf8),
        Just(LogicalType::Binary),
        Just(LogicalType::TimestampTz),
        Just(LogicalType::TimestampNaive),
        Just(LogicalType::Date),
        Just(LogicalType::Time),
        Just(LogicalType::Uuid),
        Just(LogicalType::Json),
    ]
}

proptest! {
    #[test]
    fn commutative(a in any_logical_type(), b in any_logical_type()) {
        prop_assert_eq!(widen(a, b), widen(b, a));
    }

    #[test]
    fn idempotent(a in any_logical_type()) {
        prop_assert_eq!(widen(a, a), a);
    }

    /// Associativity is what makes inference order-insensitive: any arrival order of
    /// observed types converges to the same column type.
    #[test]
    fn associative(
        a in any_logical_type(),
        b in any_logical_type(),
        c in any_logical_type(),
    ) {
        prop_assert_eq!(widen(widen(a, b), c), widen(a, widen(b, c)));
    }

    /// The join is an upper bound of both inputs (types only move upward):
    /// joining either input with the result gives the result back.
    #[test]
    fn monotone_upper_bound(a in any_logical_type(), b in any_logical_type()) {
        let joined = widen(a, b);
        prop_assert_eq!(widen(a, joined), joined);
        prop_assert_eq!(widen(b, joined), joined);
    }

    /// Json is the absorbing top of the lattice.
    #[test]
    fn json_is_top(a in any_logical_type()) {
        prop_assert_eq!(widen(a, LogicalType::Json), LogicalType::Json);
    }

    /// Widening never leaves the valid decimal space.
    #[test]
    fn decimal_results_stay_valid(a in any_logical_type(), b in any_logical_type()) {
        if let LogicalType::Decimal { precision, scale } = widen(a, b) {
            prop_assert!(scale <= precision);
            prop_assert!(precision <= DECIMAL_MAX_PRECISION);
        }
    }
}
