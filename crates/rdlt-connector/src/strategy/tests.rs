use std::sync::Arc;

use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;

use super::{cut, narrower};
use crate::{DecimalType, LogicalType, TimeUnit};

fn decimal(precision: u8, scale: u8) -> LogicalType {
    LogicalType::Decimal(DecimalType::new(precision, scale).unwrap())
}

/// Scalars, each with what [`narrower`] narrows it to, in order.
fn narrowings() -> Vec<(LogicalType, Vec<LogicalType>)> {
    use LogicalType as T;
    use TimeUnit as U;
    let utc = |unit| T::Timestamp(unit, Some(Arc::from("UTC")));
    vec![
        (T::Int8, vec![T::Int8, T::Null]),
        (T::Int16, vec![T::Int8, T::Int16, T::Null]),
        (T::Int32, vec![T::Int8, T::Int16, T::Int32, T::Null]),
        (
            T::Int64,
            vec![T::Int8, T::Int16, T::Int32, T::Int64, T::Null],
        ),
        (
            T::Float64,
            vec![T::Float32, T::Int8, T::Int16, T::Int32, T::Float64, T::Null],
        ),
        (
            decimal(5, 2),
            vec![T::Int8, decimal(4, 2), decimal(5, 2), T::Null],
        ),
        (decimal(1, 1), vec![decimal(1, 1), T::Null]),
        (
            decimal(20, 0),
            vec![
                T::Int8,
                T::Int16,
                T::Int32,
                T::Int64,
                decimal(19, 0),
                decimal(20, 0),
                T::Null,
            ],
        ),
        (
            utc(U::Millisecond),
            vec![
                utc(U::Second),
                utc(U::Millisecond),
                T::Date,
                utc(U::Millisecond),
                T::Null,
            ],
        ),
        (
            T::Time(U::Microsecond),
            vec![
                T::Time(U::Second),
                T::Time(U::Millisecond),
                T::Time(U::Microsecond),
                T::Time(U::Microsecond),
                T::Null,
            ],
        ),
        (
            T::Duration(U::Second),
            vec![T::Duration(U::Second), T::Duration(U::Second), T::Null],
        ),
        (T::Json, vec![T::Bool, T::Int64, T::Utf8, T::Json, T::Null]),
        (T::Utf8, vec![T::Utf8, T::Null]),
    ]
}

#[test]
fn a_scalar_is_narrowed_to_itself_null_and_each_scalar_it_holds() {
    for (logical, expected) in narrowings() {
        let narrowed = narrower(&logical);
        for narrow in &narrowed {
            assert_eq!(logical.join(narrow), logical, "{logical} holds {narrow}");
        }
        assert_eq!(narrowed, expected, "{logical}");
    }
}

#[test]
fn a_cut_narrows_a_scalar_now_and_then() {
    let mut runner = TestRunner::deterministic();
    let strategy = cut(&LogicalType::Int64);
    let narrowed = (0..200)
        .map(|_| strategy.new_tree(&mut runner).unwrap().current())
        .filter(|logical| !matches!(logical, LogicalType::Int64 | LogicalType::Null))
        .count();
    assert!(narrowed > 0);
}
