//! Drift values: drawn from each row's seed, within what every column their type may widen into
//! holds, so a phase converges instead of refusing a value no unit can hold.

use rdlt_connector::{LogicalType, TimeUnit};
use rdlt_testkit::canon::{DAY, nanos};
use rdlt_testkit::draw::{draw, mix};
use rdlt_testkit::drawn::{Encoding, Scalar, Shape, values};

/// Tries at a value that converges before a drift value is null instead.
const TRIES: u64 = 16;

/// A value of `shape` drawn from `seed` that `keep` keeps, null one time in five.
pub(super) fn drawn(shape: &Shape, seed: u64, keep: impl Fn(&Scalar) -> bool) -> Scalar {
    let strategy = values::value(shape, true);
    (0..TRIES)
        .map(|attempt| draw(&strategy, mix(seed ^ attempt)))
        .find(|value| keep(value))
        .unwrap_or(Scalar::Null)
}

/// Whether every float in `value` is finite, so JSON holds it as a number.
pub(super) fn finite(value: &Scalar) -> bool {
    match value {
        Scalar::Float64(float) => float.is_finite(),
        Scalar::Struct(fields) => fields.iter().all(|(_, inner)| finite(inner)),
        Scalar::List(items) => items.iter().all(finite),
        _ => true,
    }
}

/// Whether every type `logical` widens into holds `value`: dates and instants within the
/// nanoseconds an `i64` holds, a day to spare for zones; times of day within a `Time32` of
/// milliseconds; durations within an `i64` of nanoseconds.
pub(super) fn convergent(value: &Scalar, logical: &LogicalType) -> bool {
    use LogicalType as T;
    let spared = i128::from(i64::MAX) - DAY;
    match (value, logical) {
        (Scalar::Date(days), _) => (i128::from(*days) * DAY).abs() <= spared,
        (Scalar::Temporal(value), T::Timestamp(unit, _)) => {
            (i128::from(*value) * nanos(*unit)).abs() <= spared
        }
        (Scalar::Temporal(value), T::Time(unit)) => {
            let millis = i128::from(*value) * nanos(*unit) / nanos(TimeUnit::Millisecond);
            i32::try_from(millis).is_ok()
        }
        (Scalar::Temporal(value), T::Duration(unit)) => {
            i64::try_from(i128::from(*value) * nanos(*unit)).is_ok()
        }
        (Scalar::Struct(fields), T::Struct(types)) => fields.iter().all(|(name, inner)| {
            types
                .iter()
                .find(|field| field.name() == name)
                .is_none_or(|field| convergent(inner, field.logical_type()))
        }),
        (Scalar::List(items), T::List(item)) => items
            .iter()
            .all(|inner| convergent(inner, item.logical_type())),
        _ => true,
    }
}

/// `shape`, or it with every encoding plain unless `encodings`.
pub(super) fn plain_unless(shape: Shape, encodings: bool) -> Shape {
    if encodings {
        return shape;
    }
    plain(shape)
}

fn plain(shape: Shape) -> Shape {
    Shape {
        logical: shape.logical,
        encoding: Encoding::Plain,
        children: shape.children.into_iter().map(plain).collect(),
    }
}
