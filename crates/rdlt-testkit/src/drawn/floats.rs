//! Floats as sources send them, their edges included.

use proptest::prelude::*;

/// Any double: proptest's, which never draws NaN or an infinity, and one time in ten an edge:
/// NaN, an infinity, a signed zero or an extreme.
pub(super) fn doubles() -> BoxedStrategy<f64> {
    let edges = vec![
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.0,
        f64::MIN,
        f64::MAX,
        f64::MIN_POSITIVE,
        f64::EPSILON,
    ];
    prop_oneof![9 => any::<f64>(), 1 => proptest::sample::select(edges)].boxed()
}

/// Any single, with its edges as [`doubles`] draws a double's.
pub(super) fn singles() -> BoxedStrategy<f32> {
    let edges = vec![
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        -0.0,
        f32::MIN,
        f32::MAX,
        f32::MIN_POSITIVE,
        f32::EPSILON,
    ];
    prop_oneof![9 => any::<f32>(), 1 => proptest::sample::select(edges)].boxed()
}
