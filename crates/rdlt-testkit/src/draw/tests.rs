use proptest::prelude::*;

use super::{draw, mix};

#[test]
fn a_draw_is_a_pure_function_of_its_seed() {
    let strategy = any::<(u64, String)>();
    assert_eq!(draw(&strategy, 7), draw(&strategy, 7));
    let draws: std::collections::BTreeSet<(u64, String)> =
        (0..16).map(|seed| draw(&strategy, seed)).collect();
    assert_eq!(draws.len(), 16, "different seeds draw different values");
}

#[test]
fn mixing_spreads_neighboring_states() {
    assert_ne!(mix(1), mix(2));
    assert_ne!(mix(0), 0);
    assert_eq!(mix(1), mix(1));
}
