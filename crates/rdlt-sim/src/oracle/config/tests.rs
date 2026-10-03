use rdlt_engine::GrowthLimits;

use super::growth;
use crate::seed::Seed;

#[test]
fn some_worlds_hold_few_writers_open_and_the_rest_the_default() {
    let drawn: Vec<GrowthLimits> = (0..1000).map(|seed| growth(Seed::new(seed))).collect();
    let few = drawn
        .iter()
        .filter(|growth| growth.writers().get() <= 3)
        .count();
    assert!((100..500).contains(&few), "{few} of 1000 hold few writers");
    for writers in 1..=3 {
        assert!(drawn.iter().any(|growth| growth.writers().get() == writers));
    }
    let defaults = GrowthLimits::default();
    for growth in drawn {
        assert!(growth.writers() <= defaults.writers());
        assert_eq!(growth.child_tables(), defaults.child_tables());
    }
}
