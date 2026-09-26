use super::{World, capabilities};
use crate::rng::SplitMix64;
use crate::swarm::Features;

#[test]
fn some_destinations_cannot_add_columns_until_granted() {
    let drawn: Vec<_> = (0..300)
        .map(|seed| capabilities(&mut SplitMix64::new(seed), Features::ALL))
        .collect();
    assert!(drawn.iter().any(|caps| !caps.schema_changes.add_column));
    let unset = Features {
        settings: false,
        ..Features::ALL
    };
    for seed in 0..100 {
        let caps = capabilities(&mut SplitMix64::new(seed), unset);
        assert!(caps.schema_changes.add_column);
    }
    let world = World::register("granted", &mut SplitMix64::new(3));
    world.grant_add_column();
    assert!(world.capabilities().schema_changes.add_column);
    World::unregister("granted");
}
