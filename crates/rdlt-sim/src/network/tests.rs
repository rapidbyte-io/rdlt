use super::{Placing, options, run_networked};
use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::world::World;

#[test]
fn connectors_on_the_simulated_network_are_placed_there_and_serve_the_engine() {
    let (source, destination) = run_networked(Seed::new(1), |_env, net| async move {
        let mut rng = SplitMix64::new(7);
        let name = "network-placed";
        let _world = World::register(name, &mut rng);
        let config = serde_json::json!({ "world": name });
        let placing = Placing::new(&net, options(&mut rng));
        let source = placing.source(&config).await.check().await;
        let destination = placing.destination(&config).await.check().await;
        World::unregister(name);
        (source, destination)
    });
    source.expect("the source serves its check");
    destination.expect("the destination serves its check");
}
