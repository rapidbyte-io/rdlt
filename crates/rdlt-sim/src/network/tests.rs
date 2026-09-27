use std::time::Duration;

use rdlt_connector::ConnectorErrorKind;

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
        let placing = Placing::new(net, options(&mut rng));
        let source = placing.source(&config).await.check().await;
        let destination = placing.destination(&config).await.check().await;
        World::unregister(name);
        (source, destination)
    });
    source.expect("the source serves its check");
    destination.expect("the destination serves its check");
}

#[test]
fn a_disrupted_network_loses_connectors_and_the_healed_one_serves_them_again() {
    let (lost, served) = run_networked(Seed::new(1), |_env, net| async move {
        let mut rng = SplitMix64::new(7);
        let name = "network-disrupted";
        let _world = World::register(name, &mut rng);
        let config = serde_json::json!({ "world": name });
        let placing = Placing::new(net, options(&mut rng));
        let source = placing.source(&config).await;
        // The source is checked again and again while the network is disrupted.
        let checks = async {
            let mut lost = 0;
            for _ in 0..200 {
                if let Err(error) = source.check().await {
                    assert_eq!(error.kind(), ConnectorErrorKind::Transient, "{error}");
                    lost += 1;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            lost
        };
        let lost = placing.disrupting(SplitMix64::new(11), checks).await;
        let served = placing.source(&config).await.check().await;
        World::unregister(name);
        (lost, served)
    });
    assert!(lost > 0, "no check failed while the network was disrupted");
    served.expect("the healed network serves the source again");
}
