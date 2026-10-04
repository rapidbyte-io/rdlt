use rdlt_engine::GrowthLimits;

use super::growth;
use crate::seed::Seed;

#[test]
fn some_worlds_hold_few_writers_open_and_the_rest_the_default() {
    let drawn: Vec<GrowthLimits> = (0..1000)
        .map(|seed| growth(Seed::new(seed), None))
        .collect();
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

#[test]
fn half_the_worlds_whose_streams_checkpoint_as_they_read_keep_small_logs() {
    use rdlt_connector::Checkpointing;

    use crate::rng::SplitMix64;
    use crate::swarm::Features;
    use crate::workload::Workload;

    let defaults = GrowthLimits::default().log_bytes();
    let (mut small, mut natural) = (0, 0);
    for seed in 0..120 {
        let features = Features {
            wal: true,
            ..Features::ALL
        };
        let workload = Workload::generate(&mut SplitMix64::new(seed), features);
        let checkpoints = workload
            .streams
            .iter()
            .all(|stream| stream.checkpointing == Checkpointing::Natural);
        let growth = growth(Seed::new(seed), Some(&workload));
        natural += usize::from(checkpoints);
        if growth.log_bytes() != defaults {
            assert!(checkpoints, "seed {seed}: a world checkpointing on demand");
            assert!((128 << 10..=1 << 20).contains(&growth.log_bytes().get()));
            small += 1;
        }
        let unlogged = Workload {
            features: Features {
                wal: false,
                ..features
            },
            ..workload
        };
        assert_eq!(
            growth_of(seed, &unlogged),
            defaults,
            "seed {seed}: a world keeping no log"
        );
    }
    assert!(
        small > natural / 4 && small < natural * 3 / 4,
        "{small} of {natural}"
    );
}

fn growth_of(seed: u64, workload: &crate::workload::Workload) -> std::num::NonZeroU64 {
    growth(Seed::new(seed), Some(workload)).log_bytes()
}
