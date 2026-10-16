use rdlt_engine::GrowthLimits;

use super::{growth, log, tiny};
use crate::seed::Seed;

#[test]
fn some_worlds_hold_few_writers_open_and_the_rest_the_default() {
    let drawn: Vec<GrowthLimits> = (0..1000)
        .map(|seed| growth(Seed::new(seed), None, 4))
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
        let growth = growth(Seed::new(seed), Some(&workload), 4);
        natural += usize::from(checkpoints);
        if growth.log_bytes() != defaults {
            assert!(checkpoints, "seed {seed}: a world checkpointing on demand");
            let bytes = growth.log_bytes().get();
            assert!(
                (128 << 10..=1 << 20).contains(&bytes)
                    || tiny(&workload) == Some(bytes)
                    || bytes == log::least(&workload, 4),
                "seed {seed}: {bytes}"
            );
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
    growth(Seed::new(seed), Some(workload), 4).log_bytes()
}

#[test]
fn some_worlds_whose_streams_push_plain_arrow_keep_a_log_twice_what_they_hold_unsealed() {
    use crate::rng::SplitMix64;
    use crate::swarm::Features;
    use crate::workload::Workload;

    let mut tiny_logs = 0;
    for seed in 0..2_000 {
        let features = Features {
            wal: true,
            json: false,
            drift: false,
            ..Features::ALL
        };
        let workload = Workload::generate(&mut SplitMix64::new(seed), features);
        let open: u64 = workload
            .streams
            .iter()
            .map(|stream| stream.partitions.len() as u64 * stream.checkpoint_every)
            .sum();
        let expected = (open * 2 * (4 << 10)).max(16 << 10);
        assert_eq!(tiny(&workload), Some(expected), "seed {seed}");
        if growth(Seed::new(seed), Some(&workload), 4)
            .log_bytes()
            .get()
            == expected
        {
            tiny_logs += 1;
        }
    }
    assert!(tiny_logs > 0, "no world keeps a tiny log");
    // A world whose streams push JSON, or have columns that drift, keeps no tiny log.
    for seed in 0..200 {
        let workload = Workload::generate(&mut SplitMix64::new(seed), Features::ALL);
        let plain = workload
            .streams
            .iter()
            .all(|stream| !stream.json && stream.drift.is_empty());
        assert_eq!(tiny(&workload).is_some(), plain, "seed {seed}");
    }
}

#[test]
fn no_world_keeps_a_log_smaller_than_the_least_the_engine_promises_its_load() {
    use crate::rng::SplitMix64;
    use crate::swarm::Features;
    use crate::workload::Workload;

    let defaults = GrowthLimits::default().log_bytes().get();
    let mut floored = 0;
    // Seed 133046 drew a log of 128 KiB, which its four partitions, each holding three pushes of
    // a table and its child table unsealed, filled.
    for seed in [133_046].into_iter().chain(0..500) {
        let mut rng = SplitMix64::new(seed);
        let features = Features::draw(&mut rng);
        let workload = Workload::generate(&mut rng, features);
        for partitions in 1..=4 {
            let bytes = growth(Seed::new(seed), Some(&workload), partitions)
                .log_bytes()
                .get();
            if bytes == defaults || tiny(&workload) == Some(bytes) {
                continue;
            }
            let least = log::least(&workload, partitions);
            assert!(
                bytes >= least,
                "seed {seed}, {partitions} read at once: {bytes} < {least}"
            );
            floored += usize::from(bytes == least);
        }
    }
    assert!(floored > 0, "no world's log is the least its load needs");
}
