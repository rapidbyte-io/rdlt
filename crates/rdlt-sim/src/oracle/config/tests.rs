use rdlt_engine::{BatchPolicy, GrowthLimits};

use super::{batch, growth, waited};
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

#[test]
fn a_world_whose_calls_may_hang_waits_five_to_twenty_seconds_for_each() {
    use std::time::Duration;

    let drawn: Vec<Duration> = (0..200)
        .filter_map(|seed| waited(Seed::new(seed), true))
        .collect();
    assert_eq!(drawn.len(), 200);
    assert!(
        drawn
            .iter()
            .all(|wait| (Duration::from_secs(5)..=Duration::from_secs(20)).contains(wait)),
        "{drawn:?}"
    );
    assert!(
        drawn.iter().any(|wait| *wait != drawn[0]),
        "the waits differ by seed"
    );
    assert_eq!(waited(Seed::new(1), false), None);
}

#[test]
fn some_worlds_cut_json_flushes_into_small_jobs_and_the_rest_by_the_default_limit() {
    let drawn: Vec<BatchPolicy> = (0..1000).map(|seed| batch(Seed::new(seed))).collect();
    let defaults = BatchPolicy::default();
    let small: Vec<usize> = drawn
        .iter()
        .map(|policy| policy.chunk_bytes().get())
        .filter(|limit| *limit <= 64 << 10)
        .collect();
    assert!(
        (100..500).contains(&small.len()),
        "{} of 1000 cut small",
        small.len()
    );
    for limit in [64, 64 << 10] {
        assert!(small.contains(&limit), "{limit} is drawn");
    }
    for policy in drawn {
        assert!(
            policy.chunk_bytes() == defaults.chunk_bytes()
                || policy.chunk_bytes().get() <= 64 << 10
        );
        assert_eq!(
            (
                policy.target_bytes(),
                policy.max_rows(),
                policy.max_latency()
            ),
            (
                defaults.target_bytes(),
                defaults.max_rows(),
                defaults.max_latency()
            )
        );
    }
}
