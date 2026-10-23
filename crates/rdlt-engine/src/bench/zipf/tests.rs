use std::num::NonZeroU64;

use super::Zipf;
use crate::bench::Mix;

/// How often each of `zipf`'s keys below `below` is drawn in `draws` draws.
fn drawn(zipf: &Zipf, draws: u32, below: u64) -> Vec<u32> {
    let mut mix = Mix::new(3);
    let mut counts = vec![0; usize::try_from(below).unwrap()];
    for _ in 0..draws {
        let key = zipf.draw(&mut mix);
        assert!(key < zipf.items(), "{key} drawn of {} items", zipf.items());
        if let Some(count) = counts.get_mut(usize::try_from(key).unwrap()) {
            *count += 1;
        }
    }
    counts
}

/// The share of draws `counts` holds of `draws`, over the share `expected`.
fn ratio(counts: &[u32], draws: u32, expected: f64) -> f64 {
    f64::from(counts.iter().sum::<u32>()) / f64::from(draws) / expected
}

#[test]
fn each_key_is_drawn_as_often_as_its_weight_says() {
    let zipf = Zipf::new(NonZeroU64::new(4).unwrap(), 1.0);
    assert_eq!(zipf.items(), 4);
    let draws = 100_000;
    let counts = drawn(&zipf, draws, 4);
    let total = 1.0 + 1.0 / 2.0 + 1.0 / 3.0 + 1.0 / 4.0;
    for (key, weight) in [1.0, 1.0 / 2.0, 1.0 / 3.0, 1.0 / 4.0]
        .into_iter()
        .enumerate()
    {
        let ratio = ratio(&counts[key..=key], draws, weight / total);
        assert!((0.98..1.02).contains(&ratio), "key {key}: {ratio}");
    }
}

#[test]
fn a_hundred_thousand_keys_skewed_by_0_99_are_drawn_by_their_zipf_shares() {
    let items = NonZeroU64::new(100_000).unwrap();
    let zipf = Zipf::new(items, 0.99);
    assert_eq!(zipf.items(), items.get());
    let draws = 200_000;
    let counts = drawn(&zipf, draws, items.get());
    let weight = |rank: u32| f64::from(rank).powf(-0.99);
    let zeta: f64 = (1..=100_000).map(weight).sum();
    let share = |ranks: std::ops::RangeInclusive<u32>| ranks.map(weight).sum::<f64>() / zeta;
    for (keys, ranks, within) in [
        (0..1, 1..=1, 0.02),
        (1..2, 2..=2, 0.03),
        (0..1_000, 1..=1_000, 0.01),
        (1_000..10_000, 1_001..=10_000, 0.02),
        (10_000..100_000, 10_001..=100_000, 0.02),
    ] {
        let first = keys.start;
        let ratio = ratio(&counts[keys], draws, share(ranks));
        assert!(
            (1.0 - within..1.0 + within).contains(&ratio),
            "keys from {first}: {ratio}"
        );
    }
}

#[test]
fn the_same_seed_draws_the_same_keys() {
    let zipf = Zipf::new(NonZeroU64::new(1_000).unwrap(), 0.99);
    let keys = |seed| {
        let mut mix = Mix::new(seed);
        (0..100).map(|_| zipf.draw(&mut mix)).collect::<Vec<_>>()
    };
    assert_eq!(keys(5), keys(5));
    assert_ne!(keys(5), keys(6));
}
