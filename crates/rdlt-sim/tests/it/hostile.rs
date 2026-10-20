use rdlt_sim::{Features, Seed, SplitMix64, Weight, check_exactly_once, for_each_seed};

/// The first hostile seeds the swarm draws.
fn hostile(count: usize) -> impl Iterator<Item = Seed> {
    (0..)
        .map(Seed::new)
        .filter(|seed| {
            let features = Features::draw(&mut SplitMix64::new(seed.value()));
            features.network && features.hostile
        })
        .take(count)
}

#[test]
fn a_hostile_host_meets_typed_refusals_and_no_secret_leaves_the_engine_or_its_host() {
    for_each_seed("hostile", hostile(8), Weight::One, check_exactly_once);
}
