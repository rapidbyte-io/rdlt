use rdlt_sim::{Features, Seed, SplitMix64, Weight, check_exactly_once, for_each_seed};

/// Hostile seeds that each found a defect with its fix reverted, kept so they stay caught.
#[rustfmt::skip]
const FOUND: [u64; 8] = [
    // Two configurations of one connection at once each connected.
    7, 19,
    // An open beyond a connection's sessions closed the oldest while a call still held it.
    12, 42,
    // A connector started again under another version, named after a secret, saw it quoted.
    343, 459,
    // A line of a secret of several lines, said alone, was kept as said.
    2, 9,
];

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
    let first = hostile(8).filter(|seed| !FOUND.contains(&seed.value()));
    let seeds = FOUND.into_iter().map(Seed::new).chain(first);
    for_each_seed("hostile", seeds, Weight::One, check_exactly_once);
}
